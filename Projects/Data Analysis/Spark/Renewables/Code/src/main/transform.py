from pyspark.sql.functions import window, col, avg, min, max, stddev, last, from_utc_timestamp
from logger import get_logger


logger = get_logger("Transformation")

def without_nulls(df):
    try:
        logger.info("Removing duplicates and dropping NA values.")
        df_non_dup = df.dropDuplicates()
        logger.info(f"Record count after dropping duplicates: {df_non_dup.count()}")
        df_na_dropped = df_non_dup.dropna()
        logger.info(f"Record count after dropping NA values: {df_na_dropped.count()}")
        return df_na_dropped

    except Exception as e:
        logger.error(f"Error during without_nulls transformation: {e}")
        raise


def with_nulls(df):
    try:
        logger.info("Removing duplicates but retaining NA values.")
        df_non_dup = df.dropDuplicates()
        logger.info(f"Record count after dropping duplicates: {df_non_dup.count()}")
        return df_non_dup
    except Exception as e:
        logger.error(f"Error during with_nulls transformation: {e}")
        raise


def aggregations(df, window_duration):
    try:
        logger.info(f"Starting aggregation with window duration: {window_duration}")
        agg_window = window("timestamp", window_duration)
        aggregated_df = (df.groupBy("device", agg_window)
                         .agg(avg("value").alias("WTUR1_W_Avg"),
                              min("value").alias("WTUR1_W_Min"),
                              max("value").alias("WTUR1_W_Max"),
                              stddev("value").alias("WTUR1_W_Stddev"),
                              last("value").alias("WTUR1_W_Last"))
                         )

        aggregated_df = aggregated_df.withColumn("timestamp", col("window.start"))
        aggregated_df = aggregated_df.withColumn("timestamp_z", from_utc_timestamp(col("timestamp"), "America/Los_Angeles"))
        aggregated_df = aggregated_df.select("device", "timestamp", "WTUR1_W_Avg", "WTUR1_W_Min", "WTUR1_W_Max", "WTUR1_W_Stddev", "WTUR1_W_Last", "timestamp_z")
        logger.info("Aggregation completed successfully.")
        return aggregated_df

    except Exception as e:
        logger.error(f"Error during aggregation: {e}")
        raise