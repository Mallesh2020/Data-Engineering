from pyspark.sql import SparkSession
import extract, transform, load, visualization
from logger import get_logger


logger = get_logger("Main")

def main():
    input_path = 'D://Work_Items/GIT Repos/Data-Engineering/Projects/Data Analysis/Spark/Renewables/Dataset/*.csv'
    output_path_aggregates_with_nulls = 'D://Work_Items/Projects_Output/Renewables/Output/Aggregates_With_Nulls/'
    output_path_aggregates_without_nulls = 'D://Work_Items/Projects_Output/Renewables/Output/Aggregates_Without_Nulls/'
    window_duration = "10 minutes"
    plot_save_path = 'D://Work_Items/Projects_Output/Renewables/Output/Plots'

    try:
        logger.info("Starting the Spark session.")
        spark = (SparkSession
                 .builder
                 .appName("Renewables")
                 .getOrCreate()
                 )
        spark.sparkContext.setLogLevel("ERROR")

        logger.info(f"Extracting data from the input path: {input_path}")
        devices_df = extract.extractor(spark, input_path)
        # devices_df.printSchema()

        # Transforming, Displaying and Loading Data - Retaining Nulls
        logger.info("Transforming data by retaining null values.")
        with_nulls_df = transform.with_nulls(devices_df)
        aggregated_with_nulls_df = transform.aggregations(with_nulls_df, window_duration)
        logger.info("Printing Top 20 Rows of Aggregated data With Nulls.")
        load.display_data(aggregated_with_nulls_df)
        load.save_data(aggregated_with_nulls_df, output_path_aggregates_with_nulls)

        # Transforming, Displaying and Loading Data - Removing Nulls
        logger.info("Transforming data by removing null values.")
        without_nulls_df = transform.without_nulls(devices_df)
        aggregated_without_nulls_df = transform.aggregations(without_nulls_df, window_duration)
        logger.info("Printing Top 20 Rows of Aggregated data without nulls.")
        load.display_data(aggregated_without_nulls_df)
        load.save_data(aggregated_without_nulls_df, output_path_aggregates_without_nulls)

        # Getting Devices List for Generating Visalizations
        logger.info("Fetching distinct devices.")
        distinct_devices = (devices_df
                            .select("device")
                            .distinct()
                            .rdd.flatMap(lambda x: x)
                            .collect()
                            )
        logger.info(f"Distinct devices are: {distinct_devices}")

        # Generate visualizations for devices
        logger.info("Generating visualizations.")
        for device in distinct_devices:
            try:
                logger.info(f"Generating visualization for {device}")
                visualization.plot(aggregated_with_nulls_df, device, "with-nulls", plot_save_path)
                visualization.plot(aggregated_without_nulls_df, device, "without-nulls", plot_save_path)
            except Exception as e:
                logger.error(f"Failed to generate visualization for {device}: {e}")
        logger.info("Processing complete.")

    except Exception as e:
        logger.error(f"An error occurred during processing: {e}")


if __name__ == "__main__":
    main()