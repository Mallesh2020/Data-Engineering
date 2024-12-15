from pyspark.sql.types import StructType, StructField, StringType, TimestampType
from logger import get_logger


logger = get_logger("Extraction")

def extractor(spark, path):
    try:
        logger.info("Defining schema for the devices dataset.")
        devices_schema = StructType([
            StructField('timestamp', TimestampType(), False),
            StructField('variable', StringType(), True),
            StructField('value', StringType(), True),
            StructField('device', StringType(), False)
        ])

        logger.info(f"Reading data from path: {path}")
        devices = (spark.read
                   .format("csv")
                   .option("header", "true")
                   .schema(devices_schema)
                   .load(path)
                   )
        logger.info("Data is successfully loaded in a DataFrame.")
        return devices

    except Exception as e:
        logger.error(f"Error during data extraction: {e}")
        raise