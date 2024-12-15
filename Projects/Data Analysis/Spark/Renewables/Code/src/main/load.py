from logger import get_logger


logger = get_logger("Display and Load")

def display_data(df):
    try:
        logger.info("Displaying data from the DataFrame.")
        df.show()
        df.printSchema()
        logger.info("Data displayed successfully.")

    except Exception as e:
        logger.error(f"Error displaying data: {e}")
        raise


def save_data(df, path):
    try:
        logger.info(f"Saving data to {path} in CSV format.")
        df.write.mode('overwrite').partitionBy('device').format('csv').save(path)
        logger.info(f"Data saved successfully to {path}.")

    except Exception as e:
        logger.error(f"Error saving data to {path}: {e}")
        raise