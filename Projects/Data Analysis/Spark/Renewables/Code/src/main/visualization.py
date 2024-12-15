import matplotlib.pyplot as plt
from pyspark.sql.functions import col
from logger import get_logger
import os


logger = get_logger("Visualization")

def plot(df, device, null_status, save_path):
    try:
        logger.info(f"Creating visualization for {device} {null_status}")

        # Filter data for the specified device
        device_data = df.filter(col("device") == device).toPandas()

        # Create the plot
        plt.figure(figsize=(10, 6))
        plt.plot(device_data["timestamp"], device_data["WTUR1_W_Avg"], label=f"{device} Data")
        plt.title(f"10-Min Aggregation for {device} {null_status}")
        plt.xlabel("Timestamp")
        plt.ylabel("WTUR1_W_Avg")
        plt.legend()
        plt.xticks(rotation=45)
        plt.tight_layout()
        # plt.show()   # -- Un-comment this to display the plots

        if not os.path.exists(save_path):
            os.makedirs(save_path, exist_ok=True)

        file_path = os.path.join(save_path, f"{device}_{null_status}.png")
        plt.savefig(file_path, format='png')  # Saving plots to a directory
        plt.close()

        logger.info(f"Visualization saved successfully for {device} {null_status} at {save_path}")
        logger.info(f"Visualization successfully created for {device} {null_status}")

    except Exception as e:
        logger.error(f"Error during visualization for {device}: {e}")
        raise