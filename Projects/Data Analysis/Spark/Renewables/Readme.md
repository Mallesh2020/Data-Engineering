# Renewable Energy Data Processing Pipeline

This project implements a data processing pipeline for renewable energy datasets using Apache Spark. The pipeline handles data extraction, transformation, aggregation, and visualization for analysis. Logs are maintained for debugging and monitoring.

---

## Table of Contents
- [Overview](#overview)
- [Pipeline Stages](#pipeline-stages)
- [Dependencies](#dependencies)
- [Setup and Execution](#setup-and-execution)
- [Directory Structure](#directory-structure)
- [Logging](#logging)
- [Example Usage](#example-usage)

---

## Overview
The pipeline processes CSV files containing renewable energy data collected from various devices. The key tasks include:
1. Extracting data from input files.
2. Cleaning and transforming the data.
3. Aggregating data based on specified time windows.
4. Visualizing the results for analysis.
5. Saving the processed data and visualizations to specified directories.

---

## Pipeline Stages

### 1. **Extraction**
- Reads CSV files from a specified input path.
- Applies a predefined schema for consistency.

### 2. **Transformation**
- Removes duplicates and handles NULL values.
- Provides two datasets: one retaining NULLs and another excluding NULLs.

### 3. **Aggregation**
- Aggregates data using a 10-minute window.
- Computes metrics such as average, minimum, maximum, standard deviation, and the last recorded value (ignoring NULLs).

### 4. **Visualization**
- Generates line plots for each device, showing the aggregated metrics over time.
- Saves plots in PNG format to a specified directory.

### 5. **Loading**
- Saves processed data in partitioned CSV format by device.

---

## Dependencies

### Python Libraries
- Apache Spark
- Matplotlib
- Logging

### Installation
1. Install [Apache Spark](https://spark.apache.org/).
2. Use Python 3.10 or later.
3. Install required Python libraries:
   ```bash
   pip install pyspark matplotlib
   ```

---

## Setup and Execution

### Input Dataset
- Place CSV files in the directory specified by `input_path` in the `main.py` script.

### Configuration
- Update file paths and parameters in the `main.py` script:
  - `input_path`: Path to the input dataset.
  - `output_path_aggregates_with_nulls`: Directory to save aggregated data retaining NULLs.
  - `output_path_aggregates_without_nulls`: Directory to save aggregated data excluding NULLs.
  - `plot_save_path`: Directory to save generated plots.
  - `window_duration`: Time window for data aggregation (default: "10 minutes").

### Execution
Run the pipeline with:
```bash
python main.py
```

---

## Directory Structure

```
|-- Dataset
|   |-- *.csv
|-- Output
|   |-- Aggregates_With_Nulls
|   |-- Aggregates_Without_Nulls
|   |-- Plots
|-- Logs
|   |-- *.log
|-- Scripts
|   |-- extract.py
|   |-- transform.py
|   |-- load.py
|   |-- visualization.py
|   |-- main.py
|-- README.md
```

---

## Logging
- Logs are generated for each module and saved in the `Logs` directory.
- Example log messages:
  - "Starting the Spark session."
  - "Removing duplicates and dropping NA values."
  - "Visualization successfully created for DeviceX with-nulls."

---

## Example Usage
1. Extract data:
   ```python
   devices_df = extract.extractor(spark, input_path)
   ```
2. Transform data:
   ```python
   without_nulls_df = transform.without_nulls(devices_df)
   with_nulls_df = transform.with_nulls(devices_df)
   ```
3. Aggregate data:
   ```python
   aggregated_df = transform.aggregations(with_nulls_df, "10 minutes")
   ```
4. Visualize data:
   ```python
   visualization.plot(aggregated_df, device, "with-nulls", plot_save_path)
   
