# Mutual Fund Data ETL Pipeline
This project is an ETL (Extract, Transform, Load) pipeline that extracts mutual fund data from a source URL, transforms the data into a structured format, and loads it into a MySQL database. 
After loading into Stage Table, data from Stage table is loaded into Fact and Dimension Tables on which we can run our queries for Analyis purpose.


# Project Structure
The project is organized into the following modules:

src 
- main.py: Calls the extract, transform, and load functions to perform the operations.
- config.py: Contains the configuration details for connecting to the MySQL database.
- log.py: Sets up logging for tracking the flow and debugging the ETL process. Create a log folder to capture the logs.
- extract.py: Handles the extraction of data from source URL.
- transform.py: Transforms the extracted data into a clean and structured format (handling missing values).
- load.py: Handles the creation of the database, tables, and insertion of data into the database.

sql/ddl (Table Create Statements)
- create_stage.sql: Defines the structure of the Stage table.
- create_fact.sql: Defines the structure of the Fact table.
- create_scheme_dim.sql: Defines the structure of the Scheme Dimension table.
- create_category_dim.sql: Defines the structure of the Category Dimension table.

sql/dml (Table Load Statements)
- load_fact.sql: Query lo Load data into Fact table.
- load_category_dim.sql: Query lo Load data into Category Dimension table.
- load_scheme_dim.sql: Query lo Load data into Schema Dimension table.


# Requirements
Before running the project, ensure that the following Python packages are installed:
- pandas: For data transformation and manipulation.
- requests: For making HTTP requests to extract data.
- beautifulsoup4: For parsing the extracted HTML data.
- pymysql: For interacting with the MySQL database.

Command: %pip install pandas requests beautifulsoup4 pymysql
