import pymysql, logger
from config import mySqlConfig


logger = logger.get_logger("Database Operations")


def connect_mysql():
    try:
        connection = pymysql.connect(**mySqlConfig)
        cursor = connection.cursor()
        logger.info("Connection Established")
        return connection, cursor 
    
    except Exception as e:
        logger.error(f"Failure in Connection --> {e}")


def read_sql_file(path):
    try:
        with open(path, 'r') as file:
            return file.read()
        logger.info("SQL File at path {path} read successfully")
        
    except Exception as e:
        logger.error(f"SQL file at {path} doesn't exists or could not be read --> {e}")


def create_database(connection, cursor, dbName):
    try:
        query = f"CREATE DATABASE IF NOT EXISTS {dbName};"
        cursor.execute(query)
        connection.commit()
        logger.info(f"Database '{dbName}' is Created if Not Exist")

    except Exception as e:
        logger.error(f"Couldn't create Database '{dbName}' --> {e}")


def create_table(connection, cursor, path, databaseName, tableName):
    try:
        cursor.execute(read_sql_file(path))
        connection.commit()
        logger.info(f"Table '{databaseName}.{tableName}' is created if not exists")

    except Exception as e:
        logger.error(f"Could not create Table '{databaseName}.{tableName}' --> {e}")