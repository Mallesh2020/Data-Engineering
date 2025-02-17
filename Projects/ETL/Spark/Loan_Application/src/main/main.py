import logger, extract, database, transform
from pyspark.sql import SparkSession
import sys
import traceback


logger = logger.get_logger("Loan Application")


def main():

    try:
        spark = (
            SparkSession
            .builder
            .master("local[4]")
            .appName("Loan_Applications_Data")
            .config('spark.jars', 'file:///C://spark/spark-3.5.3-bin-hadoop3/jars/mysql-connector-j-8.0.33.jar')
            .getOrCreate()
        )
        logger.info("Spark session initialized successfully.")

    except Exception as e:
        print(f"Failed to initialize Spark session: {e}")
        sys.exit(1)


    ## Connect to MySQL Database 
    try:
        connection, cursor = database.connect_mysql()
        if not connection or not cursor:
            logger.info("MySQL DB Connection is not established")
        else:
            logger.info("Connected to MySQL")

    except Exception as e:
        print(f"Failed to connect to MySQL: {e}")
        spark.stop()
        sys.exit(1)


    ## Create Database if Not Exists
    try:
        database_name = 'loan_app'
        database.create_database(connection, cursor, database_name)
        logger.info("Database Created Successfully")

    except Exception as e:
        print(f"Failed to create database: {e}")
        connection.close()
        spark.stop()
        sys.exit(1)

    ddl_directory_path = 'D://Work_Items/Projects/FairMoney_Assignment/src/sql/ddl'


    # Execute Each Pipeline in the below order 

    # 1. Users Pipeline
    try: 
        users_df = extract.exrtactor(spark, extract.users_data, extract.users_schema)
        users_ddl = f'{ddl_directory_path}/users.sql'
        database.create_table(connection, cursor, users_ddl, database_name, "users")
        transform.users_pipeline(spark, cursor, users_df)
        logger.info("Users Pipeline executed successfully.")

    except Exception as e:
        logger.error(f"Error in Users Pipeline: {e}", exc_info=True)
        traceback.print_exc()


    # 2. User KYC Pipleine
    try:
        kyc_df = extract.exrtactor(spark, extract.kyc_data, extract.kyc_schema)
        user_kyc_ddl = f'{ddl_directory_path}/user_kyc.sql'
        database.create_table(connection, cursor, user_kyc_ddl, database_name, "user_kyc")
        transform.user_kyc_piepline(spark, cursor, kyc_df)
        logger.info("User KYC Pipeline executed successfully.")

    except Exception as e:
        logger.error(f"Error in User KYC Pipeline: {e}", exc_info=True)
        traceback.print_exc()


    # 3. Loan Offers Pipeline
    try:
        loan_offers_df = extract.exrtactor(spark, extract.loan_offers_data, extract.loan_offers_schema)
        loan_offers_ddl = f'{ddl_directory_path}/loan_offers.sql'
        database.create_table(connection, cursor, loan_offers_ddl, database_name, "loan_offers")
        transform.loan_offers_pipeline(spark, cursor, loan_offers_df)
        logger.info("Loan Offers Pipeline executed successfully.")

    except Exception as e:
        logger.error(f"Error in Loan Offers Pipeline: {e}", exc_info=True)
        traceback.print_exc()


    # 4. User Loans Pipeline
    try:
        user_loans_df = extract.exrtactor(spark, extract.user_loans_data, extract.user_loans_schema)
        user_loans_ddl = f'{ddl_directory_path}/user_loans.sql'
        database.create_table(connection, cursor, user_loans_ddl, database_name, "user_loans")
        transform.user_loans_pipeline(spark, cursor, user_loans_df)
        logger.info("User Loans Pipeline executed successfully.")

    except Exception as e:
        logger.error(f"Error in User Loans Pipeline: {e}", exc_info=True)
        traceback.print_exc()


    # 5. Transactions Pipeline
    try:
        transactions_df = extract.exrtactor(spark, extract.transactions_data, extract.transactions_schema)
        transactions_ddl = f'{ddl_directory_path}/transactions.sql'
        database.create_table(connection, cursor, transactions_ddl, database_name, "transactions")
        transform.transactions_piepline(spark, transactions_df)
        logger.info("Transactions Pipeline executed successfully.")

    except Exception as e:
        logger.error(f"Error in Transactions Pipeline: {e}", exc_info=True)
        traceback.print_exc()


    # 6. Loan Balances Pipeline
    try:
        loan_balances_ddl = f'{ddl_directory_path}/loan_balances.sql'
        database.create_table(connection, cursor, loan_balances_ddl, database_name, "loan_balances")
        transform.loan_balances_pipeline(spark, cursor)
        logger.info("Loan Balances Pipeline executed successfully.")

    except Exception as e:
        logger.error(f"Error in Loan Balances Pipeline: {e}", exc_info=True)
        traceback.print_exc()


    # 7. Defaulters Pipeline
    try:
        defaulters_ddl = f'{ddl_directory_path}/defaulters.sql'
        database.create_table(connection, cursor, defaulters_ddl, database_name, "defaulters")
        transform.defaulters_pipeline(spark, cursor)
        logger.info("Defaulters Pipeline executed successfully.")

    except Exception as e:
        logger.error(f"Error in Defaulters Pipeline: {e}", exc_info=True)
        traceback.print_exc()


    # 8. Resolutions Pipleine
    try:
        resolutions_df = extract.exrtactor(spark, extract.resolutions_data, extract.resolutions_schema)
        resolutions_ddl = f'{ddl_directory_path}/resolutions.sql'
        database.create_table(connection, cursor, resolutions_ddl, database_name, "defaulters")
        transform.resolutions_pipeline(spark, cursor, resolutions_df)
        logger.info("Resolutions Pipeline executed successfully.")

    except Exception as e:
        logger.error(f"Error in Resolutions Pipeline: {e}", exc_info=True)
        traceback.print_exc()

    finally:
        connection.close()
        spark.stop()


if __name__ == "__main__":
    main()