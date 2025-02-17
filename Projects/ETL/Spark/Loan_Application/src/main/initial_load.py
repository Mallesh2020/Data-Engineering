import logger, config, extract, database, transform
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import traceback


logger = logger.get_logger("Initial Loads")


def initial_load(dataframe, table_name):
    logger.info(f"Executing Initial Loads for {table_name}.......")
        
    try:
        dataframe.write.jdbc(
        url=config.mysql_url,
        table=table_name,
        mode="append", 
        properties=config.mysql_properties
    )
        logger.info(f"Initial Load for {table_name} Completed Successfully")

    except Exception as e:
        logger.error(f"Error in Initial Load for {table_name}: {e}", exc_info=True)
    

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
        logger.error(f"Failed to initialize Spark session: {e}", exc_info=True)


    ## Connect to MySQL Database 
    try:
        connection, cursor = database.connect_mysql()
        if not connection or not cursor:
            logger.info("MySQL DB Connection is not established")
        else:
            logger.info("Connected to MySQL")
            
    except Exception as e:
        logger.error(f"Failed to connect to MySQL: {e}", exc_info=True)


    ## Create Database if Not Exists
    try:
        database_name = 'loan_app'
        database.create_database(connection, cursor, database_name)
        logger.info("Database Created Successfully")

    except Exception as e:
        logger.error(f"Failed to create database: {e}", exc_info=True)
        connection.close()


    data_path = 'D://Work_Items/Projects/FairMoney_Assignment/src/data/initial_load'
    users_initial = f"{data_path}/users.json"
    kyc_initial = f"{data_path}/user_kyc.json"
    loan_offers_initial = f"{data_path}/loan_offers.json"
    user_loans_initial = f"{data_path}/user_loans.json"
    transactions_initial = f"{data_path}/transactions.json"
    resolutions_initial = f"{data_path}/resolutions.json"
    
    ddl_directory_path = 'D://Work_Items/Projects/FairMoney_Assignment/src/sql/ddl'


    # Execute Each Pipeline in the below order 

    # 1. Users Pipeline
    try: 
        users_df = extract.exrtactor(spark, users_initial, extract.users_schema)
        
        users_df_updated = (users_df
        .withColumn("updated_at", F.current_timestamp()) 
        .withColumn("account_status", F.when(F.col("account_closed_date").isNull(), F.lit("Active")).otherwise(F.lit("Deleted")))
        .select("user_id", "user_name", "email", "phone", "date_of_birth", "address", "account_status", "sign_up_date", "account_closed_date", "updated_at"))
        
        users_ddl = f'{ddl_directory_path}/users.sql'
        database.create_table(connection, cursor, users_ddl, database_name, "users")
        initial_load(users_df_updated, "users")
        logger.info("Users Pipeline executed successfully.")

    except Exception as e:
        logger.error(f"Error in Users Pipeline: {e}", exc_info=True)
        traceback.print_exc()


    # 2. User KYC Pipleine
    try:
        kyc_df = extract.exrtactor(spark, kyc_initial, extract.kyc_schema)
        user_kyc_updated = kyc_df.withColumn("updated_at", F.current_timestamp())
        user_kyc_ddl = f'{ddl_directory_path}/user_kyc.sql'
        database.create_table(connection, cursor, user_kyc_ddl, database_name, "user_kyc")
        initial_load(user_kyc_updated, "user_kyc")
        logger.info("User KYC Pipeline executed successfully.")

    except Exception as e:
        logger.error(f"Error in User KYC Pipeline: {e}", exc_info=True)
        traceback.print_exc()


    # 3. Loan Offers Pipeline
    try:
        loan_offers_df = extract.exrtactor(spark, loan_offers_initial, extract.loan_offers_schema)
        loan_offers_updated = loan_offers_df.withColumn("updated_at", F.current_timestamp())
        loan_offers_ddl = f'{ddl_directory_path}/loan_offers.sql'
        database.create_table(connection, cursor, loan_offers_ddl, database_name, "loan_offers")
        initial_load(loan_offers_updated, "loan_offers")
        logger.info("Loan Offers Pipeline executed successfully.")

    except Exception as e:
        logger.error(f"Error in Loan Offers Pipeline: {e}", exc_info=True)
        traceback.print_exc()


    # 4. User Loans Pipeline
    try:
        user_loans_df = extract.exrtactor(spark, user_loans_initial, extract.user_loans_schema)
        user_loans_updated = user_loans_df.withColumn("updated_at", F.current_timestamp())
        user_loans_ddl = f'{ddl_directory_path}/user_loans.sql'
        database.create_table(connection, cursor, user_loans_ddl, database_name, "user_loans")
        initial_load(user_loans_updated, "user_loans")
        logger.info("User Loans Pipeline executed successfully.")

    except Exception as e:
        logger.error(f"Error in User Loans Pipeline: {e}", exc_info=True)
        traceback.print_exc()


    # 5. Transactions Pipeline
    try:
        transactions_df = extract.exrtactor(spark, transactions_initial, extract.transactions_schema)
        transactions_ddl = f'{ddl_directory_path}/transactions.sql'
        database.create_table(connection, cursor, transactions_ddl, database_name, "transactions")
        initial_load(transactions_df, "transactions")
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
        resolutions_df = extract.exrtactor(spark, resolutions_initial, extract.resolutions_schema)
        resolutions_ddl = f'{ddl_directory_path}/resolutions.sql'
        database.create_table(connection, cursor, resolutions_ddl, database_name, "defaulters")
        initial_load(resolutions_df, "resolutions")
        logger.info("Resolutions Pipeline executed successfully.")

    except Exception as e:
        logger.error(f"Error in Resolutions Pipeline: {e}", exc_info=True)
        traceback.print_exc()

    finally:
        connection.close()
        spark.stop()


if __name__ == "__main__":
    main()