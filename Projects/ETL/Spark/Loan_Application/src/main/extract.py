from pyspark.sql.types import StructType, StructField, IntegerType, DoubleType, StringType, DecimalType, TimestampType, DateType
import logger


logger = logger.get_logger("Data Extraction")
Business_date = '20250108'                     # Edit Business date as per the date we run 


def exrtactor(spark, path, schema):
    try:
        print(f'Reading {path} data......')
        df = (
            spark.read
            .format("json")
            .option("multiline", True)
            .load(path=path, schema=schema)
            )
        print(f'Successfully read {path} file data')
        logger.info(f'Data from {path} extracted successfully')
        return df
    
    except Exception as e:
        print(f'There is an Error --> {e}')
        logger.error(f"There is an error in extracting data in {path}: {e}", exc_info=True)


data_path = f'D://Work_Items/Projects/FairMoney_Assignment/src/data/{Business_date}'
users_data = f"{data_path}/users*.json"
kyc_data = f"{data_path}/user_kyc*.json"
loan_offers_data = f"{data_path}/loan_offers*.json"
user_loans_data = f"{data_path}/user_loans*.json"
transactions_data = f"{data_path}/transactions*.json"
resolutions_data = f"{data_path}/resolutions*.json"


# Users Schema
users_schema = StructType([
    StructField("user_id", IntegerType(), False),
    StructField("user_name", StringType(), False),
    StructField("email", StringType(), False),
    StructField("phone", StringType(), False),
    StructField("date_of_birth", DateType(), False),  #format is YYYY-MM-DD
    StructField("address", StringType(), True),
    StructField("sign_up_date", TimestampType(), False),
    StructField("account_closed_date", TimestampType(), True)  ## This will be the date on which user Deletes his account in Loan App
])

# KYC Schema
kyc_schema = StructType([
    StructField("kyc_id", IntegerType(), False),
    StructField("user_id", IntegerType(), False),        
    StructField("document_type", StringType(), False),    # Aadhar, PAN etc  
    StructField("document_number", StringType(), False),
    StructField("kyc_status", StringType(), False),       # Success, Fail, Pending 
    StructField("submitted_at", TimestampType(), False)
])

# Available Loan Offers Schema
loan_offers_schema = StructType([
    StructField("loan_id", IntegerType(), True),
    StructField("loan_amount", IntegerType(), True),
    StructField("tenure", IntegerType(), True),
    StructField("interest_rate", DoubleType(), True),
    StructField("total_amount", DecimalType(), True), 
    StructField("offer_status", StringType(), True)          # Expired, Active 
])

# Loans Schema
user_loans_schema = StructType([
    StructField("loan_id", IntegerType(), False),
    StructField("user_id", IntegerType(), False),
    StructField("status", StringType(), False),     # 'Applied', 'Approved', 'Rejected', 'Active', 'Closed', 'Defaulted'
    StructField("applied_date", TimestampType(), False),
    StructField("approved_date", TimestampType(), False),
])


# Transactions Schema 
transactions_schema = StructType([
    StructField("transaction_id", IntegerType(), False),
    StructField("user_id", IntegerType(), False),
    StructField("loan_id", IntegerType(), False),
    StructField("amount", DecimalType(10,2), False),
    StructField("transaction_type", StringType(), False),  # Borrow for Loan Disbursement, Repay for Laon Repayment
    StructField("transaction_date", TimestampType(), False)
])


# Resolutions Schema 
resolutions_schema = StructType([
    StructField("executive_id", IntegerType(), False),  
    StructField("defaulter_id", IntegerType(), True),  
    StructField("executive_name", StringType(), False),  
    StructField("contact_number", StringType(), False),  
    StructField("is_resolved", StringType(), True),     # Yes/No
    StructField("assigned_at", TimestampType(), True),  
    StructField("updated_at", TimestampType(), True)  
])