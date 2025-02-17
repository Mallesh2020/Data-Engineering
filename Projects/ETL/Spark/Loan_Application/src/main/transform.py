import config, logger, traceback
import pyspark.sql.functions as F


logger = logger.get_logger("Pipelines")


#############################################################################################################
###                              Users Pipeline Loads data into Users Table                               ###
#############################################################################################################

def users_pipeline(spark, cursor, users_df):

    logger.info("Executing Users Pipeline.......")

    users_updated = (users_df
        .withColumn("updated_at", F.current_timestamp()) 
        .withColumn("account_status", 
                    F.when(F.col("account_closed_date").isNull(), F.lit("Active"))
                    .otherwise(F.lit("Deleted")))
        .select("user_id", "user_name", "email", "phone", "date_of_birth", "address", "account_status", "sign_up_date", "account_closed_date", "updated_at")
    )

    ## Upsert Logic --> New Users will be Inserted and Existing Users details will be updated 

    existing_table_df = (
        spark.read
        .jdbc(config.mysql_url, table = "users", properties = config.mysql_properties)
    )

    # New users --> From Incoming data
    new_users_df = users_updated.join(
        existing_table_df,
        on="user_id",
        how="left_anti"  
    )

    # Update Existing Records
    existing_users_df = (users_updated.alias("new").join(
        existing_table_df.alias("existing"),
        on="user_id",
        how="inner"  
        )
    .select(F.col("existing.user_id"), 
            F.col("new.user_name"),
            F.col("new.email"),
            F.col("new.phone"), 
            F.col("new.date_of_birth"), 
            F.col("new.address"), 
            F.col("new.account_status"), 
            F.col("new.sign_up_date"), 
            F.col("new.account_closed_date"), 
            F.col("new.updated_at")
            )
    )

    #Use stage table to load existing users data
    cursor.execute("SET FOREIGN_KEY_CHECKS=0;")
    cursor.execute("CREATE DATABASE IF NOT EXISTS stage_layer;")
    cursor.execute("CREATE TABLE IF NOT EXISTS stage_layer.users_staging LIKE loan_app.users;")
    cursor.execute("TRUNCATE TABLE stage_layer.users_staging")

    # Write Existing Users to the Stage Table
    try:
        existing_users_df.write.jdbc(
        url=config.mysql_url,
        table="stage_layer.users_staging",  
        mode="append",                      
        properties=config.mysql_properties
    )
        
    except Exception as e:
        logger.error(f"Error in writing data: {e}", exc_info=True)
        traceback.print_exc()

    # Update Existing Users in the Table
    cursor.execute("DELETE FROM loan_app.users WHERE user_id IN (SELECT user_id FROM stage_layer.users_staging);")
    cursor.execute("INSERT INTO loan_app.users SELECT * FROM stage_layer.users_staging;")

    # Insert New Users into the Table 
    try:
        new_users_df.write.jdbc(
        url=config.mysql_url,
        table="users",
        mode="append",  
        properties=config.mysql_properties
    )
        
    except Exception as e:
        logger.error(f"Error in writing data: {e}", exc_info=True)
        traceback.print_exc()
    
    cursor.execute("SET FOREIGN_KEY_CHECKS=1;")

    logger.info("Users Pepeline Completed Successfully")


#############################################################################################################
###                              User_Kyc Pipeline Loads data into User_kyc Table                         ###
#############################################################################################################

def user_kyc_piepline(spark, cursor, kyc_df):

    logger.info("Executing Users KYC Pipleline.......")

    user_kyc_updated = kyc_df.withColumn("updated_at", F.current_timestamp())
        

    ## Upsert Logic --> New user_kyc will be Inserted and Existing user_kyc details will be updated 

    existing_kyc_table_df = (
        spark.read
        .jdbc(config.mysql_url, table = "user_kyc", properties = config.mysql_properties)
    )

    # New user_kyc --> From Incoming data
    new_user_kyc_df = user_kyc_updated.join(
        existing_kyc_table_df,
        on=["user_id", "kyc_id"],
        how="left_anti"  # Keeps only user_kyc that are NOT in MySQL
    )

    # Update existing user_kyc (user_kyc already in MySQL)
    existing_user_kyc_df = (user_kyc_updated.alias("new").join(
        existing_kyc_table_df.alias("existing"),
        on=["user_id", "kyc_id"],
        how="inner"  
        )
    .select(F.col("existing.kyc_id"),
            F.col("existing.user_id"),  
            F.col("new.document_type"),
            F.col("new.document_number"),
            F.col("new.kyc_status"),
            F.col("new.submitted_at"),
            F.col("new.updated_at")
            )
    )

    #Use stage table to load existing user_kyc data
    cursor.execute("SET FOREIGN_KEY_CHECKS=0;")
    cursor.execute("CREATE DATABASE IF NOT EXISTS stage_layer;")
    cursor.execute("CREATE TABLE IF NOT EXISTS stage_layer.user_kyc_staging LIKE loan_app.user_kyc;")
    cursor.execute("TRUNCATE TABLE stage_layer.user_kyc_staging")

    try:
        existing_user_kyc_df.write.jdbc(
        url=config.mysql_url,
        table="stage_layer.user_kyc_staging",  
        mode="append",                         
        properties=config.mysql_properties
    )
        
    except Exception as e:
        logger.error(f"Error in writing data: {e}", exc_info=True)
        traceback.print_exc()

    # Write Existing user_kyc data to the Table
    cursor.execute("DELETE FROM loan_app.user_kyc WHERE (user_id, kyc_id) IN (SELECT user_id, kyc_id FROM stage_layer.user_kyc_staging);")
    cursor.execute("INSERT INTO loan_app.user_kyc SELECT * FROM stage_layer.user_kyc_staging;")

    # Insert New user_kyc into the Table 
    try:
        new_user_kyc_df.write.jdbc(
        url=config.mysql_url,
        table="user_kyc",
        mode="append",             
        properties=config.mysql_properties
    )
    except Exception as e:
        logger.error(f"Error in writing data: {e}", exc_info=True)
        traceback.print_exc()

    cursor.execute("SET FOREIGN_KEY_CHECKS=1;")

    logger.info("Users KYC Pipeline Completed Successfully")


#############################################################################################################
###                              Loan_Offers Pipeline Loads data into Loan_Offers Table                   ###
#############################################################################################################

def loan_offers_pipeline(spark, cursor, loan_offers_df):

    logger.info("Executing Loan Offers Pipleline.......")

    loan_offers_updated = loan_offers_df.withColumn("updated_at", F.current_timestamp())

    existing_loan_offers_table_df = (
        spark.read
        .jdbc(config.mysql_url, table = "loan_offers", properties = config.mysql_properties)
    )

    new_loan_offers_df = loan_offers_updated.join(
        existing_loan_offers_table_df,
        on="loan_id",
        how="left_anti" 
    )

    existing_loan_offers_df = (loan_offers_updated.alias("new").join(
        existing_loan_offers_table_df.alias("existing"),
        on="loan_id",
        how="inner" 
        )
    .select(F.col("existing.loan_id"), 
            F.col("new.loan_amount"),
            F.col("new.tenure"),
            F.col("new.interest_rate"), 
            F.col("new.total_amount"), 
            F.col("new.offer_status"), 
            F.col("new.updated_at")
            )
    )

    cursor.execute("SET FOREIGN_KEY_CHECKS=0;")
    cursor.execute("CREATE DATABASE IF NOT EXISTS stage_layer;")
    cursor.execute("CREATE TABLE IF NOT EXISTS stage_layer.loan_offers_staging LIKE loan_app.loan_offers;")
    cursor.execute("TRUNCATE TABLE stage_layer.loan_offers_staging")

    try:
        existing_loan_offers_df.write.jdbc(
        url=config.mysql_url,
        table="stage_layer.loan_offers_staging",  
        mode="append",  
        properties=config.mysql_properties
    )
        
    except Exception as e:
        logger.error(f"Error in writing data: {e}", exc_info=True)
        traceback.print_exc()
        
    cursor.execute("DELETE FROM loan_app.loan_offers WHERE loan_id IN (SELECT loan_id FROM stage_layer.loan_offers_staging);")
    cursor.execute("INSERT INTO loan_app.loan_offers SELECT * FROM stage_layer.loan_offers_staging;")

    try:
        new_loan_offers_df.write.jdbc(
        url=config.mysql_url,
        table="loan_offers",
        mode="append",  
        properties=config.mysql_properties
    )
    except Exception as e:
        logger.error(f"Error in writing data: {e}", exc_info=True)
        traceback.print_exc()
    
    cursor.execute("SET FOREIGN_KEY_CHECKS=1;")

    logger.info("Loan Offers Pipleline Completed Successfully")


#############################################################################################################
###                              User_Loans Pipeline Loads data into User_Loans Table                     ###
#############################################################################################################

def user_loans_pipeline(spark, cursor, user_loans_df):

    logger.info("Executing User Loans Pipeline........")

    user_loans_updated = user_loans_df.withColumn("updated_at", F.current_timestamp())
        
    existing_user_loans_table_df = (
        spark.read
        .jdbc(config.mysql_url, table = "user_loans", properties = config.mysql_properties)
    )

    new_user_loans_df = (user_loans_updated.join(
        user_loans_updated,
        on=["loan_id","user_id"],
        how="left_anti"  
    ))

    existing_user_loans_df = (user_loans_updated.alias("new").join(
        existing_user_loans_table_df.alias("existing"),
        on=["loan_id","user_id"],
        how="inner"  
        )
    .select(F.col("existing.loan_id"), 
            F.col("existing.user_id"),
            F.col("new.status"),
            F.col("new.applied_date"), 
            F.col("new.approved_date"), 
            F.col("new.updated_at")
            )
    )

    cursor.execute("SET FOREIGN_KEY_CHECKS=0;")
    cursor.execute("CREATE DATABASE IF NOT EXISTS stage_layer;")
    cursor.execute("CREATE TABLE IF NOT EXISTS stage_layer.user_loans_staging LIKE loan_app.user_loans;")
    cursor.execute("TRUNCATE TABLE stage_layer.user_loans_staging;")

    try:
        existing_user_loans_df.write.jdbc(
        url=config.mysql_url,
        table="stage_layer.user_loans_staging",  
        mode="append",  
        properties=config.mysql_properties
    )
    except Exception as e:
        logger.error(f"Error in writing data: {e}", exc_info=True)
        traceback.print_exc()
        
    cursor.execute("""DELETE FROM loan_app.user_loans
                   WHERE EXISTS (SELECT 1
                   FROM stage_layer.user_loans_staging s
                   WHERE loan_app.user_loans.loan_id = s.loan_id
                   AND loan_app.user_loans.user_id = s.user_id)""")
    cursor.execute("INSERT INTO loan_app.user_loans SELECT * FROM stage_layer.user_loans_staging;")

    try:
        new_user_loans_df.write.jdbc(
        url=config.mysql_url,
        table="user_loans",
        mode="append",  
        properties=config.mysql_properties
    )
    except Exception as e:
        logger.error(f"Error in writing data: {e}", exc_info=True)
        traceback.print_exc()
        
    cursor.execute("SET FOREIGN_KEY_CHECKS=1;")

    logger.info("User Loans Pipeline Completed Successfully")


#############################################################################################################
###                              Transactions Pipeline Loads data into Transactions Table                 ###
#############################################################################################################
# Transactions will be direct insert 

def transactions_piepline(spark, transactions_df):

    logger.info("Executing Transactions Pipleine.......")

    try:
        transactions_df.write.jdbc(
        url=config.mysql_url,
        table="transactions",
        mode="append", 
        properties=config.mysql_properties
    )
    except Exception as e:
        logger.error(f"Error in writing data: {e}", exc_info=True)
        traceback.print_exc()
    
    logger.info("Transactions Pipeline Completed Successfully")


#############################################################################################################
###                              Loan_Balances Pipeline Loads data into Loan_Balances Table               ###
#############################################################################################################

def loan_balances_pipeline(spark, cursor):

    logger.info("Executing Loan Balanaces Pipeline.....")

    loan_balances_existing_df = spark.read.jdbc(config.mysql_url, table = "loan_balances", properties = config.mysql_properties)
    loan_offers_df = spark.read.jdbc(config.mysql_url, table = "loan_offers", properties = config.mysql_properties)
    user_loans_df = spark.read.jdbc(config.mysql_url, table = "user_loans", properties = config.mysql_properties)
    transactions_df = spark.read.jdbc(config.mysql_url, table = "transactions", properties = config.mysql_properties)

    # Step 1: Identify New Loans (Not in Existing Loan Balances Table)
    new_loans_df = (user_loans_df.alias("ul").join(loan_balances_existing_df.alias("lb"), 
                                                on="loan_id", 
                                                how="left_anti"  # Select loans that are NOT in loan_balances
                                                ).join(loan_offers_df.alias("lo"), 
                                                        on="loan_id", 
                                                        how="inner")
                                                .select(F.col("ul.loan_id"),
                                                        F.col("ul.user_id"),
                                                        F.col("lo.total_amount").alias("loan_amount"),
                                                        F.date_add(F.col("ul.approved_date"), F.col("lo.tenure") * 30).alias("due_date"),  # Approximate month as 30 days
                                                        F.col("lo.total_amount").alias("remaining_balance")
                                                        )
                                                .filter(F.col("ul.status").isin(["Approved", "Active"]))
                    )

    # Compute Borrow Transactions (Loan Disbursement)
    Borrow_transactions = (transactions_df.filter(F.col("transaction_type") == "Borrow")
                        .groupBy("loan_id")
                        .agg(F.sum(F.col("amount").cast("decimal(10,2)")).alias("total_borrow")) 
                        )

    new_loans_df = new_loans_df.join(Borrow_transactions, on="loan_id", how="left").fillna({"total_borrow": 0})

    # Compute Repayment Transactions (Loan Repayments)
    repayment_transactions = (transactions_df.filter(F.col("transaction_type") == "Repay")
                        .groupBy("loan_id")
                        .agg(F.sum(F.col("amount").cast("decimal(10,2)")).alias("total_repayment")) 
                        )

    new_loans_df = new_loans_df.join(repayment_transactions, on="loan_id", how="left").fillna({"total_repayment": 0})

    # Compute Remaining Balance & Days Past Due
    new_loans_df = (new_loans_df.withColumn("remaining_balance", F.col("loan_amount") - F.col("total_repayment"))
                    .withColumn("days_past_due", F.datediff(F.current_date(), F.col("due_date")))
                    .withColumn("updated_at", F.current_timestamp())
                    )

    new_loans_df = (new_loans_df.withColumn("days_past_due", F.when((F.col("days_past_due") >= 0) & (F.col("remaining_balance") >= 0), F.col("days_past_due"))
                                            .otherwise(0))
                                            )

    # Merge New Loans with Existing Loan Balances
    final_loan_balances_df = loan_balances_existing_df.unionByName(new_loans_df)

    cursor.execute("SET FOREIGN_KEY_CHECKS=0;")
    try:
        final_loan_balances_df.write.jdbc(
        url=config.mysql_url,
        table="loan_balances",
        mode="overwrite", 
        properties=config.mysql_properties
    )
    except Exception as e:
        logger.error(f"Error in writing data: {e}", exc_info=True)
        traceback.print_exc()

    cursor.execute("SET FOREIGN_KEY_CHECKS=1;")
    logger.info("Loan Balances Pipeline Completed Successfully")


#############################################################################################################
###                              Defaulters Pipeline Loads data into Defaulters Table                     ###
#############################################################################################################

def defaulters_pipeline(spark, cursor):

    logger.info("Executing Defaulters Pipeline.....")

    loan_balances_df = (spark.read
                            .jdbc(config.mysql_url, 
                                table = "loan_balances", 
                                properties = config.mysql_properties)
                        )

    defaulters_df = (loan_balances_df.filter((F.col("days_past_due") > 0) & (F.col("remaining_balance") > 0))
                    .select(F.col("user_id").alias("defaulter_id"),
                            "loan_id",
                            "loan_amount",
                            "remaining_balance",
                            "days_past_due",
                            "updated_at")
                    )

    cursor.execute("SET FOREIGN_KEY_CHECKS=0;")

    try:
        defaulters_df.write.jdbc(
        url=config.mysql_url,
        table="defaulters",
        mode="overwrite", 
        properties=config.mysql_properties
    )
        
    except Exception as e:
        logger.error(f"Error in writing data: {e}", exc_info=True)
        traceback.print_exc()

    cursor.execute("SET FOREIGN_KEY_CHECKS=1;")

    logger.info("Defaulters Pipeline Completed Successfully")


#############################################################################################################
###                              Resolutions Pipeline Loads data into Resolutions Table                   ###
#############################################################################################################

def resolutions_pipeline(spark, cursor, resolutions_df):

    logger.info("Executing Resolutions Pipleine.........")

    defaulters_df = spark.read.jdbc(config.mysql_url, table = "defaulters", properties = config.mysql_properties)

    resolved_defaulters_df = (defaulters_df.join(resolutions_df, 
                                                on="defaulter_id",
                                                how="inner")
                                        .filter(resolutions_df["is_resolved"] == "Yes") 
                                        .select("defaulter_id", "loan_id"))

    # Update loan_balances for resolved defaulters, setting their remaining_balance to 0
    loan_balances_df = spark.read.jdbc(config.mysql_url, table = "loan_balances", properties = config.mysql_properties)

    if resolved_defaulters_df.count() != 0:
            new_loan_balances_df = (loan_balances_df.alias("lb").join(resolved_defaulters_df.alias("rd"),
                                                            (F.col("lb.loan_id") == F.col("rd.loan_id")) & (F.col("lb.user_id") == F.col("rd.defaulter_id")),
                                                            "left")
                                                    .withColumn("remaining_balance", F.when(F.col("rd.loan_id").isNotNull(), F.lit(0))  # If defaulter is found, set balance to 0
                                                                .otherwise(F.col("lb.remaining_balance")))
                                                    .select("lb.*")
                                    ) 
            
            try:
                new_loan_balances_df.write.jdbc(
                url=config.mysql_url,
                table="loan_balances",
                mode="overwrite", 
                properties=config.mysql_properties
            )
                
            except Exception as e:
                logger.error(f"Error in writing data: {e}", exc_info=True)
                traceback.print_exc()

            cursor.execute("SET FOREIGN_KEY_CHECKS=1;")

    logger.info("resolutions Pipleine Completed Successfully")