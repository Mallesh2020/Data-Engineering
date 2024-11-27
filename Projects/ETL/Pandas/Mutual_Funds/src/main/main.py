import extract, transform, load, dbConnection
import pandas as pd


def main():
    try:
	    # Step-1: Connect to Source 
        response = extract.connect_source()
        if not response:
            print("Could not connect to Source. Please check the URL")
	    
        
        # Step-2: Prepare Data
        header, data = extract.prepare_data(response)
        if not (header and data):
            print("There is no data to be loaded")
        
	    
        # Step-3: Transform Data
        df = transform.dataframe(header, data)
        if df.shape[0] == 0:
            print("No Data to Load....")
        
	    
        # Step-4: Connect to MySQL Database 
        connection, cursor = dbConnection.connect_mysql()
        if not connection or not cursor:
            print("MySQL DB Connection is not established")
        
        
        try:
            # Step-4: Create Database 
            dbName = 'mutual_fund_analysis'
            load.create_database(connection, cursor, dbName)
            
	        
            # Step-5: Create and Load Stage Table. 
            # Create Index on Scheme Code and Record Date
            stage_ddl_path = 'src/sql/ddl/create_mf_stage.sql'
            stage_dml_path = 'src/sql/dml/load_mf_stage.sql'
            load.create_table(connection, cursor, stage_ddl_path, 'mutual_fund_analysis', 'mf_stage')
            load.create_index(connection, cursor, 'mutual_fund_analysis', 'mf_stage')
            load.load_stage_table(connection, cursor, df, stage_dml_path, 'mutual_fund_analysis', 'mf_stage')
	        
	        
            # Step-6: Create Fact and Dimension Tables (Data Model)
            # Create Index on Fact Table
            category_dim_ddl_path = 'src/sql/ddl/create_category_dim.sql'
            scheme_dim_ddl_path = 'src/sql/ddl/create_scheme_dim.sql'
            fact_ddl_path = 'src/sql/ddl/create_mf_fact.sql'
            load.create_table(connection, cursor, category_dim_ddl_path, 'mutual_fund_analysis', 'categories')
            load.create_table(connection, cursor, scheme_dim_ddl_path, 'mutual_fund_analysis', 'schemes')
            load.create_table(connection, cursor, fact_ddl_path, 'mutual_fund_analysis', 'mutualfunds_nav')
            load.create_index(connection, cursor, 'mutual_fund_analysis', 'mutualfunds_nav')
	        
	        
            # Step-7: Load Fact and Dimension Tables
            category_dim_dml_path = 'src/sql/dml/load_category_dim.sql'
            scheme_dim_dml_path = 'src/sql/dml/load_scheme_dim.sql'
            fact_dml_path = 'src/sql/dml/load_mf_fact.sql'
            load.load_tables(connection, cursor, category_dim_dml_path, 'mutual_fund_analysis', 'categories')
            load.load_tables(connection, cursor, scheme_dim_dml_path, 'mutual_fund_analysis', 'schemes')
            load.load_tables(connection, cursor, fact_dml_path, 'mutual_fund_analysis', 'mutualfunds_nav')
	        
        except Exception as e:
            print(f"There was a error in Database Operations --> {e}")
        finally:
            cursor.close()
            connection.close()
            print("Connection is closed")
    
    except Exception as e:
	    print(f"There was an error in the Main Execution --> {e}")


if __name__ == '__main__':
    main()