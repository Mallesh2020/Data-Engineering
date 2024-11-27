def read_sql_file(path):
    try:
        with open(path, 'r') as file:
            return file.read()
    except Exception as e:
        print(f"SQL file doesn't exists or could not be read --> {e}")


def create_database(connection, cursor, dbName):
    try:
        query = f"CREATE DATABASE IF NOT EXISTS {dbName};"
        cursor.execute(query)
        connection.commit()
        print(f"Database '{dbName}' is Created if Not Exist")
    except Exception as e:
        print(f"Couldn't create Database '{dbName}' --> {e}")


def create_table(connection, cursor, path, databaseName, tableName):
    try:
        cursor.execute(read_sql_file(path))
        connection.commit()
        print(f"Table '{databaseName}.{tableName}' is created if not exists")
    except Exception as e:
        print(f"Could not create Table '{databaseName}.{tableName}' --> {e}")


def load_tables(connection, cursor, path, databaseName, tableName):
    '''
    Use this to load Fact and Dimension Tables only 
    Use load_stage_table to Load Stage Table
    '''
    try:
        cursor.execute(read_sql_file(path))
        connection.commit()
        print(f"Data is loaded into Table - '{databaseName}.{tableName}' successfully")
    except Exception as e:
        print(f"There was an error loading into Table '{databaseName}.{tableName}' --> {e}")


def create_index(connection, cursor, databaseName, tableName):
    try:
        check = (
        f""" 
        SELECT COUNT(*) 
        FROM information_schema.statistics 
        WHERE table_schema = '{databaseName}' 
        AND table_name = '{tableName}' 
        AND index_name = 'idx_scheme_code_record_date'; 
        """)

        create_index = (
            f"""
            CREATE INDEX idx_scheme_code_record_date 
            ON {databaseName}.{tableName} (record_date);
            """)

        cursor.execute(check)
        index_exists = cursor.fetchone()[0]
        if index_exists == 0:
            cursor.execute(create_index)
            connection.commit()
            print(f"Index created successfully on table '{databaseName}.{tableName}'")
        else:
            print("Index already exists")
    except Exception as e:
        print(f"Could not create Index on table '{databaseName}.{tableName}' --> {e}")


def load_stage_table(connection, cursor, df, path, databaseName, tableName):
    try:
        records = df.to_records(index=False).tolist()
        cursor.executemany(read_sql_file(path), records)
        connection.commit()
        print(f"Data is loaded Successfully into '{databaseName}.{tableName}'")
    except Exception as e:
        print(f"Could not load data into table '{databaseName}.{tableName}' --> {e}")