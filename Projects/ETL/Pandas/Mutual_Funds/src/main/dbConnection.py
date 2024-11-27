import pymysql

mySqlConfig = {
    'host': '127.0.0.1',
    'user': 'test',
    'password': 'mysql',
    'database': 'world'
}


def connect_mysql():
    try:
        connection, cursor = None, None
        connection = pymysql.connect(**mySqlConfig)
        cursor = connection.cursor()
        return connection, cursor
    
    except Exception as e:
        print(f"Connection to MySQL DB Failed --> {e}")
        return None, None 