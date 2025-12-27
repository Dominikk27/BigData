import psycopg2 as postgres
from utils.Database.databaseConfig import db_config

    
def check_table(connection, table_name):
    try:
        cursor = connection.cursor()
        cursor.execute(f"""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_schema = 'public' 
                AND table_name = '{table_name}'
            );
        """)
        exists = cursor.fetchone()[0]
        if exists:
            print(f"Table {table_name} exists in the database.")
        else:
            print(f"Table {table_name} does not exist in the database.")
            print(f"Creating table {table_name}...")
            create_table(connection, table_name)
    except postgres.Error as e:
        print(f"Error checking table in database {table_name}: {e}")


def create_table(connection, table_name):
    try:
        cursor = connection.cursor()
        cursor.execute(f"""
            CREATE TABLE {table_name} (
                id SERIAL PRIMARY KEY,
                plant_id INT NOT NULL,
                metric_type VARCHAR(50) NOT NULL,
                metric_value FLOAT NOT NULL,
                timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)
        connection.commit()
        print(f"Table {table_name} created successfully.")
    except postgres.Error as e:
        print(f"Error creating table {table_name}: {e}")


def insert_data():
    pass





def close_connection(connection):
    if connection:
        connection.close()
        print("Database connection closed.")



class DatabaseManager:
    def __init__(self):
        self.conn = None
        self.cursor = None
    

    def connect_database(self):
        try:
            self.conn = postgres.connect(**db_config)
            self.cursor = self.conn.cursor()
            print("Database connection established.")
            return self.conn
        except postgres.Error as e:
            print(f"Error connecting to database: {e}")
            return None
        
    
    def checkTable_existence(self, table_name):
        if not self.cursor:
            raise Exception("connection to db doesnt exist!")
        
        try:
            self.cursor.execute("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables
                    WHERE table_schema = 'public'
                    AND table_name = %s                
                );
            """,(table_name, ))

            exists = self.cursor.fetchone()[0]
            if(exists):
                print(f"{table_name} exists!")
            else:
                print(f"{table_name} doesnt exist!")
                create_table(table_name)

            return exists
        except Exception as e:
            print(f"Error while checking table '{table_name}': {e}")
            raise e
    

    def create_table(self, table_name):
        try:
            print("Create table")
            self.cursor.execute("""
                CREATE TABLE %s (
                    
                )
            """)
        except Exception as e:
            print(f"Error with creating table '{table_name}': {e}")
            raise e