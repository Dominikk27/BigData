import psycopg2 as postgres
from utils.Database.databaseConfig import db_config


def connect_database():
    try:
        connection = postgres.connect(**db_config)
        print("Database connection established.")
        return connection
    except postgres.Error as e:
        print(f"Error connecting to database: {e}")
        return None
    
def checkTable(connection, table_name):
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
            createTable(connection, table_name)
    except postgres.Error as e:
        print(f"Error checking table in database {table_name}: {e}")


def createTable(connection, table_name):
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
