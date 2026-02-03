import psycopg2 as postgres
import time
from datetime import datetime, timezone
import json

from utils.Database.databaseConfig import db_config, tables_structures

class DatabaseManager:
    def __init__(self):
        self.conn = None
        self.cursor = None
    

    def connect_database(self):
        try:
            self.conn = postgres.connect(**db_config)
            cursor = self.conn.cursor()
            #print("Database connection established.")
            return self.conn
        except postgres.Error as e:
            print(f"Error connecting to database: {e}")
            return None
        
    def init_database(self, conn):
        """ 
        conn = self.connect_database()
        if not conn:
            print("Failed to connect to database.")
            return 
        """
        cursor = self.conn.cursor()

        #print("Initializing database schema...")
        try: 

            for table_name, columns in tables_structures.items():
                column_defs = []

                for col_name, col_def in columns.items():
                    if col_name == "PRIMARY KEY":
                        column_defs.append(f"{col_name} {col_def}")
                    else:
                        column_defs.append(f"{col_name} {col_def}")

                createTable_query = f"CREATE TABLE IF NOT EXISTS {table_name} ({', '.join(column_defs)});"

                cursor.execute(createTable_query)
                conn.commit()
                #print(f"Table '{table_name}' is ready!")


        except Exception as e:
            print(f"Error creating devices table: {e}")
            self.conn.rollback()
            return
        finally:
            cursor.close()
            print("Database initialization complete.")
    

    def register_device(self, 
                        device_code,
                        device_type):
        
        cursor = self.conn.cursor()
        query = """
            INSERT INTO devices (device_code, device_type)
            VALUES (%s, %s)
            ON CONFLICT (device_code) DO UPDATE SET device_type = EXCLUDED.device_type
            RETURNING device_id;
        """
        try:
            cursor.execute(query, (device_code, device_type))
            device_id = cursor.fetchone()[0]
            self.conn.commit()
            #print("Data inserted successfully.")
            return device_id
        except postgres.Error as e:
            print(f"Error inserting data: {e}")
            self.conn.rollback()
            return None
        finally:
            cursor.close()
            print("Devices registration complete.")
    
    def register_sensor(self,
                        device_id,
                        sensor_code,
                        measurement_type,
                        unit,
                        depth_cm=None,
                        extras=None):
        
        cursor = self.conn.cursor()
        extras_json = json.dumps(extras) if extras else None
        query = """
            INSERT INTO sensors (device_id, sensor_code, measurement_type, unit, depth_cm, extras)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (device_id, sensor_code) 
            DO UPDATE SET measurement_type = EXCLUDED.measurement_type
            RETURNING sensor_id;
        """
        try:
            cursor.execute(query, (device_id, 
                                    sensor_code, 
                                    measurement_type, 
                                    unit, 
                                    depth_cm, 
                                    extras_json))
            sensor_id = cursor.fetchone()[0]
            self.conn.commit()

            #print("Sensor registered successfully.")
            return sensor_id
        except postgres.Error as e:
            print(f"Error registering sensor: {e}")
            self.conn.rollback()
            return None
        finally:
            cursor.close()
            print("Sensor registration complete.")
    

    
    def close_connection(self):
        if self.conn:
            self.conn.close()
            print("Database connection closed.")


    

    def insert_measurement(self,
                           timestamp,
                           measurements):
        
        if isinstance(timestamp, (int, float)):
            transformed_timestamp = self.transform_timestamp(timestamp)
        else:
            transformed_timestamp = timestamp
 
        if self.conn is None:
            print("No database connection.")
            return
        
        cursor = self.conn.cursor()
        query = """
            INSERT INTO measurements (time, sensor_id, value, status)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (time, sensor_id) DO NOTHING;
        """
        try:
            for m in measurements:
                cursor.execute(query, (transformed_timestamp, 
                                       m['sensor_id'], 
                                       m['value'], 
                                       m.get('status', 0)
                                    ))
            self.conn.commit()
            #print("Measurements inserted successfully.")
        except Exception as e:
            print(f"Error inserting measurement: {e}")
            self.conn.rollback()
        finally:
            cursor.close()
            #print("Measurement insertion complete.")
        

    def transform_timestamp(self, timestamp):
        date_time = datetime.fromtimestamp(timestamp / 1000.0, tz=timezone.utc)
        return date_time