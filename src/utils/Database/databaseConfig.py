db_config = {
    'host': 'host.docker.internal',
    'port': 5432,
    'database': 'bigdata_db',
    'user': 'dominik',
    'password': 'Dominikk27!',
}  

tables_structures = {
    "devices":{
        "id": "SERIAL PRIMARY KEY",
        "device_code": "TEXT UNIQUE NOT NULL",
        "device_type": "TEXT NOT NULL",
    },
    "sensors":{
        "id": "SERIAL PRIMARY KEY",
        "device_id": "INT NOT NULL REFERENCES devices(id) ON DELETE CASCADE",
        "sensor_code": "TEXT NOT NULL",
        "measurement_type": "TEXT NOT NULL",
        "unit": "TEXT NOT NULL",
        "depth_cm": "INT",
        "extras": "JSONB"   
    },
    "measurements":{
        "time": "TIMESTAMPTZ NOT NULL",
        "sensor_id": "INT NOT NULL REFERENCES sensors(id) ON DELETE CASCADE",
        "value": "DOUBLE PRECISION",
        "status": "INT",
        "PRIMARY KEY": "(time, sensor_id)"
    }
}
