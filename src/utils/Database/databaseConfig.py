db_config = {
    'host': 'host.docker.internal',
    'port': 5432,
    'database': 'bigdata_db',
    'user': 'dominik',
    'password': 'Dominikk27!',
}  

tables_structures = {
    "devices": {
        "device_id": "SERIAL PRIMARY KEY",
        "device_code": "VARCHAR(50) UNIQUE NOT NULL",
        "device_type": "TEXT NOT NULL",
        "created_at": "TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP"
    },
    "sensors": {
        "sensor_id": "SERIAL PRIMARY KEY",
        "device_id": "INT NOT NULL REFERENCES devices(device_id) ON DELETE CASCADE",
        "sensor_code": "TEXT NOT NULL",
        "measurement_type": "TEXT NOT NULL",
        "unit": "TEXT NOT NULL",
        "depth_cm": "INT",
        "extras": "JSONB",
        "created_at": "TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP",
        "UNIQUE": "(device_id, sensor_code)"
    },
    "measurements": {
        "time": "TIMESTAMPTZ NOT NULL",
        "sensor_id": "INT NOT NULL REFERENCES sensors(sensor_id) ON DELETE CASCADE",
        "value": "DOUBLE PRECISION",
        "status": "INT",
        "PRIMARY KEY": "(time, sensor_id)",
    },
    "measurements_analytics": {
        "time": "TIMESTAMPTZ NOT NULL",
        "sensor_id": "INT NOT NULL REFERENCES sensors(sensor_id) ON DELETE CASCADE",
        "original_value": "DOUBLE PRECISION",
        "moving_avg": "DOUBLE PRECISION",
        "PRIMARY KEY": "(time, sensor_id)",
    }
}