import time
import json

class DeviceSimulator:
    def __init__(self, mqtt_publisher):
        self.mqtt_publisher = mqtt_publisher

    def start_simulation(self, device, dataset, simulation_delay=1, stop_event=None):
        sensors_map = {}
        
        for sensor in device.sensors:
            extras = sensor.get('extras', {})
            
            key = (
                sensor['measurement_type'],
                sensor.get('depth_cm'),
                extras.get('location'),
                extras.get('index'),
                extras.get('reference', False)
            )

            sensors_map[key] = sensor.get('db_id')
            print("=============================================")
            print(f"[SIMULATION] Mapa senzorov pre zariadenie {device.device_code}: {sensors_map}")
            print("=============================================")

        if not dataset:
            print(f"No dataset available for device {device.device_code}. Simulation aborted.")
            return

        for row in dataset:
            if stop_event and stop_event.is_set():
                print(f"Simulation for device {device.device_code} stopped.")
                break

            timestamp = row.get('timestamp')
            sensor_key = (
                row.get("measurement_type"),
                row.get("depth_cm"),
                row.get("location"), 
                row.get("index"),
                row.get("reference", False)
            )

            sensor_id = sensors_map.get(sensor_key)
            if not sensor_id:
                print(f"Sensor not found for key {sensor_key} in device {device.device_code}. Skipping record.")
                continue

            value = row.get('value')
            message = {
                "device_code": device.device_code,
                "sensor_id": sensor_id,
                "value": value,
                "timestamp": timestamp
            }

            topic = f"device/{device.device_code}/data"
            self.mqtt_publisher.send(topic, json.dumps(message))

            time.sleep(simulation_delay)
        print(f"[SIMULATION] Vlákno zariadenia {device.device_code} skončilo.")