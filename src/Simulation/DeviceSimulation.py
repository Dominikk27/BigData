import time
import json
from collections import defaultdict

class DeviceSimulator:
    def __init__(self, mqtt_publisher):
        self.mqtt_publisher = mqtt_publisher

    def start_simulation(self, device, dataset, simulation_delay=1, stop_event=None):
        sensors_map = {}
        
        for sensor in device.sensors:
            extras = sensor.get('extras', {})
            
            key = self.create_sensor_key(
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
        

        group_data = defaultdict(list)

        for row in dataset:
            timestamp = row.get('timestamp')
            group_data[timestamp].append(row)
            


        for timestamp, rows in group_data.items():
            if stop_event and stop_event.is_set():
                print(f"Simulation for device {device.device_code} stopped.")
                break

            measurements = []

            for row in rows:
                sensor_key = self.create_sensor_key(
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


                #value = row.get('value')
                """ 
                message = {
                    "device_code": device.device_code,
                    "sensor_id": sensor_id,
                    "value": value,
                    "timestamp": timestamp
                } 
                """

                measurements.append({
                    "sensor_id": sensor_id,
                    "value": row.get("value"),
                    "status": row.get("status")
                })

                if not measurements:
                    print(f"No valid measurements for timestamp {timestamp} in device {device.device_code}. Skipping.")
                    break

            message = {
                "device_code": device.device_code,
                "measurements": measurements,
                "timestamp": timestamp
            } 

            topic = f"device/{device.device_code}/data"
            self.mqtt_publisher.send(topic, json.dumps(message))

            time.sleep(simulation_delay)
        print(f"[SIMULATION] Vlákno zariadenia {device.device_code} skončilo.")


    def create_sensor_key(
            self,   
            measurement_type,
            depth_cm=None,
            location=None,
            index=None,
            reference=False
    ):
        return (
            measurement_type,
            depth_cm,
            location,
            index,
            reference
        )