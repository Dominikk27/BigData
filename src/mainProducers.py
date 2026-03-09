import threading
import time
import re


from Database.DatabaseManager import DatabaseManager
from Database.Devices import SSA3Lysimeter, URBLysimeter, WeatherStation, SSA4Schacht    
from MQTT import MQTTPublisher
from utils.ReadFile import DatasetManager
from Simulation.DeviceSimulation import DeviceSimulator


from Kafka.TopicProducer import ProducerClient
from MQTT import MQTTClient

from Kafka.TopicConsumer import ConsumerClient


#from utils.Database.databaseConfig import devices_list


def main():
    stop_event = threading.Event()
    
    
    dbManager = DatabaseManager()
    dbConn = DatabaseManager.connect_database(dbManager)
    dbManager.init_database(dbConn)

    datasetManager = DatasetManager()
    publisher = MQTTPublisher()

    simulator = DeviceSimulator(publisher)

    devices = []

    URB_Lysimeter = URBLysimeter()
    SSA3_Lysimeter = SSA3Lysimeter()
    SSA4_Schacht = SSA4Schacht()
    Weather_Station = WeatherStation()

    
    devices.append(URB_Lysimeter)
    devices.append(SSA3_Lysimeter)
    devices.append(SSA4_Schacht)
    devices.append(Weather_Station)

    #urb_dataset = datasetManager.load_dataset("URB")


    """     
    print("=== DEVICE AND SENSORS INFO ===")
    print("DEVICE:", URB_Lysimeter.device_code)
    print("SENSORS:")
    for s in URB_Lysimeter.sensors:
        print(s)
    print("================================")
    """


    thread_data_map = {}
    device_code=None

    for device in devices:
        device.sync_database(dbManager)

        device_code = device.device_code.strip().lower().split('_')[0]
        device_code = device.device_code.lower()
        
        data = datasetManager.load_dataset(device_code)
        if data:
            thread_data_map[device_code] = data
        else:

            print(f"[MAIN] No dataset for device {device.device_code}, skipping simulation.")
            return
    
    threads = []
    for device in devices:
        current_device_code = device.device_code.lower()

        if current_device_code in thread_data_map:
            t = threading.Thread(
                target=simulator.start_simulation,
                args=(device, thread_data_map[current_device_code], 1, stop_event)
            )
            threads.append(t)
            t.start()
    
    try:
        while any (t.is_alive() for t in threads):
            time.sleep(1)


    except KeyboardInterrupt:
        print("[MAIN] Stopping simulations...")
        stop_event.set()

    
    dbManager.close_connection()
    print("[MAIN] Done!")



if __name__ == "__main__":
    main()
