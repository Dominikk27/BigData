import threading
import time
import re


from Database.DatabaseManager import DatabaseManager
from Database.Devices import LysimeterDevice
from MQTT import MQTTPublisher
from utils.ReadFile import DatasetManager
from Simulation.DeviceSimulation import DeviceSimulator


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

    
    URB_Lysimeter = LysimeterDevice("URB")
    URB_Lysimeter.add_tension_sensor(None, "outside", False)
    URB_Lysimeter.add_tension_sensor(None, "inside", False)
    URB_Lysimeter.add_tension_sensor(None, "outside", True)
    
    URB_Lysimeter.add_level_sensor()
    URB_Lysimeter.add_level_sensor(None, None, True)

    URB_Lysimeter.add_percolation_sensor()

    URB_Lysimeter.add_discharge_sensor(1)

    URB_Lysimeter.add_vacuum_sensor(None)

    URB_Lysimeter.add_temperature_control_sensor()
    URB_Lysimeter.add_temperature_sensor(None, "inside")
    URB_Lysimeter.add_temperature_sensor(None, "outside")

    devices.append(URB_Lysimeter)

    urb_dataset = datasetManager.load_dataset("URB")


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
        if not data:
            print(f"[MAIN] No dataset for device {device.device_code}, skipping simulation.")
            break
        thread_data_map[device_code] = data
    
    threads = []
    for device in devices:
        if device_code in thread_data_map:
            t = threading.Thread(
                target=simulator.start_simulation,
                args=(device, thread_data_map[device_code], 1, stop_event)
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
