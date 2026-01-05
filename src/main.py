from Database.DatabaseManager import DatabaseManager

#from utils.Database.databaseConfig import devices_list

from Database.Devices import LysimeterDevice





def main():
    #dbManager = DatabaseManager()
    #conn = dbManager.connect_database()
    #tableExist = dbManager.checkTable_existence("plant_metrics")
    #print(tableExist)

    #check_table(db_conn, "plants_metrics")

    URB_Lysimeter = LysimeterDevice("URB")
    URB_Lysimeter.add_tension_sensor(None, "outside", False)
    URB_Lysimeter.add_tension_sensor(None, "inside", False)
    URB_Lysimeter.add_tension_sensor(None, "outside", True)
    
    URB_Lysimeter.add_level_sensor()
    URB_Lysimeter.add_level_sensor(True)

    URB_Lysimeter.add_percolation_sensor()

    URB_Lysimeter.add_discharge_sensor(1)

    URB_Lysimeter.add_vacuum_sensor(None)

    URB_Lysimeter.add_temperature_control_sensor()
    URB_Lysimeter.add_temperature_sensor(None, "inside")
    URB_Lysimeter.add_temperature_sensor(None, "outside")

    print("DEVICE:", URB_Lysimeter.device_code)
    print("SENSORS:")
    for s in URB_Lysimeter.sensors:
        print(s)
    

    URB_Lysimeter.mqtt_publisher.send("device/URB", "Hello world")



if __name__ == "__main__":
    main()
