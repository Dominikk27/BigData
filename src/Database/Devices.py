import json
import time
import pandas as pd

from MQTT.Publisher import MQTTPublisher
from Database.Dataset import DeviceDataset


#===========================================
# DEVICES
#===========================================
class Device:
    def __init__(self, device_name, device_type):
        self.device_code = device_name
        self.device_type = device_type
        self.sensors = []

        self.db_id = None

        #self.dataset = DeviceDataset(self.device_code)

        self.mqtt_publisher = MQTTPublisher()


    ###############################################
    ######### DATABASE INSERTION METHODS ##########
    ###############################################
    def sync_database(self, dbManager):
        
        self.db_id = dbManager.register_device(
            device_code=self.device_code,
            device_type=self.device_type
        )

        for sensor in self.sensors:
            sensor_id = dbManager.register_sensor(
                device_id=self.db_id,
                sensor_code=sensor["sensor_code"],
                measurement_type=sensor["measurement_type"],
                unit=sensor["unit"],
                depth_cm=sensor.get("depth_cm"),
                extras=sensor.get("extras") 
            )

            sensor['db_id'] = sensor_id
        
    """ 
    def start_simulation(self):
        print(f"[SIM START] {self.device_code}")

        for row in self.dataset.iter_rows():

            payload = {
                "device_id": self.device_code,
                "device_type": self.device_type,
                "sensor_code": row["sensor_type"],
                "timestamp": row["timestamp"].isoformat(),
                "value": round(float(row["value"]), 2),
            }

            # voliteľné polia
            if "depth_cm" in row and not pd.isna(row["depth_cm"]):
                payload["depth_cm"] = int(row["depth_cm"])

            if "sensor_index" in row and not pd.isna(row["sensor_index"]):
                payload["sensor_index"] = int(row["sensor_index"])

            self.mqtt_publisher.send(self.topic, json.dumps(payload))

            time.sleep(1)
    """

    def _build_sensor_name(self, 
                            measurement: str,
                            **extras
                        ):
        parts = [self.device_code, self.device_type, measurement]

        if 'depth' in extras and (extras['depth'], int) is not None:
            parts.append(str(extras['depth']))
    
        if 'location' in extras and extras['location'] is not None:
            parts.append(str(extras['location']))

        if 'reference' in extras and extras['reference'] is not None:
            parts.append("ref")

        if 'index' in extras and extras['index'] is not None:
            parts.append(f"({extras['index']})")
        
        return "_".join(parts).lower()

    def _build_extras(self, depth=None, location=None, reference=None, index=None):
        extras = {}

        if isinstance(depth, int):
            extras['depth'] = depth
        
        if location:
            extras['location'] = location
        
        if reference:
            extras['reference'] = True

        if index is not None:
            extras['index'] = index
        
        return extras

    def add_sensors(
                self, 
                sensor_code,        # STR: UNIQUE IDENTIFICATOR
                measurement_type,   # STR: TYPE OF MEASURED QUANTITY
                unit,               # STR: UNIT OF MEASURED QUANTITY
                depth_cm=None,      # INT: (NONE / DEPTH OF SENSOR IN CM)
                extras=None         # DICT: (NONE / ADDITONAL METADATA)
                ):
        self.sensors.append({
            "sensor_code": sensor_code,
            "measurement_type": measurement_type,
            "unit": unit,
            "depth_cm": depth_cm,
            "extras": extras or {}
        })
    

    def start_device_simulation(self):
        self.sim_running = True
        self.mqtt_publisher.connect()

        while self.sim_running:
            for sensor in self.sensors:
                payload = sensor.read()
                payload["device_id"] = self.device_code
                self.mqtt_publisher.send(self.topic, json.dumps(payload))
            time.sleep(1)


    def stop_device_simulation(self):
        self.sim_running = False
        self.mqtt_publisher.disconnect()


#===========================================
# LYSIMETER DEVICES
#===========================================
class LysimeterDevice(Device):
    def __init__(
            self,
            device_name
        ):
        super().__init__(device_name,device_type="Lysimeter")
    
    #TENSION SENSOR
    def add_tension_sensor(
                    self, 
                    depth_cm: int | None = None, 
                    location: str | None = None,  
                    reference: bool = False ):

        measurement_type = "tension"

        extras = self._build_extras(
                                depth = depth_cm, 
                                location = location, 
                                reference = reference
                            )
        sensor_code = self._build_sensor_name(measurement_type, **extras)

        self.add_sensors(
            sensor_code=sensor_code, 
            measurement_type=measurement_type, 
            unit="kPa", 
            extras=extras
        )
    
    #TEMPERATURE SENSOR
    def add_temperature_sensor(
                    self, 
                    depth_cm: int | None = None,
                    location: str | None = None,
                    reference: bool = False
                ):

        measurement_type = "temperature"

        extras = self._build_extras(
                                depth = depth_cm, 
                                location = location, 
                                reference = reference
                            )
        sensor_code = self._build_sensor_name(measurement_type, **extras)

        self.add_sensors(
            sensor_code=sensor_code, 
            measurement_type=measurement_type, 
            unit="degC", 
            extras=extras
        )

    #ELECTRICAL CONDUCTIVITY (EC) SENSOR
    def add_ec_sensor(
                    self, 
                    depth_cm: int | None = None,
                    location: str | None = None,
                    reference: bool = False
                ):
        
        measurement_type = "ec"

        extras = self._build_extras(
                                depth = depth_cm, 
                                location = location, 
                                reference = reference
                            )
        sensor_code = self._build_sensor_name(measurement_type, **extras)

        self.add_sensors(
            sensor_code=sensor_code,
            measurement_type=measurement_type,
            unit="mS/cm",
            extras=extras
        )

    #SOIL WATER POTENTIONAL (UMP) SENSOR
    def add_ump_sensor(
                    self, 
                    depth_cm: int | None = None,
                    location: str | None = None,
                    reference: bool = False
                ):
        
        measurement_type = "ump"

        extras = self._build_extras(
                                depth = depth_cm, 
                                location = location, 
                                reference = reference
                            )
        sensor_code = self._build_sensor_name(measurement_type, **extras)

        self.add_sensors(
            sensor_code=sensor_code,
            measurement_type=measurement_type,
            unit="%",
            extras=extras
        )
    
    #BATTERY SENSOR
    def add_battery_sensor(
                        self, 
                        depth_cm: int | None = None,
                        location: str | None = None,
                        reference: bool = False
                    ):
        
        measurement_type = "battery"

        extras = self._build_extras(
                                depth = depth_cm, 
                                location = location, 
                                reference = reference
                            )
        sensor_code = self._build_sensor_name(measurement_type, **extras)


        self.add_sensors(
            sensor_code=sensor_code,
            measurement_type=measurement_type,
            unit="V",
            extras=extras
        )

    #DISCHARGE SENSOR
    def add_discharge_sensor(
                        self,
                        index: int, 
                        depth_cm: int | None = None,
                        location: str | None = None,
                        reference: bool = False
                    ):
        measurement_type = "discharge"

        extras = self._build_extras(
                                index = index,
                                depth = depth_cm, 
                                location = location, 
                                reference = reference
                            )
        sensor_code = self._build_sensor_name(measurement_type, **extras)
        self.add_sensors(
            sensor_code=sensor_code,
            measurement_type=measurement_type,
            unit="l",
            extras=extras
        )
    
    #VACUUM SENSOR
    def add_vacuum_sensor(
                        self, 
                        depth_cm: int | None = None,
                        location: str | None = None,
                        reference: bool = False
                        ):
        
        measurement_type = "vacuum"

        extras = self._build_extras(
                                depth = depth_cm, 
                                location = location, 
                                reference = reference
                            )
        sensor_code = self._build_sensor_name(measurement_type, **extras)

        self.add_sensors(
            sensor_code=sensor_code,
            measurement_type=measurement_type,
            unit="kPa",
            extras=extras
        )

    #LEVEL SENSOR 
    def add_level_sensor(
                    self, 
                    depth_cm: int | None = None,
                    location: str | None = None,
                    reference: bool = False
                ):
        
        measurement_type = "level"

        extras = self._build_extras(
                                depth = depth_cm, 
                                location = location, 
                                reference = reference
                            )
        sensor_code = self._build_sensor_name(measurement_type, **extras)
        
        self.add_sensors(
            sensor_code=sensor_code,
            measurement_type=measurement_type,
            unit="cm",
            extras=extras
        )


    #PERCOLATION PUMP SENSOR
    def add_percolation_sensor(
                        self, 
                        depth_cm: int | None = None,
                        location: str | None = None,
                        reference: bool = False
                        ):
        measurement_type = "percolation_pump"

        extras = self._build_extras(
                                depth = depth_cm, 
                                location = location, 
                                reference = reference
                            )
        sensor_code = self._build_sensor_name(measurement_type, **extras)
        self.add_sensors(
            sensor_code=sensor_code,
            measurement_type=measurement_type,
            unit="mV",
            extras=extras
        )


    #TEMPERATURE CONTROL
    def add_temperature_control_sensor(
                                self, 
                                depth_cm: int | None = None,
                                location: str | None = None,
                                reference: bool = False
                            ):
        
        measurement_type = "temperature_control"

        extras = self._build_extras(
                                depth = depth_cm, 
                                location = location, 
                                reference = reference
                            )
        sensor_code = self._build_sensor_name(measurement_type, **extras)
        self.add_sensors(
            sensor_code=sensor_code,
            measurement_type=measurement_type,
            unit="mV",
            extras=extras
        )

    #SCALE SENSOR
    def add_scale_sensor(
                    self,
                    index: int, 
                    depth_cm: int | None = None,
                    location: str | None = None,
                    reference: bool = False
                ):
        
        measurement_type = "scale"

        extras = self._build_extras(
                                index = index,
                                depth = depth_cm, 
                                location = location, 
                                reference = reference
                            )
        sensor_code = self._build_sensor_name(measurement_type, **extras)
        self.add_sensors(
            sensor_code=sensor_code,
            measurement_type=measurement_type,
            unit="kg",
            extras=extras
        )


#===========================================
# WEATHER STATION DEVICES
#===========================================
class WeatherDevice(Device):
    def __init__(
            self,
            device_name
        ):
        super().__init__(device_name,device_type="Weather")
    

    #BATTERY SENSOR
    def add_battery_sensor(
                    self, 
                    depth_cm: int | None = None,
                    location: str | None = None,
                    reference: bool = False
                ):
        measurement_type = "battery"

        extras = self._build_extras(
                                depth = depth_cm, 
                                location = location, 
                                reference = reference
                            )
        sensor_code = self._build_sensor_name(measurement_type, **extras)
        self.add_sensors(
            sensor_code=sensor_code,
            measurement_type=measurement_type,
            unit="V"
        )

    #HUMIDITY SENSOR
    def add_humidity_sensor(
                        self, 
                        depth_cm: int | None = None,
                        location: str | None = None,
                        reference: bool = False
                    ):
        measurement_type = "humidity"

        extras = self._build_extras(
                                depth = depth_cm, 
                                location = location, 
                                reference = reference
                            )
        sensor_code = self._build_sensor_name(measurement_type, **extras)
        self.add_sensors(
            sensor_code=sensor_code,
            measurement_type=measurement_type,
            unit="%"
        )
    
    #AIR PRESSURE SENSOR
    def add_air_pressure_sensor(
                            self, 
                            depth_cm: int | None = None,
                            location: str | None = None,
                            reference: bool = False
                        ):
        measurement_type = "air_pressure"

        extras = self._build_extras(
                                depth = depth_cm, 
                                location = location, 
                                reference = reference
                            )
        sensor_code = self._build_sensor_name(measurement_type, **extras)
        self.add_sensors(
            sensor_code=sensor_code,
            measurement_type=measurement_type,
            unit="hPa"
        )
    
    #UMP SENSOR
    def add_ump_sensor(
                    self,
                    measurement: str, 
                    depth_cm: int | None = None,
                    location: str | None = None,
                    reference: bool = False
        ):

        unit_map = {
            "temperature": "degC",
            "ec": "mS/cm",
            "moisture": "%"
        }

        #measurement_type = "temperature_control"

        extras = self._build_extras(
                                depth = depth_cm, 
                                location = location, 
                                reference = reference
                            )
        sensor_code = self._build_sensor_name(measurement, **extras)

        self.add_sensors(
            sensor_code=sensor_code,
            measurement_type=measurement,
            unit=unit_map[measurement]
        )

    #WIND SPEED SENSOR
    def add_wind_speed_sensor(
                        self, 
                        depth_cm: int | None = None,
                        location: str | None = None,
                        reference: bool = False
                    ):
        measurement_type = "wind_speed"

        extras = self._build_extras(
                                depth = depth_cm, 
                                location = location, 
                                reference = reference
                            )
        sensor_code = self._build_sensor_name(measurement_type, **extras)
        self.add_sensors(
            sensor_code=sensor_code,
            measurement_type=measurement_type,
            unit="m/s"
        )

    #RADIATION SENSOR
    def add_radiation_sensor(
                        self, 
                        depth_cm: int | None = None,
                        location: str | None = None,
                        reference: bool = False
                    ):
        measurement_type = "radiation"

        extras = self._build_extras(
                                depth = depth_cm, 
                                location = location, 
                                reference = reference
                            )
        sensor_code = self._build_sensor_name(measurement_type, **extras)
        self.add_sensors(
            sensor_code=sensor_code,
            measurement_type=measurement_type,
            unit="W/qm"
        )
    

    #AIR TEMPERATURE SENSOR
    def add_air_temperature_sensor(
                        self, 
                        depth_cm: int | None = None,
                        location: str | None = None,
                        reference: bool = False
                    ):
        measurement_type = "air_temperature"

        extras = self._build_extras(
                                depth = depth_cm, 
                                location = location, 
                                reference = reference
                            )
        sensor_code = self._build_sensor_name(measurement_type, **extras)
        self.add_sensors(
            sensor_code=sensor_code,
            measurement_type=measurement_type,
            unit="degC"
        )
    

    #TEMPERATURE +5 SENSOR
    def add_temperature_plus5_sensor(
                        self, 
                        depth_cm: int | None = None,
                        location: str | None = None,
                        reference: bool = False
                    ):
        measurement_type = "temperature_plus5"

        extras = self._build_extras(
                                depth = depth_cm, 
                                location = location, 
                                reference = reference
                            )
        sensor_code = self._build_sensor_name(measurement_type, **extras)
        self.add_sensors(
            sensor_code=sensor_code,
            measurement_type=measurement_type,
            unit="degC"
        )
    

    #WIND DIRECTION SENSOR
    def add_wind_direction_sensor(
                        self, 
                        depth_cm: int | None = None,
                        location: str | None = None,
                        reference: bool = False
                    ):
        measurement_type = "wind_direction"

        extras = self._build_extras(
                                depth = depth_cm, 
                                location = location, 
                                reference = reference
                            )
        sensor_code = self._build_sensor_name(measurement_type, **extras)
        self.add_sensors(
            sensor_code=sensor_code,
            measurement_type=measurement_type,
            unit="deg"
        )
    

    #PRECIPITATION SENSOR
    def add_precipitation_sensor(
                        self, 
                        depth_cm: int | None = None,
                        location: str | None = None,
                        reference: bool = False
                    ):
        measurement_type = "precipitation"

        extras = self._build_extras(
                                depth = depth_cm, 
                                location = location, 
                                reference = reference
                            )
        sensor_code = self._build_sensor_name(measurement_type, **extras)
        self.add_sensors(
            sensor_code=sensor_code,
            measurement_type=measurement_type,
            unit="mm"
        )


#===========================================
# SCHACHT DEVICES
#===========================================
class SchachtDevice(Device):
    def __init__(
            self,
            device_name
        ):
        super().__init__(device_name,device_type="Schacht")
    
    #BATTERY SENSOR
    def add_battery_sensor(
                    self, 
                    depth_cm: int | None = None,
                    location: str | None = None,
                    reference: bool = False
                ):
        
        measurement_type = "battery"

        extras = self._build_extras(
                                depth = depth_cm, 
                                location = location, 
                                reference = reference
                            )
        sensor_code = self._build_sensor_name(measurement_type, **extras)

        self.add_sensors(
            sensor_code=sensor_code,
            measurement_type=measurement_type,
            unit="V"
        )
    
    #DISCHARGE SENSOR
    def add_discharge_sensor(
                        self,
                        index: int, 
                        depth_cm: int | None = None,
                        location: str | None = None,
                        reference: bool = False
                    ):
        measurement_type = "discharge"

        extras = self._build_extras(
                                index = index,
                                depth = depth_cm, 
                                location = location, 
                                reference = reference
                            )
        sensor_code = self._build_sensor_name(measurement_type, **extras)
        self.add_sensors(
            sensor_code=sensor_code,
            measurement_type=measurement_type,
            unit="l",
            extras=extras
        )

    #EC SENSOR
    def add_ec_sensor(
                    self, 
                    index: int,
                    depth_cm: int | None = None,
                    location: str | None = None,
                    reference: bool = False
                ):
        
        measurement_type = "ec"

        extras = self._build_extras(
                                index = index,
                                depth = depth_cm, 
                                location = location, 
                                reference = reference
                            )
        sensor_code = self._build_sensor_name(measurement_type, **extras)

        self.add_sensors(
            sensor_code=sensor_code,
            measurement_type=measurement_type,
            unit="mS/cm",
            extras=extras
        )

    #TEMPERATURE SENSOR
    def add_temperature_sensor(
                    self, 
                    index: int,
                    depth_cm: int | None = None,
                    location: str | None = None,
                    reference: bool = False
                ):
        
        measurement_type = "temperature"

        extras = self._build_extras(
                                index = index,
                                depth = depth_cm, 
                                location = location, 
                                reference = reference
                            )
        sensor_code = self._build_sensor_name(measurement_type, **extras)

        self.add_sensors(
            sensor_code=sensor_code, 
            measurement_type=measurement_type, 
            unit="degC", 
            extras=extras
        )


    #VACUUM SENSOR
    def add_vacuum_sensor(
                        self, 
                        depth_cm: int | None = None,
                        location: str | None = None,
                        reference: bool = False
                        ):
        
        measurement_type = "vacuum"

        extras = self._build_extras(
                                depth = depth_cm, 
                                location = location, 
                                reference = reference
                            )
        sensor_code = self._build_sensor_name(measurement_type, **extras)

        self.add_sensors(
            sensor_code=sensor_code,
            measurement_type=measurement_type,
            unit="kPa",
            extras=extras
        )
    

    # UMP SENSOR
    def add_ump_sensor(
                    self, 
                    index: int,
                    depth_cm: int | None = None,
                    location: str | None = None,
                    reference: bool = False
                ):
        
        measurement_type = "ump"

        extras = self._build_extras(
                                index = index,
                                depth = depth_cm, 
                                location = location, 
                                reference = reference
                            )
        sensor_code = self._build_sensor_name(measurement_type, **extras)

        self.add_sensors(
            sensor_code=sensor_code,
            measurement_type=measurement_type,
            unit="%",
            extras=extras
        )
    
""" def main():
    dev = LysimeterDevice("TEST")

    dev.add_tension_sensor(depth_cm=30, location="outside")
    dev.add_temperature_sensor(depth_cm=30)
    dev.add_battery_sensor()

    print("DEVICE:", dev.device_code)
    print("SENSORS:")
    for s in dev.sensors:
        print(s) """

def mainTest():
        
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
    

    SSA3_LYSimeter = LysimeterDevice("SSA3")
    SSA3_LYSimeter.add_tension_sensor(30, None, False)
    SSA3_LYSimeter.add_tension_sensor(75, None, False)
    SSA3_LYSimeter.add_tension_sensor(120, None, False)

    SSA3_LYSimeter.add_vacuum_sensor(30, None, False)
    SSA3_LYSimeter.add_vacuum_sensor(75, None, False)
    SSA3_LYSimeter.add_vacuum_sensor(120, None, False)

    SSA3_LYSimeter.add_ump_sensor(30, None, False)
    SSA3_LYSimeter.add_ump_sensor(75, None, False)
    SSA3_LYSimeter.add_ump_sensor(120, None, False)

    SSA3_LYSimeter.add_temperature_sensor(30, None, False)
    SSA3_LYSimeter.add_temperature_sensor(75, None, False)
    SSA3_LYSimeter.add_temperature_sensor(120, None, False)
    
    SSA3_LYSimeter.add_battery_sensor(None, None, False)
    SSA3_LYSimeter.add_scale_sensor(1, None, None, False)

    SSA3_LYSimeter.add_ec_sensor(30, None, False)
    SSA3_LYSimeter.add_ec_sensor(75, None, False)
    SSA3_LYSimeter.add_ec_sensor(120, None, False)



    """ lys = LysimeterDevice("Test")
    lys.add_tension_sensor("tension", 30, "Outside", True) """

    print (URB_Lysimeter.device_code)

    for sensor in URB_Lysimeter.sensors:
        print(sensor)

    print ("---------------------")
    print (SSA3_LYSimeter.device_code)

    for sensor in SSA3_LYSimeter.sensors:
        print(sensor)




""" if __name__ == "__main__":
    main() """