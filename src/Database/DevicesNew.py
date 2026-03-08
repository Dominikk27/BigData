class Device:
    
    UNIT_MAP = {
        "tension": "kPa",
        "vacuum": "kPa",
        "level": "cm",
        "percolation_pump": "mV",
        "temperature_control": "mV",
        "temperature": "degC",
        "discharge": "l", 
        "ec": "mS/cm",
        "ump": "%",
        "battery": "V",
        "scale": "kg",
        "humidity": "%",
        "air_pressure": "hPa",
        "wind_speed": "m/s",
        "wind_direction": "deg",     
        "radiation": "W/qm",
        "air_temperature": "degC",
        "precipitation": "mm",
        "temperature_plus5": "degC"
    }
    
    def __init__(self, device_name, device_type):
        self.device_code = device_name
        self.device_type = device_type
        self.sensors = []

        self.db_id = None

        #self.dataset = DeviceDataset(self.device_code)

    def add_sensor(self, m_type, depth_cm=None, location=None, reference=False, index=None):
        unit = self.UNIT_MAP.get(m_type, "unknown")

        parts = [self.device_code, self.device_type, m_type]
        if depth_cm:
            parts.append(str(depth_cm))
        if location:
            parts.append(location)
        if reference:
            parts.append("ref")
        sensor_code = "_".join(parts).lower()

        self.sensors.append({
            "sensor_code": sensor_code,
            "measurement_type": m_type,
            "unit": unit,
            "depth_cm": depth_cm,
            "location": location,
            "index": index,
            "reference": reference
        })
        
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
                depth_cm=sensor.get("depth_cm", None),
                location=sensor.get("location", None),
                index=sensor.get("index", None),
                reference=sensor.get("reference", False)
            )

            sensor['db_id'] = sensor_id
    

class URBLysimeter(Device):
    def __init__(self):
        super().__init__("URB", "Lysimeter")
        self.add_sensor("tension", location="outside")
        self.add_sensor("tension", location="inside")
        self.add_sensor("tension", location="outside", reference=True)
        self.add_sensor("vacuum")
        self.add_sensor("level")
        self.add_sensor("level", reference=True)
        self.add_sensor("percolation_pump")
        self.add_sensor("temperature_control")
        self.add_sensor("temperature", location="outside")
        self.add_sensor("temperature", location="inside")
        self.add_sensor("discharge", index=1)


class SSA3Lysimeter(Device):
    def __init__(self):
        super().__init__("SSA3", "Lysimeter")
        for depth in [30, 75, 120]:
            self.add_sensor("tension", depth_cm=depth)
            self.add_sensor("vacuum", depth_cm=depth)
            self.add_sensor("ump", depth_cm=depth)
            self.add_sensor("temperature", depth_cm=depth)
            self.add_sensor("ec", depth_cm=depth)
        self.add_sensor("battery")
        self.add_sensor("scale", index=1)

class SSA4Schacht(Device):
    def __init__(self):
        super().__init__("SSA4", "Schacht")
        for depth in [30, 75, 120]:
            for i in [1, 2, 3]:
                self.add_sensor("ump", depth_cm=depth, index=i)
                self.add_sensor("temperature", depth_cm=depth, index=i)
                self.add_sensor("ec", depth_cm=depth, index=i)
        for depth in [30, 75, 120, 140]:
            self.add_sensor("vacuum", depth_cm=depth)
        for i in [1, 2, 3]: 
            self.add_sensor("discharge", index=i)
        self.add_sensor("battery")

class WeatherStation(Device):
    def __init__(self):
        super().__init__("WeatherStation", "Weather")
        self.add_sensor("battery")
        self.add_sensor("ump", index=1)
        self.add_sensor("ec", index=1)
        self.add_sensor("temperature", index=1)
        self.add_sensor("humidity")
        self.add_sensor("air_pressure")
        self.add_sensor("wind_speed")
        self.add_sensor("radiation")
        self.add_sensor("air_temperature")
        self.add_sensor("temperature_plus5")
        self.add_sensor("wind_direction")
        self.add_sensor("precipitation")