import pandas as pd


df = pd.read_csv("../../../dataset/2019-03.csv", sep=";")

cols = df.columns


print("Columns:")
for i, col in enumerate(df.columns):
    print(i, repr(col))

sensors = [col for col in cols if "Datum+Uhrzeit" not in col and "Status" not in col]
devices = {}

for col in sensors:
    words = col.split()
    if len(words) < 2:
        continue  
    
    if words[0] == "SSA" and words[1].isdigit():
        deviceName = " ".join(words[:3])  # SSA 3 
    elif words[0] == "URB":
        deviceName = " ".join(words[:2])  # URB 
    elif words[0] == "Weather":
        deviceName = " ".join(words[:2])  # Weather
    else:
        deviceName = words[0]
    if deviceName not in devices:
        devices[deviceName] = []
    devices[deviceName].append(col)

print(f"Num of devices: {len(devices)}")
for device, sensors in devices.items():
    print(f"{device}: {len(sensors)} sensors")