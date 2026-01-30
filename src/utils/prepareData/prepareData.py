"""
import pandas as pd
import glob
from pathlib import Path
import re

INPUT_DIR = "../../../dataset/rawData"
OUTPUT_DIR = Path("../../../dataset/preparedData")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

files = glob.glob(f"{INPUT_DIR}/*.xls") + glob.glob(f"{INPUT_DIR}/*.xlsx")

if not files:
    raise RuntimeError("No input files found")

# =========================
# LOAD + MERGE
# =========================
df_list = []
for f in files:
    df = pd.read_csv(f, engine="python", sep="\t", on_bad_lines="skip")
    print(f"Loaded {f}")
    df_list.append(df)

combined_df = pd.concat(df_list, ignore_index=True)
print("All files combined")

rows = []
cols = combined_df.columns.tolist()

for _, row in combined_df.iterrows():
    for i in range(0, len(cols), 3):
        if i + 2 >= len(cols):
            continue

        rows.append({
            "timestamp": row[cols[i]],
            "measurement_name": cols[i+1],
            "value": row[cols[i+1]],
            "status": row[cols[i+2]]
        })

df = pd.DataFrame(rows)

# =========================
# PARSING HELPERS
# =========================
def get_device(name: str):
    n = name.lower()
    if "urb" in n:
        return "URB"
    if "ssa 3" in n:
        return "SSA3"
    if "ssa 4" in n:
        return "SSA4"
    if "weather" in n:
        return "WEATHER"
    return None


def parse_sensor(name: str):
    text = name.lower()

    idx = re.search(r"\((\d+)\)", text)
    depth = re.search(r"(\d+)\s*cm", text)

    if "ump" in text:
        sensor = "UMP"
    elif "vacuum" in text:
        sensor = "VACUUM"
    elif "temperature" in text:
        sensor = "TEMPERATURE"
    elif "ec" in text:
        sensor = "EC"
    else:
        sensor = None

    return pd.Series({
        "sensor_type": sensor,
        "sensor_index": int(idx.group(1)) if idx else None,
        "depth_cm": int(depth.group(1)) if depth else None
    })

# =========================
# APPLY PARSING
# =========================
df["device_id"] = df["measurement_name"].apply(get_device)

sensor_cols = df["measurement_name"].apply(parse_sensor)
df = pd.concat([df, sensor_cols], axis=1)

# =========================
# CLEAN DATA
# =========================
df["timestamp"] = pd.to_datetime(
    df["timestamp"],
    format="%d.%m.%Y %H:%M",
    errors="coerce"
)

df["value"] = (
    df["value"]
    .astype(str)
    .str.strip()
    .replace("", pd.NA)
    .str.replace(",", ".", regex=False)
)
df["value"] = pd.to_numeric(df["value"], errors="coerce")

df = df.dropna(subset=["timestamp", "value", "device_id", "sensor_type"])

df["sensor_index"] = df["sensor_index"].astype("Int64")
df["depth_cm"] = df["depth_cm"].astype("Int64")

# =========================
# FINAL STREAM SCHEMA
# =========================
final_cols = [
    "timestamp",
    "device_id",
    "sensor_type",
    "sensor_index",
    "depth_cm",
    "value",
    "status"
]

df = df[final_cols]

# =========================
# SPLIT PER DEVICE
# =========================
for device, ddf in df.groupby("device_id"):
    out = OUTPUT_DIR / f"{device.lower()}_stream.csv"
    ddf.sort_values("timestamp").to_csv(out, index=False)
    print(f"Saved {out}")

print("✅ Data prepared for streaming")
"""



import pandas as pd
import glob
from pathlib import Path
import re

INPUT_DIR = "../../../dataset/rawData"
OUTPUT_DIR = Path("../../../dataset/preparedData")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

files = glob.glob(f"{INPUT_DIR}/*.xls") + glob.glob(f"{INPUT_DIR}/*.xlsx")

if not files:
    raise RuntimeError("No input files found")

# =========================
# LOAD + MERGE
# =========================
df_list = []
for f in files:
    df = pd.read_csv(f, engine="python", sep="\t", on_bad_lines="skip")
    print(f"Loaded {f}")
    df_list.append(df)

combined_df = pd.concat(df_list, ignore_index=True)
print("All files combined")

rows = []
cols = combined_df.columns.tolist()

for _, row in combined_df.iterrows():
    for i in range(0, len(cols), 3):
        if i + 2 >= len(cols):
            continue

        rows.append({
            "timestamp": row[cols[i]],
            "sensor_label": cols[i + 1],   # RAW NAME ZACHOVANÝ
            "value": row[cols[i + 1]],
            "status": row[cols[i + 2]]
        })

df = pd.DataFrame(rows)

# =========================
# PARSING HELPERS
# =========================
def get_device(name: str):
    n = name.lower()
    if "urb" in n:
        return "URB", "Lysimeter"
    if "ssa 3" in n:
        return "SSA3", "Lysimeter"
    if "ssa 4" in n:
        return "SSA4", "Schacht"
    if "weather" in n:
        return "WEATHER", "Weather"
    return None, None


UNIT_MAP = {
    "temperature": "degc",
    "vacuum": "kpa",
    "ec": "mscm",
    "ump": "pct",
    "battery": "v",
    "level": "cm",
}


def parse_sensor(name: str):
    text = name.lower()

    # INDEX: (1), (2), (3), (5+)
    idx = re.search(r"\((\d+)", text)

    # DEPTH
    depth = re.search(r"(\d+)\s*cm", text)

    # LOCATION
    location = None
    if "outside" in text:
        location = "outside"
    elif "inside" in text:
        location = "inside"

    # REFERENCE
    reference = "reference" in text

    # MEASUREMENT TYPE
    if "air temperature" in text:
        measurement = "air_temperature"
    elif "temperature" in text:
        measurement = "temperature"
    elif "tension" in text:
        measurement = "tension"
    elif "vacuum" in text:
        measurement = "vacuum"
    elif "humidity" in text:
        measurement = "humidity"
    elif "air pressure" in text:
        measurement = "air_pressure"
    elif "wind speed" in text:
        measurement = "wind_speed"
    elif "wind direction" in text:
        measurement = "wind_direction"
    elif "radiation" in text:
        measurement = "radiation"
    elif "precipitation" in text:
        measurement = "precipitation"
    elif "ec" in text:
        measurement = "ec"
    elif "ump" in text:
        measurement = "ump"
    elif "battery" in text:
        measurement = "battery"
    elif "level" in text:
        measurement = "level"
    elif "discharge" in text:
        measurement = "discharge"
    elif "scale" in text:
        measurement = "scale"
    elif "percolation pump" in text:
        measurement = "percolation_pump"
    elif "temperature control" in text:
        measurement = "temperature_control"
    else:
        measurement = None

    unit = UNIT_MAP.get(measurement)

    return pd.Series({
        "measurement_type": measurement,
        "unit": unit,
        "sensor_index": int(idx.group(1)) if idx else None,
        "depth_cm": int(depth.group(1)) if depth else None,
        "location": location,
        "reference": reference
    })



def build_sensor_code(row):
    parts = [
        row.device_id.lower(),
        row.device_type.lower(),
        row.measurement_type
    ]

    if row.location:
        parts.append(row.location)

    if pd.notna(row.depth_cm):
        parts.append(f"{int(row.depth_cm)}cm")

    if pd.notna(row.sensor_index):
        parts.append(f"i{int(row.sensor_index)}")

    if row.reference:
        parts.append("ref")

    if row.unit:
        parts.append(row.unit)

    return "_".join(parts)

# =========================
# APPLY PARSING
# =========================
df[["device_id", "device_type"]] = df["sensor_label"].apply(
    lambda x: pd.Series(get_device(x))
)

sensor_cols = df["sensor_label"].apply(parse_sensor)
df = pd.concat([df, sensor_cols], axis=1)

# =========================
# CLEAN DATA
# =========================
df["timestamp"] = pd.to_datetime(
    df["timestamp"],
    format="%d.%m.%Y %H:%M",
    errors="coerce"
)

df["value"] = (
    df["value"]
    .astype(str)
    .str.strip()
    .replace("", pd.NA)
    .str.replace(",", ".", regex=False)
)
df["value"] = pd.to_numeric(df["value"], errors="coerce")

df = df.dropna(subset=[
    "timestamp",
    "value",
    "device_id",
    "measurement_type"
])

df["sensor_index"] = df["sensor_index"].astype("Int64")
df["depth_cm"] = df["depth_cm"].astype("Int64")

# =========================
# SENSOR CODE
# =========================
df["sensor_code"] = df.apply(build_sensor_code, axis=1)

# =========================
# FINAL STREAM SCHEMA
# =========================
final_cols = [
    "timestamp",
    "device_id",
    "device_type",
    "sensor_code",
    "sensor_label",
    "measurement_type",
    "unit",
    "location",
    "depth_cm",
    "sensor_index",
    "reference",
    "value",
    "status"
]

df = df[final_cols]

# =========================
# SPLIT PER DEVICE
# =========================
for device, ddf in df.groupby("device_id"):
    out = OUTPUT_DIR / f"{device.lower()}_stream.csv"
    ddf.sort_values("timestamp").to_csv(out, index=False)
    print(f"Saved {out}")

print("✅ Data prepared for streaming")
