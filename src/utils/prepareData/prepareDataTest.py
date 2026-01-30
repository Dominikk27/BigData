import pandas as pd
import re
import glob
import os
import json


INPUT_DIR = "../../../dataset/rawData"
OUTPUT_DIR = "../../../dataset/preparedData"

os.makedirs(OUTPUT_DIR, exist_ok=True)

def parse_sensor_metadata(col_name):
    """Extrahuje metadáta z názvu stĺpca."""
    s = str(col_name).replace('\n', ' ').strip()
    s = re.sub(r'\s+', ' ', s)

    full_dev_name = s

    unit_pattern = r'(W/qm|m/s|mS/cm|degC|kPa|cm|mV|l|%|V|kg|hPa|mm|deg)$'
    unit_match = re.search(unit_pattern, s)
    unit = unit_match.group(1) if unit_match else None

    depth = None
    depth_match = re.search(r'(\d+)cm', s)
    if depth_match:
        depth = int(depth_match.group(1))
    elif "(5+)" in s:
        depth = 5

    index_match = re.search(r'\((\d+)\)', s)
    index = int(index_match.group(1)) if index_match else None

    is_ref = "reference" in s.lower() or "ref" in s.lower()

    clean_name = s
    prefixes = ["URB Lysimeter", "SSA 4 Schacht", "SSA 3 Lysimeter", "Weather Station"]
    for p in prefixes:
        clean_name = clean_name.replace(p, "")
    
    if unit: clean_name = clean_name.replace(unit, "")
    if depth and not "(5+)" in s: clean_name = clean_name.replace(f"{depth}cm", "")
    if index: clean_name = clean_name.replace(f"({index})", "")
    clean_name = clean_name.replace("(5+)", "").replace("reference", "").replace("ref", "")
    
    clean_name = clean_name.strip().lower()
    clean_name = re.sub(r'[^a-z0-9]+', '_', clean_name).strip('_')

    return {
        "sensor_key": clean_name,
        "full_device_name": full_dev_name,
        "unit": unit,
        "depth_cm": depth,
        "index": index,
        "is_ref": is_ref
    }

def transform_value(val):
    if pd.isna(val): return 0.00
    if isinstance(val, str):
        val = val.replace(",", ".")
        try:
            return float(val)
        except:
            return 0.00
    return float(val)

def process_file(file_path, output_dir):
    print(f"Spracovávam: {file_path}")
    try:
        if file_path.endswith('.xlsx'):
            df = pd.read_excel(file_path, engine='openpyxl')
        else:
            try:
                df = pd.read_excel(file_path, engine='xlrd')
            except:
                df = pd.read_csv(file_path, sep="\t", engine='python', on_bad_lines='skip')

    except Exception as e:
        print(f"  !! KRITICKÁ CHYBA: Súbor sa nepodarilo načítať: {e}")
        return

    if df is None or df.empty:
        return

    device_keys = ["URB", "SSA_4", "SSA_3", "WeatherStation"]
    
    handles = {
        dev: open(os.path.join(output_dir, f"{dev.lower()}.jsonl"), 'a', encoding='utf-8')
        for dev in device_keys
    }

    try:
        cols = df.columns.tolist()
        time_col = cols[0] 

        for _, row in df.iterrows():
            ts_val = row[time_col]
            ts_obj = pd.to_datetime(ts_val, dayfirst=True, errors='coerce')
            if pd.isna(ts_obj): continue
            ts_iso = ts_obj.isoformat()

            for i in range(1, len(cols)):
                col_name = str(cols[i])

                if "Status" in col_name:
                    continue

                status_val = None
                if i + 1 < len(cols) and "Status" in str(cols[i+1]):
                    status_val = row[cols[i+1]]
                dev_id = None
                if "URB" in col_name: dev_id = "URB"
                elif "SSA 4" in col_name: dev_id = "SSA_4"
                elif "SSA 3" in col_name: dev_id = "SSA_3"
                elif "Weather Station" in col_name: dev_id = "WeatherStation"

                if dev_id:
                    meta = parse_sensor_metadata(col_name)
                    val = transform_value(row[cols[i]])

                    event = {
                        "timestamp": ts_iso,
                        "full_device_name": meta["full_device_name"],
                        "device_id": dev_id,
                        "sensor": meta["sensor_key"],
                        "value": val,
                        "unit": meta["unit"],
                        "depth_cm": meta["depth_cm"],
                        "index": meta["index"],
                        "status": str(status_val) if not pd.isna(status_val) else None,
                        "is_ref": meta["is_ref"]
                    }
                    
                    handles[dev_id].write(json.dumps(event, ensure_ascii=False) + '\n')

    finally:
        for h in handles.values():
            h.close()


files = glob.glob(os.path.join(INPUT_DIR, "*.xls")) + glob.glob(os.path.join(INPUT_DIR, "*.xlsx"))

for file in files:
    process_file(file, OUTPUT_DIR)

print("Done!")