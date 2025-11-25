import json
import threading
import time

from MQTT import MQTTPublisher
from utils.ReadFile import read_csv

#
def device_sim(device_id, rows, BROKER, TOPIC, stop_sim):
    publisher = MQTTPublisher(BROKER, [TOPIC])
    publisher.on_connect()

    for ID, row in rows.iterrows():

        if stop_sim.is_set():
            print(f"[DEVICE {device_id}] Stopping...")
            return

        data = row.to_dict()
        data = {k: round(v, 2) if isinstance(v, float) 
                else v for k, v in data.items()
                }
        
        data["device_id"] = device_id
        json_data = json.dumps(data)

        try:
            publisher.send(TOPIC, json_data)
        except Exception as e:
            print(f"[DEVICE {device_id}] Publisher error!")
            raise e

        time.sleep(1)

def start_IoT_simulation(DEVICE_COUNT, BROKER, TOPIC, DATASET_PATH):
    BROKER = BROKER
    TOPIC = TOPIC
    csvData = read_csv(DATASET_PATH)

    DEVICE_COUNT = DEVICE_COUNT

    stop_sim = threading.Event()

    split_data = [
        csvData.iloc[i::DEVICE_COUNT]
        for i in range(DEVICE_COUNT)
    ]

    threads = []

    for deviceID in range(DEVICE_COUNT):
        t = threading.Thread(
            target=device_sim,
            args=(
                deviceID, 
                split_data[deviceID], 
                BROKER, 
                TOPIC,
                stop_sim
                )
        )
        threads.append(t)
        t.start()

    try:
        while any(t.is_alive() for t in threads):
            for t in threads:
                t.join(timeout=0.2)

    except KeyboardInterrupt:
        print("CLOSING APP!")
        stop_sim.set()
        for t in threads:
            t.join()
        
    print("All Devices finished jobs")

def main():
    start_IoT_simulation(
        5, 
        "host.docker.internal", 
        "test/topic", 
        "../dataset/agroDataset.csv"
    )


if __name__ == "__main__":
    main()