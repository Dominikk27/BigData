import paho.mqtt.client as mqtt

def on_connect(client, userdata, flags, rc, properties):
    print(f"Connected with result code {rc}")

    client.subscribe("test/topic")

def on_message(client, userdata, msg):
    print(f"{msg.topic} {msg.payload}")

    mgttc = mqtt.Client("MQTT_Client_1")
    mgttc.on_connect = on_connect
    mgttc.on_message = on_message

    mgttc.connect("broker.emqx.io", 1883, 60)