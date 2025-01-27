import time
from datetime import datetime

import paho.mqtt.client as mqtt
from cryptography.fernet import Fernet
from paho.mqtt import client as mqtt_client

# broker = "broker.hivemq.com"
# broker = "test.mosquitto.org"
broker = "broker.emqx.io"
# broker = "iot.coreflux.cloud"


port = 1883
topic = "python/mqtt"

cipher_key = 'aeFXbGYE7ng_wvnaq9IOOqBM6S6Q45_Jo0bHmSHWYYs='
cipher = Fernet(cipher_key)


def connect_mqtt():
    def on_connect(a, b, c, d, e):
        print("Connected to MQTT Broker!")

    client = mqtt_client.Client(mqtt.CallbackAPIVersion.VERSION2)

    client.on_connect = on_connect
    client.connect(broker, port)
    return client


def publish(client):
    msg_count = 1
    while True:
        time.sleep(0.2)
        actual_message = str(datetime.timestamp(datetime.now()))
        out_message = actual_message.encode()
        encrypted_message = cipher.encrypt(out_message)
        result = client.publish(topic=topic, payload=encrypted_message)
        status = result[0]
        if status != 0:
            print(f"Failed to send message to topic {topic}")
        else:
            msg_count += 1
        if msg_count > 40:
            break


def run():
    client = connect_mqtt()
    client.loop_start()
    publish(client)
    client.loop_stop()
    client.disconnect()


if __name__ == '__main__':
    run()