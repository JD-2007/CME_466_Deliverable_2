import sys
from datetime import datetime

import paho.mqtt.client as mqtt
from cryptography.fernet import Fernet
from paho.mqtt import client as mqtt_client

broker = "broker.hivemq.com"
# broker = "test.mosquitto.org"
# broker = "broker.emqx.io"
# broker = "mqtt.eclipseprojects.io"

port = 1883
topic = "python/mqtt"

cipher_key = 'aeFXbGYE7ng_wvnaq9IOOqBM6S6Q45_Jo0bHmSHWYYs='
cipher = Fernet(cipher_key)

l = []


def connect_mqtt():
    def on_connect(a, b, c, d, e):
        print("Connected to MQTT Broker!")

    client = mqtt_client.Client(mqtt.CallbackAPIVersion.VERSION2)

    client.on_connect = on_connect
    client.connect(broker, port)
    return client


def subscribe(client: mqtt_client):
    def on_message(client, user_data, message):
        b = message.payload.decode('utf-8')
        decrypted_msg = cipher.decrypt(b)
        curr_time = datetime.timestamp(datetime.now())

        l.append(curr_time - float(decrypted_msg))
        # l.append(curr_time - float(b))

        if len(l) == 40:
            print("Latency is", sum(l) / len(l))
            client.disconnect()
            sys.exit()

    client.subscribe(topic)
    client.on_message = on_message


def run():
    client = connect_mqtt()
    subscribe(client)
    client.loop_forever()


if __name__ == '__main__':
    run()
