import threading
import time

import paho.mqtt.client as mqtt
from cryptography.fernet import Fernet
from paho.mqtt import client as mqtt_client

broker = "broker.emqx.io"
port = 1883
cipher_key = 'aeFXbGYE7ng_wvnaq9IOOqBM6S6Q45_Jo0bHmSHWYYs='
cipher = Fernet(cipher_key)

status_topic = "edge_node_status"
command_topic = "commands"


edge_node_stat: dict = {"sys": "on", "led": "off", "dist_sens": "on", "switch": "off"}

command = "led on"


# noinspection PyArgumentList
def connect_mqtt(clientid: str):
    client = mqtt_client.Client(mqtt.CallbackAPIVersion.VERSION2, client_id=clientid)
    client.connect(broker, port)
    return client


def publish(client):
    while True:
        time.sleep(1)
        message = str(edge_node_stat)
        out_message = message.encode()
        encrypted_message = cipher.encrypt(out_message)
        result = client.publish(topic=status_topic, payload=encrypted_message)
        status = result[0]
        if status != 0:
            print("Error publishing status: " + str(status))
            continue


def subscribe(client: mqtt_client, topic: str):
    def on_message(client, user_data, message):
        global edge_node_stat
        b = message.payload.decode('utf-8')
        decrypted_msg = cipher.decrypt(b).decode()
        print("Recieved command: ", decrypted_msg)
        command_split = decrypted_msg.split(" ")
        edge_node_stat[command_split[0].lower()] = command_split[1].lower()

    client.subscribe(topic)
    client.on_message = on_message


def init_pub(clientid: str):
    client = connect_mqtt(clientid)
    client.loop_start()
    publish(client)


def init_sub(clientid: str):
    client = connect_mqtt(clientid)
    subscribe(client, command_topic)
    client.loop_forever()


if __name__ == '__main__':
    publisher_id = "pi_connect_pub"
    subcriber_id = "pi_connect_sub"

    sub_thread = threading.Thread(target=init_sub, args=(subcriber_id,))
    pub_thread = threading.Thread(target=init_pub, args=(publisher_id,))

    sub_thread.start()
    pub_thread.start()
    sub_thread.join()
    pub_thread.join()