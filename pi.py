import threading
import time
from time import sleep

import paho.mqtt.client as mqtt
from cryptography.fernet import Fernet
from gpiozero import DistanceSensor, LED, Button
from paho.mqtt import client as mqtt_client

sensor = DistanceSensor(22, 17,max_distance=50, threshold_distance = 0.5)
led = LED(18)
button = Button(19)

def sys_op():
    global edge_node_stat
    edge_node_stat["dist_sens"] = sensor.distance
    if sensor.in_range and edge_node_stat["warn"] == "on":
        led.toggle() # Blink LED
    sleep(1) # Wait 1 second


broker = "broker.emqx.io"
port = 1883
cipher_key = 'aeFXbGYE7ng_wvnaq9IOOqBM6S6Q45_Jo0bHmSHWYYs='
cipher = Fernet(cipher_key)

status_topic = "edge_node_status"
command_topic = "commands"


edge_node_stat: dict = {"sys": "off", "led": "off", "dist_sens": "off","warn": "on", "switch": "off"}

command = "led on"

def sys_set():
    global edge_node_stat
    while True:
        if button.is_active:
            edge_node_stat["switch"] = "on"
        else:
            edge_node_stat["switch"] = "off"

        if edge_node_stat["switch"] == "on":
            edge_node_stat["sys"] = "on"

        if edge_node_stat["led"] == "on":
            led.on()
        else:
            led.off()

        if edge_node_stat["sys"] == "on":
            sys_op()
        else:
            for i in edge_node_stat.keys():
                if i != "switch":
                    edge_node_stat[i] = "off"



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
        print("Received command: ", decrypted_msg)
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