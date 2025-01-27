import threading

import paho.mqtt.client as mqtt
from cryptography.fernet import Fernet
from paho.mqtt import client as mqtt_client

broker = "broker.emqx.io"
port = 1883
cipher_key = 'aeFXbGYE7ng_wvnaq9IOOqBM6S6Q45_Jo0bHmSHWYYs='
cipher = Fernet(cipher_key)

status_topic = "edge_node_status"
command_topic = "commands"

command = ""



sys_parts = ["sys", "led", "switch", "dist_sens"]
sys_states = ["on", "off"]

# noinspection PyArgumentList
def connect_mqtt(clientid: str):
    client = mqtt_client.Client(mqtt.CallbackAPIVersion.VERSION2, client_id=clientid)
    client.connect(broker, port)
    return client


def publish(client):
    global command
    while True:
        command = input("Enter command: ")
        if command == "exit":
            break
        elif len(command.split()) != 2:
            print("Invalid command")
            continue
        elif command.split()[0].lower() not in sys_parts:
            print("Invalid command")
            continue
        elif command.split()[1].lower() not in sys_states:
            print("Invalid command")
            continue
        elif command.split()[0].lower() == "switch":
            print("Invalid command: Cannot output to an input device")
            continue
        out_message = command.encode()
        encrypted_message = cipher.encrypt(out_message)
        result = client.publish(topic=command_topic, payload=encrypted_message)
        status = result[0]
        if status != 0:
            print(f"Failed to send message to topic {command_topic}")


def subscribe(client: mqtt_client, topic: str):
    def on_message(client, user_data, message):
        stat_log = open("status_log.txt", "a")
        global edge_node_stat
        log_entry = message.payload.decode('utf-8')
        decrypted_msg = cipher.decrypt(log_entry).decode()
        stat_log.write("Current status" + decrypted_msg + "\n")
        stat_log.flush()
        stat_log.close()

    client.subscribe(topic)
    client.on_message = on_message


def init_pub(clientid: str):
    client = connect_mqtt(clientid)
    client.loop_start()
    publish(client)


def init_sub(clientid: str):
    client = connect_mqtt(clientid)
    subscribe(client, status_topic)
    client.loop_forever()


if __name__ == '__main__':
    publisher_id = "pc_connect_pub"
    subcriber_id = "pc_connect_sub"

    sub_thread = threading.Thread(target=init_sub, args=(subcriber_id,))
    pub_thread = threading.Thread(target=init_pub, args=(publisher_id,))

    sub_thread.start()
    pub_thread.start()
    sub_thread.join()
    pub_thread.join()
