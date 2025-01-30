import sqlite3
import json
import paho.mqtt.client as mqtt
from datetime import datetime
import base64
import time
import os
import argparse
import logging
from queue import Queue
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# Argument parser setup
def get_args():
    parser = argparse.ArgumentParser(description="MQTT to SQLite and JSON Logger")
    parser.add_argument('--host', default=os.getenv('MQTT_BROKER', 'localhost'), help='MQTT broker host')
    parser.add_argument('--port', type=int, default=int(os.getenv('MQTT_PORT', 1883)), help='MQTT broker port')
    parser.add_argument('--username', default=os.getenv('MQTT_USERNAME', None), help='MQTT username')
    parser.add_argument('--password', default=os.getenv('MQTT_PASSWORD', None), help='MQTT password')
    parser.add_argument('--prefix', default=os.getenv('LOG_PREFIX', 'logs_'), help='Prefix for log filenames')
    
    current_time = datetime.now().strftime('%Y-%m-%d_%H-%M-%S')
    parser.add_argument('--db', default=os.getenv('SQLITE_DB', f'{os.getenv("LOG_PREFIX", "logs_")}{current_time}.db'), help='SQLite database file')
    parser.add_argument('--json', default=os.getenv('JSON_FILE', f'{os.getenv("LOG_PREFIX", "logs_")}{current_time}.json'), help='JSON file for storing messages')
    return parser.parse_args()

args = get_args()
message_queue = Queue()

# SQLite Database Setup
def setup_sqlite():
    conn = sqlite3.connect(args.db)
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS mqtt_messages (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            topic TEXT NOT NULL,
            payload TEXT NOT NULL,
            datetime TEXT NOT NULL,
            qos INTEGER NOT NULL,
            retain INTEGER NOT NULL
        )
    ''')
    conn.commit()
    return conn, cursor

# Batch insert messages into SQLite
def save_to_sqlite_batch(cursor, conn, messages):
    try:
        cursor.executemany('''
            INSERT INTO mqtt_messages (topic, payload, datetime, qos, retain)
            VALUES (?, ?, ?, ?, ?)
        ''', messages)
        conn.commit()
    except Exception as e:
        logger.error(f"Error saving to SQLite: {e}")

# Save message to JSON file
def save_to_json(topic, payload, datetime, qos, retain):
    try:
        message = {
            "topic": topic,
            "payload": payload,
            "datetime": datetime,
            "qos": qos,
            "retain": retain
        }
        with open(args.json, 'a') as f:
            f.write(json.dumps(message) + '\n')
    except Exception as e:
        logger.error(f"Error saving to JSON file: {e}")

# MQTT Callback when a message is received
def on_message(client, userdata, msg):
    topic = msg.topic
    try:
        payload = msg.payload.decode('utf-8')
    except UnicodeDecodeError:
        payload = base64.b64encode(msg.payload).decode('utf-8')
        logger.warning(f"Non-UTF-8 payload detected on topic: {topic}. Stored as base64.")
    qos = msg.qos
    retain = msg.retain
    logger.info(f"Received message: {payload} on topic: {topic}")
    message_queue.put((topic, payload, datetime.now().strftime('%Y-%m-%d %H:%M:%S'), qos, retain))

# MQTT Callback for connection
def on_connect(client, userdata, connect_flags, reason_code, properties):
    if reason_code == 0:
        logger.info("Connected to MQTT broker")
        client.subscribe("#")
    else:
        logger.error(f"Connection failed with code {reason_code}")

# MQTT Callback for disconnection
def on_disconnect(client, userdata, disconnect_flags, reason_code, properties):
    logger.warning("Disconnected from MQTT broker")
    if reason_code != 0:
        logger.warning(f"Unexpected disconnection (Reason code: {reason_code}). Reconnecting...")
        time.sleep(5)
        client.reconnect()

# MQTT Client Setup
def setup_mqtt():
    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, protocol=mqtt.MQTTv311)
    logger.info("Setting up MQTT client...")
    logger.info(f"Connecting to {args.host}:{args.port}")
    if args.username and args.password:
        client.username_pw_set(args.username, args.password)
    client.on_connect = on_connect
    client.on_disconnect = on_disconnect
    client.on_message = on_message
    client.connect(args.host, args.port, 60)
    return client

if __name__ == "__main__":
    conn, cursor = setup_sqlite()
    mqtt_client = setup_mqtt()
    mqtt_client.loop_start()
    logger.info("Listening for MQTT messages...")
    try:
        while True:
            batch_messages = []
            while not message_queue.empty():
                batch_messages.append(message_queue.get())
            if batch_messages:
                save_to_sqlite_batch(cursor, conn, batch_messages)
                for message in batch_messages:
                    save_to_json(*message)
            time.sleep(1)
    except KeyboardInterrupt:
        logger.info("Shutting down...")
    finally:
        mqtt_client.loop_stop()
        mqtt_client.disconnect()
        cursor.close()
        conn.close()
        logger.info("Shutdown complete.")
