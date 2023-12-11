"""
Run this script to start the MQTT 2 MongoDB Logger Script.
This script will run on the background and will log all the MQTT messages to MongoDB.

Args:
    --host: MongoDB Host
    --port: MongoDB Port
    --username: MongoDB Username
    --password: MongoDB Password
    --database: MongoDB Database to be Used
    
Database Structure:
    Server
    |
    |-- mqtt_logs
    |   |
    |   |-- 2023-12-08
    |   |   |
    |   |   |-- home_bedroom
    |   |   |-- home_kitchen
    |   |
    |   |-- 2023-12-09
    |       |
    |       |-- home_bedroom
    |       |-- home_kitchen
    |
"""
import argparse
import pymongo
import paho.mqtt.client as mqtt
import logging
import time
import json


def get_logger(
    level: int = logging.INFO,
    stdout: bool = False,
    stdout_stream_handler=None
) -> logging.Logger:
    """
    This function will return a logger object.
    Args:
        level: Logging Level
        stdout: Whether to log to stdout or not
        stdout_stream_handler: Stream Handler for stdout
    """

    # Create a logger object
    logger = logging.getLogger(__name__)

    # Set the logging level
    logger.setLevel(level)

    if stdout:
        # Create a stream handler
        handler = logging.StreamHandler()

        # Set the formatter
        handler.setFormatter(logging.Formatter(
            "%(asctime)s - %(name)s - %(levelname)s - %(message)s"))

        # Add the stdout stream handler to the logger
        logger.addHandler(stdout_stream_handler)

    else:
        # Create a file handler
        handler = logging.FileHandler("mqtt2mongodb.log")

        # Set the formatter
        handler.setFormatter(logging.Formatter(
            "%(asctime)s - %(name)s - %(levelname)s - %(message)s"))

        # Add the file handler to the logger
        logger.addHandler(handler)

    # Return the logger object
    return logger


def connect_to_mongodb(
    cli_args: argparse.Namespace,
    logger: logging.Logger
) -> pymongo.MongoClient:
    """
    This function will connect to MongoDB and return the database instance.
    Args:
        cli_args: ArgumentParser Arguments
        logger: Logger Object
    """

    connection_uri = f"mongodb://{cli_args.mongodb_username}:{cli_args.mongodb_password}@{cli_args.mongodb_host}:{cli_args.mongodb_port}"
    logger.info(f"Connecting to MongoDB: {connection_uri}")

    try:
        # Connect to MongoDB
        client = pymongo.MongoClient(connection_uri)
        client.server_info()
        logger.info("Connected to MongoDB")

        # Return the database instance
        return client[cli_args.mongodb_database]

    except pymongo.errors.ServerSelectionTimeoutError as e:
        logger.error(f"The server is down or has refused the connection: {e}")
        return None


def connect_to_mqtt(
    cli_args: argparse.Namespace,
    logger: logging.Logger
) -> mqtt.Client:
    """
    This function will connect to MQTT and return the MQTT Client instance.
    Args:
        cli_args: ArgumentParser Arguments
        logger: Logger Object
    """

    logger.info(
        f"Connecting to MQTT Broker: {cli_args.mqtt_host}:{cli_args.mqtt_port}")

    # Create a MQTT Client
    client = mqtt.Client(client_id="mongodb_logger", clean_session=True)

    # Set the username and password
    client.username_pw_set(cli_args.mqtt_username, cli_args.mqtt_password)

    try:
        # Connect to MQTT Broker
        client.connect(cli_args.mqtt_host, cli_args.mqtt_port)

        logger.info("Connected to MQTT Broker")
        # Return the MQTT Client
        return client

    except TimeoutError as e:
        logger.error(f"Connection timed out: {e}")
        return None

    except ConnectionRefusedError as e:
        logger.error(f"The server has refused the connection: {e}")
        return None


def on_message(
    client: mqtt.Client,
    userdata: dict,
    message: mqtt.MQTTMessage
) -> None:
    """
    This function will be called when a new message is received.
    Args:
        client: MQTT Client
        userdata: User Data
        message: MQTT Message
    """

    # Get the database instance
    db = userdata["db"]

    # Get the logger object
    logger = userdata["logger"]

    # Get the topic
    topic = message.topic

    # Get the payload
    payload = message.payload.decode("utf-8")

    # Get the current date
    date = time.strftime("%Y-%m-%d")

    # Get the current time
    time_ = time.strftime("%H:%M:%S")

    # Get the collection
    collection = db[date]

    # Create a document
    document = {
        "topic": topic,
        "date": date,
        "time": time_,
        "payload": json.loads(payload)
    }

    # Insert the document
    collection.insert_one(document)

    # Log the message
    logger.info(f"Topic: {topic} | Time: {time_} | Payload: {payload}")


if __name__ == "__main__":
    # Create an ArgumentParser object
    parser = argparse.ArgumentParser(
        description="MQTT 2 MongoDB Logger Script")

    # Add arguments
    parser.add_argument("--mongodb-host",
                        help="MongoDB Host", default="localhost")
    parser.add_argument("--mongodb-port",
                        help="MongoDB Port", default=27017)
    parser.add_argument("--mongodb-username",
                        help="MongoDB Username", default="devasheesh")
    parser.add_argument("--mongodb-password",
                        help="MongoDB Password", default="swastik123")
    parser.add_argument("--mongodb-database",
                        help="MongoDB Database to be Used", default="mqtt_logs")
    parser.add_argument("--mqtt-host",
                        help="MQTT Host", default="localhost")
    parser.add_argument("--mqtt-port",
                        help="MQTT Port", default=1883)
    parser.add_argument("--mqtt-username",
                        help="MQTT Username", default="mosquitto")
    parser.add_argument("--mqtt-password",
                        help="MQTT Password", default="mosquitto")
    parser.add_argument("--mqtt-topics", nargs='+',
                        help="MQTT Topics", default=["#"])

    # Parse arguments
    cli_args = parser.parse_args()

    # Get a logger object
    logger = get_logger(
        stdout=True, stdout_stream_handler=logging.StreamHandler())

    # Connect to MongoDB
    while True:
        db = connect_to_mongodb(cli_args, logger)
        if db is not None:
            break
        logger.info("Retrying to connect to MongoDB")
        time.sleep(5)

    # Connect to MQTT Broker
    while True:
        client = connect_to_mqtt(cli_args, logger)
        if client is not None:
            break
        logger.info("Retrying to connect to MQTT Broker")
        time.sleep(5)

    # Subscribe to MQTT Topics
    for topic in cli_args.mqtt_topics:
        client.subscribe(topic)

    # Set the userdata
    client.user_data_set({
        "db": db,
        "logger": logger
    })

    # Callback function for MQTT Client
    client.on_message = on_message

    # Start the MQTT Client
    client.loop_forever()
