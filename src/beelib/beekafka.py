import json
from kafka import KafkaProducer, KafkaConsumer
from base64 import b64encode, b64decode
import pickle
import sys

def __pickle_encoder__(v):
    """
    Encode a value using base64 and pickle.

    Parameters:
    - v (object): The value to encode.

    Returns:
    - bytes: The encoded value.
    """
    return b64encode(pickle.dumps(v))

def __pickle_decoder__(v):
    """
    Decode a value from base64 and pickle.

    Parameters:
    - v (bytes): The encoded value.

    Returns:
    - object: The decoded value.
    """
    return pickle.loads(b64decode(v))

def __json_encoder__(v):
    """
    Encode a value as a JSON string.

    Parameters:
    - v (object): The value to encode.

    Returns:
    - bytes: The JSON-encoded value.
    """
    return json.dumps(v).encode("utf-8")

def __json_decoder__(v):
    """
    Decode a JSON string.

    Parameters:
    - v (bytes): The JSON-encoded value.

    Returns:
    - object: The decoded value.
    """
    return json.loads(v.decode("utf-8"))

def __plain_decoder_encoder(v):
    """
    Identity function for plain encoding/decoding.

    Parameters:
    - v (object): The value.

    Returns:
    - object: The same value.
    """
    return v

def create_kafka_producer(kafka_conf, encoding="JSON", **kwargs):
    """
    Create a Kafka producer with the specified encoding.

    Parameters:
    - kafka_conf (dict): Kafka configuration containing 'host' and 'port'.
    - encoding (str, optional): Encoding format ('JSON', 'PICKLE', or 'PLAIN').
    - **kwargs: Additional arguments for KafkaProducer.

    Returns:
    - KafkaProducer: A configured Kafka producer.

    Raises:
    - NotImplementedError: If the encoding is unsupported.
    """
    if encoding == "PLAIN":
        encoder = __plain_decoder_encoder
    elif encoding == "PICKLE":
        encoder = __pickle_encoder__
    elif encoding == "JSON":
        encoder = __json_encoder__
    else:
        raise NotImplementedError("Unknown encoding")
    servers = [f"{kafka_conf['host']}:{kafka_conf['port']}"]
    return KafkaProducer(bootstrap_servers=servers, value_serializer=encoder, **kwargs)

def create_kafka_consumer(kafka_conf, encoding="JSON", **kwargs):
    """
    Create a Kafka consumer with the specified decoding.

    Parameters:
    - kafka_conf (dict): Kafka configuration containing 'host' and 'port'.
    - encoding (str, optional): Encoding format ('JSON', 'PICKLE', or 'PLAIN').
    - **kwargs: Additional arguments for KafkaConsumer.

    Returns:
    - KafkaConsumer: A configured Kafka consumer.

    Raises:
    - NotImplementedError: If the encoding is unsupported.
    """
    if encoding == "PLAIN":
        decoder = __plain_decoder_encoder
    elif encoding == "PICKLE":
        decoder = __pickle_decoder__
    elif encoding == "JSON":
        decoder = __json_decoder__
    else:
        raise NotImplementedError("Unknown encoding")

    servers = [f"{kafka_conf['host']}:{kafka_conf['port']}"]
    return KafkaConsumer(bootstrap_servers=servers, value_deserializer=decoder, **kwargs)

def send_to_kafka(producer, topic, key, data, **kwargs):
    """
    Send a message to a Kafka topic.

    Parameters:
    - producer (KafkaProducer): The Kafka producer.
    - topic (str): The target topic.
    - key (str, optional): Message key.
    - data (dict): Message value.
    - **kwargs: Additional metadata to include in the message.
    """
    try:
        kafka_message = {
            "data": data
        }
        kafka_message.update(kwargs)
        if key:
            producer.send(topic, key=key.encode('utf-8'), value=kafka_message)
        else:
            producer.send(topic, value=kafka_message)
    except Exception as e:
        print(e, file=sys.stderr)