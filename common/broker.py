import pika

from common.events import serialize
from common.logger import log_event
from common.topology import EXCHANGE_NAME, declare_topology


def connect():
    """Conect RabbitMQ and declare topology"""
    connection = pika.BlockingConnection(pika.ConnectionParameters(host="rabbitmq"))
    channel = connection.channel()
    declare_topology(channel)
    return connection, channel


def publish_event(channel, service, routing_key, event):
    """Publica un evento en el exchange y lo registra en la evidencia."""
    channel.basic_publish(exchange=EXCHANGE_NAME, routing_key=routing_key, body=serialize(event))
    log_event(service, "produced", event, routing_key)