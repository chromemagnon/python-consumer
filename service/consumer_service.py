
import logging
import os
import pika

from model.dto.iot_device_measurement_dto import IOTDeviceMeasurementDTO
from pika.exceptions import AMQPConnectionError
from pika.exchange_type import ExchangeType
from repository.consumer_repository import ConsumerRepository


class ConsumerService:
    """
    Service for consuming messages from a message broker and processing them.
    """
    def __init__(self):
        self.host = os.getenv('RABBIT_HOST', 'rabbit-server')
        self.exchange = os.getenv('IOT_EXCHANGE', 'iot_exchange')
        self.queue = os.getenv('IOT_QUEUE', 'iot_measurements')
        self.connection = None
        self.channel = None
        self.repository = ConsumerRepository()

    def on_message_received(self, ch, method, properties, body):
        """
        Callback function for handling received messages.
        """
        logging.info(f"Consumer: Received message: {body}")
        try:
            dto = IOTDeviceMeasurementDTO.model_validate_json(body)
            self.repository.store_measurement(dto)
        except Exception as e:
            logging.error(f"Error processing message: {e}")
            # TODO: Will throwing an exception here send a NACK?

    def setup_connection(self):
        """
        Establishes connection with the message broker.
        """
        try:
            self.connection = pika.BlockingConnection(pika.ConnectionParameters(self.host))
            self.channel = self.connection.channel()
            self.channel.exchange_declare(exchange=self.exchange, exchange_type=ExchangeType.fanout)
            self.channel.queue_declare(queue=self.queue, exclusive=True)
            self.channel.queue_bind(exchange=self.exchange, queue=self.queue)
        except AMQPConnectionError as e:
            logging.error(f"Connection error: {e}")
            raise

    def start_consuming(self):
        """
        Starts consuming messages from the queue.
        """
        logging.info("Preparing to consume")
        self.channel.basic_consume(queue=self.queue, auto_ack=True, on_message_callback=self.on_message_received)
        self.channel.start_consuming()

    def close_connection(self):
        """
        Closes the connection to the message broker.
        """
        if self.connection and not self.connection.is_closed:
            self.connection.close()

