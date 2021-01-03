import logging

import pika
import json
import sys
import time
import shutil

from sqlalchemy import exc

from database import Database
from database_connection import DatabaseConnectionParameters
from rabbitmq_connection import RabbitMQConnectionParameters


class GarbageCollector:
    QUEUE_NAME = "gc"

    def __init__(self, rabbitmq_parameters: RabbitMQConnectionParameters,
                 database_parameters: DatabaseConnectionParameters, base_repos_path: str):
        self.rabbitmq_parameters = rabbitmq_parameters
        self.database_parameters = database_parameters
        self.base_repos_path = base_repos_path

        self.log = logging.getLogger("GarbageCollector")
        self.log.setLevel(logging.DEBUG)
        handler = logging.StreamHandler(sys.stdout)
        handler.setLevel(logging.DEBUG)
        self.log.addHandler(handler)

        self.database = Database(self.database_parameters, self.log)

    def run(self):
        while True:
            try:
                rmq_connection, channel = self._connect_to_rabbitmq()
                self._connect_to_database()
                while True:
                    self.log.info('Waiting to a new message')
                    channel.basic_consume(queue=self.QUEUE_NAME,
                                          auto_ack=False,
                                          on_message_callback=self._process_message)

                    channel.start_consuming()
            except pika.exceptions.AMQPConnectionError as exception:
                self.log.error(f"AMQP Connection Error: {exception}")
                self.log.info("Reconnecting to RabbitMQ and database")
            except exc.DBAPIError as exception:
                self.log.error(f"Database connection error: {exception}")
                self.log.info("Reconnecting to RabbitMQ and database")
            except KeyboardInterrupt:
                self.log.info(" Exiting...")
                try:
                    rmq_connection.close()
                except NameError:
                    pass
                sys.exit(0)

    def _connect_to_rabbitmq(self):
        while True:
            try:
                self.log.info(f"Connecting to RabbitMQ ({self.rabbitmq_parameters.host}:"
                              f"{self.rabbitmq_parameters.port})...")
                rmq_connection = pika.BlockingConnection(pika.ConnectionParameters(host=self.rabbitmq_parameters.host,
                                                                                   port=self.rabbitmq_parameters.port))
                channel = rmq_connection.channel()
                self.log.info("Connected to RabbitMQ")
                channel.queue_declare(queue=self.QUEUE_NAME, durable=True)
                return rmq_connection, channel
            except pika.exceptions.AMQPConnectionError as exception:
                self.log.error(f"AMQP Connection Error: {exception}")
                time.sleep(5)

    def _connect_to_database(self):
        while True:
            try:
                self.log.info("Connecting to a database")
                self.database.connect()
                self.log.info("Connected to a database")
                return
            except exc.DBAPIError as exception:
                self.log.error(f"Database connection error: {exception}")
                time.sleep(5)
                if not exception.connection_invalidated:
                    raise exception

    def _process_message(self, ch, method, properties, body):
        ch.stop_consuming()
        body_json = json.loads(body.decode('utf-8'))
        repository_id = body_json['repo_id']
        language_id = body_json['language_id']

        self.log.debug(f"Got a {repository_id} repository and language with id {language_id}")

        self.log.debug('Marking language as analyzed')
        self.database.mark_language_as_analyzed(repository_id, language_id)

        if self.database.are_all_present_languages_analyzed(repository_id):
            shutil.rmtree(f"{self.base_repos_path}/{repository_id}")
            self.log.info(f"All languages has been analyzed, so repository {repository_id} has been deleted")
        else:
            self.log.debug('Not all languages has been analyzed, so repository will not been deleted')

        ch.basic_ack(delivery_tag=method.delivery_tag)
