import configparser
import pathlib
import pika
import time

from threading import Thread

GENERAL_CONFIG = {
    'rebitmq_host': 'rabbitmq',
    'rebitmq_port': '5672',
    'rebitmq_username': 'guest',
    'rebitmq_password': 'guest',
    'queue_name': 'test_queue',
}


class Simulator:
    def __init__(self):
        self.config = configparser.ConfigParser()
        self.running = False
        self._init_config()
        self._init_connection()

    def _init_config(self):
        if not pathlib.Path('config.ini').exists():
            print('Config file not found. Creating a new one.')
            self.config['General'] = GENERAL_CONFIG
            with open('config.ini', 'w') as configfile:
                self.config.write(configfile)
        else:
            print('Config file found. Reading from it.')
            self.config.read('config.ini')

    def _init_connection(self):
        _credentials = pika.PlainCredentials(self.config['General']['rebitmq_username'], self.config['General']['rebitmq_password'])
        _parameters = pika.ConnectionParameters(self.config['General']['rebitmq_host'], self.config['General']['rebitmq_port'], '/', credentials=_credentials)
        connection = pika.BlockingConnection(_parameters)
        return connection

    def _init_channel(self, connection: pika.BlockingConnection):
        channel = connection.channel()
        channel.queue_declare(queue=self.config['General']['queue_name'], durable=True)
        return channel

    def _producer(self):
        _connection = self._init_connection()
        _channel = self._init_channel(connection=_connection)
        while self.running:
            _channel.basic_publish(exchange='', routing_key=self.config['General']['queue_name'], body='Hello, World!')
            print(" [x] Sent 'Hello World!'")
            time.sleep(.5)
        _channel.close()
        _connection.close()

    def _consumer(self):
        def callback(ch, method, properties, body):
            print(f" [x] Received {body}")
            ch.basic_ack(delivery_tag=method.delivery_tag)

        def check_running():
            if not self.running:
                channel.stop_consuming()
            else:
                _connection.call_later(1, check_running)

        _connection = self._init_connection()
        channel = self._init_channel(connection=_connection)
        channel.basic_consume(queue=self.config['General']['queue_name'], on_message_callback=callback)
        print(' [*] Waiting for messages.')
        _connection.call_later(1, check_running)
        channel.start_consuming()
        channel.close()
        _connection.close()
        
    def start(self):
        self.running = True
        self.producer_thread = Thread(target=self._producer)
        self.consumer_thread = Thread(target=self._consumer)
        self.producer_thread.start()
        self.consumer_thread.start()
    
    def stop(self):
        self.running = False
        self.producer_thread.join()
        self.consumer_thread.join()


if __name__ == '__main__':
    simulator = Simulator()
    simulator.start()
    try:
        while True:
            pass
    except KeyboardInterrupt:
        simulator.stop()