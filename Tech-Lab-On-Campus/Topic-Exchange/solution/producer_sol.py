from producer_interface import mqProducerInterface
import pika
import os
import sys

class mqProducer(mqProducerInterface):
    def __init__(self, routing_key: str, exchange_name: str):
        self.routing_key = routing_key
        self.exchange_name = exchange_name
        self.ticker = sys.argv[0]
        self.price = sys.argv[1]
        self.sector = sys.argv[2]
        self.setupRMQConnection()
    
    def setupRMQConnection(self) -> None:
        con_params = pika.URLParameters(os.environ["AMQP_URL"])
        self.connection = pika.BlockingConnection(parameters=con_params)
        self.channel = self.connection.channel()
        self.exchange = self.channel.exchange_declare(exchange=self.exchange_name)

    def publishOrder(self, message: str) -> None:
        routing_key = '.'.join(sys.argv[2:]) if len(sys.argv) > 2 else 'anonymous.info'
        message = self.ticker + " is " + self.price or 'Hello World!'
        self.channel.basic_publish(
            exchange='topic_logs', routing_key=routing_key, body=message)
        print(f" [x] Sent {routing_key}:{message}")

        self.channel.close()
        self.connection.close()



    
