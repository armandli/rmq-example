import argparse
import pytz
from datetime import datetime
import json
import pika

def build_parser():
  parser = argparse.ArgumentParser(description="rmq publisher")
  parser.add_argument('--host', type=str, help='host ip of rmq', required=True)
  parser.add_argument('--port', type=int, help='host port', required=True)
  parser.add_argument('--user', type=str, help='username', required=True)
  parser.add_argument('--password', type=str, help='password', required=True)
  parser.add_argument('--exchange', type=str, help='exchange name', required=True)
  return parser

def hello_callback(ch, method, properties, body):
  print("MSG: {} METHOD: {} CHANNEL: {}".format(body, method, ch))

def consume_hello(host, port, user, password, exchange):
  credential = pika.PlainCredentials(user, password)
  connection = pika.BlockingConnection(pika.ConnectionParameters(host=host, port=port, credentials=credential))
  channel = connection.channel()
  channel.exchange_declare(exchange=exchange, exchange_type='fanout')
  # create a new queue with random name
  # exclusive deletes queue once consumer connection is closed
  result = channel.queue_declare(queue='', exclusive=True)
  queue_name = result.method.queue
  channel.queue_bind(exchange=exchange, queue=queue_name)
  channel.basic_consume(queue=queue_name, on_message_callback=hello_callback, auto_ack=True)
  # blocking call
  channel.start_consuming()

def main():
  parser = build_parser()
  args = parser.parse_args()
  consume_hello(args.host, args.port, args.user, args.password, args.exchange)

if __name__ == '__main__':
  main()
