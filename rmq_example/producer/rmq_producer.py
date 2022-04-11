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

def build_message():
  data = dict()
  utc = pytz.timezone("UTC")
  data['timestamp'] = datetime.now(utc).strftime("%Y-%m-%dT%H:%M:%S.%f %Z")
  data['content'] = "hello hello"
  s = json.dumps(data)
  return s

def publish_hello(host, port, user, password, exchange):
  credential = pika.PlainCredentials(user, password)
  connection = pika.BlockingConnection(pika.ConnectionParameters(host=host, port=port, credentials=credential))
  channel = connection.channel()
  # exchange types: direct, topic, header, fanout
  channel.exchange_declare(exchange=exchange, exchange_type='fanout')
  msg = build_message()
  channel.basic_publish(exchange=exchange, routing_key='', body=msg)
  connection.close()

def main():
  parser = build_parser()
  args = parser.parse_args()
  publish_hello(args.host, args.port, args.user, args.password, args.exchange)

if __name__ == '__main__':
  main()
