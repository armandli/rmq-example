import argparse
import pytz
from datetime import datetime
import json
import asyncio
import aio_pika

def build_message():
  data = dict()
  utc = pytz.timezone("UTC")
  data['timestamp'] = datetime.now(utc).strftime("%Y-%m-%dT%H:%M:%S.%f %Z")
  data['content'] = "hello hello"
  s = json.dumps(data)
  return s.encode()

async def publish_hello(host, port, user, password, exchange):
  connection = await aio_pika.connect_robust(host=host, port=port, login=user, password=password)
  async with connection:
    channel = await connection.channel()
    exchange = await channel.declare_exchange(name=exchange, type='fanout')
    msg = aio_pika.Message(body=build_message())
    await exchange.publish(message=msg, routing_key='')

def build_parser():
  parser = argparse.ArgumentParser(description="rmq publisher")
  parser.add_argument('--host', type=str, help='host ip of rmq', required=True)
  parser.add_argument('--port', type=int, help='host port', required=True)
  parser.add_argument('--user', type=str, help='username', required=True)
  parser.add_argument('--password', type=str, help='password', required=True)
  parser.add_argument('--exchange', type=str, help='exchange name', required=True)
  return parser

def main():
  parser = build_parser()
  args = parser.parse_args()
  asyncio.run(publish_hello(args.host, args.port, args.user, args.password, args.exchange))

if __name__ == '__main__':
  main()
