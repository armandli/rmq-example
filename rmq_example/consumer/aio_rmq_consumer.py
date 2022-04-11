import argparse
import pytz
from datetime import datetime
import json
import asyncio
import aio_pika

def build_parser():
  parser = argparse.ArgumentParser(description="rmq publisher")
  parser.add_argument('--host', type=str, help='host ip of rmq', required=True)
  parser.add_argument('--port', type=int, help='host port', required=True)
  parser.add_argument('--user', type=str, help='username', required=True)
  parser.add_argument('--password', type=str, help='password', required=True)
  parser.add_argument('--exchange', type=str, help='exchange name', required=True)
  return parser

async def hello_callback(msg: aio_pika.IncomingMessage):
  async with msg.process():
    print(msg.body)

async def consume_hello(host, port, user, password, exchange):
  connection = await aio_pika.connect_robust(host=host, port=port, login=user, password=password)
  async with connection:
    channel = await connection.channel()
    # set maximum number of messages to be processed at the same time
    await channel.set_qos(prefetch_count=1)
    exchange = await channel.declare_exchange(name=exchange, type='fanout')
    queue = await channel.declare_queue('', auto_delete=True)
    await queue.bind(exchange=exchange)
    await queue.consume(hello_callback)
    await asyncio.Future()

def main():
  parser = build_parser()
  args = parser.parse_args()
  asyncio.run(consume_hello(args.host, args.port, args.user, args.password, args.exchange))

if __name__ == '__main__':
  main()
