import argparse
import datetime
import json
import sys
import paho.mqtt.publish as mqtt
from electricity_price.entsoe import EntsoeClient

if __name__ == "__main__":
    parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('token', help='entsoe token')
    parser.add_argument('broker', help='MQTT broker hostname', nargs='?', default="localhost")
    parser.add_argument('--port', help='MQTT broker port', nargs='?', default=1883, type=int)
    parser.add_argument('--prefix', help='MQTT topic prefix', nargs='?', default="entsoe")
    parser.add_argument('--hours', help='Number of hours to fetch', nargs='?', default=24, type=int)
    args = parser.parse_args()

    client = EntsoeClient(args.token)
    domain="10YFI-1--------U" # Finland

    try:
        prices = client.day_ahead_prices(domain, datetime.datetime.now(datetime.timezone.utc), datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(hours=args.hours), vat=0.24)
    except Exception as e:
        sys.exit("Couldn't fetch prices: " + str(e))

    try:
        msgs = [
            {"topic": f"{args.prefix}/{domain}/day-ahead", "payload": json.dumps(prices, default=str)},
            {"topic": f"{args.prefix}/{domain}/current", "payload": json.dumps(prices[0], default=str)}
        ]
        mqtt.multiple(msgs, hostname=args.broker, port=args.port)
    except Exception as e:
        sys.exit("Couldn't send prices: " + str(e))