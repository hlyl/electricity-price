import os
import argparse
import datetime
import json
import sys
from typing import List
import paho.mqtt.publish as mqtt
from entsoe import EntsoeClient

# import pandas


# [{'time': datetime.datetime(2022, 9, 24, 14, 0, tzinfo=datetime.timezone.utc), 'price': 0.44268},/
# {'time': datetime.datetime(2022, 9, 24, 14, 0, tzinfo=datetime.timezone.utc), 'price': 0.44268}]
# def parsetimeprice(results):
#    records = [{[{"time": x["time"]}, {"price": x["price"]}]} for x in results]
#    logger.debug(records, serialie=True)
#    return records


if __name__ == "__main__":
    api_key = os.environ["EntSoeToken"]
    # from loguru import logger

    # logger.add("logging.txt")

    parser = argparse.ArgumentParser(
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    parser.add_argument("--token", help="entsoe token", default=api_key)
    parser.add_argument(
        "broker", help="MQTT broker hostname", nargs="?", default="localhost"
    )
    parser.add_argument(
        "--port", help="MQTT broker port", nargs="?", default=1883, type=int
    )
    parser.add_argument(
        "--prefix", help="MQTT topic prefix", nargs="?", default="entsoe"
    )
    parser.add_argument(
        "--hours", help="Number of hours to fetch", nargs="?", default=24, type=int
    )
    args = parser.parse_args()

    # client = EntsoeClient(args.token)
    client = EntsoeClient(api_key)

    # domain = "10YFI-1--------U"  # Finland
    domain = "10YDK-1--------W"  # Denmark

    try:
        prices = client.day_ahead_prices(
            domain,
            datetime.datetime.now(datetime.timezone.utc),
            datetime.datetime.now(datetime.timezone.utc)
            + datetime.timedelta(hours=args.hours),
            vat=0.25,
        )

    except Exception as e:
        sys.exit("Couldn't fetch prices: " + str(e))

    #    for x in prices:

    # atime = x["time"]
    # atime = atime.strftime("%Y%m%d%H")
    # print(atime)
    # aprice = x["price"]
    # print(aprice)

    records = [
        {"time": x["time"].strftime("%Y-%m-%dT%H:%M:%S.%f"), "price": x["price"]}
        for x in prices
    ]
    print(records)
    with open(r"timeprice.json", "w") as fp:
        json.dump(records, fp)

    try:
        msgs = [
            {
                "topic": f"{args.prefix}/{domain}/day-ahead",
                "payload": json.dumps(prices, default=str),
            },
            {
                "topic": f"{args.prefix}/{domain}/current",
                "payload": json.dumps(prices[0], default=str),
            },
        ]
        # mqtt.multiple(msgs, hostname=args.broker, port=args.port)
    except Exception as e:
        sys.exit("Couldn't send prices: " + str(e))
