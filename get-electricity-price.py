import argparse
import datetime
import json
import paho.mqtt.publish as mqtt
import requests
import sys
import xml.etree.ElementTree as ET

def send_prices(prices, hostname="localhost", port=1883, prefix=""):
    if prefix:
        topic = prefix + "/energy"
    else:
        topic = "energy"
    mqtt.single(topic, payload=json.dumps(prices, default=str), hostname=hostname, port=port)

def fetch_prices(token, start_time, end_time): 
    """
    GET /api?documentType=A44&in_Domain=10YCZ-CEPS-----N&out_Domain=10YCZ-CEPS-----N&periodStart=201512312300&periodEnd=201612312300

    HTTP/1.1 200 OK
    Content-Type: text/xml

    <Publication_MarketDocument xmlns="urn:iec62325.351:tc57wg16:451-3:publicationdocument:7:0">
        <mRID>36ac0a9c39664688829ce501372521cb</mRID>
        <revisionNumber>1</revisionNumber>
        <type>A44</type>
        <sender_MarketParticipant.mRID codingScheme="A01">10X1001A1001A450</sender_MarketParticipant.mRID>
        <sender_MarketParticipant.marketRole.type>A32</sender_MarketParticipant.marketRole.type>
        <receiver_MarketParticipant.mRID codingScheme="A01">10X1001B1001B61Q</receiver_MarketParticipant.mRID>
        <receiver_MarketParticipant.marketRole.type>A33</receiver_MarketParticipant.marketRole.type>
        <createdDateTime>2016-05-10T09:18:53Z</createdDateTime>
        <period.timeInterval>
            <start>2015-12-31T23:00Z</start>
            <end>2016-04-02T22:00Z</end>
        </period.timeInterval>
        <TimeSeries>
            <mRID>1</mRID>
            <businessType>A62</businessType>
            <in_Domain.mRID codingScheme="A01">10YCZ-CEPS-----N</in_Domain.mRID>
            <out_Domain.mRID codingScheme="A01">10YCZ-CEPS-----N</out_Domain.mRID>
            <currency_Unit.name>EUR</currency_Unit.name>
            <price_Measure_Unit.name>MWH</price_Measure_Unit.name>
            <curveType>A01</curveType>
            <Period>
                <timeInterval>
                    <start>2015-12-31T23:00Z</start>
                    <end>2016-01-01T23:00Z</end>
                </timeInterval>
                <resolution>PT60M</resolution>
                <Point>
                    <position>1</position>
                    <price.amount>16.50</price.amount>
                </Point>
                <Point>
                    <position>2</position>
                    <price.amount>15.50</price.amount>
                </Point>
                <Point>
                    <position>3</position>
                    <price.amount>14.00</price.amount>
                </Point>
                <Point>
                    <position>4</position>
                    <price.amount>10.01</price.amount>
                </Point>
                <Point>
                    <position>5</position>
                    <price.amount>8.97</price.amount>
                </Point>
                <Point>
                    <position>6</position>
                    <price.amount>12.23</price.amount>
                </Point>
                <Point>
                    <position>7</position>
                    <price.amount>12.10</price.amount>
                </Point>
                <Point>
                    <position>8</position>
                    <price.amount>14.00</price.amount>
                </Point>
                <Point>
                    <position>9</position>
                    <price.amount>5.00</price.amount>
                </Point>
                <Point>
                    <position>10</position>
                    <price.amount>10.01</price.amount>
                </Point>
                <Point>
                    <position>11</position>
                    <price.amount>14.50</price.amount>
                </Point>
                <Point>
                    <position>12</position>
                    <price.amount>5.00</price.amount>
                </Point>
                <Point>
                    <position>13</position>
                    <price.amount>6.00</price.amount>
                </Point>
                <Point>
                    <position>14</position>
                    <price.amount>11.05</price.amount>
                </Point>
                <Point>
                    <position>15</position>
                    <price.amount>21.00</price.amount>
                </Point>
                <Point>
                    <position>16</position>
                    <price.amount>25.00</price.amount>
                </Point>
                <Point>
                    <position>17</position>
                    <price.amount>31.20</price.amount>
                </Point>
                <Point>
                    <position>18</position>
                    <price.amount>34.02</price.amount>
                </Point>
                <Point>
                    <position>19</position>
                    <price.amount>35.00</price.amount>
                </Point>
                <Point>
                    <position>20</position>
                    <price.amount>34.50</price.amount>
                </Point>
                <Point>
                    <position>21</position>
                    <price.amount>34.03</price.amount>
                </Point>
                <Point>
                    <position>22</position>
                    <price.amount>30.00</price.amount>
                </Point>
                <Point>
                    <position>23</position>
                    <price.amount>28.13</price.amount>
                </Point>
                <Point>
                    <position>24</position>
                    <price.amount>21.80</price.amount>
                </Point>
            </Period>
        </TimeSeries>
        ...
    </Publication_MarketDocument>
    """

    prices = []

    if start_time >= end_time:
        return prices

    base_url = "https://transparency.entsoe.eu"
    document_type = "A44" # Day ahead prices
    domain="10YFI-1--------U" # Finland
    period_start = start_time.replace(minute=0).astimezone(datetime.timezone.utc).strftime("%Y%m%d%H%M") # API expects UTC timestamps with full hours
    period_end = end_time.replace(minute=0).astimezone(datetime.timezone.utc).strftime("%Y%m%d%H%M")

    endpoint = "/api?securityToken={0}&documentType={1}&in_Domain={2}&out_Domain={2}&periodStart={3}&periodEnd={4}".format(token, document_type, domain, period_start, period_end)

    r = requests.get(base_url + endpoint)
    r.raise_for_status()
    if r.headers['content-type'] != "text/xml":
        raise ValueError("Unexpected content type: " + r.headers['content-type'])

    xml_ns = {"d": "urn:iec62325.351:tc57wg16:451-3:publicationdocument:7:0"}
    xml_root = ET.fromstring(r.text)

    for time_series in xml_root.findall('d:TimeSeries', namespaces=xml_ns):
        if time_series.findtext('d:currency_Unit.name', namespaces=xml_ns) != "EUR" or time_series.findtext('d:price_Measure_Unit.name', namespaces=xml_ns) != "MWH":
            raise ValueError("Unexpected currency or measure unit")
        start = datetime.datetime.strptime(time_series.findtext('./d:Period/d:timeInterval/d:start', namespaces=xml_ns), "%Y-%m-%dT%H:%MZ").replace(tzinfo=datetime.timezone.utc)
        end = datetime.datetime.strptime(time_series.findtext('./d:Period/d:timeInterval/d:end', namespaces=xml_ns), "%Y-%m-%dT%H:%MZ").replace(tzinfo=datetime.timezone.utc)

        skip_begin = (start_time - start) / datetime.timedelta(hours=1)
        skip_end = (end - end_time) / datetime.timedelta(hours=1)
        for point in time_series.findall('./d:Period/d:Point', namespaces=xml_ns):
            pos = int(point.findtext('d:position', namespaces=xml_ns))
            price = float(point.findtext('d:price.amount', namespaces=xml_ns))
            if pos > skip_begin and pos <= 24 - skip_end:
                prices.append({"time": start + (pos - 1) * datetime.timedelta(hours=1),"price": 1.24 * price / 1000000}) # Add VAT, convert to EUR/Wh

    return prices

if __name__ == "__main__":
    parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('token', help='entsoe token')
    parser.add_argument('broker', help='MQTT broker hostname', nargs='?', default="localhost")
    parser.add_argument('--port', help='MQTT broker port', nargs='?', default=1883, type=int)
    parser.add_argument('--prefix', help='MQTT topic prefix', nargs='?', default="")
    parser.add_argument('--hours', help='Number of hours to fetch', nargs='?', default=24, type=int)
    args = parser.parse_args()

    try:
        prices = fetch_prices(args.token, datetime.datetime.now(datetime.timezone.utc), datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(hours=args.hours))
        send_prices(prices, hostname=args.broker, port=args.port, prefix=args.prefix)
    except Exception as e:
        sys.exit("Error occurred: " + str(e))