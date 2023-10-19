from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
import datetime
import bson
import ssl
import websocket  # pip install websocket-client
from urllib.request import urlopen
import json

uri = 'put uri in file mongo_uri'
with open('mongo_uri', 'r') as reader:
    uri = reader.read()
client = MongoClient(uri, server_api=ServerApi('1'))
db = client["universalis"]
collection = db["ws"]


def on_open(ws):
    # 1043紫水栈桥；1045摩杜纳；1107静语庄园；1169延夏；1177海猫茶屋；1178柔风海湾；1179琥珀原
    ws.send(bson.encode({"event": "subscribe", "channel": "listings/add{world=1043}"}))
    ws.send(bson.encode({"event": "subscribe", "channel": "listings/add{world=1045}"}))
    ws.send(bson.encode({"event": "subscribe", "channel": "listings/add{world=1107}"}))
    ws.send(bson.encode({"event": "subscribe", "channel": "listings/add{world=1169}"}))
    ws.send(bson.encode({"event": "subscribe", "channel": "listings/add{world=1177}"}))
    ws.send(bson.encode({"event": "subscribe", "channel": "listings/add{world=1178}"}))
    ws.send(bson.encode({"event": "subscribe", "channel": "listings/add{world=1179}"}))
    print("Connection opened. Ready to retrieve data.")


def on_close():
    print("Connection was closed.")

def mongoInsert(itemID):
    insertion = {"itemID":itemID, "lastUploadTime": datetime.datetime.utcnow()}
    mongoResult = collection.insert_one(insertion)

def nameLookup(itemID):
    itemURL = "https://cafemaker.wakingsands.com/item/" + str(itemID)
    itemInfoRaw = urlopen(itemURL).read().decode('utf-8')
    # print(itemInfoRaw)
    itemInfo = json.loads(itemInfoRaw)
    return itemInfo['Name']

def on_message(ws, message):
    decoded = bson.decode(message)
    # print(bson.decode(message))
    itemID = decoded['item']
    mongoInsert(itemID)
    # print(str(itemID) + ' ' + nameLookup(itemID))    # just for console fun

def on_error(ws, err):
    print("Error: ", err)

ws = websocket.WebSocketApp("wss://universalis.app/api/ws", on_open=on_open,
                            on_close=on_close, on_message=on_message, on_error=on_error)
ws.run_forever(sslopt={"cert_reqs": ssl.CERT_NONE}, reconnect=2)
