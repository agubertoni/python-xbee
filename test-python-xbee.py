import serial
from xbee import ZigBee
import datetime
import time
import pymongo
from pymongo import MongoClient

#################################################
#       Conectarse a Coordinador ZigBee         #
#################################################
print('---------------------------------------------------')
print('Conectando a Coordinador ZigBee...   ')

serial_port = serial.Serial('/dev/ttyUSB3', 9600)
xbee = ZigBee(serial_port)
print(xbee)

#################################################
# Conectarse con la mongoDB de Meteor (running) #
#################################################

print('---------------------------------------------------')
print('Conectando a MongoDB...   ')

mongoConnected = False
while not mongoConnected:
    try:
        client = MongoClient('mongodb://127.0.0.1:3001/meteor')
        mongoConnected = True
    except pymongo.errors.ConnectionFailure:
        pass

print('mongo running detected!')
print(client)
databases = str(client.database_names())
print('DBs disponibles: ' + databases)
db = client.meteor

collectionsAvailable = False
while not collectionsAvailable:
    try:
        collections = str(db.collection_names())
        collectionsAvailable = True
    except pymongo.errors.AutoReconnect:
        pass

# Recovery nodes' address in "nodesInMongo" list
nodesInMongo = []
cursor = db.sensors.find({}, {"node": 1, "_id": 0})
for record in cursor:
    nodesInMongo.append(record["node"])
print('Nodes in mongo:', nodesInMongo)

print('Colecciones disponibles: ' + collections)
print('---------------------------------------------------')

###############################
#           while(1)          #
###############################
tx_number = 0

while True:
    try:

        frame = xbee.wait_read_frame()

        # Timing -----------------------------------
        now = datetime.datetime.now()
        timestamp = now.isoformat()
        timestamp_ms = int(time.time() - time.timezone) * 1000

        hour = now.replace(minute=0, second=0, microsecond=0).isoformat()
        minute = now.minute

        source_addr_long = frame['source_addr_long']
        source_addr_long = ':'.join("{:02X}".format(ord(c)) for c in source_addr_long)
        print('MAC_addr = ' + source_addr_long)

        source_addr = frame['source_addr']
        source_addr = ':'.join("{:02X}".format(ord(c)) for c in source_addr)
        print('NWK_addr = ' + source_addr)

        rf_data = frame['rf_data'].decode('ascii')

        print(timestamp)

        # convert string to python dictionary (key/value pairs)
        key_value = dict(u.split(":") for u in rf_data.split(","))

        # convert values (string) to int
        key_value['temp'] = int(key_value['temp'])
        key_value['brix'] = int(key_value['brix'])
        key_value['alco'] = int(key_value['alco'])

        node_temp = key_value.get('temp')
        print(node_temp)
        node_brix = key_value.get('brix')
        print(node_brix)
        node_alco = key_value.get('alco')
        print(node_alco)

        tx_number = tx_number + 1;
        print('TX number: ' + str(tx_number))
        print('---------------------------------------------')

        ######################################
        #        "doc_type": "raw_doc"       #
        ######################################

        db.frames.update(
            {"node": source_addr_long, "doc_type": "raw_doc"},
            {"$push": {"reads": {
                "timestamp": timestamp,
                "temp": node_temp,
                "brix": node_brix,
                "alco": node_alco}}},
            True
        )

        ######################################
        #       "doc_type": "hour_doc"       #
        ######################################

        db.frames.update(
            {"node": source_addr_long, "doc_type": "hour_doc",
             "hour": hour, "parameter": "temp"},
            {"$push": {"values": node_temp}},
            True
        )

        ######################################
        #      "doc_type": "morris_doc"      #
        ######################################

        db.frames.update(
            {"node": source_addr_long, "doc_type": "morris_doc", "hour": hour},
            {"$push": {"values": {
                "timestamp": timestamp,
                "temp": node_temp,
                "brix": node_brix,
                "alco": node_alco}}},
            True
        )

        ######################################
        #       "doc_type": "high_doc"       #
        ######################################

        db.frames.insert(
            {"node": source_addr_long, "doc_type": "high_doc",
             "hour": hour, "timestamp": timestamp_ms,
             "temp": node_temp, "brix": node_brix, "alco": node_alco}
        )

    except KeyboardInterrupt:
        break

serial_port.close()