import serial
from xbee import ZigBee
import datetime
import pymongo
from pymongo import MongoClient

#################################################
#       Conectarse a Coordinador ZigBee         #
#################################################
print('---------------------------------------------------')
print('Conectando a Coordinador ZigBee...   ')

serial_port = serial.Serial('/dev/ttyUSB1', 9600)
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
        print(minute)

        if rf_data == 'ED Joined':
            print(rf_data)
        else:
            # convert string to python dictionary (key/value pairs)
            key_value = dict(u.split(":") for u in rf_data.split(","))

            # convert flow value (string) to int to manipulate after
            key_value['flow'] = int(key_value['flow'])

            nodeFlowInput = key_value.get('flow')
            print(nodeFlowInput)
            nodeRssiInput = key_value.get('rssi')
            print(nodeRssiInput)

        tx_number = tx_number + 1;
        print('TX number: ' + str(tx_number))
        print('---------------------------------------------')

        ######################################
        #        "doc_type": "raw_doc"       #
        ######################################

        db.sensors.update(
            {"node": source_addr_long, "doc_type": "raw_doc"},
            {"$push": {"reads": {
                "timestamp": timestamp,
                "flow": nodeFlowInput,
                "rssi": nodeRssiInput}}},
            True
        )

        ######################################
        #       "doc_type": "hour_doc"       #
        ######################################

        db.sensors.update(
            {"node": source_addr_long, "doc_type": "hour_doc", "hour": hour, "parameter": "temp"},
            {"$push": {"values": nodeFlowInput}},
            True
        )

        ######################################
        #      "doc_type": "morris_doc"      #
        ######################################

        db.sensors.update(
            {"node": source_addr_long, "doc_type": "morris_doc", "hour": hour},
            {"$push": {"values": {
                "timestamp": timestamp,
                "temp": nodeFlowInput,
                "rssi": nodeRssiInput}}},
            True
        )

    except KeyboardInterrupt:
        break

serial_port.close()