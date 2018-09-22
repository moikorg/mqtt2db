import paho.mqtt.client as mqtt
import argparse
import configparser
import pymysql
import datetime
import pytz
import json


def configSectionMap(config, section):
    dict1 = {}
    options = config.options(section)
    for option in options:
        try:
            dict1[option] = config.get(section, option)
            if dict1[option] == -1:
                print("skip: %s" % option)
        except:
            print("exception on %s!" % option)
            dict1[option] = None
    return dict1


def connectDB(configfile):
    config = configparser.ConfigParser()
    config.read(configfile)

    conn = pymysql.connect(host=configSectionMap(config, "DB")['host'],
                           user=configSectionMap(config, "Credentials")['username'],
                           password=configSectionMap(config, "Credentials")['password'],
                           db=configSectionMap(config, "DB")['db'],
                           port=int(configSectionMap(config, "DB")['port']),
                           charset='utf8')
    return conn


def parseTheArgs() -> object:
    parser = argparse.ArgumentParser(description='Listen to specific messages on MQTT and write them to DB')
    parser.add_argument('-d', dest='verbose', action='store_true',
                        help='print debugging information')
    parser.add_argument('-f', help='path and filename of the config file, default is ./config.rc',
                        default='/code/config.rc')

    args = parser.parse_args()
    return args


# The  callback for the JSON format data
def callback_json(client, userdata, message):
    print("Received JSON message '" + str(message.payload) + "' on topic '"
          + message.topic + "' with QoS " + str(message.qos))

    parsed_json = json.loads(message.payload)

    c = userdata.cursor()
    sql = "INSERT INTO temp_sensor (timestamp, topic, value) VALUES (FROM_UNIXTIME(%s), %s, %s);"
    c.execute(sql, (parsed_json['ts'], 'sensor/json', parsed_json['temp']))
    userdata.commit()


# The  callback for the temp or hum data
def callback_temp_or_hum(client, userdata, message):
    print("Received direct message @" + str(datetime.datetime.now()) + " : " + str(message.payload) + " on topic '"
          + message.topic + "' with QoS " + str(message.qos), " --> write to DB")

    c = userdata.cursor()
    sql = "INSERT INTO temp_sensor (timestamp, topic, value) VALUES (FROM_UNIXTIME(%s), %s, %s);"
    c.execute(sql, (datetime.datetime.now(pytz.timezone('Europe/Berlin')).timestamp(), message.topic, float(message.payload)))
    userdata.commit()





# The collector callback for when a PUBLISH message is received from the server.
def on_message(client, userdata, message):
    print("Received garbage collector message '" + str(message.payload) + "' on topic '"
          + message.topic + "' with QoS " + str(message.qos))
    #    print("topic = ", str(message.topic))
    #    print("received message = ",str(message.payload.decode("utf-8")))


def on_connect(client, userdata, rc):
    print("connecting reason  "  +str(rc))
    client.disconnect_flag=False
    client.connected_flag=True


def on_disconnect(client, userdata, rc):
    print("disconnecting reason  "  +str(rc))
    client.connected_flag=False
    client.disconnect_flag=True


def main():
    args = parseTheArgs()
    config = configparser.ConfigParser()
    config.read(args.f)

    broker = configSectionMap(config, "MQTT")['host']
    client = mqtt.Client( "mqtt2db") 


    #######Bind function to callback
    client.on_message = on_message
    client.on_connect = on_connect
    client.on_disconnect = on_disconnect

    print("connecting to broker ", broker)
    client.username_pw_set(configSectionMap(config, "MQTT")['username'], configSectionMap(config, "MQTT")['password'])
    # todo: try catch .... if the broker can not be connected
    try:
        client.connect(broker)
    except:
        print("ERROR: Can not connect to MQTT broker")
        return 1

    # create the DB connection and pass it to the callback function
    conn = connectDB(args.f)

    client.user_data_set(conn)

    # subscribe
    print("subscribing ")
    # client.subscribe("#")   #subscribe
    client.message_callback_add("tele/sensor/temphum",callback_json)
    client.message_callback_add("sensor/humidity",callback_temp_or_hum)
    client.message_callback_add("sensor/temperature",callback_temp_or_hum)
    client.subscribe([("sensor/temperature", 0), ("sensor/humidity", 0),
                      ("tele/sensor/temperature", 0), ("tele/sensor/humidity", 0),
                      ("tele/sensor/temphum", 0)])




    # the loop_forever cope also with reconnecting if needed
    client.loop_forever()


# this is the standard boilerplate that calls the main() function
if __name__ == '__main__':
    # sys.exit(main(sys.argv)) # used to give a better look to exists
    main()
