import paho.mqtt.client as mqtt
import argparse
import configparser
import pymysql
import datetime
import json

mqtt_received_data = ()

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


def on_disconnect(client, userdata, rc):
    print("disconnecting reason  " + str(rc))
    print(userdata)
    print(client)
    client.connected_flag=False
    client.disconnect_flag=True


# The callback for when the client receives a CONNACK response from the server.
def on_connect(client, userdata, flags, rc):
    print("Connected with result code "+str(rc))
    # Subscribing in on_connect() means that if we lose the connection and
    # reconnect then subscriptions will be renewed.
   #client.subscribe("$SYS/#")
    client.subscribe("sensor/meteo/1")


# The callback for when a PUBLISH message is received from the server.
def on_message(client, userdata, msg):
    print(msg.topic+" "+str(msg.payload))

    try:
        parsed_json = json.loads(msg.payload)
    except:
        print("ERROR: Can not parse JSON")
        return 1

    temp = round(float(parsed_json['temp']), 1)
    hum = round(float(parsed_json['hum']), 1)
    press = round(float(parsed_json['press']), 1)
    timestamp = datetime.datetime.fromtimestamp(int(parsed_json['ts'])).strftime('%Y-%m-%d %H:%M:%S')
    timestamp_epoch = int(parsed_json['ts'])
    sensor_ID = 1
    # check DB connection
    try:
        userdata.ping(reconnect=True)
    except Exception as e:
        print("Connection lost, reconnect FAILED")
        return 1

    c = userdata.cursor()
    sql = """INSERT IGNORE INTO meteo_sensor (ts, ts_epoch, temperature, humidity, pressure, sensor_id) 
              VALUES (%s, %s, %s, %s, %s, %s);"""
    try:
        c.execute(sql, (timestamp, timestamp_epoch, temp, hum, press, sensor_ID))
        userdata.commit()
    except Exception as e:
        print("Error in SQL execution: " + str(e))



def main():
    args = parseTheArgs()
    config = configparser.ConfigParser()
    config.read(args.f)

    broker = configSectionMap(config, "MQTT")['host']

    client = mqtt.Client()
    client.on_connect = on_connect
    client.on_message = on_message
    client.on_disconnect = on_disconnect

    client.username_pw_set(username=configSectionMap(config, "MQTT")['username'],
                           password= configSectionMap(config, "MQTT")['password'])

    try:
        client.connect(broker, 1883, 600)
    except:
        print("ERROR: Can not connect to MQTT broker")
        return 1

    print("subscribed")

    # create the DB connection and pass it to the callback function
    conn = connectDB(args.f)
    client.user_data_set(conn)

    # the loop_forever cope also with reconnecting if needed
    client.loop_forever()


# this is the standard boilerplate that calls the main() function
if __name__ == '__main__':
    # sys.exit(main(sys.argv)) # used to give a better look to exists
    main()
