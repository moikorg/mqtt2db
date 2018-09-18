import paho.mqtt.client as mqtt
import argparse
import configparser
import pymysql
import datetime
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
                                   charset = 'utf8')
    return conn


def parseTheArgs() -> object:
    parser = argparse.ArgumentParser(description='Listen to specific messages on MQTT and write them to DB')
    parser.add_argument('-d', dest='verbose', action='store_true',
                        help='print debugging information')
    parser.add_argument('-f', help='path and filename of the config file, default is ./config.rc',
                        default='./config.rc')

    args = parser.parse_args()
    return args





# The callback for when a PUBLISH message is received from the server.
def on_message(client, userdata, message):
    print("Received message '" + str(message.payload) + "' on topic '"
          + message.topic + "' with QoS " + str(message.qos))
#    print("topic = ", str(message.topic))
#    print("received message = ",str(message.payload.decode("utf-8")))

    json1_data = json.loads(message.payload)[0]
#    ts = datetime.datetime.fromtimestamp(ts_epoch).strftime('%Y-%m-%d %H:%M:%S')
    print (json1_data)

    c = userdata.cursor()

    sql = "INSERT INTO temp_sensor (timestamp, topic, value) VALUES (%s, %s, %s);"
    c.execute(sql, ('2018-09-13', message.topic, float(message.payload)))
    userdata.commit()


def main():
    args = parseTheArgs()
    broker="docker.moik.org"
    client= mqtt.Client("mqtt2db") #create client object client1.on_publish = on_publish #assign function to callback client1.connect(broker,port) #establish connection client1.publish("house/bulb1","on")

    #######Bind function to callback
    client.on_message=on_message

    print("connecting to broker ",broker)
    client.username_pw_set("sonoff", "***REMOVED***")
    #todo: try catch .... if the broker can not be connected
    client.connect(broker)

    # create the DB connection and pass it to the callback function
    conn = connectDB(args.f)

    client.user_data_set(conn)

    #subscribe
    print("subscribing ")
    #client.subscribe("#")   #subscribe
    client.subscribe([("sensor/temperature", 0), ("sensor/humidity", 0),
                      ("tele/sensor/temperature", 0), ("tele/sensor/humidity", 0),
                      ("tele/sensor/temphum", 0)])

    run = True
    while run:
        client.loop()


# this is the standard boilerplate that calls the main() function
if __name__ == '__main__':
    # sys.exit(main(sys.argv)) # used to give a better look to exists
    main()
