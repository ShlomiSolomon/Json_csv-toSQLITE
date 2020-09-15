import pika
import json
import sqlite3
from sqlite3 import Error
from sender import stringJsonMsg
import pandas as pd


# define a new function to init the key-value of the json msg and call to :db build

def parse_recievedMsg(data):
    json_data = json.loads(data)
    file_path = json_data['file-path']
    Format = json_data['file-format']
    table_name = json_data['table-name']
    db_build(file_path,Format,table_name)


# define a new function to create a connection and then to run the query
def db_build(file_path, format, table_name):
    # load data
    if format == 'json':
        df = pd.read_json(file_path)
    else:
        df = pd.read_csv(file_path)
    # strip whitespace from headers
    df.columns = df.columns.str.strip()
    # sort headers by name
    df = df.sort_index(axis=1)
    try:
        conn = sqlite3.connect("database.db")
    # drop data into database
        df.to_sql(table_name, conn, if_exists='append')
        conn.close()
    except Error as e:
        print(e)

# build the message as string in json structure using the array and the path const  ,'table name':table_name
stringJsongraph = {'table-name': table_name}
# convert the string into json valid format
convertStringToJsonData = json.dumps(stringJsongraph)
print(convertStringToJsonData)

                                                        # # # MQ # # #

connection = pika.BlockingConnection( pika.ConnectionParameters(host= 'localhost' ) )
channel = connection.channel()

channel.queue_declare(queue='message')

def callback(ch,method,properties,body):
    data = body
    parse_recievedMsg(data)
    print(("[x] received %s" % data))


channel.basic_consume(queue='message', auto_ack=True, on_message_callback=callback)
print( '[*] Waiting for msgs' )
try:
    channel.start_consuming()
finally:
    connection.close()


#
# channel = connection.channel()
# #declare on the queue name we are going to listen to on the recieve.py class
# channel.queue_declare(queue='graph')
# #public the channel with the given info/params
# #exchange    == (str or unicode) – The exchange to publish to
# #routing_key == (str or unicode) – The routing key to bind on
# #body        == (str or unicode) – The message body
# channel.basic_publish(exchange='', routing_key='graph', body=convertStringToJsonData)
# print("[x] sent end of load and run %s graph " %convertStringToJsonData )
# #close the connection when finish
# try:
#     channel.start_consuming()
# finally:
#     connection.close()
