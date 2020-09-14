import pika
import json
import os
import sqlite3
from sqlite3 import Error
from sender import FORMAT
import csv
import platform
from sender import stringJsonMsg
import pandas as pd


# define empty string for the received msg
DATA = stringJsonMsg
# define the File_Path string value
File_Path = ''
#define the FORMAT of the output
FORMAT = ''
#define the table name
table_name = ''

# define a new function to init the key-value of the json msg and call to :create_connection_and_run_query

def parse_recievedMsg_and_CreateConnToDb(data):
    json_data = json.loads(data)
    File_Path = json_data['file-path']
    FORMAT = json_data['file-format']
    table_name = json_data['table-name']
    db_build(File_Path,FORMAT,table_name)


# define a new function to create a connection and then to run the query
def db_build(File_Path,FORMAT,table_name):
    # load data
    if (FORMAT == 'json'):
        df = pd.read_json(File_Path)
    else:
        df = pd.read_csv(File_Path)
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




                                                        # # # MQ # # #

connection = pika.BlockingConnection( pika.ConnectionParameters(host= 'localhost' ) )
channel = connection.channel()

channel.queue_declare(queue='db_conn')

def callback(ch,method,properties,body):
    DATA = body
    parse_recievedMsg_and_CreateConnToDb(DATA)
    print(("[x] received %s" % DATA))

channel.basic_consume(queue='db_conn', auto_ack=True, on_message_callback=callback)
print( '[*] Waiting for msgs' )
try:
    channel.start_consuming()
finally:
    connection.close()

#                                        # # # MQ # # #
# # connect to localhost using pika : Pika is a pure-Python implementation of the AMQP 0-9-1 protocol
# connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
# channel = connection.channel()
# #declare on the queue name we are going to listen to on the recieve.py class
# channel.queue_declare(queue='graph')
# #public the channel with the given info/params
# #exchange    == (str or unicode) – The exchange to publish to
# #routing_key == (str or unicode) – The routing key to bind on
# #body        == (str or unicode) – The message body
# channel.basic_publish(exchange='', routing_key='graph', body="end of load")
# print("[x] sent end of load")
# #close the connection when finish
# connection.close()
#
