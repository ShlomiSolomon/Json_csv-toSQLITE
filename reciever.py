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


connection = pika.BlockingConnection( pika.ConnectionParameters(host= 'localhost' ) )
channel = connection.channel()

channel.queue_declare(queue='message')

def callback(ch,method,properties,body):
    data = body
    parse_recievedMsg(data)
    print(("[x] received %s" % data))
    json_data = json.loads(data)
    table_name = json_data['table-name']
    stringJsongraph = {'table-name': table_name}
    # convert the string into json valid format
    convertStringToJsonData = json.dumps(stringJsongraph)
    channel.basic_publish(exchange='', routing_key='graph', body=convertStringToJsonData)
    print("[x] sent end of load and run %s graph " %convertStringToJsonData )


channel.basic_consume(queue='message', auto_ack=True, on_message_callback=callback)
print( '[*] Waiting for msgs' )
try:
    channel.start_consuming()
finally:
    connection.close()
