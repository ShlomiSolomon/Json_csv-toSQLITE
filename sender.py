import pika
import json

# THIS .PY CLASS SENDS A JSON MSG TO THE MQ WITH FILE PATH AND WITH ...

# C:\Test\invoices_2012.csv
# db path : user have to send the full path
#NOTE: if the path will look like that  : /user/Folder/filename , then we can't reach the file and won't know the format.
#So, please send the path that way :      /user/Folder/filename.json in order to connect the existing database
File_Path = input("Please enter file path: ")

FORMAT = input("Please enter the file format (json or csv) : ")

while FORMAT != 'csv' and FORMAT!= 'json' :
    FORMAT = input("Please enter valid format (json or csv) only : ")

table_name = input("Please enter the table name : ")
# build the message as string in json structure using the array and the path const  ,'table name':table_name
stringJsonMsg = {'file-path': File_Path, 'file-format': FORMAT, 'table-name': table_name}
# convert the string into json valid format
convertStringToJsonData = json.dumps(stringJsonMsg)

                                       # # # MQ # # #
# connect to localhost using pika : Pika is a pure-Python implementation of the AMQP 0-9-1 protocol
connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
channel = connection.channel()
#declare on the queue name we are going to listen to on the recieve.py class
channel.queue_declare(queue='db_conn')
#public the channel with the given info/params
#exchange    == (str or unicode) – The exchange to publish to
#routing_key == (str or unicode) – The routing key to bind on
#body        == (str or unicode) – The message body
channel.basic_publish(exchange='', routing_key='db_conn', body=convertStringToJsonData)
print("[x] sent %s" % convertStringToJsonData)
#close the connection when finish
connection.close()
