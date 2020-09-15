import pika
import json
import os


# THIS .PY CLASS SENDS A JSON MSG TO THE MQ WITH FILE PATH FILE NAME AND FILE FORMAT

# db path : user have to send the full path
#NOTE: if the path will look like that  : /user/Folder/filename , then we can't reach the file and won't know the format.
#So, please send the path that way :      /user/Folder/filename.json in order to connect the existing database
File_Path = input("Please enter file path: ")

#get the file format from the path
Format = os.path.splitext(File_Path)[1][1:].strip()

while Format != 'csv' and Format!= 'json' :
    File_Path = input("Please enter valid file path (json or csv only) : ")
    Format = os.path.splitext(File_Path)[1][1:].strip()

#the name of the table in default is the file name , but you can use input like here
# table_name = input("Please enter the table name : ")
Table_name =  os.path.basename(File_Path)
Table_name = os.path.splitext(Table_name)[0][0:].strip()

# build the message as string in json structure using the array and the path const  ,'table name':table_name
stringJsonMsg = {'file-path': File_Path, 'file-format': Format, 'table-name': Table_name}
# convert the string into json valid format
convertStringToJsonData = json.dumps(stringJsonMsg)

                                       # # # MQ # # #
# connect to localhost using pika : Pika is a pure-Python implementation of the AMQP 0-9-1 protocol
connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
channel = connection.channel()
#declare on the queue name we are going to listen to on the recieve.py class
channel.queue_declare(queue='message')
#public the channel with the given info/params
#exchange    == (str or unicode) – The exchange to publish to
#routing_key == (str or unicode) – The routing key to bind on
#body        == (str or unicode) – The message body
channel.basic_publish(exchange='', routing_key='message', body=convertStringToJsonData)
print("[x] sent %s" % convertStringToJsonData)
#close the connection when finish
connection.close()
