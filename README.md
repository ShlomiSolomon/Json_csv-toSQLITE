# Json_csv-toSQLITE

This project will recieve a json or csv file and create (or update) table in the database (SQLITE)
This program contains three modules : sender, receiver and graph , which connected by RabbitMQ (Message queue).


The sender module get file path and send a json message to MQ with three parameters : file path, file name and file format .
The receiver module listens to the first queue and load the file into specific table in the "database.db" .
The graph module listens to the second queue and make a real-time graph from the table that load in the receiver , The graph show total sales and number of costumers from each month from the "database.db" .

# Instructions :
Run receiver.py , now both Receiver.py and sender.py will run , and you need to input the file path (you can use the files from the program). now the database will create/update

Run graph.py , now you can watch the graph.


You can run sender.py many times with different file path.
NOTHE :
If you use the same table name the data in the table will update in the DB as well as the graph, if you use a new name to the table the receiver will create another table in the DB as well as the graph .

## Built With :
RabbitMQ - The message queue
SQLite - Database managment Ver. 3
Pandas - Python Data Analysis Library
Python Ver. 3.7


## Author:
Shlomi Solomon
