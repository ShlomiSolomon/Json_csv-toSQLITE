import sqlite3
import pika
from matplotlib import style
import matplotlib.pyplot as plt
import matplotlib.animation as animation
import json


def parse_recieveddb(data):
    json_data = json.loads(data)
    table_name = json_data['table-name']
    graph_build(table_name)


def graph_build(table_name):
    style.use(['seaborn'])
    fig = plt.figure()
    ax1 = fig.add_subplot(1, 1, 1)

    def animate(i):
        conn = sqlite3.connect("database.db")
        cur = conn.cursor()
        #get month/year
        cur.execute("SELECT DISTINCT strftime('%m\%Y', InvoiceDate) FROM "+ table_name)
        date_data = cur.fetchall()

        #get total by date per month
        cur.execute("SELECT SUM(Total) FROM "+table_name+" GROUP BY (strftime('%m\%Y', InvoiceDate)) ")
        total_data = cur.fetchall()

        #get number of שctive customer per month
        cur.execute("SELECT COUNT(CustomerId) FROM "+table_name+" GROUP BY (strftime('%m\%Y', InvoiceDate))")
        active_customer_data = cur.fetchall()

        dates = []
        values = []
        customer = []

        for row in date_data:
            dates.append(row[0])

        for row in total_data:
            values.append(row[0])

        for row in active_customer_data:
            customer.append(row[0])
        ax1.clear()
        ax1.plot(dates, values,marker = 'o', label='Sales')
        ax1.plot(dates, customer,marker = 'o', label='Customers')
        plt.title(table_name)
        plt.legend( loc='upper left')
    ani = animation.FuncAnimation(fig, animate, interval=1000,repeat=True)
    plt.show()


       # # # MQ # # #

connection = pika.BlockingConnection( pika.ConnectionParameters(host= 'localhost' ) )
channel = connection.channel()

channel.queue_declare(queue='graph')

def callback(ch,method,properties,body):
    Data = body
    print(("[x] received %s" % Data))
    parse_recieveddb(Data)

channel.basic_consume(queue='graph', auto_ack=True, on_message_callback=callback)
print( '[*] Waiting for msgs' )
try:
    channel.start_consuming()
finally:
    connection.close()
