
import parser
import sqlite3
from sqlite3 import Error
import matplotlib.pyplot as pp
import matplotlib.dates as mdates
import matplotlib.ticker as mticker
import matplotlib.animation as animation
from IPython import get_ipython
from matplotlib import style
import matplotlib.pyplot as plt
import matplotlib.animation as animation
import pika
import pandas as pd
import datetime

style.use(['seaborn'])

fig = plt.figure()

ax1 = plt.subplot(1,1,1)


conn = sqlite3.connect("database.db")
cur = conn.cursor()

# all_tables =

#get month/year
cur.execute("SELECT DISTINCT strftime('%m\%Y', InvoiceDate) FROM myy  ")
date_data = cur.fetchall()

#get total by date per month
cur.execute("SELECT SUM(Total) FROM myy GROUP BY (strftime('%m\%Y', InvoiceDate)) ")
total_data = cur.fetchall()

#get number of שctive customer per month
cur.execute("SELECT COUNT(CustomerId) FROM myy GROUP BY (strftime('%m\%Y', InvoiceDate))")
active_customer_data = cur.fetchall()
print (active_customer_data)
print (total_data)
print (date_data)


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
ax1.plot(dates,values,customer)


plt.show()

#        # # # MQ # # #
#
# connection = pika.BlockingConnection( pika.ConnectionParameters(host= 'localhost' ) )
# channel = connection.channel()
#
# channel.queue_declare(queue='graph')
#
# def callback(ch,method,properties,body):
#     DATA = body
#     print(("[x] received json %s" % DATA))
#
# channel.basic_consume(queue='graph', auto_ack=True, on_message_callback=callback)
# print( '[*] Waiting for msgs' )
# try:
#     channel.start_consuming()
# finally:
#     connection.close()