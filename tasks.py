from bs4 import BeautifulSoup
import pandas as pd
import psycopg2 as pg
import sqlite3
from sqlalchemy import create_engine

#this is a global variable to be used in task 4 (ensure that update happens just once)
updated=False
#Function to print results
def print_query_result(cur):
   for row in cur.fetchall():
      print(row)

##Task 1:
def task1(cur):
    cur.execute("SELECT visitor_id, revenue FROM Transactions WHERE revenue=(SELECT MAX(revenue) FROM Transactions)")
    print_query_result(cur)

##Task 2:
def task2(cur):
    cur.execute("SELECT date(datetime),SUM(revenue) sumrev FROM Transactions GROUP BY date(datetime) HAVING device_type=(SELECT id from devices WHERE device_name='Mobile Phone') ORDER BY sumrev desc limit 1")
    print_query_result(cur)

##Task 3:
def task3(con):
    df = pd.read_sql("SELECT t1.id,t1.datetime, t1.visitor_id, t1.revenue, t1.tax, t1.device_type,t2.device_name FROM Transactions t1 INNER JOIN Devices t2 ON t1.device_type=t2.id", con)
    df.to_csv('results.csv', index = False)
    print(df)

##Task 4:
def task4(cur):
   global updated
   #Read the data from the xml file provided 
   with open('eurofxref-hist-90d.xml', 'r') as f:
       data = f.read()
    #extract the conversion rate using the beautifulsoup parser
   cur.execute('SELECT DISTINCT date(datetime) FROM transactions ORDER BY date(datetime)')
   rows = cur.fetchall()
   for row in rows:
      if BeautifulSoup(data, "xml").find('Cube',{'time':row[0]})!=None:
         value = BeautifulSoup(data, "xml").find('Cube',{'time':row[0]}).find('Cube',{'currency':'USD'}).get('rate')
      else:
        #default value for missing conversion rates
         value=1.1
      rate=float(value)
      if not updated:
         cur.execute("Update transactions SET revenue= revenue * ? WHERE date(datetime)=?",[rate,row[0]])
         updated=True
   cur.execute("SELECT revenue FROM transactions ORDER BY date(datetime)")
   print_query_result(cur)
   print(updated)
  

#Task 5:
def task5():
   #Importing the data from an other DBMS to our sqlite db as well as exporting the results of the task 3 in a new table 
   #in our postgresql DB
   #connect to the postgresql DB
    conn = pg.connect(
    database="test", user='postgres', password='1111', host='localhost', port= '5432'
    )
    conn.autocommit = True
    cursor = conn.cursor()
    #get the data stored in the table details
    cursor.execute(' ' 'SELECT * FROM details ' ' ')
    Data=cursor.fetchall()
    cols=[]
    for row in cursor.description:
        cols.append(row[0])
    #Store the extracted data in a pandas dataframe
    df = pd.DataFrame(data=Data, columns=cols)
    #print(df)
    #connect to our sqlite db
    con=sqlite3.connect("transactions.db")
    curr=con.cursor()
    #export the data to our sqlite db
    df.to_sql('details', con,if_exists='replace', index = False)
    #to verify
    curr.execute('SELECT * FROM details')
    print("data is successfully imported from the postgresql db to our sqlite db")
    #print_query_result(curr)
    #close connections
    con.close()
    #to insert in a new tab in postgresql
    df = pd.read_csv('results.csv')
    db = create_engine('postgresql://postgres:1111@localhost:5432/test')
    conn = db.connect()
    df = pd.read_csv('results.csv')
    df.to_sql('support', con=conn, if_exists='replace',
      index=False)
    print("Results from task 3 are successfully exported to the postgresql db")
    conn = pg.connect('postgresql://postgres:1111@localhost:5432/test')
    #conn.autocommit = True
    cursor = conn.cursor()
    cursor.execute('select * from support')
    #print_query_result(cursor)
    conn.close()