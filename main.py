import sqlite3
import tasks as tsk


con = sqlite3.connect("transactions.db")
cur = con.cursor()
#updated=False
##Task 1:
print("***************Task 1: Visitor with highest revenue:***************")
tsk.task1(cur)
##Task 2:
print("****************Task 2: Day with highest revenue for mobile phone users:**************")
tsk.task2(cur)
##Task 3:
print("****************Task 3: Join 2 tables:***************")
tsk.task3(con)
##Task 4:
print("***********Task 4: currency conversion:**************")
tsk.task4(cur)
con.close()
#Task 5:
print("************Task 5: exchange data between a Postgresql DB and an sqlite db:************")
tsk.task5()