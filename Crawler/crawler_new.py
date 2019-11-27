#!/usr/bin/python3

import mysql.connector
from mysql.connector import Error



#https://pynative.com/python-mysql-select-query-to-fetch-data/

try:
    connection = mysql.connector.connect(host='localhost',
                                         database='testdb',
                                         user='root',
                                         password='HDBWwinf')

    sql_select_Query = "select * from keywords"
    cursor = connection.cursor()
    cursor.execute(sql_select_Query)
    records = cursor.fetchall()
    print("Total number of rows in Laptop is: ", cursor.rowcount)

    print("\nPrinting each laptop record")
    for row in records:
        print("BLA = ", row[0], )
        print("Name = ", row[1])
except Error as e:
    print("Error reading data from MySQL table", e)
finally:
    if (connection.is_connected()):
        connection.close()
        cursor.close()
        print("MySQL connection is closed")
