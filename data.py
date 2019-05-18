#!/usr/bin/env python

import mysql.connector


def connect():
    print("here");
    cnx = mysql.connector.connect(user='davidisenberg', password='vpnzv8zy',
                                  host='127.0.0.1',
                                  database='ff')

    try:

       cursor = cnx.cursor()
       cursor.execute("""
          select FlyFrom, count(*) as count from Directs group by FlyFrom
       """)
       #result = cursor.fetfetchall()
       #print(result);

       for (FlyFrom, count) in cursor:
           print("{}: {}".format(
               FlyFrom, count))
    finally:
        print("ending")
        cursor.close()
        cnx.close()


connect()