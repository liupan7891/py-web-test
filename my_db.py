import pymysql

config = {
          'host':'192.168.0.216',
          'port':3306,
          'user':'pxwx',
          'password':'pxwx.7KJdq>*97',
          'database':'online2_db',
          'connect_timeout':30000,
          'charset':"utf8",
          }
mysql_db = pymysql.connect(**config)