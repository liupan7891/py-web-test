import pymysql

config = {
          'host':'39.96.70.216',
          'port':3306,
          'user':'ready_erp',
          'password':'Abc.u.7KJdq>*97',
          'database':'ready_erp',
          'connect_timeout':30000,
          'charset':"utf8",
          }
mysql_db = pymysql.connect(**config)