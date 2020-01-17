
import csv
import bson

from my_db import mysql_db
from pymongo import MongoClient


conn = MongoClient('123.56.14.166', 27917)
my_db = conn.log
collection = my_db.subscribe_export_records
read_line = csv.reader(open('daochu.csv',mode='r', encoding='UTF-8'))



def admin_resource():


    for user in read_line:
        print(user[1])
        if (user[0] != '_id'):

            sql = "SELECT user_login from user where user_id= "+ user[1] + " or staff_id=" +user[1]+" limit 1"

            cursor = mysql_db.cursor()
            mysql_db.ping(reconnect=True)
            print(sql)
            cursor.execute(sql)
            data = cursor.fetchall()
            print(data[0][0])

            userName = data[0][0];

            try:

                print(bson.int64.Int64(user[1]))
                if(len(data)>0):
                    collection.update_many({"create_uid":bson.int64.Int64(user[1])},{"$set":{"create_uname":userName}})

                #data = collection.find({"create_uid":bin(user[1])})
                #print(data[0])


            except Exception as e:
                print(e)

            mysql_db.commit()
            cursor.close()





def recommend():
    results = collection.aggregate([{ "$match" : { "system_name" :"PMS"}},{ "$group" : { "_id" : { "create_uid" : "$create_uid" },"id" : { "$max" : "$_id" } } }])
    for i in results:

        print(bson.objectid(i[0]))


recommend()