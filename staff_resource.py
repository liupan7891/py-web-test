#encoding=utf-8
from my_db import mysql_db
from my_snowflake import generator
import time

def select_reco():
    sql = "SELECT  st.staff_id from staff st join  recommend  r  on st.moblie = r.recommend_phone  "

    cursor = mysql_db.cursor()
    mysql_db.ping(reconnect=True)
    # print(sql)
    cursor.execute(sql)
    # 使用fetall()获取全部数据
    data = cursor.fetchall()
    print(data)

    mysql_db.commit()
    cursor.close()
    s = generator(10, 10)
    for d in data:
        # id生成
        staff_manger_resource_id = s.__next__()
        #print(staff_manger_resource_id)
        staff_id = d[0]
        print(staff_id)
        inser_staff_manger_resource(staff_manger_resource_id,staff_id,2)


def select_user():
    sql = "SELECT   st.staff_id from staff st left join  `user`  u  on st.moblie= u.mobile  "

    cursor = mysql_db.cursor()
    mysql_db.ping(reconnect=True)
    # print(sql)
    cursor.execute(sql)
    # 使用fetall()获取全部数据
    data = cursor.fetchall()
    print(data)

    mysql_db.commit()
    cursor.close()
    s = generator(10, 10)
    for d in data:
        # id生成
        staff_manger_resource_id = s.__next__()
        #print(staff_manger_resource_id)
        staff_id = d[0]
        print(staff_id)
        inser_staff_manger_resource(staff_manger_resource_id,staff_id,1)


def inser_staff_manger_resource(staff_manger_resource_id,staff_id,manger_resource_id):
    sql = "insert into `staff_manger_resource`(staff_manger_resource_id,manger_resource_id,staff_id,create_uid,modify_uid,create_time,modify_time) " \
          "VALUES (%d,%d,%d,%d,%d,%d,%d)" % (staff_manger_resource_id, manger_resource_id, staff_id, staff_id, staff_id, time.time(), time.time())
    print(sql)
    cursor = mysql_db.cursor()
    mysql_db.ping(reconnect=True)
    cursor.execute(sql)
    # 使用fetall()获取全部数据
    mysql_db.commit()
    cursor.close()


select_reco()#给分销员刷登陆资源mms 数据
#select_user()#给erp用户刷登陆资源登陆erp资源数据 数据