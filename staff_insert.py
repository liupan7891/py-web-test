import csv
from my_db import mysql_db
from my_snowflake import generator
import time


read_line = csv.reader(open('user.csv',mode='r', encoding='UTF-8'))
read_recommend_line = csv.reader(open('recommend.csv',mode='r', encoding='UTF-8'))
read_re_line = csv.reader(open('recommend_20190529163634.csv',mode='r', encoding='UTF-8'))



s = generator(10, 10)

def read_user_file():

    for user_id, last_login_time, user_status, user_login, user_pass, user_email, last_login_ip, mobile, create_uid, modify_uid, create_time, modify_time in read_line:
        staff_id = s.__next__()
        if(user_email is ''):
            user_email = mobile +'@pxjy.com'

        print(user_status)
        if(user_status is '1'):

            try:
                inser_staff(staff_id, user_login, mobile, user_email, user_pass, last_login_ip, last_login_time, staff_id, staff_id, time.time(), time.time(),user_status)

            except Exception as e :
                print(e)


def read_recommend_file():

    for recommend_id,recommend_name,recommend_phone,status,create_uid,modify_uid, create_time,modify_time in read_recommend_line:
        staff_id = s.__next__()
        user_email = recommend_phone + '@pxjy.com'
        print(recommend_name)
        if(status is '1'):
            try:
                inser_staff(staff_id, recommend_name, recommend_phone, user_email, '', '', 0, staff_id, staff_id, time.time(), time.time(),status)
            except Exception as e :
                print(e)

def inser_staff(staff_id, name, moblie, email, password, last_login_ip, last_login_time, create_uid, modify_uid, create_time, modify_time, user_status):
    cursor = mysql_db.cursor()
    mysql_db.ping(reconnect=True)
    print(staff_id, name, moblie, email, password, last_login_ip, last_login_time, create_uid, modify_uid, create_time, modify_time, user_status)
    sql = "insert into `staff`(`staff_id`, `name`, `moblie`, `email`, `password`, `last_login_ip`, last_login_time, `create_uid`, `modify_uid`, `create_time`, `modify_time`, `status`)" \
          " values  ('%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s')" % (staff_id,name,moblie,email,password,last_login_ip, last_login_time,create_uid,modify_uid,create_time,modify_time,user_status);
    print(sql)
    cursor = mysql_db.cursor()
    mysql_db.ping(reconnect=True)
    cursor.execute(sql)
    # 使用fetall()获取全部数据
    mysql_db.commit()
    cursor.close()


def read_re_file():
    index = -1
    for mobile,user_name in read_re_line:

        staff_id = s.__next__()
        staff_manger_resource_id = s.__next__()
        print(staff_id)
        print(staff_manger_resource_id)
        user_email = mobile + '@pxjy.com'
        print(mobile)
        if  index == 1:
            try:
                inser_staff(staff_id, user_name, mobile, user_email, '', '', 0, staff_id, staff_id, time.time(),
                            time.time(), 1)
                inser_staff_manger_resource(staff_manger_resource_id, staff_id, 2)

            except Exception as e :
                print(e)

        index = 1



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


#read_user_file()#导入erp用户数据
#read_recommend_file()#导入分销员数据
#read_re_file()#从文件导入到分销员

