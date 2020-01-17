#encoding=utf-8
from my_db import mysql_db
from my_snowflake import generator
import time
import csv

read_mobile_line = csv.reader(open('mobile.csv',mode='r', encoding='UTF-8'))
read_fams_line = csv.reader(open('fams.csv',mode='r', encoding='UTF-8'))

def admin_resource():
    sql = "SELECT   distinct st.staff_id from staff st left join  `user`  u  on st.moblie= u.mobile  where u.user_type =1 or u.user_login ='admin'"

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
        try:
            inser_staff_manger_resource(staff_manger_resource_id,staff_id,8)
            inser_staff_manger_resource(staff_manger_resource_id, staff_id, 9)

        except Exception as e:
            print(e)




def flush_staff_resource_mms():
    for mobile in read_mobile_line:
        print(mobile[0])
        if(mobile[0] !='mobile'):
            sql = "SELECT  st.staff_id from staff st where st.moblie= '"+ mobile[0] +"'"

            print(sql)
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
                # print(staff_manger_resource_id)
                staff_id = d[0]
                print(staff_id)
                try:
                    inser_staff_manger_resource(staff_manger_resource_id, staff_id,2)

                except Exception as e:
                    print(e)

def flush_staff_resource_ems():
    for mobile in read_mobile_line:
        print(mobile[0])
        if(mobile[0] !='mobile'):
            sql = "SELECT  st.staff_id from staff st where st.moblie= '"+ mobile[0] +"'"

            print(sql)
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
                # print(staff_manger_resource_id)
                staff_id = d[0]
                print(staff_id)
                try:
                    inser_staff_manger_resource(staff_manger_resource_id, staff_id,9)

                except Exception as e:
                    print(e)
                except Exception as e:
                    print(e)

flush_staff_resource_ems()

flush_staff_resource_ems()
