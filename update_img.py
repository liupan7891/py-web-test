from my_db import mysql_db


def course():
    sql = "select course_id,app_img from course where app_img like '%coursemanage%'"
    cursor = mysql_db.cursor()
    mysql_db.ping(reconnect=True)
    # print(sql)
    cursor.execute(sql)
    # 使用fetall()获取全部数据
    data = cursor.fetchall()

    mysql_db.commit()
    cursor.close()
    for d in data:

        img=str(d[1]).split('/')
        my_img=''
        if len(img)>0:
            my_img='https://resource.puxinwangxiao.com/'+img[len(img)-1]
        update_course(d[0],my_img)
        print(d[0],my_img)

def update_course(course_id,img):
    sql="update course set app_img='%s',pc_img='%s',other_img='%s' where course_id=%d" %(img,img,img,course_id)
    cursor = mysql_db.cursor()
    mysql_db.ping(reconnect=True)
    # print(sql)
    cursor.execute(sql)
    # 使用fetall()获取全部数据
    mysql_db.commit()
    cursor.close()


def product():
    sql = "select product_id,product_app_img from product where product_app_img like '%coursemanage%'"
    cursor = mysql_db.cursor()
    mysql_db.ping(reconnect=True)
    # print(sql)
    cursor.execute(sql)
    # 使用fetall()获取全部数据
    data = cursor.fetchall()

    mysql_db.commit()
    cursor.close()
    for d in data:

        img=str(d[1]).split('/')
        my_img=''
        if len(img)>0:
            my_img='https://resource.puxinwangxiao.com/'+img[len(img)-1]
            update_product(d[0],my_img)
        print(d[0],my_img)

def update_product(course_id,img):
    sql="update product set product_app_img='%s',product_pc_img='%s',product_other_img='%s' where product_id=%d" %(img,img,img,course_id)
    cursor = mysql_db.cursor()
    mysql_db.ping(reconnect=True)
    # print(sql)
    cursor.execute(sql)
    # 使用fetall()获取全部数据
    mysql_db.commit()
    cursor.close()


course()
product()