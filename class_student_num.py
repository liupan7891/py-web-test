from my_db import mysql_db

def class_student_clear():
    sql = "update class_model set student_num=0"
    cursor = mysql_db.cursor()
    mysql_db.ping(reconnect=True)
    # print(sql)
    cursor.execute(sql)
    # 使用fetall()获取全部数据
    mysql_db.commit()
    cursor.close()



def class_student_assistant():
    sql = "select class_id,count(*) as num from class_student group by class_id"
    cursor = mysql_db.cursor()
    mysql_db.ping(reconnect=True)
    # print(sql)
    cursor.execute(sql)
    # 使用fetall()获取全部数据
    data = cursor.fetchall()

    mysql_db.commit()
    cursor.close()
    for d in data:
        update_class_student_num(d[1],d[0])
        print(d[1],d[0])

def class_student_teacher():
    sql = "select class_id_main,count(*) as num from class_student group by class_id_main"
    cursor = mysql_db.cursor()
    mysql_db.ping(reconnect=True)
    # print(sql)
    cursor.execute(sql)
    # 使用fetall()获取全部数据
    data = cursor.fetchall()

    mysql_db.commit()
    cursor.close()
    for d in data:
        update_class_student_num(d[1],d[0])
        print(d[1],d[0])

def update_class_student_num(student_num,class_id):
    sql="update class_model set student_num=%d where class_id=%d" % (student_num,class_id)
    cursor = mysql_db.cursor()
    mysql_db.ping(reconnect=True)
    # print(sql)
    cursor.execute(sql)
    # 使用fetall()获取全部数据
    mysql_db.commit()
    cursor.close()

class_student_clear()
class_student_assistant()
class_student_teacher()