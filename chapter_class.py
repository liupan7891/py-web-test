#encoding=utf-8
from my_db import mysql_db
from my_snowflake import generator
import time
import sys



def settlement_start():
    # 获取正常的时间小于当前时间的章节
    chapters = get_chapter_info()

    for chapter_time in chapters:

        class_id=chapter_time[4]
        chapter_class_id=chapter_time[5]

        courses = course_num(class_id)
        class_student_infos=get_class_student(class_id)

        if not class_student_infos:
            #print("cource_id:" + str(chapter_time[3])+" 没有人购买")
            continue

        courcesInfo = get_course_info(chapter_time[3]);
        quarter_mode = courcesInfo[1]
        grade_mode = courcesInfo[3]
        subject_mode = courcesInfo[4]
        course_mode = courcesInfo[6]



        chapter_id = chapter_time[0]
        start_time = chapter_time[1]
        end_time = chapter_time[2]

        settlement_num=get_chapter_count(class_id, start_time)+1

        for class_student_info in class_student_infos:
            product_id = class_student_info[0]
            order_id = class_student_info[1]
            student_id = class_student_info[2]
            class_student_id = class_student_info[3]

            student_settlement_class_count = get_settlement_count(student_id,chapter_class_id)
            if student_settlement_class_count > 0:
                print('error=====================重复结转' + str(class_student_id) + "====存在异常")
                continue;


            #id生成
            s = generator(10, 10)
            settlement_id = s.__next__()

            od = order_detail_price(class_student_info[1], class_student_info[0])
            if not od:
                print('error=====================订单detail' + str(class_student_info[1]) + "====存在异常")
                continue
            product_name = od[2]
            teacher_main_name = od[3]

            order_info = get_order_info(class_student_info[1])
            if not order_info:
                print('error=====================订单' + str(class_student_info[1]) + "====存在异常")
                continue
            # 支付方式
            pay_type = order_info[1]
            if pay_type is None:
                pay_type = 0
            # 支付平台
            pay_os = order_info[0]
            if pay_os is None:
                pay_os = 0

            price = od[0]
            order_detail_id = od[1]
            assistant_code = od[5]
            assistant_name = ""
            stage=""
            if assistant_code != "":
                assistant_name_info = get_assistant_info(od[5])
                if assistant_name_info is not None:
                    assistant_name = assistant_name_info[0]
                    stage = assistant_name_info[1]

            if price is None:
                price=0
            chapter_price = price / courses
            # 获取学生信息
            if not get_student_info(student_id):
                print('error=====================学生' + str(student_id) + "=========class_id:" + str(
                    class_id) + "====存在异常")
                continue
            student_date = get_student_info(student_id)
            student_name = student_date[0]
            student_mobile = str(student_date[1])
            month=time.strftime("%Y-%m", time.localtime())
            year=int(time.strftime("%Y", time.localtime()))
            #添加结转
            insert_settlement(settlement_id,student_id,product_id,order_id,order_detail_id,settlement_num,chapter_price,start_time,end_time,month,str(chapter_id),student_name,student_mobile,year,quarter_mode,grade_mode,subject_mode,product_name,course_mode,teacher_main_name,pay_type,pay_os,class_student_id,str(assistant_name),str(stage))
            #追加chapter_ids
            update_class_student(class_student_id,chapter_id)
        update_chapter_class_status(chapter_class_id)

#查看课次
def course_num(class_id):
    sql="select count(*) from chapter_class where class_id=%d" % (class_id);
    cursor = mysql_db.cursor()
    mysql_db.ping(reconnect=True)
    cursor.execute(sql)
    # 使用fetall()获取全部数据
    data = cursor.fetchone()
    mysql_db.commit()
    cursor.close()
    if not data:
        return None
    return data[0]

def get_class_student(class_id):
    sql = "select product_id,order_id,student_id,class_student_id,chapter_ids from class_student where class_id=%d and `status`=1" % (
    class_id)
    cursor = mysql_db.cursor()
    mysql_db.ping(reconnect=True)
    #print("查询class_student:  " + sql)
    cursor.execute(sql)
    # 使用fetall()获取全部数据
    data = cursor.fetchall()
    if not data:
        return None
    return data

def update_class_student(class_student_id,chapter_id):
    sql="update class_student set chapter_ids=CONCAT(chapter_ids,'%s,') where class_student_id=%d" % (chapter_id,class_student_id)
    #print("追加SQL:"+sql)
    cursor = mysql_db.cursor()
    mysql_db.ping(reconnect=True)
    cursor.execute(sql)
    # 使用fetall()获取全部数据
    mysql_db.commit()
    cursor.close()


def update_chapter_class_status(chapter_class_id):
    sql="update chapter_class set settlement_status=1 where chapter_class_id=%d" % (chapter_class_id)
    cursor = mysql_db.cursor()
    mysql_db.ping(reconnect=True)
    cursor.execute(sql)
    # 使用fetall()获取全部数据
    mysql_db.commit()
    cursor.close()


#查询是否有结转记录
def get_settlement_count(student_id,chapter_class_id):
    sql="select * from settlement where  student_id=%d and chapter_class_id=%d " % (student_id,chapter_class_id);
    print(sql)
    cursor = mysql_db.cursor()
    mysql_db.ping(reconnect=True)
    cursor.execute(sql)
    # 使用fetall()获取全部数据
    data = cursor.fetchall()
    mysql_db.commit()
    cursor.close()

    return len(data)

def insert_settlement(settlement_id,student_id,product_id,order_id,order_detail_id,settlement_num,amount,start_time,end_time,month,remark,student_name,student_mobile,year_detail,quarter_mode,grade_mode,subject_mode,product_name,course_mode,teacher_main_name,pay_type,pay_os,chapter_class_id,assistant_name,stage):
    sql="insert into `settlement`(settlement_id,student_id,product_id,order_id,order_detail_id,settlement_num,amount,settlement_mode,course_start_time,course_end_time,create_time,`month`,remark,student_name,stu_mobile,year_detail,quarter_mode,grade_mode,subject_mode,product_name,course_mode,teacher_main_name,pay_type,pay_os,chapter_class_id,assistant_name,stage)" \
        " VALUES(%d,%d,%d,%d,%d,%d,%.2f,%d,%d,%d,%d,'%s','%s','%s','%s',%d,'%s','%s','%s','%s','%s','%s','%s','%s',%d,'%s','%s')" % (settlement_id,student_id,product_id,order_id,order_detail_id,settlement_num,amount,1,start_time,end_time,int(time.time()),str(month),str(remark),student_name,student_mobile,year_detail,quarter_mode,grade_mode,subject_mode,product_name,course_mode,teacher_main_name,pay_type,pay_os,chapter_class_id,assistant_name,stage)
    print(sql)
    cursor = mysql_db.cursor()
    mysql_db.ping(reconnect=True)
    cursor.execute(sql)
    # 使用fetall()获取全部数据
    mysql_db.commit()
    cursor.close()




def order_detail_price(order_id,product_id):
    sql="select price,order_detail_id,product_name,teacher_name,order_id,assistant_code from order_detail where `status`=1 and product_id=%d and order_id=%d" % (product_id,order_id)
    print("查询订单详情:"+sql)
    cursor = mysql_db.cursor()
    mysql_db.ping(reconnect=True)
    cursor.execute(sql)
    # 使用fetall()获取全部数据
    data = cursor.fetchone()
    mysql_db.commit()
    cursor.close()
    if not data:
        return None
    return data


def get_order_info(order_id):
    sql="select pay_tag_type,pay_type from `order` where  order_id=%d" % (order_id)
    cursor = mysql_db.cursor()
    mysql_db.ping(reconnect=True)
    cursor.execute(sql)
    # 使用fetall()获取全部数据
    data = cursor.fetchone()
    mysql_db.commit()
    cursor.close()
    if not data:
        return None
    return data


def get_chapter_info():
    sql="select chapter_id,start_time,end_time,course_id,class_id,chapter_class_id from chapter_class where  start_time<unix_timestamp(NOW()) and `status`=1 and settlement_status=0 order by start_time asc"
    print("查询未结转的课:"+sql)
    cursor = mysql_db.cursor()
    mysql_db.ping(reconnect=True)
    cursor.execute(sql)
    # 使用fetall()获取全部数据
    data = cursor.fetchall()
    mysql_db.commit()
    cursor.close()
    return data

#获取是第几节课
def get_chapter_count(class_id,start_time):
    sql="select * from chapter_class where  start_time<%d and class_id=%d and `status`=1  order by start_time asc" % (start_time,class_id);
    print(sql)
    cursor = mysql_db.cursor()
    mysql_db.ping(reconnect=True)
    cursor.execute(sql)
    # 使用fetall()获取全部数据
    data = cursor.fetchall()
    mysql_db.commit()
    cursor.close()

    return len(data)


#获取学生信息
def get_student_info(student_id):
    sql="select stu_name,stu_mobile from student where student_id=%d" % (student_id);
    cursor = mysql_db.cursor()
    mysql_db.ping(reconnect=True)
    cursor.execute(sql)
    # 使用fetall()获取全部数据
    data = cursor.fetchone()
    mysql_db.commit()
    cursor.close()
    if not data:
        return None
    return data

#获取课程信息
def get_course_info(course_id):
    sql="select course_name,quarter_mode,part_mode,grade_mode,subject_mode,class_mode,course_mode from course where course_id=%d" % (course_id);
    cursor = mysql_db.cursor()
    mysql_db.ping(reconnect=True)
    cursor.execute(sql)
    # 使用fetall()获取全部数据
    data = cursor.fetchone()
    mysql_db.commit()
    cursor.close()
    if not data:
        return None
    return data

#获取助教老师姓名
def get_assistant_info(assistant_code):
    sql="select u.user_nickname,cm.stage from class_model cm,user u where cm.assistant_id=u.user_id and assistant_code='%s'" % (assistant_code);
    print("查询助教名称:"+sql)
    cursor = mysql_db.cursor()
    mysql_db.ping(reconnect=True)
    cursor.execute(sql)
    # 使用fetall()获取全部数据
    data = cursor.fetchone()
    mysql_db.commit()
    cursor.close()
    if not data:
        return None
    return data

settlement_start()