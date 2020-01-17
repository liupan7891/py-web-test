import pymysql
import pymongo
from my_snowflake import generator
import time
#mongo_client = pymongo.MongoClient('123.56.14.166', 27917)
mongo_client = pymongo.MongoClient('172.17.18.79', 27917)
db = mongo_client['log']
config = {
          #'host':'39.96.70.216',
          'host': '172.17.18.82',
          'port':3306,
          'user':'pxwx',
          'password':'pxwx.7KJdq>*97',
          'database':'erp_db',
          'connect_timeout':30000,
          'charset':"utf8",
          }
# config = {
#           'host':'39.96.70.216',
#           'port':3306,
#           'user':'ready_erp',
#           'password':'Abc.u.7KJdq>*97',
#           'database':'ready_erp',
#           'connect_timeout':30000,
#           'charset':"utf8",
#           }
mysql_db = pymysql.connect(**config)


def get_product_id_main(product_id):
    sql="select product_id,product_id_main from product where product_id=%d" %(product_id)
    cursor = mysql_db.cursor()
    mysql_db.ping(reconnect=True)
    # print("查询class_student:  " + sql)
    cursor.execute(sql)
    # 使用fetall()获取全部数据
    data = cursor.fetchone()
    if data:
        if data[1]==0:
            return data[0]
        else:
            return data[1]
    return None


def get_channel_code_id(channel_code_parameter):
    sql="SELECT channel_code_id FROM `channel_code` where channel_code_parameter='%s'" % (channel_code_parameter)
    cursor = mysql_db.cursor()
    mysql_db.ping(reconnect=True)
    # print("查询class_student:  " + sql)
    cursor.execute(sql)
    # 使用fetall()获取全部数据
    data = cursor.fetchone()
    if data:
        return data[0]
    return None

def get_recomment_id(recommend_phone):
    sql="select recommend_id from recommend where recommend_phone=%s'" %(recommend_phone)
    cursor = mysql_db.cursor()
    mysql_db.ping(reconnect=True)
    # print("查询class_student:  " + sql)
    cursor.execute(sql)
    # 使用fetall()获取全部数据
    data = cursor.fetchone()
    if data:
        return data[0]
    return None


sql='SELECT o.order_id,o.pay_time,od.product_id,s.stu_mobile,o.source,o.`medium` FROM `order` o \
        left JOIN order_detail od on od.order_id=o.order_id \
        left join product p on p.product_id=od.product_id \
        left join class_model cm on cm.class_id=p.class_id \
        left join student s on s.student_id=o.student_id \
        where cm.class_model=5 and o.order_status=2 and od.`status`=1 and pay_time>UNIX_TIMESTAMP(now())-3600 '
cursor = mysql_db.cursor()
mysql_db.ping(reconnect=True)
# print("查询class_student:  " + sql)
cursor.execute(sql)
# 使用fetall()获取全部数据
data = cursor.fetchall()
s=generator(10,10)
localtime = time.asctime( time.localtime(time.time()) )
print('start sync time:',localtime)
for d in data:
    #print(d)
    result=db.burying_log.find_one({"order_id": d[0], "operation_name": "pay_wechat_success"})
    if not result:
        product_id=get_product_id_main(d[2])
        if product_id:
            if d[4]:  #如果是推荐人
                result2 = db.group_burying_log.find_one({"module_id":product_id,"operation_name" : "pay_wechat_success","source":d[4]})
                group_burying_log_id=s.__next__()
                recomment_id=0
                if result2:   #如果已存在这个大类，则修改大类
                    print(result2)

                    #print('hahahah')
                    group_burying_log_id=result2["group_burying_log_id"]
                    recomment_id=result2["source"]
                    #print(group_burying_log_id)

                    db.group_burying_log.update_one({"module_id": product_id, "operation_name": "pay_wechat_success", "source": d[4]},{"$set":{"$inc":"total_count"}})
                else:  #不存在大类，则插入新的大类
                    recomment_id=get_recomment_id(d[4])
                    data={"group_burying_log_id":group_burying_log_id,
                          "operation_type" : 4,
                          "operation_name": "pay_wechat_success",
                          "module_type": 4,
                          "total_count": 1,
                          "module_id": product_id,
                          "source":str(recomment_id),
                          "create_time": d[1],
                          "modify_time": d[1]
                          }
                    print(data)
                    db.group_burying_log.insert_one(data)
                burying_log_data={
                    "burying_id": s.__next__(),
                    "group_burying_log_id": group_burying_log_id,
                    "operation_type": 4,
                    "operation_name": "pay_wechat_success",
                    "module_type": 4,
                    "openid": "",
                    "module_id": product_id,
                    "source": str(recomment_id),
                    "mobile": d[3],
                    "client_type": "3",
                    "order_id": d[0],
                    "order_status": 2,
                    "create_time": d[1]
                }
                print(burying_log_data)
                db.burying_log.insert_one(burying_log_data)


            if d[5]:   #如果是渠道码
                result2 = db.group_burying_log.find_one({"module_id":product_id,"operation_name" : "pay_wechat_success","channel_parameter":d[5]})
                group_burying_log_id=s.__next__()
                medium_id=0
                if result2:   #如果已存在这个大类，则修改大类
                    print(result2)
                    #print('hahahah')
                    group_burying_log_id=result2["group_burying_log_id"]
                    medium_id=result2["medium"]
                    #print(group_burying_log_id)

                    db.group_burying_log.update_one({"module_id": product_id, "operation_name": "pay_wechat_success", "channel_parameter": d[5]},{"$inc":{"total_count":1}})
                else:  #不存在大类，则插入新的大类
                    medium_id=get_channel_code_id(d[5])
                    data={"group_burying_log_id":group_burying_log_id,
                          "operation_type" : 4,
                          "operation_name": "pay_wechat_success",
                          "module_type": 4,
                          "total_count": 1,
                          "module_id": product_id,
                          "channel_parameter": d[5],
                          "medium": str(medium_id),
                          "create_time": d[1],
                          "modify_time": d[1]
                          }
                    db.group_burying_log.insert_one(data)
                    print(data)
                burying_log_data={
                    "burying_id": s.__next__(),
                    "group_burying_log_id": group_burying_log_id,
                    "operation_type": 4,
                    "operation_name": "pay_wechat_success",
                    "module_type": 4,
                    "openid": "",
                    "module_id": product_id,
                    "channel_parameter": d[5],
                    "mobile": d[3],
                    "client_type": "3",
                    "medium": str(medium_id),
                    "order_id": d[0],
                    "order_status": 2,
                    "create_time": d[1]
                }
                print(burying_log_data)
                db.burying_log.insert_one(burying_log_data)
localtime = time.asctime( time.localtime(time.time()) )
print('end sync time:',localtime)