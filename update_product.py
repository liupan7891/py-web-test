from my_db import mysql_db
def class_model():
    sql = "SELECT product_id,product_service FROM `product` where product_service is not null and LENGTH(product_service)>3"
    cursor = mysql_db.cursor()
    mysql_db.ping(reconnect=True)
    # print(sql)
    cursor.execute(sql)
    # 使用fetall()获取全部数据
    data = cursor.fetchall()

    mysql_db.commit()
    cursor.close()
    for d in data:
        str_service=d[1]
        if str_service[0]==',':
            print(str_service)
            str_service=str_service[1:]
            update_product(d[0],str_service)
            print(str_service)


def update_product(product_id,product_service):
    sql="update product set product_service='%s'  where product_id=%d" %(product_service,product_id)
    cursor = mysql_db.cursor()
    mysql_db.ping(reconnect=True)
    # print(sql)
    cursor.execute(sql)
    # 使用fetall()获取全部数据
    mysql_db.commit()
    cursor.close()

class_model()