import requests
import xlrd
import json
url = 'https://manage.puxinwangxiao.com/api/v1.0/order/admin_add_order'

# products1=[{
#                 "product_id": 40445800049057792,
#                 "class_id": 24454507082850304
#                 },{
#                 "product_id": 40445341875871744,
#                 "class_id": 24158343297015808
#                 },{
#                 "product_id": 40445208178237440,
#                 "class_id": 24191190107987968
#                 }
#             ]

products1=[{
                "product_id": 40816869821947904,
                "class_id": 31386490354573312
                },{
                "product_id": 40816047394430976,
                "class_id": 31386531781713920
                },{
                "product_id": 40819600259325952,
                "class_id": 31391361514381312
                }
            ]

def add_order(mobile, username, products):
    data = {'mobile': mobile,
            'username': username,
            'ori_price': 99,
            'price': 99,
            'remark': '4.28山东临沂的线下报名',
            'pay_type': 4,
            'third_other_code': '',
            'products': products
            }
    headers = {'content-type': 'application/json',
               'User-Agent': 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:22.0) Gecko/20100101 Firefox/22.0',
               'authorization': 'bearer;eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJuYW1lIjoi6YOt5qKm6Iy5IiwidXNlcmlkIjoiNDA4NDU0NjY5Mzg5NDE0NDAiLCJpc3MiOiJyZXN0YXBpdXNlciIsImF1ZCI6IjA5OGY2YmNkNDYyMWQzNzNjMjEzNGU4MzI2MjdiNGY2In0.1nSQxMTg4H8l26r-yWYeoNZOplQaVLZKmb9aV2LDceI'}

    r = requests.post(url, data=json.dumps(data), headers=headers)
    print(r.text)


wb = xlrd.open_workbook(filename=r'E:/order.xls')
sheet1 = wb.sheet_by_index(0)  # 通过索引获取表格
for row in sheet1.get_rows():
    if row[0].value != sheet1.row_values(0)[0]:
        username = str(row[1].value).strip()
        mobile = str(int(row[2].value)).strip()
        classname = str(row[7].value).strip()
        if(classname=='2019新四年级暑假特价班一期'):
            print(username,mobile,classname,products1)
            add_order(mobile,username,products1)
        # else:
        #     print(username,mobile,classname,products2)
        #     add_order(mobile, username, products2)
