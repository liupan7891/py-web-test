import pymongo
mongo_client = pymongo.MongoClient('123.56.14.166', 27917)
db = mongo_client['log']
data = db.group_burying_log.aggregate([{"$group":{"_id":{"channel_parameter":"$channel_parameter","module_id":"$module_id","module_type":"$module_type","operation_name":"$operation_name"},"total":{"$sum":1}}},{"$match":{"total":{"$gt":1}}}])
for d in data:
    list = db.group_burying_log.find(d["_id"])
    i=0
    id = None
    group_burying_log_id=None
    for l in list:
        if i==0:
            id=l["_id"]
            group_burying_log_id=l["group_burying_log_id"]
            print(id)
        else:
            count=l["total_count"]
            db.group_burying_log.update_one({"_id":id},{"$inc":{"total_count":count}})
            db.group_burying_log.delete_one({"_id":l["_id"]})
            print(l["total_count"],id,l["_id"],group_burying_log_id)
        i=i+1
    query_data=d["_id"]
    #print(query_data)
    db.burying_log.update_many(query_data,{"$set":{"group_burying_log_id":group_burying_log_id}})
    #for s in dd:
     #   print(s)
    print(d)