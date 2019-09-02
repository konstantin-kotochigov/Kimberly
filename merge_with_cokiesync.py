import pandas
import time
import datetime

from pyspark.sql.types import ArrayType, StringType

def cj_id(cj_ids, arg_id, arg_key=-1):
        result = []
        for id in cj_ids['uids']:
            if id['id'] == arg_id and id['key'] == arg_key:
                result += [id['value']]
        return result

def cj_attr(cj_attributes, arg_id, arg_key=None):
        result = []
        if cj_attributes is not None:
            for attr in cj_attributes:
                for member_id in range(0, 8):
                    member_name = 'member' + str(member_id)
                    if attr is not None and member_name in attr:
                        if attr[member_name] is not None and 'id' in attr[member_name]:
                            if attr[member_name]['id'] == arg_id and ('key' not in attr[member_name] or attr[member_name]['key'] == arg_key):
                                result += [attr[member_name]['value']]
        return result

org_id="34018aa8-ed03-48d5-b8df-cf7111c4cbd6"
cookiesync_path = "/user/cleverdata/data/incoming/cid={}/data_type=cookiesync".format(org_id)
from pyspark.sql.types import ArrayType, StringType
cs_all = spark.read.format("com.databricks.spark.avro").load(cookiesync_path).select("externalUserId","globalUserId").toDF("fpc","globalUserId").dropDuplicates()
dmp_event_cookies = spark.read.csv("/user/kkotochigov/kimblerly_dmp_fpcs.csv").toDF("fpc","target")
dmp_cookies = dmp_event_cookies.join(cs_all, "fpc")
dmp_cookies.coalesce(1).write.mode("overwrite").csv("/user/kkotochigov/kimberly_globaluserid/", header = 'true')
