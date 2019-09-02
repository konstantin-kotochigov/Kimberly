import pandas
import time
import datetime

from pyspark.sql.types import ArrayType, StringType
from pyspark.sql.window import Window
from pyspark.sql.functions import lead
from pyspark.sql import SparkSession
import pyspark.sql.functions as F

class CJDataExtractor:
    
    cj_path = ""
    cj_data = None
    cj_df = None
    spark = None
    
    def __init__(self, spark, org_id):
        self.cj_path = ""
        self.spark = spark
        self.spark.udf.register("cj_id", self.cj_id, ArrayType(StringType()))
        self.spark.udf.register("cj_attr", self.cj_attr, ArrayType(StringType()))
        self.cj_path = "/data/{}/.dmpkit/customer-journey/master/cdm".format(org_id)
        self.cp_path = "/data/{}/.dmpkit/profiles/master/cdm".format(org_id)
    
    @staticmethod
    def cj_id(cj_ids, arg_id, arg_key=-1):
        result = []
        for id in cj_ids['uids']:
            if id['id'] == arg_id and id['key'] == arg_key:
                result += [id['value']]
        return result
    
    @staticmethod
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
        
    
    def load(self, ts_from, ts_to):
    
        cj_all = self.spark.read.format("com.databricks.spark.avro").load(self.cj_path)
        time_from = int(time.mktime(datetime.datetime(ts_from[0],ts_from[1],ts_from[2]).timetuple())) * 1000
        time_to = int(time.mktime(datetime.datetime(ts_to[0], ts_to[1], ts_to[2]).timetuple())) * 1000
        self.cj_data = cj_all.filter('ts > {} and ts < {}'.format(time_from, time_to))
    
        self.cj_data.createOrReplaceTempView('cj')
        
        # Select CJ Attributes
        cj_df = self.spark.sql('''
            select
                cj_id(id, 10008, 10031)[0] as fpc,
                cj_attr(attributes, 10057) as event,
                cj_attr(attributes, 10018, "origin")[0] as url,
                ts/1000 as ts
            from cj c
            ''').\
            filter("url='https://es.huggies.ru'").\
            filter("array_contains(event,'PageLoad') or array_contains(event,'code_confirm')")
        
        cj_df.createOrReplaceTempView('cj_df')
        
        cj_df = self.spark.sql('''
            select distinct
                fpc,
                ts,
                case when array_contains(event,'code_confirm') then 'buy' else 'visit' end as event
            from cj_df c
            ''')
        
        self.cj_df = cj_df
        
        return self.cj_df
        
    def save(self, output_path):
        
        self.cj_df.write.mode("overwrite").parquet(wd+"/cj_extract")
        print("Written Records = {}".format(self.spark.read.parquet(wd+"/cj_extract").count()))
        
        return -1
    
    