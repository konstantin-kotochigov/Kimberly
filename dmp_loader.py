import pandas
import time
import datetime

from pyspark.sql.types import ArrayType, StringType
from pyspark.sql.window import Window
from pyspark.sql.functions import lead
from pyspark.sql import SparkSession

class CJ_Loader:
    
    cj_path = ""
    cp_path = ""
    
    cj_data = None
    cj_attributes = None
    cj_dataset = None
    cj_df = None
    cj_authorized = None
    cj_non_authorized = None
    
    cj_data_rows = None
    cj_df_rows = None
    cj_dataset_rows = None
    cj_non_authorized_rows = None
    cj_authorized_rows = None
    
    def __init__(self, spark):
        self.cj_path = ""
        self.cp_path = ""
        self.spark = spark
        self.spark.udf.register("cj_id", self.cj_id, ArrayType(StringType()))
        self.spark.udf.register("cj_attr", self.cj_attr, ArrayType(StringType()))
        
    def set_organization(self, org_id):
        self.cj_path = "/data/{}/.dmpkit/customer-journey/master/cdm".format(org_id)
        self.cp_path = "/data/{}/.dmpkit/profiles/master/cdm".format(org_id)
    
    def load_cj_all(self):
        self.cj_data = self.spark.read.format("com.databricks.spark.avro").load(self.cj_path)
        self.cj_data_rows = self.cj_data.count()
        print("Loaded CJ Rows = {}".format(self.cj_data_rows))
    
    def load_cj(self, ts_from, ts_to):
        cj_all = self.spark.read.format("com.databricks.spark.avro").load(self.cj_path)
        time_from = int(time.mktime(datetime.datetime(ts_from[0],ts_from[1],ts_from[2]).timetuple())) * 1000
        time_to = int(time.mktime(datetime.datetime(ts_to[0], ts_to[1], ts_to[2]).timetuple())) * 1000
        self.cj_data = cj_all.filter('ts > {} and ts < {}'.format(time_from, time_to))
        # self.cj_data_rows = self.cj_data.count()
        print("Loaded CJ Rows = {}".format(self.cj_data_rows))
        
    def cj_stats(self, ts_from, ts_to):
        cj_all = self.spark.read.format("com.databricks.spark.avro").load(self.cj_path)
        time_from = int(time.mktime(datetime.datetime(ts_from[0],ts_from[1],ts_from[2]).timetuple())) * 1000
        time_to = int(time.mktime(datetime.datetime(ts_to[0], ts_to[1], ts_to[2]).timetuple())) * 1000
        cj_all = cj_all.filter('ts > {} and ts < {}'.format(time_from, time_to))
        cj_all.selectExpr("date(from_unixtime(ts/1000)) as ts").groupBy("ts").count().orderBy("ts").show(100)
        cj_all.selectExpr("date(from_unixtime(min(ts/1000))) as max_ts","date(from_unixtime(max(ts/1000))) as min_ts","count(*) as cnt").show()
        return -1
    
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
        
    
    # Method to get attributes (returns dataframe)
    def extract_attributes(self):
    
        self.cj_data.createOrReplaceTempView('cj')
        
        # Link processing function
        def __get_link(raw_link):
            return (substring(substring_index(substring_index(raw_link, '?', 1), '#', 1), 19, 100))
        
        # Select CJ Attributes
        cj_df = self.spark.sql('''
            select
                cj_id(id, 10008, 10031)[0] as fpc
                cj_attr(attributes, 10057)[0] as event,
                cj_attr(attributes, 10077)[0] as url,
                ts/1000 as ts
            from cj c
        ''').filter("array_contains(event,'PageLoad'))
        
        def generate_next_feature(df, id_attribute):
            attribute_next = lead(df.ts).over(Window.partitionBy(id_attribute).orderBy("ts")).alias("next")
            return df.select(id_attribute,"ts",attribute_next,"link").toDF("fpc","ts","next","link")
        
        # Compute TS deltas between events (in hours)
        self.cj_df = cj_df
        self.cj_authorized = generate_next_feature(cj_df.filter("id is not null"), "id")
        # self.cj_non_authorized = generate_next_feature(cj_df.filter("id is null"), "fpc")
        
        # self.cj_df_rows = self.cj_df.count()
        # self.cj_authorized_rows = self.cj_authorized.count()
        # self.cj_non_authorized_rows = self.cj_non_authorized.count()
        
        print("Extracted Rows = {}".format(self.cj_df_rows))
        print("Extracted Rows (authorized) = {}".format(self.cj_authorized_rows))
        print("Extracted Rows (non authorized) = {}".format(self.cj_non_authorized_rows))
        
        return (self.cj_authorized, self.cj_non_authorized)
    
    
    def process_attributes(self, cj_df):
    
        if cj_df is None:
            cj_df = self.cj_df
    
        # Create Records
        def groupAttrs(r):
            sortedList = sorted(r[1], key=lambda y: y[0])
            dividedList = list(zip(*sortedList))
            # dt = [x[0] for x in sortedList]
            deltas = [i for i, x in enumerate(dividedList[1]) if x == None or x > 4]
            return [(r[0], dividedList[1][0:y+1], dividedList[2][0:y+1], 0 if dividedList[1][y]==None else 1) for y in deltas]
        
        # Slice By User
        y = cj_df.select(['fpc','ts','next','link']).rdd.map(lambda x: (x['fpc'], (x['ts'], x['next'], x['link']))).groupByKey().flatMap(lambda x: groupAttrs(x))
        
        # Convert To Pandas dataframe
        y_py = pandas.DataFrame(y.collect(),  columns=['fpc','dt','url','target'])
        
        # Process Types
        y_py['url'] = y_py.url.apply(lambda x:" ".join(x))
        y_py['dt'] = y_py.dt.apply(lambda x:[y for y in x if y != None])
        
        self.cj_dataset = y_py
        
        self.cj_dataset_rows = self.cj_dataset.shape[0]
        print("Dataset of Processed Rows (cj_dataset) = {}".format(self.cj_dataset_rows))
        
        return self.cj_dataset