import sys
from pyspark.sql import SparkSession
from hdfs import InsecureClient

import time
import datetime
import pandas

from cj_loader import CJ_Loader
from cj_predictor import CJ_Predictor
from cj_export import CJ_Export

start_processing = time.time()

# Static Parameters
# hdfs_host = "http://136.243.150.16:50070"
org_id = "fe8cf20f-c2a6-4ba9-bd28-36a9d126afc8"
wd = "/tmp/"
# temporary_local_file_path = "./data_export"

def parse_arguments():

    argument_vector = sys.argv[1:]
    if len(sys.argv) != 6:
        argument_vector = ['true','true','2010-01-01','2020-01-01','/user/kkotochigov/']
        raise Exception("command must have 6 arguments")
    
    arg_send_update, arg_refit, arg_from_dt, arg_to_dt, arg_descriptor_path = argument_vector
    
    arg_send_update = bool(arg_send_update)
    arg_refit = bool(arg_refit)
    arg_from_dt = arg_from_dt.split("-")
    arg_to_dt = arg_to_dt.split("-")
    
    print("Send_update = {}, Refit Model = {}; Period = ({},{})".format(arg_send_update, arg_refit, "/".join(arg_from_dt), "/".join(arg_to_dt)))
    return arg_send_update, arg_refit, arg_from_dt, arg_to_dt, arg_descriptor_path


# arg_send_update, arg_refit, arg_from_dt, arg_to_dt, arg_descriptor_path = parse_arguments()

# Create or Get Spark Session
spark = SparkSession.builder.appName('analytical_attributes').getOrCreate()

# hdfs_client = InsecureClient(hdfs_host, "hdfs")


# Check whether We Need to Refit
# update_model_every = 60*24*7 # in seconds
# model_file_list = hdfs_client.list(wd+"models/", status=True)
# model_modification_ts = next(iter([model_filename[1]['modificationTime'] for model_filename in model_file_list if model_filename[0] == "model.h5"]), None)
# model_needs_update = True if (model_modification_ts == None) or (time.time() - model_modification_ts > update_model_every) or (arg_refit) else False
# print("Refit = {}".format(model_needs_update))

# Load Data
cjp = CJ_Loader(spark)
cjp.set_organization(org_id)
cjp.load_cj(ts_from=[int(x) for x in arg_from_dt], ts_to=[int(x) for x in arg_to_dt])
# cjp.cj_stats(ts_from=(2018,12,1), ts_to=(2018,12,31))
cjp.cj_data.createOrReplaceTempView('cj')
(cj_authorized, cj_non_authorized) = cjp.extract_attributes()
data_authorized = cjp.process_attributes(cj_authorized)
# data_non_authorized = cjp.process_attributes(cj_non_authorized)

data_authorized.to_parquet(temporary_local_file_path)

data_authorized = pandas.read_parquet(temporary_local_file_path)

# Make Model
predictor = CJ_Predictor(wd+"models/")
predictor.set_data(data_authorized)
predictor.optimize(batch_size=4096)
start_fitting = time.time()

result = predictor.fit(update_model=model_needs_update, batch_size=4096)
scoring_distribution = result.return_score.value_counts(sort=False)

print("Got Result Table with Rows = {}".format(result.shape[0]))
print("Score Distribution = \n{}".format(scoring_distribution))


# Make Delta
df = spark.createDataFrame(result)
dm = CJ_Export(org_id, "model_update", hdfs_host, "schema.avsc")

mapping = {
    'id': {
        'fpc': {
            'primary': 10005,
            'secondary': -1
        }
    },
    'attributes': {
        'return_score': {
            'primary': 10035,
            'mapping': {
                '1': 10000,
                '2': 10001,
                '3': 10002,
                '4': 10003,
                '5': 10005
            }
        }
    }
}

# Publish Delta
print("Send Update To Production = {}".format(arg_send_update))
dm.make_delta(df, mapping, send_update=arg_send_update)

finish_fitting = time.time()

log_data = {
        "dt":[datetime.datetime.today().strftime('%Y-%m-%d %H-%m-%S')],
        "loaded_rows":[cjp.cj_data_rows],
        "extracted_rows":[cjp.cj_df_rows],
        "processed_rows":[cjp.cj_dataset_rows],
        "refit_flag":[model_needs_update],
        "send_to_prod_flag":[arg_send_update],
        "processing_time":[round((start_fitting - start_processing)/60, 2)],
        "fitting_time":[round((finish_fitting - start_fitting)/60, 2)],
        "target_rate":[0.05],
        "train_auc":[predictor.train_auc],
        "test_auc":[predictor.test_auc[0]],
        "test_auc_std":[predictor.test_auc_std[0]],
        "test_auc_lb":[predictor.test_auc[0] - predictor.test_auc_std[0]],
        "test_auc_ub":[predictor.test_auc[0] + predictor.test_auc_std[0]],
        "q1":[scoring_distribution[0]],
        "q2":[scoring_distribution[1]],
        "q3":[scoring_distribution[2]],
        "q4":[scoring_distribution[3]],
        "q5":[scoring_distribution[4]]
    }

df = spark.createDataFrame(pandas.DataFrame(log_data))
df=df.withColumn("dt",df.dt.astype("Date"))

df.write.jdbc(url="jdbc:postgresql://159.69.60.71:5432/analytics_monitoring", table="mac_stats", mode="append", properties = {"password":"liquibase", "user":"liquibase"})












