import sys
from pyspark.sql import SparkSession
from hdfs import InsecureClient

import datetime
import pandas

from CJDataExtractor import CJDataExtractor

org_id = "fe8cf20f-c2a6-4ba9-bd28-36a9d126afc8"
wd = "/tmp/"

# Create or Get Spark Session
spark = SparkSession.builder.appName('analytical_attributes').getOrCreate()

ts_from=(2018,5,15)
ts_to=(2020,8,15)

extractor = CJDataExtractor(spark, org_id)
extractor.load(ts_from, ts_to)
extractor.save(output_path=wd+"/cj_extract")

# Custom Dataset Transformation
x = spark.read.parquet(wd+"/cj_extract")
y = x.groupBy("fpc").agg(F.collect_set("event").alias("events"))
y = y.withColumn("target",F.when(F.size(y.events)>1, 1).otherwise(0))
y.select("fpc","target").coalesce(1).write.mode("overwrite").csv(wd+"model_train_dataset")



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












