#!/usr/bin/env bash
export HADOOP_USER_NAME=dmpkit;
PYSPARK_PYTHON=./venv/bin/python spark2-submit --master yarn --deploy-mode cluster \
 --conf "spark.pyspark.virtualenv.enabled=true" \
 --conf "spark.pyspark.virtualenv.type=native" \
 --conf "spark.pyspark.virtualenv.bin.path=./venv/bin" \
 --conf "spark.pyspark.python=./venv/bin/python" \
 --jars "spark-avro_2.11-3.2.0.jar,postgres_jdbc.jar" \
 --archives venv.zip#venv \
 --num-executors 12 \
 --conf "spark.executor.memory=2g" \
 --conf "spark.driver.memory=5g" \
 --conf "spark.shuffle.service.enabled=True" \
 --conf "spark.yarn.executor.memoryOverhead=1g" \
 --conf "spark.kryoserializer.buffer.max=2047m" \
 --conf "spark.driver.maxResultSize=2g" \
 --conf "spark.rpc.maxSize=1g" \
 --conf "spark.driver.cores=4" \
 --files schema.avsc \
 --py-files "cj_loader.py,cj_predictor.py,cj_export.py" \
 --name $1 \
 --queue root.model.return_model \
 main.py true true 2010-01-01 2020-01-01 /user/kkotochigov/