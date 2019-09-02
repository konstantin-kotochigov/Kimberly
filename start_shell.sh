#!/usr/bin/env bash
export HADOOP_USER_NAME=kkotochigov
export SPARK_DIST_CLASSPATH=$(hadoop classpath)
export HADOOP_CONF_DIR=${HADOOP_CONF_DIR:-/etc/hadoop/conf}
PYSPARK_PYTHON=./venv/bin/python pyspark2 --master yarn --deploy-mode client \
 --conf "spark.pyspark.virtualenv.enabled=true" \
 --conf "spark.pyspark.virtualenv.type=native" \
 --conf "spark.pyspark.virtualenv.bin.path=./venv/bin" \
 --conf "spark.pyspark.python=./venv/bin/python" \
 --jars "spark-avro_2.11-3.2.0.jar" \
 --archives venv.zip#venv \
 --num-executors 6 \
 --conf "spark.executor.memory=2g" \
 --conf "spark.driver.memory=5g" \
 --conf "spark.shuffle.service.enabled=True" \
 --conf "spark.yarn.executor.memoryOverhead=1g" \
 --conf "spark.kryoserializer.buffer.max=2047m" \
 --conf "spark.driver.maxResultSize=2g" \
 --conf "spark.rpc.maxSize=1g" \
 --conf "spark.driver.cores=4" \
 --files schema.avsc \
 --name analytical_attributes_shell \
 --queue root.model.return_model \
 --py-files "cj_loader.py,cj_predictor.py,cj_export.py"
