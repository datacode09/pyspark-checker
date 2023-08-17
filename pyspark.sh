#!/bin/bash

# Set the paths to your PySpark and Python executables
PYSPARK_PATH="/path/to/pyspark"
PYTHON_PATH="/path/to/python"

# Set the PySpark script path
PYSPARK_SCRIPT_PATH="/path/to/your/pyspark_script.py"

# Set your Hadoop cluster's resource manager and HDFS settings
RESOURCE_MANAGER="yarn"
HDFS_WAREHOUSE_DIR="/user/hive/warehouse"
HADOOP_CONF_DIR="/path/to/your/hadoop/conf"

# Specify Tez queue name
TEZ_QUEUE="your_tez_queue_name"

# Execute the PySpark script on the Hadoop cluster with Tez queue
$PYSPARK_PATH \
  --master $RESOURCE_MANAGER \
  --deploy-mode cluster \
  --driver-memory 4g \
  --conf spark.sql.warehouse.dir="hdfs://$HDFS_WAREHOUSE_DIR" \
  --conf spark.hadoop.hive.exec.dynamic.partition=true \
  --conf spark.hadoop.hive.exec.dynamic.partition.mode=nonstrict \
  --conf spark.sql.catalogImplementation=hive \
  --executor-memory 4g \
  --executor-cores 2 \
  --conf spark.executorEnv.HADOOP_CONF_DIR=$HADOOP_CONF_DIR \
  --conf spark.yarn.appMasterEnv.HADOOP_CONF_DIR=$HADOOP_CONF_DIR \
  --conf spark.hadoop.fs.defaultFS=hdfs://your_hdfs_host:your_hdfs_port \
  --conf spark.yarn.queue=$TEZ_QUEUE \
  --py-files $PYSPARK_SCRIPT_PATH \
  --archives $HADOOP_CONF_DIR.zip#hadoop_conf \
  --files $HADOOP_CONF_DIR/hdfs-site.xml,$HADOOP_CONF_DIR/core-site.xml \
  $PYSPARK_SCRIPT_PATH
