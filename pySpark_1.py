import os
import sys

os.environ['SPARK_HOME'] = "/usr/hdp/3.0.1.0-187/spark2"
os.environ['HIVE_HOME'] = "/usr/hdp/3.0.1.0-187/hive"
os.environ["HADOOP_USER_NAME"] = "spark"
os.environ['PYSPARK_SUBMIT_ARGS'] = '--master yarn --deploy-mode client  ' \
                                    '--num-executors 11  --executor-memory 19G --executor-cores 5 ' \
                                    '--driver-memory 1G pyspark-shell'
sys.path.append("/usr/hdp/3.0.1.0-187/spark2/python")

from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("pySpark1_redeyesofangel").getOrCreate()
spark.sql("select current_timestamp() ").show()
spark.stop()
