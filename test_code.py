import sys
from pyspark.sql import SparkSession, Window, SQLContext
from pyspark.sql import functions
from dateutil.parser import parse
from pyspark.sql.types import *
from pyspark import StorageLevel
import pandas as pd
import numpy as np
import json
import datetime
import time
from py_saas_algorithm.alarm import gen_alarm_id, gen_hash_code
from py_saas_algorithm.recorder import ErrorRecorder
from py_saas_algorithm.algorithm import Algorithm
from py_saas_algorithm.data_source import read_battery_data
from collections import defaultdict
from py_saas_algorithm.recorder import ErrorRecorder
from py_saas_algorithm import proxima

spark = SparkSession.builder \
    .appName("AnalysingBatteryAlarms") \
    .config("spark.default.parallelism", "600") \
    .config("spark.sql.shuffle.partitions", "600") \
    .enableHiveSupport() \
    .getOrCreate()  # prod
sql_context = SQLContext(spark)



