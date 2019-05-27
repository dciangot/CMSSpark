#!/usr/bin/env python
# coding: utf-8

from pyspark.sql.functions import udf, regexp_extract
from pyspark.sql.functions import split, col, first
from pyspark.sql.functions import lit, sum, count, col, split
# system modules
import os
import re
import sys
import time
import json

from pyspark import SparkContext, StorageLevel
from pyspark.sql import SQLContext

# CMSSpark modules
from CMSSpark.spark_utils import cmssw_tables, print_rows
from CMSSpark.spark_utils import spark_context, split_dataset
from CMSSpark.utils import info
from CMSSpark.conf import OptionParser

from pyspark.sql.types import BooleanType
from CMSSpark.spark_utils import condor_tables
from pyspark.sql import SQLContext
import pyspark.sql.functions as fn

from pyspark.sql import Column
from pyspark.sql.functions import first, struct, from_unixtime, explode, size, collect_set

# CMSSpark modules
def condor_date(date):
    "Convert given date into AAA date format"
    if not date:
        date = time.strftime("%Y/%m/%d", time.gmtime(time.time()-60*60*24))
        return date
    if len(date) != 8:
        raise Exception("Given date %s is not in YYYYMMDD format" % date)
    year = date[:4]
    month = date[4:6]
    day = date[6:]
    return '%s/%s/%s' % (year, month, day)


def condor_date_unix(date):
    "Convert AAA date into UNIX timestamp"
    return time.mktime(time.strptime(date, '%Y/%m/%d'))


def col_all(columns):
    return reduce(Column.__and__, columns)

sc = spark_context('cms', True, True)

verbose = True
sqlContext = SQLContext(sc)
tables = {}


def time_range_filter(string_date, filename):
    now = datetime.strptime(string_date, '%Y-%m-%d %H:%M:%S')
    start = datetime.strptime(conv_filename[filename], '%Y-%m-%d')
    keepit = (now > start)

    return keepit

time_filter = udf(lambda z: time_range_filter(z[0], z[1]), BooleanType())

temp = {}

#for file_ in filenames:

tables.update(cmssw_tables(sc, sqlContext, date="year=2018/month={7,8,9,10,11,12}/day=*", verbose=verbose))

df = tables['cmssw_df'].withColumn('DAY', (col('END_TIME')-col('END_TIME')%fn.lit(86400)))\
                       .groupBy(["DAY" ,"APP_INFO", "USER_DN", "SITE_NAME", "CLIENT_DOMAIN", "SERVER_DOMAIN", "FILE_LFN"])\
                                              .agg( sum("READ_BYTES_AT_CLOSE").alias("READ_BYTES"),
                                                    (sum("END_TIME") - sum("START_TIME")).alias('DURATION') )

df_new = df.filter(regexp_extract('APP_INFO', r'(crab)', 1)!='')\
   .withColumn('CRAB_TASK', regexp_extract('APP_INFO', r'.*/(\d{6}:\d{6}.*)',1))\
   .withColumn('USER', regexp_extract('APP_INFO', r'.*/\d{6}:\d{6}:(\w+):.*',1))\
   .withColumn('DATA_TYPE', regexp_extract('FILE_LFN', r'/\w+/([^/]+)/.*', 1))\
   .withColumn('DATA_CAMPAIGN', regexp_extract('FILE_LFN', r'/\w+/[^/]+/([^/]+)/.*', 1))\
   .withColumn('DATA_PROCESS', regexp_extract('FILE_LFN', r'/\w+/[^/]+/[^/]+/([^/]+)/.*', 1))\
   .withColumn('DATA_TIER', regexp_extract('FILE_LFN', r'/\w+/[^/]+/[^/]+/[^/]+/([A-Z|-]+)/.*', 1))\
   .withColumn('DATASET', regexp_extract('FILE_LFN', r'/(.*)/\d+{5}/.*.root$', 1))\
   .drop('APP_INFO').drop('USER_DN')


df_new.persist(StorageLevel.MEMORY_AND_DISK)

df_reduced = df_new.groupBy([
 'DAY',
 'SITE_NAME',
 'CLIENT_DOMAIN',
 'SERVER_DOMAIN',
 'CRAB_TASK',
 'USER',
 'DATA_TYPE',
 'DATA_CAMPAIGN',
 'DATA_PROCESS',
 'DATA_TIER',
 'DATASET']).agg(sum("READ_BYTES").alias("SUM_BYTES_READ"), sum("DURATION").alias("SUM_DURATION"))

df_reduced.coalesce(1).write.format("com.databricks.spark.csv").save("hdfs://analytix/cms/users/dciangot/cmssw_reduced_18-7_12" )
