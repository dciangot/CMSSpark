#!/usr/bin/env python
from pyspark.sql.functions import udf
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
from CMSSpark.spark_utils import dbs_tables, phedex_tables, print_rows
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

sqlContext = SQLContext(sc)
tables = {}
block_size = sqlContext.read.parquet("hdfs://analytix/cms/users/dciangot/block_size_2018")


verbose = True
filenames = ['01', '02', '03','04','05','06','07','08','09','10','11','12']
#filenames = ['01','02','03', '04']

def time_range_filter(string_date, filename):
    now = datetime.strptime(string_date, '%Y-%m-%d %H:%M:%S')
    start = datetime.strptime(conv_filename[filename], '%Y-%m-%d')
    keepit = (now > start)

    return keepit

time_filter = udf(lambda z: time_range_filter(z[0], z[1]), BooleanType())

temp = {}

for file_ in filenames:

    filename = '19_%s' % file_.replace("{", "").replace("}", "").replace(",", "")
    tables.update(condor_tables(sqlContext, date="2019/%s/*" % file_, verbose=verbose))
        


    tb_condor = tables["condor_df"].filter(col('data.DESIRED_CMSDataset').isNotNull())\
                                   .withColumn('day', (col('data.RecordTime')-col('data.RecordTime')%fn.lit(86400000))/fn.lit(1000))\
                                   .select('day', 'data.CRAB_DataBlock', 'data.Type', 'data.CMSSite', 'data.CRAB_Workflow', 'data.CRAB_UserHN',
                                           'data.WallClockHr', 'data.OVERFLOW_CHECK', 'data.JobCpus', 'data.InputData', 'data.CpuTimeHr', 'data.CpuEff',
                                           'data.CoreHr', 'data.Chirp_CRAB3_Job_ExitCode', 'data.OverflowType')

    tb_condor.persist(StorageLevel.MEMORY_AND_DISK)

    block_size.registerTempTable('block_size')
    tb_condor.registerTempTable('tb_condor')

    query = "SELECT * FROM tb_condor JOIN block_size ON b_block_name = CRAB_DataBlock"  # % ','.join(cols)
    jm_agg_df = sqlContext.sql(query)



    partition_columns = ['CRAB_DataBlock', 'Type', 'CMSSite', 'CRAB_Workflow', 'CRAB_UserHN', 'OVERFLOW_CHECK', 'JobCpus', 'InputData', 'data_tier', 'block_size', 'OverflowType', 'Chirp_CRAB3_Job_ExitCode']


    filt = [] #, tool, , good_sites]

    working_set_day = jm_agg_df.groupBy('day', *partition_columns).agg(
                                                    sum('WallClockHr').alias('WallClock'),
                                                    sum('CpuTimeHr').alias('CPUTime'),
                                                    sum('CoreHr').alias('CoreTime')
                                        )

    # working_set_day.toPandas().to_pickle("./ws_%s.pkl" % filename)
    working_set_day.write.option("compression","gzip").parquet("hdfs://analytix/cms/users/dciangot/ws_classAds_%s" % filename)

