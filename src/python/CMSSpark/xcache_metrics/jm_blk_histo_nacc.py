#!/usr/bin/env python
# coding: utf-8

import re
import time
import argparse
import hashlib

from pyspark import SparkContext, StorageLevel
from pyspark.sql import SQLContext
import os
# CMSSpark modules
from CMSSpark.spark_utils import dbs_tables, cmssw_tables, aaa_tables_enr, eos_tables, jm_tables, phedex_tables
from CMSSpark.spark_utils import spark_context, print_rows, split_dataset
from CMSSpark.utils import elapsed_time
from CMSSpark.data_collection import  yesterday, short_date_string, long_date_string, output_dataframe, run_query, short_date_to_unix
from pyspark.sql.functions import desc
from pyspark.sql.functions import split, col
import re
import sys
import gzip
import time
import json

# pyspark modules
from pyspark import SparkContext, StorageLevel
from pyspark.sql import SQLContext
from pyspark.sql.functions import lit, sum, count, col, split
from pyspark.sql.functions import udf
from pyspark.sql.types import DoubleType

# CMSSpark modules
from CMSSpark.spark_utils import dbs_tables, phedex_tables, print_rows, unionAll
from CMSSpark.spark_utils import spark_context, condor_tables, split_dataset
from CMSSpark.utils import info, split_date
from CMSSpark.conf import OptionParser

# load pyspark functions to be used here to redefine any previous usage of those names
from pyspark.sql.functions import udf, regexp_extract, first, struct, from_unixtime, explode, size, lit, sum, count, col, split, collect_set, collect_list
from pyspark.sql.functions import max as mx
from pyspark.sql import Column

from pyspark.sql.window import Window

def condor_date(date):
    "Convert given date into AAA date format"
    if  not date:
        date = time.strftime("%Y/%m/%d", time.gmtime(time.time()-60*60*24))
        return date
    if  len(date) != 8:
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
readavro = sqlContext.read.format("com.databricks.spark.avro").option("IGNORE_CORRUPT_FILES", "true")
inst='GLOBAL'
# read DBS and Phedex tables
dtables = ['daf', 'ddf', 'dtf' ,'bdf', 'fdf', 'aef', 'pef', 'mcf', 'ocf', 'rvf']
tables.update(dbs_tables(sqlContext, inst=inst, verbose=verbose, tables=dtables))
tables.update(phedex_tables(sqlContext, verbose=verbose))
phedex_df = tables['phedex_df']
daf = tables['daf'] # dataset access table
ddf = tables['ddf'] # dataset table
dtf = tables['ddf'] # dataset table
bdf = tables['bdf'] # block table
fdf = tables['fdf'] # file table
aef = tables['aef'] # acquisition era
pef = tables['pef'] # processing era table
mcf = tables['mcf'] # output mod config table
ocf = tables['ocf'] # output module table
rvf = tables['rvf'] # release version table


@udf("string")
def clean_site_name(s):
    """
    Splits site name by _ (underscore), takes no more than first three parts and joins them with _.
    This way site name always have at most three parts, separated by _.
    First three parts represent a tier, a country and a lab/university name.
    """
    split = s.split('_')
    split = split[0:3]

    # Remove empty strings which may appear when s is T0_USA_
    split = filter(None, split)

    join = '_'.join(split)
    return join

cols = ['f_logical_file_name AS file_name',
        'b_block_name AS block_name',
        'b_block_id',
        'node_name AS site_name',
        'd_dataset AS dataset_name',
        'd_data_tier_id',
        'd_dataset_id']

# Join FDF and BDF by f_block_id and b_block_id
query = ("SELECT %s FROM fdf " \
         "JOIN bdf ON fdf.f_block_id = bdf.b_block_id "\
         "JOIN ddf ON fdf.f_dataset_id = ddf.d_dataset_id "\
         "JOIN phedex_df ON bdf.b_block_name = phedex_df.block_name") % ','.join(cols)

if verbose:
    print('Will run query to generate temp file_block_site table')

result = sqlContext.sql(query)
result.registerTempTable('f_b_all_df')

query_distinct = ("SELECT DISTINCT * FROM f_b_all_df ORDER  BY file_name")
result_distinct = sqlContext.sql(query_distinct)
result_distinct.registerTempTable('f_b_s_df')


"""

    {"JobId":"1672451388","FileName":"//store/file.root","IsParentFile":"0","ProtocolUsed":"Remote",
    "SuccessFlag":"1","FileType":"EDM","LumiRanges":"unknown","StrippedFiles":"0","BlockId":"602064",
    "StrippedBlocks":"0","BlockName":"Dummy","InputCollection":"DoesNotApply","Application":"CMSSW",
    "Type":"reprocessing","SubmissionTool":"wmagent","InputSE":"","TargetCE":"","SiteName":"T0_CH_CERN",
    "SchedulerName":"PYCONDOR","JobMonitorId":"unknown","TaskJobId":"1566463230",
    "SchedulerJobIdV2":"664eef36-f1c3-11e6-88b9-02163e0184a6-367_0","TaskId":"35076445",
    "TaskMonitorId":"wmagent_pdmvserv_task_S_640","JobExecExitCode":"0",
    "JobExecExitTimeStamp":1488375506000,"StartedRunningTimeStamp":1488374686000,
    "FinishedTimeStamp":1488375506000,"WrapWC":"820","WrapCPU":"1694.3","ExeCPU":"0",
    "UserId":"124370","GridName":"Alan Malta Rodrigues"}
"""

@udf("array<string>")
def lfnList(indices, lfnarray):
    lfns = []
    for ilfn in indices:
        lfns.append(lfnarray[ilfn])
    return lfns


filenames = ['{1,2,3,4,5}','{5,6,7,8,9,10}', '{10,11,12}']


block_size = tables['bdf'].join(tables['fdf'], col('f_block_id')==col('b_block_id')) \
                          .groupBy(col('b_block_name'), col('b_block_id')).agg(
                              sum('f_file_size').alias('block_size'),
                          )
block_size.persist(StorageLevel.MEMORY_AND_DISK)

for file_ in filenames:
    print (file_)


    filename = '18_%s_blk' % file_.replace("{","").replace("}","").replace(",","")


    tables.update(jm_tables(sc, sqlContext, date="year=2018/month=%s/day=*" % file_, verbose=verbose))

    # TODO: define columns
    query = "SELECT * FROM jm_df JOIN f_b_s_df ON f_b_s_df.file_name = jm_df.FileName" # % ','.join(cols)
    jm_agg_df = sqlContext.sql(query)

    jm_agg_df.printSchema()

    partition_columns = ['d_data_tier_id', 'SiteName', 'SubmissionTool', 'UserId', 'TaskMonitorId']


    working_set_day = jm_agg_df.withColumn('day', col('FinishedTimeStamp')-col('FinishedTimeStamp')%lit(86400000))\
                               .groupBy('day', 'TaskMonitorId', 'userId', *partition_columns).agg(
                                                 collect_set('b_block_id').alias('working_set_blocks'),
                                                 #collect_set('jm_df.FileName').alias('working_set_files'),
                                                 #collect_set('d_dataset_id').alias('working_set_datasets'),
                                                 )

    working_set_day.persist(StorageLevel.MEMORY_AND_DISK)

    rolling_month = Window().partitionBy(*partition_columns).orderBy(col('day')).rangeBetween(-int(86400000*365.25/12), 0)

    distinct_set = working_set_day.select(
            from_unixtime(col('day')/1000).alias('date'),
            #'TaskMonitorId',
            #'UserId', 
            collect_set(col('working_set_files')).over(rolling_month).alias('working_set_set'),
            *partition_columns
        ) \
        .select('date',  explode('working_set_set').alias('working_set'), *partition_columns) \
        .select('date', explode('working_set').alias('elements'), *partition_columns) \
        .distinct()

    partition_columns = ['d_data_tier_id', 'SiteName', 'SubmissionTool']


    # TODO: this change for files
    setme = distinct_set.join(block_size, col('elements')==col('b_block_id')) \
            .groupby('date', 'elements', *partition_columns).agg(
                first(col('block_size').cast('float')).alias('working_set_size'),
                size(collect_set('TaskMonitorId')).alias('unique_acc'),
                size(collect_set('UserId')).alias('unique_user'),
            )#.write.format('json').save('hdfs://analytix/cms/users/dciangot/list_test_2.json')

    setme.persist(StorageLevel.MEMORY_AND_DISK)

    sites = ["T2_IT_Pisa", "T2_IT_Legnaro", "T2_IT_Rome", "T2_IT_Bari"]
    tiers = [31223,31224,9,21]

    ntype = ['unique_acc', 'unique_user']

    good_sites =    (col('SiteName').isin(sites))
    tool =          (col('SubmissionTool')=='crab3')
    for ntp in ntype:
        for tier in tiers:
            good_tiers =    (col('d_data_tier_id').isin(tier))

            filt = [good_sites, good_tiers,  tool]

            #group = ['date', 'SiteName', 'SubmissionTool', 'elements']
            group = ['date', 'elements']

            setme.withColumn('size', col('working_set_size')/1000000000000).drop('working_set_size')\
                          .filter(col_all(filt)).groupby(*group).agg(sum(col(ntp)).alias('nacc'), first('size').alias('el_size'))\
                          .dropDuplicates(['date', 'elements'])\
                          .groupby('date', 'nacc').agg(size(collect_set(col('elements'))).alias('nblocks'), sum(col('el_size')))\
                          .write.format("com.databricks.spark.csv").option("header", "true").save('hdfs://analytix/cms/users/dciangot/jm_file/hist_size_%s/files_%s_%s_%s_T2_IT' % ( ntp, filename, "every", tier))


            filt = [good_tiers, tool]


            setme.withColumn('size', col('working_set_size')/1000000000000).drop('working_set_size')\
                          .filter(col_all(filt)).groupby(*group).agg(sum(col(ntp)).alias('nacc'), first('size').alias('el_size'))\
                          .dropDuplicates(['date', 'elements'])\
                          .groupby('date', 'nacc').agg(size(collect_set(col('elements'))).alias('nblocks'), sum(col('el_size')))\
                          .write.format("com.databricks.spark.csv").option("header", "true").save('hdfs://analytix/cms/users/dciangot/jm_file/hist_size_%s/files_%s_%s_%s_all' % ( ntp ,filename, "every", tier))