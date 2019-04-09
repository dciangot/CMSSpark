#!/usr/bin/env python
# coding: utf-8

import time
from datetime import datetime, timedelta

# CMSSpark modules
from CMSSpark.spark_utils import dbs_tables, jm_tables, phedex_tables
from CMSSpark.spark_utils import spark_context, print_rows, split_dataset
from CMSSpark.spark_utils import spark_context

# pyspark modules
from pyspark.sql.functions import split, col
from pyspark import StorageLevel
from pyspark.sql import SQLContext
from pyspark.sql.functions import lit, sum, count, col, split
from pyspark.sql.functions import udf
from pyspark.sql.types import BooleanType
from pyspark.sql.types import ArrayType, StringType
from pyspark.sql.functions import first, struct, from_unixtime, explode, size, collect_set
from pyspark.sql.functions import max as mx
from pyspark.sql.functions import min as minimum
from pyspark.sql import Column
from pyspark.sql.window import Window


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
readavro = sqlContext.read.format("com.databricks.spark.avro").option("IGNORE_CORRUPT_FILES", "true")
inst = 'GLOBAL'
# read DBS and Phedex tables
dtables = ['daf', 'ddf', 'dtf', 'bdf', 'fdf', 'aef', 'pef', 'mcf', 'ocf', 'rvf']
tables.update(dbs_tables(sqlContext, inst=inst, verbose=verbose, tables=dtables))
tables.update(phedex_tables(sqlContext, verbose=verbose))
phedex_df = tables['phedex_df']
daf = tables['daf']  # dataset access table
ddf = tables['ddf']  # dataset table
dtf = tables['ddf']  # dataset table
bdf = tables['bdf']  # block table
fdf = tables['fdf']  # file table
aef = tables['aef']  # acquisition era
pef = tables['pef']  # processing era table
mcf = tables['mcf']  # output mod config table
ocf = tables['ocf']  # output module table
rvf = tables['rvf']  # release version table


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
query = ("SELECT %s FROM fdf "
         "JOIN bdf ON fdf.f_block_id = bdf.b_block_id "
         "JOIN ddf ON fdf.f_dataset_id = ddf.d_dataset_id "
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

block_size = tables['bdf'].join(tables['fdf'], col('f_block_id') == col('b_block_id')) \
                          .groupBy(col('b_block_name'), col('b_block_id')).agg(
                              sum('f_file_size').alias('block_size'),
                          )

block_size.persist(StorageLevel.MEMORY_AND_DISK)

# Split read year in 3 chunks
filenames = ['{1,2,3,4,5,6}', '{5,6,7,8,9,10}', '{9,10,11,12}']
conv_filename = {'18_123456_blk': '2018-02-01',
                 '18_5678910_blk': '2018-07-01',
                 '18_9101112_blk': '2018-12-01'}


def time_range_filter(string_date, filename):
    now = datetime.strptime(string_date, '%Y-%m-%d %H:%M:%S')
    start = datetime.strptime(conv_filename[filename], '%Y-%m-%d')
    keepit = (now > start)

    return keepit


time_filter = udf(lambda z: time_range_filter(z[0], z[1]), BooleanType())

temp = {}

my_tiers = [31223, 31224]  # , 9, 21]
naccs = [0, 1, 2]

for file_ in filenames:

    filename = '18_%s_blk' % file_.replace("{", "").replace("}", "").replace(",", "")
    tables.update(jm_tables(sc, sqlContext, date="year=2018/month=%s/day=*" % file_, verbose=verbose))

    # TODO: define columns
    query = "SELECT * FROM jm_df JOIN f_b_s_df ON f_b_s_df.file_name = jm_df.FileName"  # % ','.join(cols)
    jm_agg_df = sqlContext.sql(query)

    jm_agg_df.printSchema()

    partition_columns = ['SubmissionTool']
    partition_columns_pop = ['b_block_id']

    for tier in my_tiers:
        tiers = [tier]
        sites = ["T2_IT_Pisa", "T2_IT_Legnaro", "T2_IT_Rome", "T2_IT_Bari"]

        # TODO: ProtocolUsed
        tool = (col('SubmissionTool') == 'crab3')
        good_tiers = (col('d_data_tier_id').isin(tiers))
        good_sites = (col('SiteName').isin(sites))

        filt = [tool, good_tiers, good_sites]

        working_set_day = jm_agg_df.filter(col_all(filt)).withColumn('day', col('FinishedTimeStamp')-col('FinishedTimeStamp') % lit(86400000))\
                                   .groupBy('day', *partition_columns).agg(
                                                     collect_set('b_block_id').alias('block_set'),
                                                     )

        working_set_day_pop = jm_agg_df.filter(col_all(filt)).withColumn('day', col('FinishedTimeStamp')-col('FinishedTimeStamp') % lit(86400000))\
                                       .groupBy('day', *partition_columns_pop).agg(
                                                     collect_set('TaskMonitorId').alias('task_set'),
                                                     sum(col('WrapCPU')/col('NCores')).alias('CoreTime'),
                                                     sum(col('WrapWC')).alias('WallTime')
                                                     ).withColumn('d', col('day')/1000).drop('day')

        # working_set_day.persist(StorageLevel.MEMORY_AND_DISK)
        # pop_day.persist(StorageLevel.MEMORY_AND_DISK)

        rolling_month = Window().partitionBy(*partition_columns).orderBy(col('day')).rangeBetween(-int(86400000*365.25/12), 0)
        rolling_month_pop = Window().partitionBy(*partition_columns_pop).orderBy(col('day')).rangeBetween(-int(86400000*365.25/12), 0)

        distinct_set_block = working_set_day.select(
                'day',
                collect_set(col('block_set')).over(rolling_month).alias('block_set_set'),
                *partition_columns
            ) \
            .select('day', explode('block_set_set').alias('block_set'), *partition_columns) \
            .select('day', explode('block_set').alias('elements'), *partition_columns) \
            .withColumn('end', col('day')/1000)\
            .withColumn('start', (col('day')-int(86400000*365.25/12))/1000)\
            .distinct()

        distinct_set = distinct_set_block.join(working_set_day_pop, (col('elements') == col('b_block_id')) & col('d').between(col('start'),col('end')))\
            .select('day',
                    collect_set(col('task_set')).over(rolling_month_pop).alias('task_set_set'),
                    sum('CoreTime').over(rolling_month_pop).alias('tot_CoreHrs'),
                    sum('WallTime').over(rolling_month_pop).alias('tot_WallHrs'),
                    *partition_columns_pop)\
            .select('day', 'tot_CoreHrs', 'tot_WallHrs', explode('task_set_set').alias('task_set'), *partition_columns_pop) \
            .select('day', 'tot_CoreHrs', 'tot_WallHrs', explode('task_set').alias('task'), *partition_columns_pop) \
            .dropDuplicates(['day', 'elements', 'task'])\
            .distinct().withColumn('date', from_unixtime(col('day')/1000)).withColumnRenamed('b_block_id', 'elements')\
            .groupby('date', 'elements')\
            .agg(
               size(collect_set('task')).alias('nacc'),
               mx('tot_CoreHrs').alias('CoreTime'),
               mx('tot_WallHrs').alias('WallTime')
               )

        setme = distinct_set.join(block_size, col('elements') == col('b_block_id')).dropDuplicates(['date', 'elements'])

        setme.persist(StorageLevel.MEMORY_AND_DISK)

        for n in naccs:
            nacc = (col('nacc') > n)
            filt = [nacc]

            temp.update({
                "%s_%s_%s" % (tier, filename, n): setme.filter(col_all(filt))
                                                       .groupby('date', 'elements').agg(
                                                                                   mx(col('block_size')).alias('el_size'),
                                                                                   mx('CoreTime').alias('ct'),
                                                                                   mx('WallTime').alias('wt')
                                                                                   )
                                                       .groupby('date').agg(
                                                                       sum(col('el_size')).alias('working_set_size'),
                                                                       sum('ct').alias('sum_CoreTime'),
                                                                       sum('wt').alias('sum_WallTime'),
                                                                       )
                                                       .withColumn('size', col('working_set_size')/1000000000000).drop('working_set_size')
                                                       .withColumn('CPUEff', (col('sum_CoreTime')/col('sum_WallTime')))
                                                       .withColumn('filename', lit(filename))
                                                       .filter(time_filter(struct(col('date'), col('filename'))))
                                                       .drop('filename')
                        })

            temp["%s_%s_%s" % (tier, filename, n)].persist(StorageLevel.MEMORY_AND_DISK)

for tier in my_tiers:
    for n in naccs:
        summe = None
        for file_ in filenames:
            filename = '18_%s_blk' % file_.replace("{", "")\
                                          .replace("}", "")\
                                          .replace(",", "")
            if not summe:
                summe = temp["%s_%s_%s" % (tier, filename, n)]
            else:
                summe = summe.union(temp["%s_%s_%s" % (tier, filename, n)])
        summe.write.format("com.databricks.spark.csv").option("header", "true")\
             .save('hdfs://analytix/cms/users/dciangot/jm_blk/nacc_every/blocks_%s_%s_%s_all' % ("T2_IT", tier, n))
