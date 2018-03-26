# # Initialization
# ## Import relevant Spark and Python libraries
from pyspark.sql import SparkSession, functions as F, types as T
from pyspark import StorageLevel as S

from datetime import datetime as dt
import os

# ## Connect to the Spark cluster
spark = SparkSession\
  .builder\
  .appName("ProcessDoubleClick")\
  .getOrCreate()

# # Data Import
# Read in raw data
impressionFields = [T.StructField('advertiserID', T.IntegerType(), False),
                   T.StructField('domain', T.StringType(), False),
                   T.StructField('viewable', T.BooleanType(), False),
                   T.StructField('city', T.StringType(), False),
                   T.StructField('mobileDevice', T.StringType(), False),
                   T.StructField('country', T.StringType(), False),
                   T.StructField('sellerPrice', T.IntegerType(), False),
                   T.StructField('userID', T.IntegerType(), False),
                   T.StructField('impressionID', T.IntegerType(), False),
                   T.StructField('postalCode', T.StringType(), False),
                   T.StructField('carrier', T.StringType(), False),
                   T.StructField('eventType', T.StringType(), False),
                   T.StructField('lineItemID', T.IntegerType(), False),
                   T.StructField('time', T.TimestampType(), False),
                   T.StructField('duration', T.IntegerType(), False),
                   T.StructField('browser', T.StringType(), False),
                   T.StructField('os', T.StringType(), False),
                   T.StructField('audienceSegmentID', T.IntegerType(), False)]
impressionSchema = T.StructType(impressionFields)

currentDate = dt.now().strftime('%Y-%m-%d')
impressionsFiles = os.path.join('gs://sgupta_doubleclick','staging','*',currentDate,'impressions.csv')

rawImpressions = spark.read.load(impressionsFiles, format="csv", header=True, schema=impressionSchema)\
  .withColumn('filename', F.input_file_name())\
  .withColumn('clientID',F.regexp_extract('filename','.*staging/([0-9]*)/.*',1).cast('int'))\
  .withColumn('date',F.regexp_extract('filename','.*staging/[0-9]*/([0123456789-]*)/.*',1).cast('date'))\
  .drop('filename')
rawImpressions.printSchema()
rawImpressions.persist(S.MEMORY_ONLY)
rawImpressions.show()
rawImpressions.registerTempTable('rawimpressions')

matchSchema = T.StructType([T.StructField('matchType', T.StringType(), False),
                            T.StructField('matchID', T.IntegerType(), False),
                            T.StructField('matchName', T.StringType(), False)])

matchFiles = os.path.join('gs://sgupta_doubleclick','staging','*',currentDate,'match.csv')

rawMatches = spark.read.load(matchFiles, format="csv", header=True, schema=matchSchema)\
  .withColumn('filename', F.input_file_name())\
  .withColumn('clientID',F.regexp_extract('filename','.*staging/([0-9]*)/.*',1).cast('int'))\
  .withColumn('date',F.regexp_extract('filename','.*staging/[0-9]*/([0123456789-]*)/.*',1).cast('date'))\
  .drop('filename')
rawMatches.printSchema()
rawMatches.persist(S.MEMORY_ONLY)
rawMatches.show()
rawMatches.registerTempTable('rawmatches')

spark.sql('create temporary view impressions1 as \
           select i.*, m.matchName as advertiser \
           from rawimpressions i, rawmatches m \
           where m.matchType="advertiser" \
            and i.advertiserID = m.matchID \
            and i.clientID = m.clientID \
            and i.date = m.date')

spark.sql('create temporary view impressions2 as \
           select i.*, m.matchName as lineItem \
           from impressions1 i, rawmatches m \
           where m.matchType="line_item" \
            and i.lineItemID = m.matchID \
            and i.clientID = m.clientID \
            and i.date = m.date')

impressions = spark.sql('select i.*, m.matchName as audienceSegment \
                         from impressions2 i, rawmatches m \
                         where m.matchType="audience_segment" \
                          and i.lineItemID = m.matchID \
                          and i.clientID = m.clientID \
                          and i.date = m.date')\
  .withColumn('adSize',F.split('lineItem','_')[0])\
  .withColumn('adLocation',F.split('lineItem','_')[1])\
  .withColumn('gender',F.split('audienceSegment','_')[0])\
  .withColumn('age',F.split('audienceSegment','_')[1])\
  .withColumn('segment',F.split('audienceSegment','_')[2])\
  .withColumn('clientName', F.concat(F.lit('Client '), F.col('clientID')))\
  .drop('advertiserID','lineItemID','audienceSegmentID','lineItem','audienceSegment')

impressions.printSchema()

impressions.write.parquet(os.path.join('gs://sgupta_doubleclick','impressions'), 
                          mode='append', compression='snappy', partitionBy=['clientID','date'])