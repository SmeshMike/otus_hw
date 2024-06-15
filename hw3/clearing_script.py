from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType, IntegerType, DoubleType
from pyspark.sql.functions import *


def clear_data(input_s3_path, output_s3_path):
    spark = SparkSession.builder.getOrCreate()
    hadoopConf=spark.sparkContext._jsc.hadoopConfiguration()
    hadoopConf.set("fs.s3a.endpoint", "https://storage.yandexcloud.net")
    hadoopConf.set("fs.s3a.access.key", "***")
    hadoopConf.set("fs.s3a.secret.key", "***")
    hadoopConf.set('spark.jars.packages', 'org.apache.hadoop:hadoop-aws:3.2.0')
    hadoopConf.set('spark.hadoop.fs.s3a.aws.credentials.provider', 'org.apache.hadoop.fs.s3a.AnonymousAWSCredentialsProvider')

    schema = __get_schema()
    original_df = spark.read.schema(schema).csv(input_s3_path)
    df = __drop_nulls(original_df)
    df = __drop_dublicates_by_tranaction_id(df)
    df = __filter_zero_value_tranzation(df)
    df = __filter_negative_customer_id(df)
    df.coalesce(200).write.parquet(output_s3_path)



def __get_schema():
      schema = StructType() \
            .add("tranaction_id",IntegerType()) \
            .add("tx_datetime",StringType()) \
            .add("customer_id",IntegerType()) \
            .add("terminal_id",IntegerType()) \
            .add("tx_amount",DoubleType()) \
            .add("tx_time_seconds",IntegerType()) \
            .add("tx_time_days",IntegerType()) \
            .add("tx_fraud",IntegerType()) \
            .add("tx_fraud_scenario",IntegerType())
      return schema

def __drop_nulls(df):
    clear_df = df.filter(df.tranaction_id.isNotNull()) \
    .filter(df.tx_datetime.isNotNull()) \
    .filter(df.customer_id.isNotNull()) \
    .filter(df.terminal_id.isNotNull()) \
    .filter(df.tx_amount.isNotNull()) \
    .filter(df.tx_time_seconds.isNotNull()) \
    .filter(df.tx_time_days.isNotNull()) \
    .filter(df.tx_fraud.isNotNull()) \
    .filter(df.tx_fraud_scenario.isNotNull())
    return clear_df

def __drop_dublicates_by_tranaction_id(df):
    return df.dropDuplicates(['tranaction_id'])

def __filter_zero_value_tranzation(df):
    return df.filter(df.tx_amount != 0.0)

def __filter_negative_customer_id(df):
    return df.filter(df.customer_id > 0)