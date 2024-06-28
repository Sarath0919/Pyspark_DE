import findspark
findspark.init()
from pyspark.sql import SparkSession
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from src.main.utility.logging_config import *
from src.main.utility import encrypt_decrypt
from resources.dev import config

Access_key_ID=encrypt_decrypt.decrypt(config.aws_access_key)
Secret_access_key=encrypt_decrypt.decrypt(config.aws_secret_key)

def spark_session():
    spark = SparkSession.builder.master("local[*]") \
        .appName("spark_DE")\
        .config("spark.driver.extraClassPath", "C:\\my_sql_jar\\mysql-connector-java-8.0.26.jar") \
        .getOrCreate()
    spark.sparkContext._jsc.hadoopConfiguration().set("com.amazonaws.services.s3.enableV4", "true")
    spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.aws.credentials.provider","com.amazonaws.auth.InstanceProfileCredentialsProvider,com.amazonaws.auth.DefaultAWSCredentialsProviderChain")
    spark.sparkContext._jsc.hadoopConfiguration().set("fs.AbstractFileSystem.s3a.impl", "org.apache.hadoop.fs.s3a.S3A")
    spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.access.key", Access_key_ID)
    spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.secret.key", Secret_access_key)
    spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.endpoint", "s3.ap-south-1.amazonaws.com")

    logger.info("spark session %s",spark)
    return spark