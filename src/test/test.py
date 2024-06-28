from pyspark.sql import *
from pyspark.sql.functions import *
from resources.dev import config
from src.main.utility import encrypt_decrypt
from pyspark import SparkConf
from pyspark.sql import SparkSession


conf = SparkConf()
conf.setMaster("local")
#conf.set("spark.jars.packages","org.apache.hadoop:hadoop-aws:3.2.0")
conf.set("spark.hadoop.fs.s3a.aws.credentials.provider","com.amazonaws.auth.InstanceProfileCredentialsProvider,com.amazonaws.auth.DefaultAWSCredentialsProviderChain")
conf.set("spark.hadoop.fs.s3a.access.key", encrypt_decrypt.decrypt(config.aws_access_key))
conf.set("spark.hadoop.fs.s3a.secret.key", encrypt_decrypt.decrypt(config.aws_secret_key))
conf.set("spark.hadoop.fs.s3a.endpoint","s3.ap-south-1.amazonaws.com")
#conf.set("spark.hadoop.fs.s3a.session.token", "IQoJb3JpZ2luX2VjEAIaCmFwLXNvdXRoLTEiRjBEAiBz94YabaAHSrFxjaTHMboZAzTutFxtmRGnDwp0lVS0hQIgZFs0WdrO5crohzzuYw4fuRT4kqwzo4sAMQCKC9CpGPQq8wEIq///////////ARADGgw2MjkwNTg5OTAyODEiDK3lXDOEgny+LmliiyrHAUmLh0yXNW/ldi5BefUwO+JkwQNddySwh/h7F48yruKvz0rsu6EjVMYTWR3qTfFx61NEem30MI8t6EOVIiawANZAfMMpRXnQlJiI0ahaGZxbutUIwLMdf8uzpBdir6tgZ7iOvpjBP6byVtmusl7fcNXlXInkQq6sq9/i+V0V2EK1ZbDGSW1dsN8g0dOW7n1qhH5NZLmxz6ukItHB3Z6MHhaqE5UMQwNFZ7vnwPkLb56x1PVbnydZadPSxS1HTnezpSXb46bl4kQw2+HmswY6mQH1/J7FJgSRy87JeJAoeo3W0XHLkenrWgwpiMahXNXGVlrgZxi41ev7mcuMtdGzNk6I2sRzadAu81sjdd3tAo1dvAMCxdA4vpc/cTT5S9yuFJTptVlGyq+3uG/ZIwbxS6XZogS8cKMHDvmay9vokQ2PMI1SmQM0u8q8r3R2pZ77crBIdpZWeqtF/q5TwS2vge0YP7vY8zfNwbw=")

spark = SparkSession.builder.config(conf=conf).getOrCreate()

#aws sts assume-role --role-arn arn:aws:iam::629058990281:role/s3_spark --role-session-name "Sparksession" --profile sarath > assume-role-output.txt

df=spark.read.csv('s3a://spark-project-testing/sales_data/sarath_accessKeys.csv')
df.show()