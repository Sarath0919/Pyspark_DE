import datetime
import os
import shutil

from resources.dev import config
from src.main.download.aws_file_download import S3FileDownloader
from src.main.move.move_files import move_s3_to_s3
from src.main.utility.s3_client_object import *
from src.main.utility.logging_config import *
from src.main.utility.encrypt_decrypt import *
from src.main.utility.my_sql_session import *
from src.main.read.aws_read import *
from src.main.utility.spark_session import spark_session
from pyspark.sql.types import *
from pyspark.sql.functions import *

aws_access_key = config.aws_access_key
aws_secret_key = config.aws_secret_key

s3_client_provider = S3ClientProvider(decrypt(aws_access_key), decrypt(aws_secret_key))
s3_client = s3_client_provider.get_client()

response = s3_client.list_buckets()

logger.info("List of buckets : %s", response['Buckets'])

csv_files = [file for file in os.listdir(config.local_directory) if file.endswith('.csv')]

connection = get_mysql_connection()
cursor = connection.cursor()

total_csv_files = []

if csv_files:
    for file in csv_files:
        total_csv_files.append(file)

    statement = f''' 
    select distinct file_name from
    spark_de.product_staging_table
    where file_name in ({str(total_csv_files)[1:-1]}) and status='I'
    '''
    logger.info(f"dynamically statement created: {statement}")
    cursor.execute(statement)
    data = cursor.fetchall()

    if data:
        logger.info("your last run was failed please check")
    else:
        logger.info("Last run was successful")
else:
    logger.info("Last run was successful")

try:
    s3_reader = S3Reader()

    folder_path = config.s3_source_directory
    s3_absolute_path = s3_reader.list_files(s3_client, config.bucket_name, folder_path)

    logger.info('Absolute path on s3 bucket for csv file %s', s3_absolute_path)

    if not s3_absolute_path:
        logger.info(f"No file available at {folder_path}")
        raise Exception("No data available to process")

except Exception as e:
    logger.error("Excited with error :- %s", e)
    raise e

bucket_name = config.bucket_name
local_directory = config.local_directory

prefix = f"s3://{bucket_name}/"
file_paths = [url[len(prefix):] for url in s3_absolute_path]
file_paths_csv = [url[len(prefix):] for url in s3_absolute_path if url.endswith('.csv')]
logging.info("File path available on s3 under %s bucket and folder name is %s", bucket_name, file_paths)
logger.info("File paths of csv : %s", file_paths_csv)

try:
    downloader = S3FileDownloader(s3_client, bucket_name, local_directory)
    downloader.download_files(file_paths)
except Exception as e:
    logger.error("File download error : %s", e)
    sys.exit()

all_files = os.listdir(local_directory)
logger.info("List of files present at my local directory after download - %s", all_files)

if all_files:
    csv_files = []
    error_files = []
    for files in all_files:
        if files.endswith('.csv'):
            csv_files.append(os.path.abspath(os.path.join(local_directory, files)))
        else:
            error_files.append(os.path.abspath(os.path.join(local_directory, files)))

    if not csv_files:
        logger.error("No csv data available to process the request")
        raise Exception("No csv data available to process the request")
else:
    logger.error("There is no data to process")
    raise Exception("There is no data to process")

logger.info("*****************Listing the file*******************")
logger.info(f"List of CSV files that needs to be processed {csv_files}")

logger.info("*****************Creating spark session**************")
spark = spark_session()

logger.info("*****************Spark session started*****************")

logger.info("*****************Checking schema for files in s3*****************")

correct_files = []
for data in csv_files:
    data_schema = spark.read.format('csv') \
        .option('header', 'true') \
        .load(data).columns
    logger.info(f"Schema for the {data} is {data_schema}")
    logger.info(f"Mandatory columns in schema is {config.mandatory_columns}")
    missing_columns = set(config.mandatory_columns) - set(data_schema)
    logger.info(f"{data} has missing columns : {missing_columns}")

    if missing_columns:
        error_files.append(data)
    else:
        correct_files.append(data)
        logger.info(f"No missing column for the {data}")

logger.info(f"**************list of correct files *******************{correct_files}")
logger.info(f"**************list of error files *********************{error_files}")
logger.info(f"**************Moving error data to error directory if any ***************")

error_file_local_path = config.error_folder_path_local

if error_files:
    for file in error_files:
        if os.path.exists(file):
            file_name = os.path.basename(file)
            destination_path = os.path.join(error_file_local_path, file_name)

            shutil.move(file, destination_path)
            logger.info(f"moved {file_name} from S3 file path to {destination_path}")

            source_prefix = config.s3_source_directory
            destination_prefix = config.s3_error_directory
            message = move_s3_to_s3(s3_client, config.bucket_name, source_prefix, destination_prefix,file_name)
            logger.info(f"{message}")
        else:
            logger.error(f"{file} does not exist")
else:
    logger.info(f"***********There is no error files available at our dataset****************")

logger.info(f"****************Updating the product_staging_table that we have started******************")
current_date = datetime.datetime.now()
insert_statements = []
db_name = config.database_name
formatted_date = current_date.strftime("%Y-%m-%d %H:%M:%S")

if correct_files:
    for file in correct_files:
        filename = os.path.basename(file)
        statement = f"INSERT INTO {db_name}.{config.product_staging_table} "\
                    f"(file_name, file_location, created_date, status)" \
                    f"VALUES ('{filename}','{filename}','{formatted_date}','A')"
        insert_statements.append(statement)

    logger.info(f"Insert statement created for staging table --- {insert_statements}")
    logger.info(f"*********************connecting with Mysql server ************************")

    connection=get_mysql_connection()
    cursor=connection.cursor()
    logger.info(f"******************Mysql server connected successfully *******************")
    for statement in insert_statements:
        cursor.execute(statement)
        connection.commit()

    cursor.close()
    connection.close()
else:
    logger.error(f"************There is no files to process")
    raise Exception(f"******************No data available with correct data***************")


logger.info(f"****************Staging table updated successfully**************")
logger.info(f"****************Fixing extra column coming from source ********************")

schema=StructType([
    StructField("customer_id", IntegerType(), True),
    StructField("store_id", IntegerType(), True),
    StructField("product_name", StringType(), True),
    StructField("sales_date", DateType(), True),
    StructField("sales_person_id", IntegerType(), True),
    StructField("price", FloatType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("total_cost", FloatType(), True),
    StructField("additional_column", StringType(), True),
])

final_df_to_process=spark.createDataFrame(data=[],schema=schema)

for data in correct_files:
    data_df=spark.read.format('csv')\
                      .option('header','true')\
                      .option('inferSchema','true')\
                      .load(data)
    data_schema=data_df.columns
    extra_columns=list(set(data_schema)-set(config.mandatory_columns))

    if extra_columns:
        logger.info(f"Extra columns present for {data} is {extra_columns}")
        data_df=data_df.withColumn("additional_column", concat_ws(",",*extra_columns))\
                       .select("customer_id","store_id","product_name","sales_date","sales_person_id",
                               "price","quantity","total_cost","additional_column")
        logger.info(f"Processed {data} and added 'additional-column' ")
    else:
        data_df=data_df.withColumn("additional_column",lit(None))\
                       .select("customer_id","store_id","product_name","sales_date","sales_person_id",
                               "price","quantity","total_cost","additional_column")
        logger.info(f"No additional columns were found for {data}")

    final_df_to_process=final_df_to_process.union(data_df)

logger.info(f"********* Final Dataframe from source which will be going to final processing*************")

final_df_to_process.show()
