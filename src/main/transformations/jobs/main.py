from resources.dev import config
from src.main.utility.s3_client_object import *
from src.main.utility.logging_config import *
from src.main.utility.encrypt_decrypt import *

aws_access_key = config.aws_access_key
aws_secret_key = config.aws_secret_key

s3_client_provider = S3ClientProvider(decrypt(aws_access_key), decrypt(aws_secret_key))

s3_client = s3_client_provider.get_client()

response = s3_client.list_buckets()
print(response)
response1=s3_client.list_objects_v2(Bucket='spark-project-testing')
logger.info("List of buckets : %s", response['Buckets'])
logger.info("List of buckets : %s", response1['Contents'])
