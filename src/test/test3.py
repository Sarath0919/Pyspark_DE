import os
import shutil

from resources.dev import config
from src.main.move.move_files import move_s3_to_s3
from src.main.utility.logging_config import *
from src.main.transformations.jobs.main import error_file_local_path, s3_client

error_files=['D:\\Data+engineering\\file_from_s3\\sales_data_less_2024-06-25.csv', 'D:\\Data+engineering\\file_from_s3\\sarath_accessKeys.csv', 'D:\\Data+engineering\\file_from_s3\\sarath_credentials.csv']
if error_files:
    for file in error_files:
        if os.path.exists(file):
            file_name = os.path.basename(file)
            destination_path = os.path.join(error_file_local_path, file_name)

            shutil.move(file, destination_path)
            logger.info(f"moved {file_name} from S3 file path to {destination_path}")

            source_prefix = config.s3_source_directory
            destination_prefix = config.s3_error_directory
            logger.info(f"{file_name}")
            message = move_s3_to_s3(s3_client, config.bucket_name, source_prefix, destination_prefix, file_name)
            logger.info(f"{message}")
        else:
            logger.error(f"{file} does not exist")
else:
    logger.ino(f"***********There is no error files available at our dataset****************")