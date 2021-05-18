import requests
from datamodel.custom_exceptions import DataAccessError
import dataaccess.data_model_utils as data_utils
import boto3
from botocore.exceptions import ClientError
import os
import logging

logging.basicConfig(level=logging.INFO)

class ProductManagerDataAccess:
    """ 
    Class for getting data and adding data to database and other sources

    """

    def __init__(self):
        self._upload_bucket = os.environ.get('s3_file_upload_bucket')
        self._s3_client = boto3.client('s3')
        bulk_manager_table = os.environ.get('bulk_manager_table')
        self._dynamodb = boto3.resource('dynamodb')
        self._bulk_manager_table =  self._dynamodb.Table(bulk_manager_table) 
    
    def save_to_s3 (self, file_key, file_content):
        try:
            response = self._s3_client.put_object (
                Bucket=self._upload_bucket,
                Body=file_content,
                Key=file_key
            )
            return True
        except ClientError as error:
            raise DataAccessError(error)

    
    def put_file(self, file_obj):
        db_file = data_utils.convert_to_db_file(file_obj)
        
        try:
            response = self._bulk_manager_table.put_item(
                Item=db_file
            )
            logging.info('Put File in Database Successfully. Details: %s', db_file)
            return True
        except ClientError as error:
            raise DataAccessError(error)
        except Exception as error:
            raise DataAccessError(error)
