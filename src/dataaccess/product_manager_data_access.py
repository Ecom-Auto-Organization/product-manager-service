from datamodel.custom_exceptions import DataAccessError
import dataaccess.data_model_utils as data_utils
from utility import utils
import boto3
from botocore.exceptions import ClientError
from boto3.dynamodb.conditions import Key
import os
import logging
import json

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
        self._sns_client = boto3.client('sns')
    
    
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


    def get_user_by_id(self, user_id):
        user_to_get = {'id': user_id}
        db_user = data_utils.convert_to_db_user(user_to_get)

        try:
            response = self._bulk_manager_table.get_item(Key=db_user)
            user = None
            if 'Item' in response:
                db_user = response['Item']
                user = data_utils.extract_user_details(db_user)

            return user
        
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


    def get_file_by_id(self, file_id):
        file_to_get = {'id': file_id}
        db_file = data_utils.convert_to_db_file(file_to_get)

        try:
            response = self._bulk_manager_table.get_item(Key=db_file)
            file_obj = None
            if 'Item' in response:
                db_file = response['Item']
                file_obj = data_utils.extract_file_details(db_file)
            return file_obj
        except ClientError as error:
            raise DataAccessError(error)


    def basic_file_update(self, file_obj):
        if 'id' not in file_obj:
            raise KeyError('\'id\' value for file cannot be null')

        db_file = data_utils.convert_to_db_file(file_obj)
        # assign primary key to Keys Attribute and remove primary keys 
        # from db_file since we don't intent to modify them
        primary_key = {'PK': db_file['PK'], 'SK': db_file['SK']}
        del db_file['PK']
        del db_file['SK']

        expression_attr_values = utils.get_expression_attr_values(db_file)
        update_expression = utils.get_update_expression(expression_attr_values)
        
        try:
            response = self._bulk_manager_table.update_item(
                Key=primary_key,
                UpdateExpression=update_expression,
                ExpressionAttributeValues=expression_attr_values,
                ReturnValues='UPDATED_NEW'
            )

            logging.info('Updated file successfully: %s', response)
            return True
        except ClientError as error:
            raise DataAccessError(error)
        except Exception as error:
            raise DataAccessError(error)


    def create_job(self, job):
        db_job = data_utils.convert_to_db_job(job)

        try:
            response = self._bulk_manager_table.put_item(
                Item=db_job
            )

            logging.info('Created Job successfully. Details: %s', db_job)
            return data_utils.extract_job_details(db_job)
        except ClientError as error:
            raise DataAccessError(error)
        except Exception as error:
            raise DataAccessError(error)


    def get_jobs(self, user_id, lastEvaluatedKey=None):
        user_id = utils.join_str('user#', user_id)
        limit = 500

        try:
            # For now boto3 doesn't allow Empty or none value for ExclusiveStartKey so if there is an 
            # exclusive start key, I make another method :(
            if lastEvaluatedKey is None:
                response = self._bulk_manager_table.query(
                    IndexName = 'GSI2',
                    KeyConditionExpression=Key('SK').eq(user_id),
                    ScanIndexForward=False,
                    Limit=limit,
                )
            else:
                response = self._bulk_manager_table.query(
                    IndexName = 'GSI2',
                    KeyConditionExpression=Key('SK').eq(user_id),
                    ScanIndexForward=False,
                    Limit=limit,
                    ExclusiveStartKey=lastEvaluatedKey
                )

            # The 'Item' property should always exist in the query response.
            if 'Items' not in response: 
                raise DataAccessError('Error occurred whiles in user jobs query. Details: user_id: ' + user_id + ' lastKey: ' + lastEvaluatedKey)
            
            response_items = response['Items']
            jobs = [data_utils.extract_job_details(item) for item in response_items]
            lastKey = None
            if 'LastEvaluatedKey' in response: lastKey = response['LastEvaluatedKey']['PK'] + '~' + response['LastEvaluatedKey']['SK']
            query_res = {'jobs': jobs}
            if lastKey is not None: query_res['lastKey'] = lastKey
            return query_res
        except ClientError as error:
            raise DataAccessError(error)


    def publish_to_product_generator(self, message):
        import_topic = os.environ.get('import_topic_arn')
        try:
            response = self._sns_client.publish(
                TopicArn=import_topic,
                Message=json.dumps(message),
                MessageAttributes={
                    'process': {
                        'DataType': 'String',
                        'StringValue': 'generate-product'
                    }
                }
            )
            if 'MessageId' in response:
                return True
        except Exception as error:
            raise Exception('Could not publish message successfully. Error:' + str(error))
