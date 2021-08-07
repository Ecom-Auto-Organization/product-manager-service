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
        self._dynamo_client = boto3.client('dynamodb')
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


    def perform_create_job_transaction(self, job, updated_file):
        expression_attr_values = {':details': { 'S': json.dumps(updated_file['field_details'])}}
        try:
            response = self._dynamo_client.transact_write_items(
                TransactItems=[
                    {
                        'Put': {
                            'TableName': os.environ.get('bulk_manager_table'),
                            'Item': {
                                'PK': { 'S': utils.join_str('job#', job['id']) },
                                'SK': { 'S': utils.join_str('user#', job['user_id']) },
                                'SK1': { 'S': utils.join_str(job['start_time']) },
                                'SK2': { 'S': utils.join_str(job['type'], '#', job['start_time']) },
                                'status': { 'S': job['status'] },
                                'options': {'S': json.dumps(job['options'])}
                            },
                            'ConditionExpression': 'attribute_not_exists(PK)'
                        }
                    },
                    {
                        'Update': {
                            'TableName': os.environ.get('bulk_manager_table'),
                            'Key': {
                                'PK': { 'S': utils.join_str('file#', updated_file['id']) },
                                'SK': { 'S': 'file' },
                            },
                            'UpdateExpression': 'SET field_details=:details',
                            'ExpressionAttributeValues': expression_attr_values
                        }
                    },
                    {
                        'Update': {
                            'TableName': os.environ.get('bulk_manager_table'),
                            'Key': {
                                'PK': { 'S': utils.join_str('user#', job['user_id']) },
                                'SK': { 'S': 'user' },
                            },
                            'UpdateExpression': 'SET job_count = job_count + :incr',
                            'ExpressionAttributeValues': {
                                ':incr': { 'N': '1' }
                            }
                        }
                    }
                ]
            )
            logging.info('Job creation transaction completed successfully. Details: %s', job)
            return {'id': job['id']}
        except ClientError as error:
            raise DataAccessError(error)
        except Exception as error:
            raise DataAccessError(error)


    def get_jobs(self, user_id):
        # The maximum size of data that can be retrieved from dynamodb is 1MB so we will be retrieving data in batches.
        user_id = utils.join_str('user#', user_id)
        limit = 500
        response_items = []
        lastEvaluatedKey = None

        try:
            response = self._bulk_manager_table.query(
                IndexName = 'GSI2',
                KeyConditionExpression=Key('SK').eq(user_id),
                ScanIndexForward=False,
                Limit=limit,
            )
            # The 'Item' property should always exist in the query response.
            if 'Items' not in response: 
                raise DataAccessError('Error occurred whiles querying for user jobs. Details: user_id: ' + user_id + ' response: ' + response)  
            response_items.extend(response['Items'])   
            
            # LastEvaluatedKey indicates that there is still data to be retrieved from the query,
            # we will keep on querying until there is not lastevaluatedkey in the response.
            if 'LastEvaluatedKey' in response: lastEvaluatedKey = response['LastEvaluatedKey']  
            while lastEvaluatedKey is not None:
                response = self._bulk_manager_table.query(
                    IndexName = 'GSI2',
                    KeyConditionExpression=Key('SK').eq(user_id),
                    ScanIndexForward=False,
                    Limit=limit,
                    ExclusiveStartKey=lastEvaluatedKey
                )
                if 'Items' not in response: 
                    raise DataAccessError('Error occurred whiles in user jobs query. Details: user_id: ' + user_id + ' response: ' + response)  
                response_items.extend(response['Items'])  
                if 'LastEvaluatedKey' in response: 
                    lastEvaluatedKey = response['LastEvaluatedKey']
                else:
                    lastEvaluatedKey = None  

            if len(response_items) == 0: return []
            jobs = [data_utils.extract_job_details(item) for item in response_items]
            return jobs
        except ClientError as error:
            raise DataAccessError(error)
        except Exception as error:
            raise DataAccessError(error)


    def get_job_details(self, jobObject):
        db_job = data_utils.convert_to_db_job(jobObject)

        try:
            response = self._bulk_manager_table.get_item(Key=db_job)
            job = None
            if 'Item' in response:
                db_job = response['Item']
                job = data_utils.extract_job_details(db_job)
            return job
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
