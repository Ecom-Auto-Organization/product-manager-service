from datetime import datetime
import json
import email
import base64
from datamodel.custom_exceptions import IllegalArgumentError
from datamodel.custom_exceptions import UserAuthenticationError
from dataaccess.product_manager_data_access import ProductManagerDataAccess
from datamodel.custom_enums import FileType
from datamodel.custom_enums import HeaderOption
from datamodel.custom_enums import TaskType
from datamodel.custom_enums import JobStatus
from datamodel.custom_enums import ExecutionType
from utility.file_reader_util import FileReader
import logging
import uuid

logging.basicConfig(level=logging.INFO)


class ProductManagerService:
    """ 
    Class to perform request actions

    Methods
    -------
    get_file_details(): Decodes excel or csv binary file 
    and returns the details for import
    """

    def __init__(self, request):
        self._request_body = None
        try:
            self._request_body = json.loads(request.get('body'))
        except Exception:
            self._request_body = request.get('body')
        self._query_params = request.get('query_params')
        self._path_params = request.get('path_params')
        self._user_context = None
        if 'user_context' in request:
            self._user_context = request.get('user_context')
        self._header = request.get('header')
        self._pm_access = ProductManagerDataAccess()
        
    
    def get_file_details(self):
        """Decodes excel or csv binary file and returns the details for import"""

        multi_form_data = base64.b64decode(self._request_body)
        content_type = None
        if self._header.get('content-type') is not None:
            content_type = self._header.get('content-type')
        elif self._header.get('Content-Type') is not None:
            content_type = self._header.get('Content-Type')
        content_type = 'Content-Type: ' + content_type + '\n'
        form_data = email.message_from_bytes(content_type.encode() + multi_form_data)
        user_id = self._user_context.get('userId')
        user_details = self._pm_access.get_user_by_id(user_id)

        if not form_data.is_multipart():
            raise IllegalArgumentError('Form data could not be processed. Multipart is False')

        form_content = {}
        for part in form_data.get_payload():
            name = part.get_param('name', header='content-disposition')
            should_decode_payload = False
            if not name:
                continue
            if name == 'file':
                should_decode_payload = True
            part_payload = {
                'content': part.get_payload(decode=should_decode_payload),
                'content_type': part.get_content_type()
            }
            if name == 'file':
                part_payload['file_name'] = part.get_filename()
            form_content[name] = part_payload
        
        file_type = None
        if form_content['file']['content_type'] == 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet' or form_content['file']['content_type'] =='application/vnd.ms-excel':
            file_type = FileType.EXCEL
        elif form_content['file']['content_type'] == 'text/csv':
            file_type = FileType.CSV
        else:
            logging.error('Does not recognize media type: ', form_content['file']['content_type'])

        reader_info = {
            'file': form_content['file']['content'],
            'type': file_type,
            'header': {
                'option': HeaderOption[form_content['header-option']['content']]
            }
        }
    
        if form_content['header-option']['content'] == 'FIND' or form_content['header-option']['content'] == 'EXACT':
            if form_content['header-option']['content'] == 'FIND':
                reader_info['header']['value'] = form_content['column-name']['content'] 
            elif form_content['header-option']['content'] == 'EXACT':
                reader_info['header']['value'] = int(form_content['header-row']['content']) - 1
        
        file_reader = FileReader(file_reader_info=reader_info)
        file_details = file_reader.get_file_details()
        locations_response = self._pm_access.get_locations(user_details.get('domain'), user_details.get('access_token'))
        locations = self.__parse_locations(locations_response)
        file_id = '' + str(uuid.uuid4())
        
        file_s3_key = file_id + '_' + form_content['file']['file_name']
        file_obj = {
            'id': file_id,
            'idle': 'false',
            'file_name': form_content['file']['file_name'],
            'file_type': file_details['fileType'],
            's3_key': file_s3_key,
            'actual_row_count': file_details['actualRowCount'],
            'header_row': file_details['headerRow']
        }

        file_details['fileName'] = form_content['file']['file_name']
        file_details['fileId'] = file_id
        file_details['locations'] = locations
        del file_details['headerRow']

        self._pm_access.save_to_s3(file_s3_key, form_content['file']['content'])
        self._pm_access.put_file(file_obj)
        return file_details


    def create_job(self):
        task_type = TaskType[self._request_body.get('taskType')]
        if task_type == TaskType.IMPORT_CREATE:
            job_details = self.__create_import_job()
            publish_message = {
                'fileId': self._request_body.get('fileId'),
                'jobId': job_details.get('jobId'),
                'userId': self._user_context.get('userId')
            }
            self._pm_access.publish_to_product_generator(publish_message)  
            return job_details
        else:
            raise IllegalArgumentError('Unrecognized job type')


    def get_user(self):
        if 'userId' not in self._user_context:
            raise IllegalArgumentError('UserId not present in request')
        
        user_id = self._user_context.get('userId')
        user_details = self._pm_access.get_user_by_id(user_id)

        # we need to check to ensure user is still active.
        if user_details.get('active') is not True:
            raise UserAuthenticationError('User is not active. User: ' + user_id)
        
        return {
           'name': user_details.get('owner'),
           'email': user_details.get('email'),
           'reviewed': user_details.get('reviewed'),
           'shopName': user_details.get('shop_name'),
           'shopDomain': user_details.get('domain'),
           'subscribtion': user_details.get('subscribtion'),
           'timeZone': user_details.get('time_zone'),
           'jobCount': int(user_details.get('job_count')),
           'activeJobCount': int(user_details.get('active_job_count'))
        }


    def get_jobs(self):
        if 'userId' not in self._user_context:
            raise IllegalArgumentError('UserId not present in request') 

        user_id = self._user_context.get('userId')
        user_jobs = self._pm_access.get_jobs(user_id)
        return user_jobs


    def get_job_results(self):
        if 'userId' not in self._user_context:
            raise IllegalArgumentError('UserId not present in request') 
        if self._path_params is None:
            raise IllegalArgumentError('Job id is not present in job results request') 
        if 'jobId' not in self._path_params:
            raise IllegalArgumentError('Job id is not present in job results request path params')
        
        job_id = self._path_params.get('jobId')
        job_results = self._pm_access.get_job_results(job_id)
        return job_results


    def get_job_details(self):
        if 'userId' not in self._user_context:
            raise IllegalArgumentError('UserId not present in request') 
        if self._path_params is None:
            raise IllegalArgumentError('Job id is not present in job details request') 
        if 'jobId' not in self._path_params:
            raise IllegalArgumentError('Job id is not present in job details request path params') 

        user_id = self._user_context.get('userId')
        job_id = self._path_params.get('jobId')
        primaryKey = {'id': job_id, 'user_id': user_id}
        job = self._pm_access.get_job_details(primaryKey)
        if job['status'] == 'COMPLETED' or job['status'] == 'FAILED' or job['status'] == 'PARTIALLY COMPLETED':
            job_results = self._pm_access.get_job_results(job_id)
            job['results'] = job_results
        return job


    def __create_import_job(self):
        """ Gets the details of a task request and create product import job to be run"""

        task_type = TaskType[self._request_body.get('taskType')]
        # execution_type = ExecutionType[self._request_body.get('executionType')]
        file_id = self._request_body.get('fileId')
        options = self._request_body.get('options')
        file_column_details = self._request_body.get('fileColumnDetails')
        shopify_field = self.__get_shopify_fields(file_column_details)

        file_obj = self._pm_access.get_file_by_id(file_id)
        updated_file = {'id': file_obj.get('id'), 'field_details': shopify_field}
        job_id = '' + str(uuid.uuid4())
        start_time = datetime.utcnow().isoformat() + 'Z'
        new_job = {
            'id': job_id,
            'user_id': self._user_context.get('userId'),
            'status': JobStatus.SUBMITTED.name,
            'type': task_type.name,
            'options': options,
            'start_time': start_time
        }
        created_job = self._pm_access.perform_create_job_transaction(new_job, updated_file)
        return {
            'jobId': created_job.get('id')
        }


    def __get_shopify_fields(self, file_column_details):
        shopify_field = {}
        location_set = set({})
        
        for column in file_column_details:
            if column['field'] is None:
                continue
            if column['field'] in shopify_field:
                if (
                    column['field'] == 'metafields' or column['field'] == 'imageSrc' or 
                    column['field'] == 'descriptionHtml' or column['field'] == 'customCollections'
                ):
                    column_field = column['field']
                    del column['field']
                    del column['name']
                    shopify_field[column_field].append(column)
                elif column['field'] == 'variantQuantity':
                    if column['location'] not in location_set:
                        column_field = column['field']
                        del column['field']
                        del column['name']
                        shopify_field[column_field].append(column)
                    else:
                        continue
            else:
                column_field = column['field']
                del column['field']
                del column['name']
                shopify_field[column_field] = [column]
                if column_field == 'variantQuantity':
                    location_set.add(column['location'])
        return shopify_field


    def __parse_locations(self, response):
        locations = []
        if response['data']['locations']['edges'] is not None:
            edges = response['data']['locations']['edges']
            for edge in edges:
                locations.append({
                    'id': edge['node']['id'],
                    'name': edge['node']['name']
                })
        else:
            raise Exception('Could not find user locations. Location Response: ', response)
        return locations

