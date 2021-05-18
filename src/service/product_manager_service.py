from datetime import datetime
import json
import email
import base64
from datamodel.custom_exceptions import IllegalArgumentError
from dataaccess.product_manager_data_access import ProductManagerDataAccess
from datamodel.custom_enums import FileType
from datamodel.custom_enums import HeaderOption
from utility.file_reader_util import FileReader
import logging
import uuid
import boto3

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
        content_type = 'Content-Type: ' + self._header.get('Content-Type') + '\n'
        form_data = email.message_from_bytes(content_type.encode() + multi_form_data)

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
        if form_content['file']['content_type'] == 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet':
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
        del file_details['headerRow']

        is_object_saved = self._pm_access.save_to_s3(file_s3_key, form_content['file']['content'])
        is_file_info_saved = self._pm_access.put_file(file_obj)

        return file_details

