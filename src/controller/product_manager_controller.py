from datamodel.custom_enums import HTTPMethod
from service.product_manager_service import ProductManagerService
from datamodel.custom_exceptions import IllegalArgumentError
from datamodel.custom_exceptions import DataAccessError
from datamodel.custom_exceptions import UserAuthenticationError
from datamodel.custom_exceptions import HeaderRowNotFoundError
from datamodel.custom_exceptions import WrongFileFormat
from datamodel.custom_exceptions import EmptySheetError
from http import HTTPStatus
import logging
import json

logging.basicConfig(level=logging.INFO)

class ProductManagerController:
    """ 
    Class to invoke service to perform request action
    and return http response to app

    Methods
    -------
    invoke (): calls the appropraite method to perform request action
    and return http response to app
    """

    def __init__(self, request, method, path):
        self._request = request
        self._method = method
        self._path = path
        self._product_manager_service = ProductManagerService(request)

    
    def invoke(self):
        """ 
        calls the appropraite method to perform request action
        and return http response to app
        """

        if self._path == '/upload' and self._method == HTTPMethod.POST.name:
            response = self.__get_file_details()
        elif self._path == '/run' and self._method == HTTPMethod.POST.name:
            response = self.__put_job()
        elif self._path == '/users' and self._method == HTTPMethod.POST.name:
            response = self.__get_user_details()
        elif self._path == '/jobs' and self._method == HTTPMethod.POST.name:
            response = self.__get_jobs()
        elif self._path == '/jobs/{jobId}' and self._method == HTTPMethod.GET.name:
            response = self.__get_job_details()
        else:
            logging.error('Invalid Path. Path: ' + self._path)
            response = {
                'statusCode': HTTPStatus.NOT_FOUND
            }
        if 'headers' not in response:
            response['headers'] = {
            "Access-Control-Allow-Headers" : "application/json",
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Methods": "OPTIONS,POST,GET"
        }
        return response

    
    def __get_file_details(self):
        """
        Decodes and gets the details of an excel or 
        csv file to be used for bulk import
        """

        try:
            file_details = self._product_manager_service.get_file_details()
            return {
                'statusCode': HTTPStatus.OK,
                'body': json.dumps(file_details)
            }
        except IllegalArgumentError as error:
            logging.exception(error)
            return {
                'statusCode': HTTPStatus.BAD_REQUEST
            }
        except EmptySheetError as error:
            logging.exception(error)
            return {
                'statusCode': HTTPStatus.BAD_REQUEST,
                'body': json.dumps({'errorCode': 'NO_PRODUCT_FOUND'})
            }
        except HeaderRowNotFoundError as error:
            logging.exception(error)
            return {
                'statusCode': HTTPStatus.BAD_REQUEST,
                'body': json.dumps({'errorCode': 'HEADER_NOT_FOUND'})
            }
        except WrongFileFormat as error:
            logging.exception(error)
            return {
                'statusCode': HTTPStatus.BAD_REQUEST,
                'body': json.dumps({'errorCode': 'WRONG_FILE_FORMAT'})
            }
        except DataAccessError as error:
            logging.exception(error)
            return {
                'statusCode': HTTPStatus.SERVICE_UNAVAILABLE
            }
        except Exception as error:
            logging.exception(error)
            return {
                'statusCode': HTTPStatus.INTERNAL_SERVER_ERROR
            }


    def __put_job(self):
        """Create job from task details and returns job details to user"""

        try:
            job_details = self._product_manager_service.create_job()
            return {
                'statusCode': HTTPStatus.OK,
                'body': json.dumps(job_details)
            }
        except IllegalArgumentError as error:
            logging.exception(error)
            return {
                'statusCode': HTTPStatus.BAD_REQUEST
            }
        except Exception as error:
            logging.exception(error)
            return {
                'statusCode': HTTPStatus.INTERNAL_SERVER_ERROR
            }


    def __get_user_details(self):
        try:
            user_details = self._product_manager_service.get_user()
            return {
                'statusCode': HTTPStatus.OK,
                'body': json.dumps(user_details)
            }
        except IllegalArgumentError as error:
            logging.exception(error)
            return {
                'statusCode': HTTPStatus.BAD_REQUEST
            }
        except UserAuthenticationError as error:
            logging.exception(error)
            return {
                'statusCode': HTTPStatus.UNAUTHORIZED
            }
        except Exception as error:
            logging.exception(error)
            return {
                'statusCode': HTTPStatus.INTERNAL_SERVER_ERROR
            }


    def __get_jobs(self):
        try:
            jobs = self._product_manager_service.get_jobs()
            return {
                'statusCode': HTTPStatus.OK,
                'body': json.dumps(jobs)
            }
        except IllegalArgumentError as error:
            logging.exception(error)
            return {
                'statusCode': HTTPStatus.BAD_REQUEST
            }
        except Exception as error:
            logging.exception(error)
            return {
                'statusCode': HTTPStatus.INTERNAL_SERVER_ERROR
            }


    def __get_job_details(self):
        try:
            job = self._product_manager_service.get_job_details()
            return {
                'statusCode': HTTPStatus.OK,
                'body': json.dumps(job)
            }
        except IllegalArgumentError as error:
            logging.exception(error)
            return {
                'statusCode': HTTPStatus.BAD_REQUEST
            }
        except Exception as error:
            logging.exception(error)
            return {
                'statusCode': HTTPStatus.INTERNAL_SERVER_ERROR
            }