from datamodel.custom_enums import HTTPMethod
from service.product_manager_service import ProductManagerService
from datamodel.custom_exceptions import IllegalArgumentError
from datamodel.custom_exceptions import DataAccessError
from datamodel.custom_exceptions import UserAuthenticationError
from datamodel.custom_exceptions import ShopifyUnauthorizedError
from datamodel.custom_exceptions import HeaderRowNotFoundError
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
            return self.__get_file_details()
        elif self._path == '/run' and self._method == HTTPMethod.POST.name:
            return self.__put_job()
        else:
            logging.error('Invalid Path. Path: ' + self._path)
            return {
                'statusCode': HTTPStatus.NOT_FOUND
            }

    
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
            return {
                'statusCode': HTTPStatus.BAD_REQUEST
            }
        except EmptySheetError as error:
            return {
                'statusCode': HTTPStatus.BAD_REQUEST,
                'body': json.dumps({'errorCode': 'NO_PRODUCT_FOUND'})
            }
        except HeaderRowNotFoundError as error:
            return {
                'statusCode': HTTPStatus.BAD_REQUEST,
                'body': json.dumps({'errorCode': 'HEADER_NOT_FOUND'})
            }
        except DataAccessError as error:
            logging.error(error)
            return {
                'statusCode': HTTPStatus.SERVICE_UNAVAILABLE
            }
        except Exception as error:
            logging.error(error)
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
            logging.error(error)
            return {
                'statusCode': HTTPStatus.BAD_REQUEST
            }
        except Exception as error:
            logging.error(error)
            return {
                'statusCode': HTTPStatus.INTERNAL_SERVER_ERROR
            }