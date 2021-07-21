from controller.product_manager_controller import ProductManagerController


def lambda_handler(event, context):
    """
    Sample pure Lambda function

    Parameters
    ----------
    event: dict, required
        API Gateway Lambda Proxy Input Format

    context: object, required
        Lambda Context runtime methods and attributes

    Returns:
        API Gateway Lambda Proxy Output Format: dict
    """

    request_path = event.get('path')
    request_method = event.get('httpMethod')
    request = {
        'body': event.get('body'), 
        'path_params': event.get('pathParameters'), 
        'query_params': event.get('queryStringParameters'),
        'header': event.get('headers')
    }
    if 'authorizer' in event['requestContext']:
        request['user_context'] = event.get('requestContext').get('authorizer')

    return route(request_path, request_method, request)


def route(request_path, request_method, request):
    """ 
    Routes a request to controller classes based on request resource
    and return an dictionary of http response

    Parameters
    ----------
    request_path: string, required
        Resource path for the request
    
    request_method: string, required
        method for the request

    request: dict, required
        the request information including body, query parameters, and path parameters

    Returns
    -------
    http response
    """

    return ProductManagerController(request, request_method, request_path).invoke()
