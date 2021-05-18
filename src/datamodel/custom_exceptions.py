class IllegalArgumentError(Exception):
    """Error thrown when request input is incorrect"""
    pass

class DataAccessError(Exception):
    """Error thrown when data access causes an error"""
    pass

class ShopifyUnauthorizedError(Exception):
    """Error thrown when request to shopify is unauthorized"""
    pass

class UserAuthenticationError(Exception):
    """Error thrown when user to authenticate cannot be found"""    


class MissingArgumentError(Exception):
    """Error thrown when a method is missing a required argument"""    

class HeaderRowNotFoundError(Exception):
    """Error thrown when a processor fails to locate header column"""  

class EmptySheetError(Exception):
    """Error thrown when a file does not contain actual non empty rows"""  
