from enum import Enum


class HTTPMethod (Enum):
    """Enum with http methods"""
    
    POST = 'POST'
    GET = 'GET'
    PUT = 'PUT'
    DELETE = 'DELETE'


class FileType (Enum):
    """Enum with file sheet types"""

    EXCEL = 'EXCEL'
    CSV = 'CSV'


class HeaderOption (Enum):
    """Enum with options to fine column headers"""

    DEFAULT = 'DEFAULT'
    FIND = 'FIND'
    EXACT = 'EXACT'