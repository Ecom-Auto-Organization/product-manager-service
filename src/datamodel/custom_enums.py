from enum import Enum


class HTTPMethod(Enum):
    """Enum with http methods"""
    
    POST = 'POST'
    GET = 'GET'
    PUT = 'PUT'
    DELETE = 'DELETE'


class FileType(Enum):
    """Enum with file sheet types"""

    EXCEL = 'EXCEL'
    CSV = 'CSV'


class HeaderOption(Enum):
    """Enum with options to fine column headers"""

    DEFAULT = 'DEFAULT'
    FIND = 'FIND'
    EXACT = 'EXACT'


class TaskType(Enum):
    """Enum with types of tasks"""

    IMPORT_CREATE = 'IMPORT_CREATE'
    IMPORT_EDIT = 'IMPORT_EDIT'
    BULK_EDIT = 'BULK_EDIT'


class ExecutionType(Enum):
    """Enum with the types of job executions"""
    
    NOW = 'NOW'
    SCHEDULED = 'SCHEDULED'
    RECURRING = 'RECURRING'


class ProductStatus(Enum):
    """Enum with product statuses"""

    ACTIVE = 'ACTIVE'
    DRAFT = 'DRAFT'
    ARCHIVED = 'ARCHIVED'


class JobStatus(Enum):
    SUBMITTED = 'SUBMITTED'
    PREPARING = 'PREPARING'
    RUNNING = 'RUNNING'
    COMPLETED = 'COMPLETED'
    PARTIAL_COMPLETE = 'PARTIALLY COMPLETED'
    FAILED = 'FAILED'