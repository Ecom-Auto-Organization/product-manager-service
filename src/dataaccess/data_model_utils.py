
from utility import utils

def convert_to_db_file (file_obj):
    """Converts file object into object that can be used in database"""
    
    db_file = {'SK': 'file'}

    db_file['PK'] = utils.join_str('file#', file_obj['id'])
    if 'idle' in file_obj:
        db_file['SK1'] = utils.join_str('idle#', file_obj['idle'])
    if 'file_name' in file_obj:
        db_file['file_name'] = file_obj['file_name']
    if 'file_type' in file_obj:
        db_file['file_type'] = file_obj['file_type']
    if 's3_key' in file_obj:
        db_file['s3_key'] = file_obj['s3_key']
    if 'actual_row_count' in file_obj:
        db_file['actual_row_count'] = file_obj['actual_row_count']
    if 'header_row' in file_obj:
        db_file['header_row'] = file_obj['header_row']

    return db_file


def extract_file_details(db_file):
    delimeter = '#'
    file = {}
    file['id'] = utils.extract_str(db_file['PK'], delimeter, 1)
    if 'SK1' in db_file:
        file['idle'] = utils.extract_str(db_file['SK1'], delimeter, 1)
    if 'file_name' in db_file:
        file['file_name'] = db_file['file_name']
    if 'file_type' in db_file:
        file['file_type'] = db_file['file_type']
    if 's3_key' in db_file:
        file['s3_key'] = db_file['s3_key']
    if 'actual_row_count' in db_file:
        file['actual_row_count'] = db_file['actual_row_count']
    if 'header_row' in db_file:
        file['header_row'] = db_file['header_row']
    
    return file