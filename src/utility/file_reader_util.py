import pandas as pd
import io
from datamodel.custom_exceptions import MissingArgumentError
from datamodel.custom_exceptions import HeaderRowNotFoundError
from datamodel.custom_exceptions import EmptySheetError
from datamodel.custom_enums import FileType
from datamodel.custom_enums import HeaderOption


class FileReader:
    """ 
    Class to read and get details from csv and excel files

    Methods
    -------
    """

    def __init__(self, file_reader_info=None, file_details=None):
        if file_reader_info is not None:
            self._file_input = file_reader_info.get('file')
            self._file_type = file_reader_info.get('type')
            self._header_option = file_reader_info.get('header').get('option')
            self._header_option_value = file_reader_info.get('header').get('value')
        elif file_details is not None:
            pass
        else:
            raise MissingArgumentError('Missing either argument for FileReader class')

    
    def get_file_details(self):
        """
        Gets the content of an excel or csv file including header columns
        """

        df = None
        header_column_row = (0, self._header_option_value) [self._header_option == HeaderOption.EXACT]
        max_num_of_sample_data = 5

        #getting dataframe and raise error if we don't have proper file type
        file_bytes = io.BytesIO(self._file_input)
        if self._file_type == FileType.EXCEL:
            df = pd.read_excel(file_bytes, header=header_column_row)
        elif self._file_type == FileType.CSV:
            df = pd.read_csv(file_bytes, header=header_column_row)
        else:
            raise MissingArgumentError('Couldn\'t process file. File Type must be either CSV or EXCEL')

        #drop all empty columns and empty row
        df = df.dropna(axis=1, how='all').dropna(axis=0, how='all')

        original_header_columns = df.columns.tolist()
        actual_columns = original_header_columns.copy()
        values_start_index = 0

        if self._header_option == HeaderOption.DEFAULT or self._header_option == HeaderOption.FIND:
            column_name = (None, self._header_option_value) [self._header_option == HeaderOption.FIND]
            actual_header = self.__get_actual_head_row(df, self._header_option, column_name)
            actual_columns = actual_header.get('columns')
            header_column_row = actual_header.get('header_row')
            values_start_index = actual_header.get('values_start_index')

        row_values = df.values.tolist()[values_start_index:]
        actual_row_count = len(row_values)
        last_sample_Data_index = min((values_start_index + max_num_of_sample_data), len(df.values.tolist()))
   
        #if no product rows then raise error
        if actual_row_count < 1: 
            raise EmptySheetError('The file does not contain any product data')

        #generating file details
        file_details = {
            'fileType': self._file_type.name,
            'headerRow': header_column_row,
            'actualRowCount': actual_row_count,
            'columnDetails': []
        }
        actual_columns = self.__modifyNullColumnNames(actual_columns)
        for index in range(len(original_header_columns)):
            column = {}
            sample_data = df[original_header_columns[index]].values.tolist()[values_start_index: last_sample_Data_index]
            filtered_sample = []
            for item in sample_data:
                if pd.notnull(item) and str(item).strip() != '':
                    filtered_sample.append(item)
            column['name'] = actual_columns[index]
            column['index'] = index
            column['sampleData'] = filtered_sample
            column['field'] = None
            file_details['columnDetails'].append(column)
        
        return file_details
        

    def __get_actual_head_row (self, dataframe, header_option, column_name=None):
        if header_option == HeaderOption.DEFAULT:
            if not self.__isEmpty(dataframe.columns.tolist()):
                return {
                    'header_row': 0,
                    'values_start_index': 0,
                    'columns': dataframe.columns.tolist()
                }
            else:
                return {
                    'header_row': dataframe.index.values.tolist()[0] + 1,
                    'values_start_index': 1,
                    'columns': dataframe.values.tolist()[0]
                }
        elif header_option == HeaderOption.FIND:
            stop_index = min(16, len(dataframe.values.tolist()))
            header_row = dataframe.index.values.tolist()[0] + 1
            values_start_index = 0
            row_values = dataframe.values.tolist()
            header_column = None

            if self.__found_with_title(dataframe.columns.tolist(), column_name):
                return {
                    'header_row': 0,
                    'values_start_index': 0,
                    'columns': dataframe.columns.tolist()
                }
            else:
                for index in range(stop_index):
                    if self.__found_with_title(row_values[index], column_name):
                        header_column = row_values[index]
                        values_start_index = index + 1
                        break
                    else:
                        header_row += 1

                if header_column is not None:
                    return {
                        'header_row': header_row,
                        'values_start_index': values_start_index,
                        'columns': header_column
                    }
                else:
                    raise HeaderRowNotFoundError("File Reader fails to locate header row")



    def __isEmpty(self, columns):
        for cell in columns:
            if 'Unnamed:' not in str(cell) and str(cell).strip() is not None and pd.notnull(cell) and str(cell).strip() != '':
                return False

        return True


    def __found_with_title(self, columns, column_name):
        for cell in columns:
            if pd.notnull(cell) and str(cell).strip().lower() == str(column_name).lower():
                return True

        return False


    def __modifyNullColumnNames(self, column_names):
        for index in range(len(column_names)):
            if 'Unnamed:' in str(column_names[index]) or str(column_names[index]).strip() is None or pd.isnull(column_names[index]) or str(column_names[index]).strip() == '':
                column_names[index] = 'Column ' + str(index + 1)
        return column_names
