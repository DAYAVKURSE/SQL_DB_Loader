import sqlite3
from functools import wraps
from datetime import datetime
import gspread
import inspect


class DBError(Exception):
    def __init__(self, message, error_code, input):
        self.message = f'Ошибка: {message}'
        self.error_code = error_code
        self.input = input
        self.tehnical_data = f'Код ошибки: {self.error_code}, Сообщение: {self.message}, Входные данные: {self.input}'
        # print(self.tehnical_data)

    def error_code(self):
        return self.error_code

    def input(self):
        return self.input

    def message(self):
        return self.message

    def __str__(self):
        return self.tehnical_data


def ensure_connection(func):
    @wraps(func)
    def wrapper(self, *args, **kwargs):
        with self.connect() as conn:
            return func(self, conn, *args, **kwargs)
    return wrapper


class Database:
    def __init__(self, db_name):  # ,table_name):
        self.db_name = db_name
        #self.table_name = table_name

    def connect(self, error_code=0):
        function_name = inspect.currentframe().f_code.co_name
        try:
            return sqlite3.connect(self.db_name)
        except sqlite3.Error as exc:
            raise DBError(
                f'Не удалось подключиться к базе данных: {exc}', error_code, input=locals())

    @ensure_connection
    def read(self, conn, table_name, out, params=None, error_code=102):
        function_name = inspect.currentframe().f_code.co_name
        try:
            cursor = conn.cursor()
            query = f'SELECT {out} FROM {table_name}'
            if params:
                params = params.split('=')
                query += f' WHERE {params[0]} = ?'
                cursor.execute(query, (params[1],))
            else:
                cursor.execute(query)
            result_list = cursor.fetchall()
            if result_list:
                for i, result in enumerate(result_list):
                    result_list[i] = result[0]
            return result_list
        except sqlite3.Error as exc:
            raise DBError(
                f'Не удалось получить данные из базы данных: {exc}', error_code, input=locals())
        except IndexError as exc:
            raise DBError(
                f'Искомые данные небыли найдены: {exc}', error_code, input=locals())

    @ensure_connection
    def write(self, conn, query, params=None, error_code=0):
        function_name = inspect.currentframe().f_code.co_name
        try:
            cursor = conn.cursor()
            cursor.execute(query, params) if params else cursor.execute(query)
            conn.commit()
        except sqlite3.Error as exc:
            raise DBError(
                f'Не удалось записать данные в базу данных: {exc}', error_code, input=locals())


class DBUpdater:
    def __init__(self, db):
        self.db = db
        gspread_account = gspread.service_account(filename='data/credits.json')
        self.gspread_client = gspread_account.open_by_key(
            'your_spreadsheet_key')

    def copy_table(self, sheet_name, table_name, column_names):
        sheet = self.gspread_client.worksheet(sheet_name)
        data = sheet.get_all_records()

        index = 0
        while True:
            column = []
            for value in data:
                if index <= len(value.values()) - 1:
                    column.append(list(value.values())[index])
                else:
                    break
            else:
                for i, string in enumerate(column):
                    if self.db.read(table_name, '*', f'id = {i+1}'):
                        self.db.write(
                            f'UPDATE {table_name} SET {column_names[index]} = ? WHERE id = ?', (string, i+1))
                    else:
                        self.db.write(
                            f'INSERT INTO {table_name} ({column_names[index]}) VALUES (?)', (string,))
                index += 1
                continue
            break

    def run_update(self, sheet_list):
        for sheet in sheet_list.keys():
            sheet_name = sheet
            table_name = sheet_list[sheet]['table_name']
            column_names = sheet_list[sheet]['column_names']
            self.copy_table(sheet_name, table_name, column_names)
            print(
                f'Данные из листа {sheet_name} записаны в таблицу {table_name}')
