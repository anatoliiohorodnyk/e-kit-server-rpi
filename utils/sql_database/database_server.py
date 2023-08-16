import datetime

import sqlalchemy as db
from sqlalchemy import ForeignKey, func, text

PATH_TO_DB = "/home/max/e-kit-server/device_list.sqlite"
SQLITE_PATH_TO_DB = "sqlite:////home/max/e-kit-server/device_list.sqlite"


class ServerDataBase:
    def __init__(self):
        self.engine = db.create_engine(SQLITE_PATH_TO_DB)
        self.data_base_connection = self.engine.connect()
        self.drop_table(['DevicesInfo', 'PowerConsumption', 'DeviceState', 'to_be_power_on'])
        self.metadata = db.MetaData()
        self.devices_info_table = db.Table('DevicesInfo', self.metadata,
                                           db.Column('Id', db.Integer(), primary_key=True),
                                           db.Column('ServiceName', db.String),
                                           db.Column('DeviceType', db.String),
                                           db.Column('Power', db.Integer),
                                           db.Column('Time',  db.DateTime(timezone=True), default=datetime.datetime.utcnow, onupdate=db.sql.func.now(), server_default=func.now()),
                                           db.Column('IsConsumer', db.Boolean),
                                           db.UniqueConstraint('ServiceName'),
                                           )
        self.power_consumption_table = db.Table('PowerConsumption', self.metadata,
                                                db.Column('Id', db.Integer(), primary_key=True),
                                                db.Column('DeviceId', db.Integer,
                                                          ForeignKey(self.devices_info_table.c.Id)),
                                                db.Column('DeviceConsumption', db.Integer),
                                                db.Column('OverallConsumption', db.Integer),
                                                db.Column('Time', db.DateTime(timezone=True), default=datetime.datetime.utcnow, onupdate=db.sql.func.now(), server_default=func.now())
                                                )
        self.device_state_table = db.Table('DeviceState', self.metadata,
                                           db.Column('Id', db.Integer(), primary_key=True),
                                           db.Column('DeviceId', db.Integer,
                                                     ForeignKey(self.devices_info_table.c.Id)),
                                           db.Column('Priority', db.Integer),
                                           db.Column('State', db.Boolean),
                                           db.Column('IsActive', db.Boolean),
                                           db.Column('Blocked', db.Boolean),
                                           db.Column('Time', db.DateTime(timezone=True), default=datetime.datetime.utcnow, onupdate=db.sql.func.now(), server_default=func.now())
                                           )
        self.device_list_to_power_on = db.Table('to_be_power_on', self.metadata,
                                                db.Column('id', db.Integer(), primary_key=True),
                                                db.Column('id_device_who_turned_off', db.Integer()),
                                                db.Column('id_device_to_turned_on', db.Integer()),
                                                db.UniqueConstraint('id_device_to_turned_on'),
                                                )
        self.devices_name_table = db.Table('updated_services_name', self.metadata,
                                           db.Column('id', db.Integer(), primary_key=True),
                                           db.Column('service_name', db.String),
                                           db.Column('new_name', db.String)
                                           )

        self.metadata.create_all(self.engine, checkfirst=True)
        self.data_base_connection.close()

    def execute_query_with_output(self, value):
        data_base_connection = self.engine.connect()
        try:
            print(value)
            result = data_base_connection.execute(text(value)).fetchall()
	    data_base_connection.commit()
            data_base_connection.close()
            print(f"result from execute_query_with_output : {result} of type {type(result)}")
            if result:
                return result
            else:
                return False
        except Exception as err:
            print("Can't execute query")
            print(err)
            data_base_connection.close()
            return False

    def execute_query_without_output(self, value):
        data_base_connection = self.engine.connect()
        try:
            print(data_base_connection)
            #print(value)
            data_base_connection.execute(text(value))
            data_base_connection.commit()
            data_base_connection.close()
            return True
        except Exception as err:
            print("Can't execute query")
            print(err)
            data_base_connection.close()
            return False

    def get_all_from_db(self, table, condition):
        select_from = f'SELECT * FROM {table} WHERE {condition}'
        try:
            return self.execute_query_with_output(select_from)
        except Exception:
            print(f"Didn't found selected data: {select_from}. Returning None")
            return None

    def insert_in_db(self, table, colums, params: list):
        insert_into = f'INSERT INTO {table} ({colums}) VALUES {params}'
        print(insert_into)
        if not self.execute_query_without_output(insert_into):
            print(f'Cant insert info in db: {insert_into}')
            return False
        return True
    
    def update_in_db(self, table, colum, param: list, condition):
        print("******************UPDATE****************")
        insert_into = f'UPDATE {table} SET {colum} = {param} WHERE Id = {condition}'
        print(insert_into)
        if not self.execute_query_without_output(insert_into):
            print(f'Cant insert info in db: {insert_into}')
            return False
        return True

    def delete_from_db(self, table, condition):
        delete_from = f'DELETE FROM {table} WHERE {condition}'
        print("**************************DELETION HERE***********************")
        print(delete_from)
        if not self.execute_query_without_output(delete_from):
            print(f'Cant delete info from db: {delete_from}')
            return False
        return True

    def drop_table(self, table_list):
        for table in table_list:
            self.execute_query_without_output(f'DROP TABLE {table}')

    #
    # def get_power_consumption_from_db(self, condition):
    #     data_base_connection = self.engine.connect()
    #     try:
    #         result = data_base_connection.execute(f'SELECT * FROM PowerConsumption WHERE {condition}').fetchall()
    #         data_base_connection.close()
    #         return result
    #     except Exception:
    #         print("Didn't found selected data. Returning None")
    #         data_base_connection.close()
    #         return None
    #
    # def insert_power_consumption_in_db(self, params: list):
    #     data_base_connection = self.engine.connect()
    #     try:
    #         self.data_base_connection.execute(
    #             f'INSERT INTO PowerConsumption (Id, DeviceId, DeviceConsumption, OverallConsumption, Time) VALUES {params}',
    #         )
    #         data_base_connection.close()
    #         return True
    #     except Exception:
    #         print('Cant insert info in db')
    #         data_base_connection.close()
    #         return False
    #
    # def delete_power_consumption_from_db(self, condition):
    #     data_base_connection = self.engine.connect()
    #     try:
    #         self.data_base_connection.execute(
    #             f'DELETE FROM PowerConsumption WHERE {condition}',
    #         )
    #         data_base_connection.close()
    #         return True
    #     except Exception:
    #         print('Cant delete info from db')
    #         data_base_connection.close()
    #         return False
    #
    # def get_device_state_from_db(self, condition):
    #     data_base_connection = self.engine.connect()
    #     try:
    #         result = data_base_connection.execute(f'SELECT * FROM DeviceState WHERE {condition}').fetchall()
    #         data_base_connection.close()
    #         return result
    #     except Exception:
    #         print("Didn't found selected data. Returning None")
    #         data_base_connection.close()
    #         return None
    #
    # def insert_device_state_in_db(self, params: list):
    #     data_base_connection = self.engine.connect()
    #     try:
    #         self.data_base_connection.execute(
    #             f'INSERT INTO DeviceState (Id, DeviceId, Priority, State, IsActive, Blocked, Time) VALUES {params}',
    #         )
    #         data_base_connection.close()
    #         return True
    #     except Exception:
    #         print('Cant insert info in db')
    #         data_base_connection.close()
    #         return False
    #
    # def delete_device_state_from_db(self, condition):
    #     data_base_connection = self.engine.connect()
    #     try:
    #         self.data_base_connection.execute(
    #             f'DELETE FROM DeviceState WHERE {condition}',
    #         )
    #         data_base_connection.close()
    #         return True
    #     except Exception:
    #         print('Cant delete info from db')
    #         data_base_connection.close()
    #         return False

