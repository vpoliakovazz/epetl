import json
import tempfile
import random
import time
import socket

import pyodbc
import psycopg2
import psycopg2.extras
import pandas as pd

from googleapiclient.discovery import build
from oauth2client.service_account import ServiceAccountCredentials
from sqlalchemy import create_engine
import pymysql
import pymongo

socket.setdefaulttimeout(60 * 60)


class Extract:

    def __init__(self):
        # loading our json file here to use it across different class methods
        self.data_sources = json.load(open("C:/projects/etl/epic-etl/etl_epic/data_config.json"))

        self.mssql_conn = self.data_sources["data_sources"]["epic_mssql"]["conn_str"]
        self.mssql_alchemy_conn = self.data_sources["data_sources"]["epic_mssql"]["sql_alch_str"]
        self.mongo_conn = self.data_sources["data_sources"]["mongo_conn"]
        self.mongo_conn_bill = self.data_sources["data_sources"]["mongo_conn_bill"]

        self.pgsql_conn = self.data_sources["data_sources"]["analytics_pgsql"]["conn_str"]
        self.pgsql_alchemy_conn = self.data_sources["data_sources"]["analytics_pgsql"]["sql_alch_str"]

        self.mysql_alchemy_conn = self.data_sources["data_sources"]["ho_mysql_prod"]
        self.fotos_mysql = self.data_sources["data_sources"]["fotos_mysql"]

        self.sc_oms_alchemy_conn = self.data_sources["data_sources"]["scalium_pgsql_oms"]["sql_alch_str"]
        self.sc_mas_alchemy_conn = self.data_sources["data_sources"]["scalium_pgsql_mas"]["sql_alch_str"]
        self.sc_pim_alchemy_conn = self.data_sources["data_sources"]["scalium_pgsql_pim"]["sql_alch_str"]
        self.sc_offers_alchemy_conn = self.data_sources["data_sources"]["scalium_pgsql_offers"]["sql_alch_str"]
        self.sc_users_alchemy_conn = self.data_sources["data_sources"]["scalium_pgsql_users"]["sql_users_str"]
        self.sc_balance_alchemy_conn = self.data_sources["data_sources"]["scalium_pgsql_balance"]["sql_alch_str"]

        self.mssql_tables = self.data_sources["data_input"]["epic_mssql"]["tables"]
        self.ga_tables = self.data_sources["data_input"]["epic_ga"]
        self.trans_tables = self.data_sources["data_input"]["trans_em_analytics"]
        self.trans_tables_ms_sql = self.data_sources["data_input"]["trans_em_analytics_ms_sql"]
        self.trans_tables_ho = self.data_sources["data_input"]["trans_em_analytics_ho"]
        self.scalium_tables = self.data_sources["data_input"]["scalium"]
        self.scalium_tables_refresh = self.data_sources["data_input"]["scalium_refresh"]
        self.api_source = self.data_sources["data_input"]["api_source"]
        self.trans_tables_independent = self.data_sources["data_input"]["trans_em_analytics_independent"]

    def safe_wrapper(self, connection):
        if connection == self.mssql_conn:
            for n in range(0, 5):
                try:
                    return pyodbc.connect(connection)
                    # return pd.read_sql("select * from {} with (nolock)".format(table), cnxn)
                except pyodbc.Error as e:
                    print(e)
                    print("im in mssql pyodbc connect wrapper")
                    time.sleep((2 ** n) + random.random())
        if connection == self.mysql_alchemy_conn or connection == self.fotos_mysql:
            for n in range(0, 5):
                try:
                    print("connect")
                    return create_engine(connection)
                except Exception as e:
                    print(e)
                    print("im in mysql_alchemy_conn wrapper")
                    time.sleep((2 ** n) + random.random())
        else:
            for n in range(0, 5):
                try:
                    return psycopg2.connect(connection)
                    # return pd.read_sql("select * from {} with (nolock)".format(table), cnxn)
                except psycopg2.Error as e:
                    print(e)
                    print("im in psycopg2 connect wrapper")
                    time.sleep((2 ** n) + random.random())

    def get_table(self, table, connection_str):
        cnxn = self.safe_wrapper(connection_str)
        if connection_str == self.mssql_conn:
            t = pd.read_sql("select * from {} with (nolock)".format(table), cnxn)
        else:
            t = pd.read_sql("select * from {}".format(table), cnxn)
        cnxn.close()
        print("SQL connection is closed for get_table")
        return t

    @staticmethod
    def sql_to_df(query, alchemy_conn_str):
        n = 0
        while n <= 6:
            try:
                db_engine = create_engine(alchemy_conn_str)
                result = db_engine.execute(query)
                df = pd.DataFrame(result)
                df.columns = result.keys()
                return df
            except Exception as e:
                print("sql_to_df exeption" + str(e))
                if n == 6:
                    raise e
            finally:
                result.close()

    @staticmethod
    def read_sql_tmp_file(query, alchemy_conn):
        i = 0
        while i < 10:
            try:
                db_engine = create_engine(alchemy_conn)
                with tempfile.TemporaryFile() as tmp_file:
                    copy_sql = "COPY ({query}) TO STDOUT WITH CSV {head}".format(
                        query=query, head="HEADER")
                    conn = db_engine.raw_connection()
                    cur = conn.cursor()
                    cur.copy_expert(copy_sql, tmp_file)
                    tmp_file.seek(0)
                    df = pd.read_csv(tmp_file)
                    return df
            except Exception as e:
                print(e)
                time.sleep(300)
                i = i + 1

    def execute_sql_code(self, sql_code, connection_str):
        cnxn = self.safe_wrapper(connection_str)
        for n in range(0, 5):
            try:
                cursor = cnxn.cursor()
                cursor.execute(sql_code)
                cnxn.commit()
                break
            except pyodbc.Error as e:
                print(e)
                time.sleep((2 ** n) + random.random())
            finally:
                try:
                    cnxn.close()
                    cursor.close()
                    print("MSSQL connection is closed for execute_sql_code")
                # UnboundLocalError - если отсутствует cnxn
                except UnboundLocalError:
                    pass

    def sql_query(self, sql_code, connection_str):
        for n in range(0, 5):
            try:
                cnxn = self.safe_wrapper(connection_str)
                print("trying")
                print(cnxn)
                return pd.read_sql(sql_code, cnxn)
                # cursor = cnxn.cursor()
                # return cursor.execute(sql_code)
            except Exception as e:
                print(e)
                print("im in sql_query exception")
                time.sleep((2 ** n) + random.random())
            finally:
                try:
                    cnxn.close()
                    # cursor.close()
                    print("MSSQL connection is closed for sql_query")
                except Exception as e:
                    print(e)
                    print("im in cnxn.close() exception")

    def mongo_agg(self, connection, db, collection, agg_pipeline):
        client = pymongo.MongoClient(connection)
        dbase = client[db]  # база
        collection = dbase[collection]  # коллекция
        return list(collection.aggregate(agg_pipeline, allowDiskUse=True))

    def mongo_find(self, connection, db, collection, match, projection):
        client = pymongo.MongoClient(connection)
        dbase = client[db]  # база
        collection = dbase[collection]  # коллекция
        data = []
        if projection:
            for doc in collection.find(match, projection):
                data.append(doc)
        else:
            for doc in collection.find(match):
                data.append(doc)
        return data


class ExtractGADataTest:

    def __init__(self):
        # loading our json file here to use it across different class methods
        self.data_sources = json.load(open("C:/etl_epic/data_config.json"))
        self.creds = self.data_sources["data_sources"]["epic_ga"]["creds"]
        self.analytics = build("analyticsreporting", "v4",
                               credentials=ServiceAccountCredentials.from_json_keyfile_name(
                                   self.creds["key_location"], self.creds["scopes"]))
        self.ga_tables = self.data_sources["data_input"]["epic_ga"]

    @staticmethod
    def build_query(start_date, end_date, dimensions_list, metrics_list, page_token="0"):
        """
        построение запроса в формате определенном гуглом
        """
        dimensions = [{"name": d} for d in dimensions_list]
        metrics = [{"expression": m} for m in metrics_list]
        body = {
            "reportRequests": [
                {
                    "viewId": "111479987",
                    "dateRanges": [{"startDate": start_date, "endDate": end_date}],
                    "metrics": metrics,
                    "dimensions": dimensions,
                    "pageSize": 100000,
                    "pageToken": page_token
                }]
        }
        return body

    @staticmethod
    def safe_wrapper(analytics, query):
        """
        https://developers.google.com/analytics/devguides/reporting/core/v4/errors?hl=ru#backoff
        """
        """
        for n in range(0, 5):
            try:
                return analytics.reports().batchGet(body=query).execute()
            except Exception as e:  # перечислить ошибки
                print("im in safe wrapper exception")
                print(e)
                time.sleep(5)
                time.sleep((2 ** n) + random.random())
        """
        n = 0
        while n <= 6:
            try:
                return analytics.reports().batchGet(body=query).execute()
            except Exception as e:  # перечислить ошибки
                if n == 6:
                    raise Exception
                print("im in safe wrapper exception")
                print(e)
                time.sleep(60 * 10)
                time.sleep((2 ** n) + random.random())
            n = n + 1

    @staticmethod
    def fulfill(api_resp, dm, mm):
        try:
            for e in api_resp["reports"][0]["data"]["rows"]:
                dm.append(e["dimensions"])
                mm.append(e["metrics"][0]["values"])
        except KeyError as e:
            print("no data for selected period")
            raise e

    @staticmethod
    def ga_data_to_df(dimension_data, metrics_data, dimension_names, metrics_names):
        dimension_names = [name.replace(':', '_') for name in dimension_names]
        metrics_names = [name.replace(':', '_') for name in metrics_names]
        df = pd.DataFrame({"dims_one_column": dimension_data,
                           "metrics_one_column": metrics_data})
        df[dimension_names] = pd.DataFrame(df.dims_one_column.values.tolist(), index=df.index)
        df[metrics_names] = pd.DataFrame(df.metrics_one_column.values.tolist(), index=df.index)
        del df["dims_one_column"]
        del df["metrics_one_column"]
        return df

    def load_report(self, start_date, end_date, dimensions_list, metrics_list, operation_type, db_table, primary_key):
        data_operation_obj = DataOperation()
        query = self.build_query(start_date, end_date, dimensions_list, metrics_list)
        response_has_pagination = False
        response_is_sampled = False

        # пытаемся получить первый api_response
        api_response = self.safe_wrapper(self.analytics, query)

        # есть ли семплирование в первоначальном ответе
        try:
            if api_response["reports"][0]["data"]["samplesReadCounts"]:
                response_is_sampled = True
        except KeyError:
            print("input data not sampled")

        # есть ли пагинация в первоначальном ответе
        try:
            if api_response["reports"][0]["nextPageToken"]:
                response_has_pagination = True
        except KeyError:
            print("input data has no pagination")

        # case 1: данные семплированные
        if response_is_sampled:
            print("data is sampled")
            dates = [date.strftime("%Y-%m-%d") for date in pd.date_range(start_date, end_date, freq="1d")]
            for date in dates:
                print("working on date: {}".format(date))
                dims_t = []
                metrics_t = []
                # построение запроса на каждую дату
                by_day_query = self.build_query(date, date, dimensions_list, metrics_list)
                api_response = self.safe_wrapper(self.analytics, by_day_query)
                # проверка содержит ли ответ пагинацию
                try:
                    if api_response["reports"][0]["nextPageToken"]:
                        print("working on pagination")
                        # ТУТ НАДО ОБРАБАТЫВАТЬ ПЕРВУЮ СТРАНИЦУ ОТВЕТА
                        self.fulfill(api_response, dims_t, metrics_t)
                        next_page_token = api_response["reports"][0]["nextPageToken"]
                        while True:
                            # пока в ответе есть nextPageToken делаем запросы и обрабатываем
                            try:
                                time.sleep(5)
                                pagination_query = self.build_query(date, date, dimensions_list,
                                                                    metrics_list, page_token=next_page_token)
                                api_response = self.safe_wrapper(self.analytics, pagination_query)
                                # ТУТ НАДО ОБРАБАТЫВАТЬ ПОСЛЕДУЮЩИЕ СТРАНИЦЫ ОТВЕТА
                                self.fulfill(api_response, dims_t, metrics_t)
                                next_page_token = api_response["reports"][0]["nextPageToken"]
                                print(str(next_page_token))
                            except:
                                print("pagination over")
                                break
                except KeyError:
                    # print("im in KeyError next_page_token exception")
                    # ТУТ НАДО ОБРАБАТЫВАТЬ СТРАНИЦУ ОТВЕТА
                    self.fulfill(api_response, dims_t, metrics_t)
                # ФИНАЛЬНАЯ ОБРАБОТКА ОТВЕТА
                data = self.ga_data_to_df(dims_t, metrics_t, dimensions_list, metrics_list)
                if operation_type == "to_csv":
                    data_operation_obj.df_to_csv(data, name=str(date))
                if operation_type == "to_db":
                    data_operation_obj.df_to_db(data, db_table, primary_key)

        # case 2: данные не семплированные и есть пагинация
        elif not response_is_sampled and response_has_pagination:
            print("working on pagination")
            dims_t = []
            metrics_t = []
            # ОБРАБОТКА ПЕРВОЙ СТРАНИЦЫ ОТВЕТА
            self.fulfill(api_response, dims_t, metrics_t)
            next_page_token = api_response["reports"][0]["nextPageToken"]
            while True:
                # пока в ответе есть nextPageToken делаем запросы и обрабатываем
                try:
                    pagination_query = self.build_query(start_date, end_date, dimensions_list,
                                                        metrics_list, page_token=next_page_token)
                    api_response = self.safe_wrapper(self.analytics, pagination_query)
                    # ТУТ НАДО ОБРАБАТЫВАТЬ ПОСЛЕДУЮЩИЕ СТРАНИЦЫ ОТВЕТА
                    self.fulfill(api_response, dims_t, metrics_t)
                    next_page_token = api_response["reports"][0]["nextPageToken"]
                    print(next_page_token)
                except:
                    print("pagination over")
                    break
            # ФИНАЛЬНАЯ ОБРАБОТКА ОТВЕТА
            data = self.ga_data_to_df(dims_t, metrics_t, dimensions_list, metrics_list)
            if operation_type == "to_csv":
                data_operation_obj.df_to_csv(data, name="have_pagination_no_sampled")
            if operation_type == "to_db":
                data_operation_obj.df_to_db(data, db_table, primary_key)

        # case 3: нет семплирования и нет пагинации
        else:
            dims_t = []
            metrics_t = []
            self.fulfill(api_response, dims_t, metrics_t)
            # ФИНАЛЬНАЯ ОБРАБОТКА ОТВЕТА
            data = self.ga_data_to_df(dims_t, metrics_t, dimensions_list, metrics_list)
            if operation_type == "to_csv":
                data_operation_obj.df_to_csv(data, name="no_pagination_no_sampled")
            if operation_type == "to_db":
                data_operation_obj.df_to_db(data, db_table, primary_key)


class DataOperation(Extract):

    def __init__(self):
        # self.data_sources = json.load(open("data_config.json"))
        # self.connection = "connection_from_data_config.json"
        # self.extract_obj = Extract()
        super().__init__()

    @staticmethod
    def df_to_csv(df, name, folder="C:/projects/etl/epic-etl/etl_epic/by session ga csvs/"):
        path = folder + name + ".csv"
        df.to_csv(path, index=False, encoding="utf-8")

    def df_to_db(self, df, db_table, primary_key=""):
        records_to_insert = [tuple(x) for x in df.values]
        names_str = tuple(df.columns)
        values_insert_str = ("%s, " * df.shape[1])[:-2]
        if primary_key:
            insert_query = "INSERT INTO {}{} values ({}) ON CONFLICT ({}) DO NOTHING;".format(db_table,
                                                                                              names_str,
                                                                                              values_insert_str,
                                                                                              primary_key)
        else:
            insert_query = "INSERT INTO {}{} values ({})".format(db_table, names_str, values_insert_str)
        insert_query = insert_query.replace('\'', '')
        n = 0
        while n <= 6:
            try:
                cnxn = self.safe_wrapper(self.pgsql_conn)
                cursor = cnxn.cursor()
                psycopg2.extras.execute_batch(cursor, insert_query, records_to_insert, page_size=1000)
                cnxn.commit()
                cnxn.close()
                print("SQL connection is closed for df_to_db")
                break
            except Exception as e:  # перечислить ошибки
                if n == 6:
                    raise Exception
                print("im in db_to_db function exception")
                print(e)
                time.sleep((2 ** n) + random.random())
            n = n + 1


class ExtractGAData:

    def __init__(self):
        # loading our json file here to use it across different class methods
        self.data_sources = json.load(open("C:/projects/etl/epic-etl/etl_epic/data_config.json"))
        self.creds = self.data_sources["data_sources"]["epic_ga"]["creds"]
        self.analytics = build("analyticsreporting", "v4",
                               credentials=ServiceAccountCredentials.from_json_keyfile_name(
                                   self.creds["key_location"], self.creds["scopes"]))
        self.ga_tables = self.data_sources["data_input"]["epic_ga"]

    @staticmethod
    def build_query(start_date, end_date, dimensions_list, metrics_list, filters, page_token="0"):
        """
        построение запроса в формате определенном гуглом
        """
        dimensions = [{"name": d} for d in dimensions_list]
        metrics = [{"expression": m} for m in metrics_list]
        body = {
            "reportRequests": [
                {
                    "viewId": "111479987",
                    "dateRanges": [{"startDate": start_date, "endDate": end_date}],
                    "metrics": metrics,
                    "dimensions": dimensions,
                    "pageSize": 100000,
                    "pageToken": page_token,
                    "dimensionFilterClauses": [
                        {"operator": "AND",
                            "filters": filters
                        }
                    ]
                }]
        }
        return body

    @staticmethod
    def safe_wrapper(analytics, query):
        """
        https://developers.google.com/analytics/devguides/reporting/core/v4/errors?hl=ru#backoff
        """
        """
        for n in range(0, 5):
            try:
                return analytics.reports().batchGet(body=query).execute()
            except Exception as e:  # перечислить ошибки
                print("im in safe wrapper exception")
                print(e)
                time.sleep(5)
                time.sleep((2 ** n) + random.random())
        """
        n = 0
        while n <= 6:
            try:
                return analytics.reports().batchGet(body=query).execute()
            except Exception as e:  # перечислить ошибки
                if n == 6:
                    raise Exception
                print("im in safe wrapper exception")
                print(e)
                time.sleep(60 * 10)
                time.sleep((2 ** n) + random.random())
            n = n + 1

    @staticmethod
    def fulfill(api_resp, dm, mm):
        try:
            for e in api_resp["reports"][0]["data"]["rows"]:
                dm.append(e["dimensions"])
                mm.append(e["metrics"][0]["values"])
        except KeyError as e:
            print("no data for selected period")
            raise e

    @staticmethod
    def ga_data_to_df(dimension_data, metrics_data, dimension_names, metrics_names):
        dimension_names = [name.replace(':', '_') for name in dimension_names]
        metrics_names = [name.replace(':', '_') for name in metrics_names]
        df = pd.DataFrame({"dims_one_column": dimension_data,
                           "metrics_one_column": metrics_data})
        df[dimension_names] = pd.DataFrame(df.dims_one_column.values.tolist(), index=df.index)
        df[metrics_names] = pd.DataFrame(df.metrics_one_column.values.tolist(), index=df.index)
        del df["dims_one_column"]
        del df["metrics_one_column"]
        return df

    def load_report(self, start_date, end_date, dimensions_list, metrics_list, filters, operation_type, db_table, primary_key):
        data_operation_obj = DataOperation()
        query = self.build_query(start_date, end_date, dimensions_list, metrics_list, filters)
        response_has_pagination = False
        response_is_sampled = False

        # пытаемся получить первый api_response
        api_response = self.safe_wrapper(self.analytics, query)

        # есть ли семплирование в первоначальном ответе
        try:
            if api_response["reports"][0]["data"]["samplesReadCounts"]:
                response_is_sampled = True
        except KeyError:
            print("input data not sampled")

        # есть ли пагинация в первоначальном ответе
        try:
            if api_response["reports"][0]["nextPageToken"]:
                response_has_pagination = True
        except KeyError:
            print("input data has no pagination")

        if db_table == 'ga_events_data':
            response_is_sampled = True
        print(response_is_sampled)

        # case 1: данные семплированные
        if response_is_sampled:
            print("data is sampled")
            dates = [date.strftime("%Y-%m-%d") for date in pd.date_range(start_date, end_date, freq="1d")]
            for date in dates:
                print("working on date: {}".format(date))
                dims_t = []
                metrics_t = []
                # построение запроса на каждую дату
                by_day_query = self.build_query(date, date, dimensions_list, metrics_list, filters)
                api_response = self.safe_wrapper(self.analytics, by_day_query)
                # проверка содержит ли ответ пагинацию
                try:
                    if api_response["reports"][0]["nextPageToken"]:
                        print("working on pagination")
                        # ТУТ НАДО ОБРАБАТЫВАТЬ ПЕРВУЮ СТРАНИЦУ ОТВЕТА
                        self.fulfill(api_response, dims_t, metrics_t)
                        next_page_token = api_response["reports"][0]["nextPageToken"]
                        while True:
                            # пока в ответе есть nextPageToken делаем запросы и обрабатываем
                            try:
                                time.sleep(5)
                                pagination_query = self.build_query(date, date, dimensions_list,
                                                                    metrics_list, filters, page_token=next_page_token)
                                api_response = self.safe_wrapper(self.analytics, pagination_query)
                                # ТУТ НАДО ОБРАБАТЫВАТЬ ПОСЛЕДУЮЩИЕ СТРАНИЦЫ ОТВЕТА
                                self.fulfill(api_response, dims_t, metrics_t)
                                next_page_token = api_response["reports"][0]["nextPageToken"]
                                print(str(next_page_token))
                            except:
                                print("pagination over")
                                break
                except KeyError:
                    # print("im in KeyError next_page_token exception")
                    # ТУТ НАДО ОБРАБАТЫВАТЬ СТРАНИЦУ ОТВЕТА
                    self.fulfill(api_response, dims_t, metrics_t)
                # ФИНАЛЬНАЯ ОБРАБОТКА ОТВЕТА
                data = self.ga_data_to_df(dims_t, metrics_t, dimensions_list, metrics_list)
                if operation_type == "to_csv":
                    data_operation_obj.df_to_csv(data, name=str(date))
                if operation_type == "to_db":
                    data_operation_obj.df_to_db(data, db_table, primary_key)

        # case 2: данные не семплированные и есть пагинация
        elif not response_is_sampled and response_has_pagination:
            print("working on pagination")
            dims_t = []
            metrics_t = []
            # ОБРАБОТКА ПЕРВОЙ СТРАНИЦЫ ОТВЕТА
            self.fulfill(api_response, dims_t, metrics_t)
            next_page_token = api_response["reports"][0]["nextPageToken"]
            while True:
                # пока в ответе есть nextPageToken делаем запросы и обрабатываем
                try:
                    pagination_query = self.build_query(start_date, end_date, dimensions_list,
                                                        metrics_list, filters, page_token=next_page_token)
                    api_response = self.safe_wrapper(self.analytics, pagination_query)
                    # ТУТ НАДО ОБРАБАТЫВАТЬ ПОСЛЕДУЮЩИЕ СТРАНИЦЫ ОТВЕТА
                    self.fulfill(api_response, dims_t, metrics_t)
                    next_page_token = api_response["reports"][0]["nextPageToken"]
                    print(next_page_token)
                except:
                    print("pagination over")
                    break
            # ФИНАЛЬНАЯ ОБРАБОТКА ОТВЕТА
            data = self.ga_data_to_df(dims_t, metrics_t, dimensions_list, metrics_list)
            if operation_type == "to_csv":
                data_operation_obj.df_to_csv(data, name="have_pagination_no_sampled")
            if operation_type == "to_db":
                data_operation_obj.df_to_db(data, db_table, primary_key)

        # case 3: нет семплирования и нет пагинации
        else:
            dims_t = []
            metrics_t = []
            self.fulfill(api_response, dims_t, metrics_t)
            # ФИНАЛЬНАЯ ОБРАБОТКА ОТВЕТА
            data = self.ga_data_to_df(dims_t, metrics_t, dimensions_list, metrics_list)
            if operation_type == "to_csv":
                data_operation_obj.df_to_csv(data, name="no_pagination_no_sampled")
            if operation_type == "to_db":
                data_operation_obj.df_to_db(data, db_table, primary_key)
