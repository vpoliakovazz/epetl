import logging
import pandas as pd

from datetime import datetime, date, timedelta
from os import walk

from data_extraction import DataOperation, ExtractGAData, Extract
from data_transform import Transformation
from data_transform_from_mssql import TransformationMsSql
from data_transform_ho import TransformationHO
from data_transform_api import TransformationAPI
from data_transform_independent import TransformationIndependent


logging.basicConfig(filename='C:/projects/etl/epic-etl/etl_epic/logs.log',
                    level=logging.DEBUG)
logging.getLogger("googleapiclient").setLevel(logging.ERROR)  # чтобы не падали warnings


class Engine:
    def __init__(self):
        self.operation_obj = DataOperation()
        self.extractObj_ga = ExtractGAData()
        self.extractsql = Extract()
        self.trans_obj = Transformation()
        self.trans_mssql_obj = TransformationMsSql()
        self.trans_ho_obj = TransformationHO()
        self.api_source_obj = TransformationAPI()
        self.trans_independent_obj = TransformationIndependent()

    def ga_handler(self):
        end_date = date.today() - timedelta(days=1)
        end_date = end_date.strftime('%Y-%m-%d')
        # start_date = "2020-03-17"
        for table, values in self.operation_obj.ga_tables.items():
            print(table, values)
            dimensions = values["dimensions"]
            metrics = values["metrics"]
            primary_key = values["primary_key"]
            filters = values["filters"]
            if table == "ga_by_session":
                # за 1 день накапливается 700к строк, пока что сохраняем каждый день в csv, дальше будем думать
                # start_date - последний файл в папке
                my_path = "by session ga csvs/"
                f = []
                for (dirpath, dirnames, filenames) in walk(my_path):
                    f.extend(filenames)
                    break
                start_date = max(f)
                start_date = start_date.replace(".csv", "")

                print("start_date: " + str(start_date))
                try:
                    self.extractObj_ga.load_report(
                        start_date, end_date, dimensions, metrics, filters, "to_csv", table, primary_key)
                    logging.info("GA {} saved successfully {}".format(
                        table, str(pd.to_datetime('today'))))
                except Exception as error:
                    logging.error("GA {} failed with {} error {}".format(
                        table, str(error), str(pd.to_datetime('today'))))
            else:
                max_date_df = self.operation_obj.sql_query("select max(ga_date) from {}".format(table),
                                                           self.operation_obj.pgsql_conn)
                start_date = max_date_df["max"][0]
                print(start_date)
                if not start_date:
                    # если данных нет, надо забрать все данные
                    print("no data")
                    start_date = values["start_date"]
                start_date = str(datetime.strptime(start_date, '%Y%m%d').date() + timedelta(days=1))
                try:
                    print(start_date, end_date, filters)
                    self.extractObj_ga.load_report(
                        start_date, end_date, dimensions, metrics, filters, "to_db", table, primary_key)
                    logging.info("GA {} managing successfully {}".format(
                        table, str(pd.to_datetime('today'))))
                except Exception as error:
                    logging.error("GA {} managing failed with {} error {}".format(
                        table, str(error), str(pd.to_datetime('today'))))

    def mssql_handler(self):
        print(self.operation_obj.mssql_tables)
        for table_input, table_output in self.operation_obj.mssql_tables.items():
            print(table_input, table_output)
            # определяем кол-во строк (table_shape) в исходной таблице
            table_shape_query = "select count(*) from {} with (nolock)".format(table_input)
            table_shape = self.operation_obj.sql_query(table_shape_query, self.extractsql.mssql_conn)
            table_shape = table_shape.iloc[0, 0]
            print(table_shape)
            if table_shape <= 1000000:
                # если таблица небольшая грузим сразу
                try:
                    df = self.extractsql.get_table(table_input, self.extractsql.mssql_conn)
                    logging.info("{} records extracted successfully from {} {}".format(
                        str(df.shape[0]), table_input, str(pd.to_datetime('today'))))
                except Exception as error:
                    logging.error("Extraction failed from {} with {} error {}".format(
                        table_input, str(error), str(pd.to_datetime('today'))))

                df = df.replace({pd.np.nan: None})
                sql_delete_query = "delete from {}".format(table_output)

                try:
                    self.extractsql.execute_sql_code(sql_delete_query, self.extractsql.pgsql_conn)
                    logging.info("deletion success from {} {}".format(table_output, str(pd.to_datetime('today'))))
                except Exception as error:
                    logging.error("deletion failed from {} with {} error {}".format(
                        table_output, str(error), str(pd.to_datetime('today'))))
                try:
                    self.operation_obj.df_to_db(df, table_output)
                    logging.info("Records inserted successfully into {} {}".format(
                        table_output, str(pd.to_datetime('today'))))
                except Exception as error:
                    logging.error("Records insert failed in {} with {} error {}".format(
                        table_output, str(error), str(pd.to_datetime('today'))))
            else:
                # если таблица большая - переливаем по 100к строк
                sql_delete_query = "delete from {}".format(table_output)
                try:
                    self.extractsql.execute_sql_code(sql_delete_query, self.extractsql.pgsql_conn)
                    logging.info("deletion success from {} {}".format(table_output, str(pd.to_datetime('today'))))
                except Exception as error:
                    logging.error("deletion failed from {} with {} error {}".format(
                        table_output, str(error), str(pd.to_datetime('today'))))
                from_row = 1  # 1
                to_row = 1000000  # 100000
                step = 1000000
                try:
                    while from_row < table_shape:
                        print("from_row: " + str(from_row))
                        print("to_row: " + str(to_row))
                        pagination_query = """
                        SELECT *
                        FROM (SELECT ROW_NUMBER() OVER(ORDER BY (select NULL as noorder)) AS RowNum, *
                              FROM {} with (nolock)
                             ) as alias
                        WHERE RowNum BETWEEN {} AND {}
                        """.format(table_input, from_row, to_row)
                        from_row = from_row + step
                        to_row = to_row + step
                        df = self.operation_obj.sql_query(pagination_query, self.extractsql.mssql_conn)
                        print(df.shape)
                        del df["RowNum"]
                        df = df.replace({pd.np.nan: None})
                        self.operation_obj.df_to_db(df, table_output)
                    logging.info("Records inserted successfully into {} {}".format(
                        table_output, str(pd.to_datetime('today'))))
                except Exception as error:
                    logging.error("Records managing failed in {} with {} error {}".format(
                        table_output, str(error), str(pd.to_datetime('today'))))

    def trans_handler(self):
        print(self.extractsql.trans_tables)
        for table in self.extractsql.trans_tables:
            if table in ("trans_orders", "trans_supplier_connector"):
                sql_delete_query = "delete from {}".format(table)
                self.extractsql.execute_sql_code(sql_delete_query, self.extractsql.pgsql_conn)
                df = self.trans_obj.test(table)
                print(df.shape)
                try:
                    self.operation_obj.df_to_db(df, table)
                    logging.info("{} records transformation and managing successfully from {} {}".format(
                        str(df.shape[0]), table, str(pd.to_datetime('today'))))
                except Exception as error:
                    logging.error("Records transformation and managing failed in {} with {} error {}".format(
                        table, str(error), str(pd.to_datetime('today'))))
            else:
                df = self.trans_obj.test(table)
                print(df.shape)
                try:
                    self.operation_obj.df_to_db(df, table)
                    logging.info("{} records transformation and managing successfully from {} {}".format(
                        str(df.shape[0]), table, str(pd.to_datetime('today'))))
                except Exception as error:
                    logging.error("Records transformation and managing failed in {} with {} error {}".format(
                        table, str(error), str(pd.to_datetime('today'))))

    def trans_handler_ms_sql(self):
        print(self.extractsql.trans_tables_ms_sql)
        for table in self.extractsql.trans_tables_ms_sql:
            if table == "trans_orders":
                sql_delete_query = "delete from {}".format(table)
                self.extractsql.execute_sql_code(sql_delete_query, self.extractsql.pgsql_conn)
                df = self.trans_mssql_obj.test(table)
                print(df.shape)
                try:
                    self.operation_obj.df_to_db(df, table)
                    logging.info("{} records transformation and managing successfully from {} {}".format(
                        str(df.shape[0]), table, str(pd.to_datetime('today'))))
                except Exception as error:
                    logging.error("Records transformation and managing failed in {} with {} error {}".format(
                        table, str(error), str(pd.to_datetime('today'))))
            else:
                df = self.trans_mssql_obj.test(table)
                print(df.shape)
                print(df)
                try:
                    self.operation_obj.df_to_db(df, table)
                    logging.info("{} records transformation and managing successfully from {} {}".format(
                        str(df.shape[0]), table, str(pd.to_datetime('today'))))
                except Exception as error:
                    logging.error("Records transformation and managing failed in {} with {} error {}".format(
                        table, str(error), str(pd.to_datetime('today'))))

    def trans_handler_ho(self):
        print(self.extractsql.trans_tables_ho)
        for table in self.extractsql.trans_tables_ho:
            if table not in ('trans_category_history', 'trans_category_company_history', 'trans_available_products'):
                df = self.trans_ho_obj.test(table)
                print(df.shape)
                print(df.head())
                sql_delete_query = "delete from {}".format(table)
                self.extractsql.execute_sql_code(sql_delete_query, self.extractsql.pgsql_conn)
                df = df.replace({pd.np.nan: None})
                self.operation_obj.df_to_db(df, table)
            else:
                df = self.trans_ho_obj.test(table)
                print(df.shape)
                print(df.head())
                print(df.columns)
                df = df.replace({pd.np.nan: None})
                self.operation_obj.df_to_db(df, table)

    def scalium_handler(self):
        print(self.operation_obj.scalium_tables)
        for db, tables in self.operation_obj.scalium_tables.items():
            for table_input, table_output in tables.items():
                print(table_input, table_output)
                sql_text = 'select * from "{}"'.format(table_input)
                if db == "oms":
                    doc_orders = self.operation_obj.read_sql_tmp_file(sql_text, self.operation_obj.sc_oms_alchemy_conn)
                if db == "mas":
                    doc_orders = self.operation_obj.read_sql_tmp_file(sql_text, self.operation_obj.sc_mas_alchemy_conn)
                if db == "pim":
                    doc_orders = self.operation_obj.read_sql_tmp_file(sql_text, self.operation_obj.sc_pim_alchemy_conn)
                if db == "users":
                    doc_orders = self.operation_obj.read_sql_tmp_file(sql_text, self.operation_obj.sc_users_alchemy_conn)
                    if table_input == "user_role":
                        print("user role testing")
                        print(doc_orders.dtypes)
                        doc_orders.user_id = doc_orders.user_id.astype(str)
                        doc_orders.role_id = doc_orders.role_id.astype(str)
                        print(doc_orders.dtypes)
                if db == "balance":
                    doc_orders = self.operation_obj.read_sql_tmp_file(sql_text, self.operation_obj.sc_balance_alchemy_conn)
                print(doc_orders.shape)
                # print(doc_orders)
                sql_delete_query = "delete from {}".format(table_output)
                self.extractsql.execute_sql_code(sql_delete_query, self.extractsql.pgsql_conn)
                doc_orders = doc_orders.replace({pd.np.nan: None})
                self.operation_obj.df_to_db(doc_orders, table_output)

    def scalium_refresh_handler(self):
        print(self.operation_obj.scalium_tables_refresh)
        for db, tables in self.operation_obj.scalium_tables_refresh.items():
            for table_input, table_output in tables.items():
                print(table_input, table_output)
                sql_text = 'select * from "{}"'.format(table_input)
                if db == "oms":
                    doc_orders = self.operation_obj.read_sql_tmp_file(sql_text, self.operation_obj.sc_oms_alchemy_conn)
                if db == "mas":
                    doc_orders = self.operation_obj.read_sql_tmp_file(sql_text, self.operation_obj.sc_mas_alchemy_conn)
                if db == "pim":
                    doc_orders = self.operation_obj.read_sql_tmp_file(sql_text, self.operation_obj.sc_pim_alchemy_conn)
                if db == "users":
                    doc_orders = self.operation_obj.read_sql_tmp_file(sql_text, self.operation_obj.sc_users_alchemy_conn)
                    if table_input == "user_role":
                        print("user role testing")
                        print(doc_orders.dtypes)
                        doc_orders.user_id = doc_orders.user_id.astype(str)
                        doc_orders.role_id = doc_orders.role_id.astype(str)
                        print(doc_orders.dtypes)
                if db == "balance":
                    doc_orders = self.operation_obj.read_sql_tmp_file(sql_text, self.operation_obj.sc_balance_alchemy_conn)
                print(doc_orders.shape)
                # print(doc_orders)
                sql_delete_query = "delete from {}".format(table_output)
                self.extractsql.execute_sql_code(sql_delete_query, self.extractsql.pgsql_conn)
                doc_orders = doc_orders.replace({pd.np.nan: None})
                self.operation_obj.df_to_db(doc_orders, table_output)


    def api_handler(self):
        print(self.extractsql.api_source)
        for pair in self.extractsql.api_source:
            df = self.api_source_obj.test(pair["table"])
            print(df.shape)
            sql_delete_query = "delete from {}".format(pair["table"])
            self.extractsql.execute_sql_code(sql_delete_query, self.extractsql.pgsql_conn)
            df = df.replace({pd.np.nan: None})
            self.operation_obj.df_to_db(df, pair["table"], pair["key"])

    def trans_handler_independent(self):
        print(self.extractsql.trans_tables_independent)
        for table in self.extractsql.trans_tables_independent:
            if table not in ('trans_available_products'):
                df = self.trans_independent_obj.test(table)
                print(df.shape)
                print(df.head())
                sql_delete_query = "delete from {}".format(table)
                self.extractsql.execute_sql_code(sql_delete_query, self.extractsql.pgsql_conn)
                df = df.replace({pd.np.nan: None})
                self.operation_obj.df_to_db(df, table)
            else:
                df = self.trans_independent_obj.test(table)
                print(df.shape)
                print(df.head())
                print(df.columns)
                df = df.replace({pd.np.nan: None})
                self.operation_obj.df_to_db(df, table)
