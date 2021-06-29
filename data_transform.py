from datetime import datetime

import pandas as pd
import numpy as np

from data_extraction import Extract, DataOperation


class Transformation:
    def __init__(self):
        self.operation_obj = DataOperation()
        self.extract_sql = Extract()

    def test(self, table_name):
        # getattr function takes in function name of class and calls it.
        return getattr(self, table_name)()

    @staticmethod
    def trans_orders():
        extract_obj = Extract()
        sql_text = """
        select o.doc_number,
                o.doc_cancel,
                o.doc_date,
                o.site_number,
                sum(o.good_amt) good_amt,
                sum(o.good_qty) good_qty,
                co.phone_number,
                gt.ga_medium,
                gt.ga_devicecategory
        from epic_static_doc_orders o
            left join epic_static_contact_orders co
                on co.order_number = o.doc_number
            left join ga_transaction_data gt
                on gt.ga_transactionid = o.site_number
            group by o.doc_number,
                 o.doc_cancel,
                 o.doc_date,
                 o.site_number,
                 co.phone_number,
                 gt.ga_medium,
                 gt.ga_devicecategory
        """

        print("read data start: {}".format(str(datetime.now())))
        doc_orders = extract_obj.read_sql_tmp_file(sql_text, extract_obj.pgsql_alchemy_conn)
        print(doc_orders.shape)
        print("read data end: {}".format(str(datetime.now())))

        print("transform start: {}".format(str(datetime.now())))
        # по некоторым заказам не подтягиваются номера телефонов
        doc_orders["phone_number"] = doc_orders["phone_number"].fillna('0')

        # c 19 года изменился формат номера телефона в бд, поэтому приводим к одному виду
        doc_orders["phone_number"] = doc_orders["phone_number"].astype(str)
        doc_orders["phone_number"] = doc_orders["phone_number"].apply(lambda x: x.replace("+", ""))

        # удаляем заказы где номера телефонов неверно записаны ("<>", "nan")
        doc_orders["to_delete"] = doc_orders["phone_number"].apply(lambda x: 0 if x.startswith("3") else 1)
        doc_orders = doc_orders[doc_orders["to_delete"] != 1]

        doc_orders["ga_medium"] = doc_orders["ga_medium"].fillna('no_data')
        doc_orders["ga_devicecategory"] = doc_orders["ga_devicecategory"].fillna('no_data')
        doc_orders["site_number"] = doc_orders["site_number"].fillna('no_data')

        # определяем порядковый номер заказа, порядковый номер успешного заказа
        doc_orders = doc_orders.sort_values(["phone_number", "doc_date"])
        doc_orders["doc_n"] = doc_orders.groupby(["phone_number"]).cumcount() + 1
        doc_orders["p_doc_n"] = doc_orders[doc_orders["doc_cancel"] == 'f'].groupby(["phone_number"]).cumcount() + 1

        # данные про время заказа
        doc_orders["doc_date"] = pd.to_datetime(doc_orders["doc_date"])
        doc_orders["doc_month"] = doc_orders.doc_date.values.astype("datetime64[M]")  # месяц заказа
        doc_orders["doc_qtr"] = pd.PeriodIndex(doc_orders.doc_date, freq='Q')  # квартал заказа
        doc_orders["doc_year"] = pd.PeriodIndex(doc_orders.doc_date, freq='Y').year  # год заказа
        # данные про время и источник первой покупки
        doc_orders = doc_orders.merge(
            doc_orders[doc_orders["doc_n"] == 1].groupby(
                ["phone_number", "doc_month", "doc_qtr", "doc_year", "ga_medium"])["doc_n"].count().reset_index(),
            how="left",
            left_on="phone_number",
            right_on="phone_number",
            suffixes=("", "_fo"))
        # данные про время и источник первой позитивной покупки
        doc_orders = doc_orders.merge(
            doc_orders[doc_orders["p_doc_n"] == 1].groupby(
                ["phone_number", "doc_month", "doc_qtr", "doc_year", "ga_medium"])["p_doc_n"].count().reset_index(),
            how="left",
            left_on="phone_number",
            right_on="phone_number",
            suffixes=("", "_fpo"))

        # порядковый номер месяца заказа от месяца первого заказа
        doc_orders["active_month"] = (
                (doc_orders["doc_month"] - doc_orders["doc_month_fo"]) / np.timedelta64(1, "M")).round().astype(int)
        # порядковый номер месяца успешного заказа от месяца первого успешного заказа
        doc_orders["active_month_po"] = (
                (doc_orders["doc_month"] - doc_orders["doc_month_fpo"]) / np.timedelta64(1, "M"))

        doc_orders["active_month_po"] = doc_orders["active_month_po"].fillna(-1)
        doc_orders["active_month_po"] = doc_orders["active_month_po"].round().astype(int)
        print("transform end: {}".format(str(datetime.now())))

        print("qtr start: {}".format(str(datetime.now())))
        # определение порядкового номера квартала

        doc_orders["doc_qtr_number"] = doc_orders["doc_date"].dt.quarter  # номер квартала заказа
        doc_orders["doc_qtr_fo_number"] = doc_orders["doc_month_fo"].dt.quarter  # номер квартала заказа
        doc_orders["doc_qtr_fpo_number"] = doc_orders["doc_month_fpo"].dt.quarter  # номер квартала заказа
        doc_orders["active_qtr"] = (doc_orders.doc_year - doc_orders.doc_year_fo) * 4 + \
                                   doc_orders.doc_qtr_number - doc_orders.doc_qtr_fo_number
        doc_orders["active_qtr_po"] = (doc_orders.doc_year - doc_orders.doc_year_fpo) * 4 + \
                                      doc_orders.doc_qtr_number - doc_orders.doc_qtr_fpo_number
        print("qtr end: {}".format(str(datetime.now())))

        doc_orders["doc_n_fo"] = doc_orders["doc_n_fo"].astype(int)
        doc_orders["p_doc_n"] = doc_orders["p_doc_n"].fillna(-1)
        doc_orders["active_qtr_po"] = doc_orders["active_qtr_po"].fillna(-1)
        doc_orders["p_doc_n"] = doc_orders["p_doc_n"].astype(int)

        doc_orders = doc_orders[['doc_number', 'doc_cancel', 'doc_date', 'site_number', 'good_amt',
                                 'good_qty', 'phone_number', 'ga_medium', 'ga_devicecategory',
                                 'doc_n', 'p_doc_n', 'doc_month', 'doc_qtr', 'doc_year',
                                 'doc_month_fo', 'doc_qtr_fo', 'doc_year_fo', 'ga_medium_fo',
                                 'doc_month_fpo', 'doc_qtr_fpo', 'doc_year_fpo', 'ga_medium_fpo',
                                 'active_month', 'active_month_po', 'active_qtr', 'active_qtr_po']]

        to_str = ["doc_year", "doc_year_fo", "doc_year_fpo", "doc_month", "doc_month_fo", "doc_month_fpo",
                  "doc_qtr", "doc_qtr_fo", "doc_qtr_fpo", "doc_number", "site_number"]
        doc_orders[to_str] = doc_orders[to_str].astype(str)
        return doc_orders

    @staticmethod
    def trans_supplier_connector():
        extract_obj = Extract()

        sql = """
            select supplier_code, articul_1c 
            from swift.dbo.supplier_connector 
            where supplier_id = '1009'
        """

        df = extract_obj.sql_query(sql, extract_obj.mssql_conn)
        return df
