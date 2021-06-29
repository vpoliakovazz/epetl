#!/usr/bin/python
# -*- coding: utf-8 -*-
import json

import pandas as pd
import numpy as np
import slack

from datetime import datetime

from data_extraction import Extract, DataOperation


class SlackMessage:

    def __init__(self):
        self.data_sources = json.load(open("C:/projects/etl/epic-etl/etl_epic/data_config.json"))
        self.slack_token = self.data_sources["slack_token"]
        self.client = slack.WebClient(token=self.slack_token)
    """
    @staticmethod
    def values_diff_day(previous_value, value, type):
        if type == "percent":
            diff = round((((value - previous_value) / previous_value) * 100), 2)
            if diff > 0:
                return "что на {}% *больше* чем вчера".format(abs(diff))
            else:
                return "что на {}% *меньше* чем вчера".format(abs(diff))
        else:
            diff = round((value - previous_value), 2)
            if diff > 0:
                return "что на {} *больше* чем вчера".format(abs(diff))
            else:
                return "что на {} *меньше* чем вчера".format(abs(diff))

    @staticmethod
    def values_diff_week(previous_value, value, type):
        if type == "percent":
            diff = round((((value - previous_value) / previous_value) * 100), 2)
            if diff > 0:
                return "и на {}% *больше* ЧЕЕЕЕМ на прошлой неделе".format(abs(diff))
            else:
                return "и на {}% *меньше* ЧЕЕЕЕМ на прошлой неделе".format(abs(diff))
        else:
            diff = round((value - previous_value), 2)
            if diff > 0:
                return "и на {} *больше* XT на прошлой неделе".format(abs(diff))
            else:
                return "и на {} *меньше* asd на прошлой неделе".format(abs(diff))
    """
    def send_message(self):
        extract_obj = Extract()
        data_yesterday = extract_obj.read_sql_tmp_file("select * from bot_stat where date = current_date - 1",
                                                       extract_obj.pgsql_alchemy_conn)
        data_past_yesterday = extract_obj.read_sql_tmp_file("select * from bot_stat where date = current_date - 2",
                                                            extract_obj.pgsql_alchemy_conn)
        data_past_week = extract_obj.read_sql_tmp_file("select * from bot_stat where date = current_date - 8",
                                                       extract_obj.pgsql_alchemy_conn)
        """
        products_100_day = self.values_diff_day(data_past_yesterday.iloc[0].products_100,
                                           data_yesterday.iloc[0].products_100, "")
        products_100_week = self.values_diff_week(data_past_week.iloc[0].products_100, data_yesterday.iloc[0].products_100,
                                             "")
        products_400_day = self.values_diff_day(data_past_yesterday.iloc[0].products_400,
                                           data_yesterday.iloc[0].products_400, "")
        products_400_week = self.values_diff_week(data_past_week.iloc[0].products_400, data_yesterday.iloc[0].products_400,
                                             "")
        categories_100_day = self.values_diff_day(data_past_yesterday.iloc[0].categories_100,
                                             data_yesterday.iloc[0].categories_100, "")
        categories_100_week = self.values_diff_week(data_past_week.iloc[0].categories_100,
                                               data_yesterday.iloc[0].categories_100, "")
        categories_400_day = self.values_diff_day(data_past_yesterday.iloc[0].categories_400,
                                             data_yesterday.iloc[0].categories_400, "")
        categories_400_week = self.values_diff_week(data_past_week.iloc[0].categories_400,
                                               data_yesterday.iloc[0].categories_400, "")
        total_products_day = self.values_diff_day(data_past_yesterday.iloc[0].total_products,
                                             data_yesterday.iloc[0].total_products, "")
        total_products_week = self.values_diff_week(data_past_week.iloc[0].total_products,
                                               data_yesterday.iloc[0].total_products, "")
        active_products_day = self.values_diff_day(data_past_yesterday.iloc[0].active_products,
                                              data_yesterday.iloc[0].active_products, "")
        active_products_week = self.values_diff_week(data_past_week.iloc[0].active_products,
                                                data_yesterday.iloc[0].active_products, "")
        total_categories_day = self.values_diff_day(data_past_yesterday.iloc[0].total_categories,
                                               data_yesterday.iloc[0].total_categories, "")
        total_categories_week = self.values_diff_week(data_past_week.iloc[0].total_categories,
                                                 data_yesterday.iloc[0].total_categories, "")
        active_categories_day = self.values_diff_day(data_past_yesterday.iloc[0].active_categories,
                                                data_yesterday.iloc[0].active_categories, "")
        active_categories_week = self.values_diff_week(data_past_week.iloc[0].active_categories,
                                                  data_yesterday.iloc[0].active_categories, "")
        revenue_positive_day = self.values_diff_day(data_past_yesterday.iloc[0].revenue_positive,
                                               data_yesterday.iloc[0].revenue_positive, "")
        revenue_positive_week = self.values_diff_week(data_past_week.iloc[0].revenue_positive,
                                                 data_yesterday.iloc[0].revenue_positive, "")
        orders_positive_day = self.values_diff_day(data_past_yesterday.iloc[0].orders_positive,
                                              data_yesterday.iloc[0].orders_positive, "")
        orders_positive_week = self.values_diff_week(data_past_week.iloc[0].orders_positive,
                                                data_yesterday.iloc[0].orders_positive, "")
        sc_orders_day = self.values_diff_day(data_past_yesterday.iloc[0].sc_orders,
                                              data_yesterday.iloc[0].sc_orders, "")
        sc_orders_week = self.values_diff_week(data_past_week.iloc[0].sc_orders,
                                              data_yesterday.iloc[0].sc_orders, "")
        sc_revenue_day = self.values_diff_day(data_past_yesterday.iloc[0].sc_revenue,
                                              data_yesterday.iloc[0].sc_revenue, "")
        sc_revenue_week = self.values_diff_week(data_past_week.iloc[0].sc_revenue,
                                              data_yesterday.iloc[0].sc_revenue, "")
        sc_merchants_day = self.values_diff_day(data_past_yesterday.iloc[0].sc_merchants,
                                              data_yesterday.iloc[0].sc_merchants, "")
        sc_merchants_week = self.values_diff_week(data_past_week.iloc[0].sc_merchants,
                                              data_yesterday.iloc[0].sc_merchants, "")
        """
        message = "{}: \n" \
                  "_Оборот (битрикс)_: {} \n" \
                  "_Кол-во заказов (битрикс)_: {} \n"\
                  "_Кол-во заказов (маркетплейс)_: {} \n" \
                  "_Оборот (маркетплейс)_: {} \n" \
                  "_Общее кол-во заказов (мп+битрикс)_: {} \n" \
                  "_Общий оборот (мп+битрикс)_: {} \n" \
                  "_Общее кол-во заказов в отмене(мп+битрикс)_: {} \n" \
                  "_Общий оборот в отмене(мп+битрикс)_: {} \n" \
                  "_Поставщиков с заказами_: {} \n"\
                  "_Товаров поставщиков_: {} \n" \
                  "_Товаров поставщиков опубликовано_: {}".format(
            data_yesterday.iloc[0].date,
            '{:,.0f}'.format(data_yesterday.iloc[0].ho_revenue).replace(',', ' '),
            '{:,.0f}'.format(data_yesterday.iloc[0].ho_orders).replace(',', ' '),
            '{:,.0f}'.format(data_yesterday.iloc[0].sc_orders).replace(',', ' '),
            '{:,.0f}'.format(data_yesterday.iloc[0].sc_revenue).replace(',', ' '),
            '{:,.0f}'.format(data_yesterday.iloc[0].total_orders).replace(',', ' '),
            '{:,.0f}'.format(data_yesterday.iloc[0].total_revenue).replace(',', ' '),
            '{:,.0f}'.format(data_yesterday.iloc[0].total_cancel_orders).replace(',', ' '),
            '{:,.0f}'.format(data_yesterday.iloc[0].total_cancel_revenue).replace(',', ' '),
            '{:,.0f}'.format(data_yesterday.iloc[0].sc_merchants).replace(',', ' '),
            '{:,.0f}'.format(data_yesterday.iloc[0].mp_all_products).replace(',', ' '),
            '{:,.0f}'.format(data_yesterday.iloc[0].mp_published).replace(',', ' '))

        """
                orders = orders[['date', 'ho_orders', 'ho_revenue', 'ho_cancel_orders',
                         'ho_cancel_orders_revenue', 'sc_merchants', 'sc_revenue', 'sc_orders', 'sc_order_cancel',
                         'sc_revenue_cancel',
                         'mp_all_products', 'mp_published', 'mp_draft', 'mp_moderating', 'mp_banned', 'mp_rework',
                         'total_orders', 'total_revenue', 'total_cancel_orders', 'total_cancel_revenue']]
        
        response = self.client.chat_postMessage(
            channel='em_operations',
            text=message)
        print(response["ts"] + " " + str(response["ok"]))

        response = self.client.chat_postMessage(
            channel='em_analytics',
            text=message)
        print(response["ts"] + " " + str(response["ok"]))

        response = self.client.chat_postMessage(
            channel='analytics_test',
            text=message)
        print(response["ts"] + " " + str(response["ok"]))
        """
        response = self.client.chat_postMessage(
            channel='analytics',
            text=message)
        print(response["ts"] + " " + str(response["ok"]))
