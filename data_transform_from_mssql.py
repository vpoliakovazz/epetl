from datetime import datetime

import pandas as pd
import numpy as np

from data_extraction import Extract, DataOperation


class TransformationMsSql:
    def __init__(self):
        self.operation_obj = DataOperation()
        self.extract_sql = Extract()

    def test(self, table_name):
        # getattr function takes in function name of class and calls it.
        return getattr(self, table_name)()

    @staticmethod
    def bot_stat():
        extract_obj = Extract()

        ho_orders_sql = """
                select
                    DATE_FORMAT(FROM_UNIXTIME(o.created_at), '%Y-%m-%d') date,
                    count(distinct o.id) ho_orders,
                    sum(op.count * op.price) ho_revenue,
                    count(distinct if (o2.status in (3, 4, 7, 11, 12, 13, 15, 17, 18, 20, 24, 28, 29, 30, 31, 32,33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 49) , op.id, null)) ho_cancel_orders,
                    sum( if (o2.status in (3, 4, 7, 11, 12, 13, 15, 17, 18, 20, 24, 28, 29, 30, 31, 32,33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 49) , op.count * op.price, 0)) ho_cancel_orders_revenue
                from `order` o
                left join outgoing o2
                    on o.id = o2.order_id
                left join outgoing_product op
                    on o2.id = op.outgoing_id
                LEFT JOIN order_status os
                    on os.status_id = o.status
                where o.company_id=11162
                and op.sku not in ('PampersEpi_1', 'PampersEpi_2')
                and o.alias not like '1000%'
                and DATE_FORMAT(FROM_UNIXTIME(o.created_at), '%Y-%m-%d') = subdate(current_date, 1)
                group by date
                order by date desc
        """

        '''
        orders_sql = """
        select dateadd(dd,-1, cast(getdate() as date)) as date,
               sum(good_amt) revenue_positive,
               sum(case when doc_cancel=1 then good_amt else 0 end) revenue_cancel,
               count (distinct doc_number ) orders_positive,
               count (distinct case when doc_cancel=1 then doc_number end) orders_cancel
        from sap.dbo.doc_orders o with (nolock)
            left join sap.dbo.prod_man p with (nolock)
                on o.art1c = p.art1c
        where doc_date between dateadd(dd,-1, cast(getdate() as date)) and dateadd(dd,0, cast(getdate() as date))
            and p.MarketPlace=1
        """
        '''

        '''
        products_sql = """
        select dateadd(dd,-1, cast(getdate() as date)) as date,
                count(*) total_products,
                count(case when active = 'Y' then art1c end) active_products,
                count(case when status=100 and active = 'Y' then art1c end) products_100,
                count(case when status=400 and active = 'Y'then art1c end) products_400,
                count(distinct new_group1_id ) total_categories,
                count(distinct case when active = 'Y' then new_group1_id end) active_categories,
                count(distinct case when status=100 and active = 'Y' then new_group1_id end) categories_100,
                count(distinct case when status=400 and active = 'Y' then new_group1_id end) categories_400 
        from sap.dbo.prod_man with (nolock)
        where MarketPlace = 1
        """
        '''

        scalium_sql = """
        select date(o.created_at) date,
           count(distinct o.company_id) sc_merchants,
           sum(i.subtotal) sc_revenue,
           count(distinct o.id) sc_orders,
           count(distinct case when o.statuscode in ('canceled','canceled_by_merchant') then o.id end) sc_order_cancel,
           sum(case when o.statuscode in ('canceled','canceled_by_merchant') then i.subtotal else 0 end) sc_revenue_cancel
        from "order" o
            left join order_address oa
                on o.id = oa.order_id
        left join order_item i
            on o.id = i.order_id
        where oa.first_name not like '%тест%' and  oa.first_name not like '%ТЕСТ%'
        and sku not in ('PampersEpi_1', 'PampersEpi_2')
        group by date
        order by date desc
        """

        scalium_products_sql = """
            select (current_date - 1) as date,
           count(*) mp_all_products,
           count(case when status_id=3 then status_id end) mp_published,
           count(case when status_id=1 then status_id end) mp_draft,
           count(case when status_id=2 then status_id end) mp_moderating,
           count(case when status_id=4 then status_id end) mp_banned,
           count(case when status_id=5 then status_id end) mp_rework
            from sc_product
            where company_id not in ('4314bd2a-5682-4b47-a358-2628dd9c8f52', '688fb41b-e697-4e64-b87b-a9e72c93a8c2',
                                    '90d56db3-8072-45fa-938f-658b08e423e0', '462df6bf-e327-4be5-a11b-97c0ca7d18f8')
        """
        orders = extract_obj.sql_query(ho_orders_sql, extract_obj.mysql_alchemy_conn)
        # products = extract_obj.sql_query(products_sql, extract_obj.mssql_conn)
        scalium_orders = extract_obj.read_sql_tmp_file(scalium_sql, extract_obj.sc_oms_alchemy_conn)
        scalium_products = extract_obj.read_sql_tmp_file(scalium_products_sql, extract_obj.sc_pim_alchemy_conn)
        scalium_orders["date"] = scalium_orders["date"].astype(str)
        orders["date"] = orders["date"].astype(str)
        # products["date"] = products["date"].astype(str)
        scalium_products["date"] = scalium_products["date"].astype(str)

        # orders = orders.merge(products, how="left", left_on="date", right_on="date")

        orders = orders.merge(scalium_orders, how="left", left_on="date", right_on="date")
        orders = orders.merge(scalium_products, how="left", left_on="date", right_on="date")
        orders["total_orders"] = orders["ho_orders"] + orders["sc_orders"]
        orders["total_revenue"] = orders["ho_revenue"] + orders["sc_revenue"]
        orders["total_cancel_orders"] = orders["ho_cancel_orders"] + orders["sc_order_cancel"]
        orders["total_cancel_revenue"] = orders["ho_cancel_orders_revenue"] + orders["sc_revenue_cancel"]


        '''
        orders = orders[['date', 'revenue_positive', 'revenue_cancel', 'orders_positive',
                         'orders_cancel', 'products_100', 'products_400', 'categories_100',
                         'categories_400', 'total_products', 'active_products',
                         'total_categories', 'active_categories', 'sc_merchants', 'sc_revenue', 'sc_orders',
                         'mp_all_products', 'mp_published', 'mp_draft', 'mp_moderating', 'mp_banned', 'mp_rework',
                         'ho_orders', 'ho_revenue', 'ho_cancel_orders',
                         'ho_cancel_orders_revenue', 'sc_order_cancel',
                         'sc_revenue_cancel']]
        '''
        orders = orders[['date', 'ho_orders', 'ho_revenue', 'ho_cancel_orders',
                         'ho_cancel_orders_revenue', 'sc_merchants', 'sc_revenue', 'sc_orders', 'sc_order_cancel',
                         'sc_revenue_cancel',
                         'mp_all_products', 'mp_published', 'mp_draft', 'mp_moderating', 'mp_banned', 'mp_rework',
                         'total_orders', 'total_revenue', 'total_cancel_orders', 'total_cancel_revenue']]
        orders = orders.fillna(0)
        return orders
