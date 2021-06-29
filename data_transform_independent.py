from data_extraction import Extract, DataOperation
import pandas as pd


class TransformationIndependent:
    def __init__(self):
        self.operation_obj = DataOperation()
        self.extract_sql = Extract()

    def test(self, table_name):
        # getattr function takes in function name of class and calls it.
        return getattr(self, table_name)()

    @staticmethod
    def trans_category_history():
        extract_obj = Extract()
        sql = """select * from
                                  (
                    select sc.id,
                           external_id,
                           sci18n.title as уровень1,
                           sci18n1.title as уровень2,
                           sci18n2.title as уровень3,
                           sci18n3.title as уровень4,
                           sci18n4.title as уровень5,
                           sci18n0.title as название,
                           slug,
                           deleted,
                           has_child,
                           concat(current_date,  sc.id) as date_cat_key,
                           current_date AS date
                    
                    from (
                    select *,
                           CASE
                               when tt->>3 is null then jsonb_build_array( tt->>4)
                               when tt->>2 is null then jsonb_build_array( tt->>3, tt->>4)
                               when tt->>1 is null then jsonb_build_array( tt->>2, tt->>3, tt->>4)
                               when tt->>0 is null then jsonb_build_array(tt->>1, tt->>2, tt->>3, tt->>4)
                           else tt
                               END extended_path
                    from(
                    select *,
                    jsonb_build_array(path->>3, path->>2, path->>1, path->>0, cast(id as varchar)) tt
                    from sc_sc_category
                    ) o) sc
                        left join sc_sc_category_i18n sci18n on sc.extended_path->>0  = cast(sci18n.category_id as varchar) and sci18n.lang_id = 1
                        left join sc_sc_category_i18n sci18n1 on sc.extended_path->>1  = cast(sci18n1.category_id as varchar) and sci18n1.lang_id = 1
                        left join sc_sc_category_i18n sci18n2 on sc.extended_path->>2  = cast(sci18n2.category_id as varchar) and sci18n2.lang_id = 1
                        left join sc_sc_category_i18n sci18n3 on sc.extended_path->>3  = cast(sci18n3.category_id as varchar) and sci18n3.lang_id = 1
                        left join sc_sc_category_i18n sci18n4 on sc.extended_path->>4  = cast(sci18n4.category_id as varchar) and sci18n4.lang_id = 1
                        left join sc_sc_category_i18n sci18n0 on sc.id  = sci18n0.category_id  and sci18n0.lang_id = 1) cats
                    left join
                    (select
                           sspc.category_id,
                           count(distinct case when p.company_id='4314bd2a-5682-4b47-a358-2628dd9c8f52' then p.id end) AS epic_all_products,
                           count(distinct case when p.company_id='4314bd2a-5682-4b47-a358-2628dd9c8f52' and status_id = 3 then p.id end) as epic_published_products,
                           count(distinct case when p.company_id='4314bd2a-5682-4b47-a358-2628dd9c8f52' and status_id = 3 and tmo.availability='in_stock' then p.id end) as epic_published_available_products,
                           count(distinct case when p.company_id='462df6bf-e327-4be5-a11b-97c0ca7d18f8' then p.id end) AS ho_all_products,
                           count(distinct case when p.company_id='462df6bf-e327-4be5-a11b-97c0ca7d18f8' and status_id = 3 then p.id end) as ho_published_products,
                           count(distinct case when p.company_id='462df6bf-e327-4be5-a11b-97c0ca7d18f8' and status_id = 3 and tmo.availability='in_stock' then p.id end) as ho_published_available_products,
                           count(distinct case when p.company_id not in ('462df6bf-e327-4be5-a11b-97c0ca7d18f8', '4314bd2a-5682-4b47-a358-2628dd9c8f52') then p.id end) AS mp_all_products,
                           count(distinct case when p.company_id not in ('462df6bf-e327-4be5-a11b-97c0ca7d18f8', '4314bd2a-5682-4b47-a358-2628dd9c8f52') and status_id = 3 then p.id end) as mp_published_products,
                           count(distinct case when p.company_id not in ('462df6bf-e327-4be5-a11b-97c0ca7d18f8', '4314bd2a-5682-4b47-a358-2628dd9c8f52') and status_id = 3 and tmo.availability='in_stock' then p.id end) as mp_published_available_products
                    from sc_sc_product p
                        join sc_sc_product_category sspc
                            on p.id = sspc.product_id and sspc.is_main=True
                        left join trans_mongo_offers tmo
                            on tmo.productid = cast(p.id as varchar)
                    GROUP BY sspc.category_id) p
                    on cats.id=p.category_id"""
        df = extract_obj.read_sql_tmp_file(sql, extract_obj.pgsql_alchemy_conn)
        df = df[['date_cat_key', 'date', 'id', 'external_id', 'уровень1', 'уровень2', 'уровень3', 'уровень4',
                   'уровень5', 'название', 'slug', 'deleted', 'has_child',
                     'epic_all_products', 'epic_published_products',
                   'epic_published_available_products', 'ho_all_products',
                   'ho_published_products', 'ho_published_available_products',
                   'mp_all_products', 'mp_published_products',
                   'mp_published_available_products']]
        return df

    @staticmethod
    def trans_category_company_history():
        extract_obj = Extract()
        sql = """select concat(current_date,  id, company_id) as date_cat_key, * from
              (
select sc.id,
       external_id,
       sci18n.title as уровень1,
       sci18n1.title as уровень2,
       sci18n2.title as уровень3,
       sci18n3.title as уровень4,
       sci18n4.title as уровень5,
       sci18n0.title as название,
       slug,
       deleted,
       has_child,
       current_date AS date

from (
select *,
       CASE
           when tt->>3 is null then jsonb_build_array( tt->>4)
           when tt->>2 is null then jsonb_build_array( tt->>3, tt->>4)
           when tt->>1 is null then jsonb_build_array( tt->>2, tt->>3, tt->>4)
           when tt->>0 is null then jsonb_build_array(tt->>1, tt->>2, tt->>3, tt->>4)
       else tt
           END extended_path
from(
select *,
jsonb_build_array(path->>3, path->>2, path->>1, path->>0, cast(id as varchar)) tt
from sc_sc_category
) o) sc
    left join sc_sc_category_i18n sci18n on sc.extended_path->>0  = cast(sci18n.category_id as varchar) and sci18n.lang_id = 1
    left join sc_sc_category_i18n sci18n1 on sc.extended_path->>1  = cast(sci18n1.category_id as varchar) and sci18n1.lang_id = 1
    left join sc_sc_category_i18n sci18n2 on sc.extended_path->>2  = cast(sci18n2.category_id as varchar) and sci18n2.lang_id = 1
    left join sc_sc_category_i18n sci18n3 on sc.extended_path->>3  = cast(sci18n3.category_id as varchar) and sci18n3.lang_id = 1
    left join sc_sc_category_i18n sci18n4 on sc.extended_path->>4  = cast(sci18n4.category_id as varchar) and sci18n4.lang_id = 1
    left join sc_sc_category_i18n sci18n0 on sc.id  = sci18n0.category_id  and sci18n0.lang_id = 1
where has_child=False) cats
left join
(select
       sspc.category_id,
       p.company_id,
       count(distinct case when status_id = 3 then p.id end) published,
       count(distinct case when status_id = 3 and tmo.availability='in_stock' then p.id end) published_available
from sc_sc_product p
    join sc_sc_product_category sspc
        on p.id = sspc.product_id and sspc.is_main=True
    left join trans_mongo_offers tmo
        on tmo.productid = cast(p.id as varchar)
GROUP BY sspc.category_id, p.company_id) p
on cats.id=p.category_id"""
        df = extract_obj.read_sql_tmp_file(sql, extract_obj.pgsql_alchemy_conn)
        df = df[['date_cat_key', 'date', 'id', 'external_id', 'уровень1', 'уровень2', 'уровень3', 'уровень4',
                 'уровень5', 'название', 'slug', 'deleted', 'has_child',
                 'company_id', 'published', 'published_available']]
        return df

    @staticmethod
    def trans_history():
        extract_obj = Extract()
        sql = """
        select h.order_id,
               h.data,
               h.created_at,
               o.created_at as order_created_date,
               o.updated_at order_last_status_update,
               o.status current_status,
               cast(data AS json)->>'from' as status_from,
               cast(data AS json)->>'to' as status_to,
               concat('from_', cast(data AS json)->>'from','_', 'to_', cast(data AS json)->>'to') as from_to
        from history_entries h
            join "order" o
                on h.order_id = o.id
        where o.created_at>='2021-01-26'
            and type = 'status_changed'
        order by order_id, h.created_at
        """
        data = extract_obj.read_sql_tmp_file(sql, extract_obj.sc_oms_alchemy_conn)
        data["created_at"] = pd.to_datetime(data["created_at"])
        data['diff'] = data.groupby(['order_id'])['created_at'].diff()
        data['diff_hours'] = data.groupby(['order_id'])['created_at'].diff().astype('timedelta64[h]')
        data["order_from_to"] = data["order_id"] + data["from_to"]
        data = data.drop_duplicates(subset="order_from_to", keep="last")
        data = data.reset_index().pivot('order_id', columns='from_to', values='diff_hours').reset_index()
        data = data[['order_id',
                    'from__to_new',
                    'from_canceled_to_new',
                    'from_completed_to_closed',
                    'from_confirmed_to_canceled',
                    'from_confirmed_to_canceled_by_merchant',
                    'from_confirmed_to_closed',
                    'from_confirmed_to_completed',
                    'from_confirmed_to_delivered',
                    'from_confirmed_to_new',
                    'from_confirmed_to_returned',
                    'from_confirmed_to_sent',
                    'from_confirmed_by_merchant_to_canceled',
                    'from_confirmed_by_merchant_to_canceled_by_merchant',
                    'from_confirmed_by_merchant_to_confirmed',
                    'from_confirmed_by_merchant_to_new',
                    'from_delivered_to_canceled',
                    'from_delivered_to_completed',
                    'from_delivered_to_sent',
                    'from_new_to_canceled',
                    'from_new_to_canceled_by_merchant',
                    'from_new_to_confirmed_by_merchant',
                    'from_sent_to_canceled',
                    'from_sent_to_completed',
                    'from_sent_to_delivered',
                    'from_sent_to_new',
                    'from_sent_to_returned',
                    'from_completed_to_returned']]
        return data

    @staticmethod
    def trans_sc_media():
        extract_obj = Extract()
        sql = """
        select product_id, count(*) images_count
        from sc_product_media
        group by product_id
        """
        df = extract_obj.read_sql_tmp_file(sql, extract_obj.sc_pim_alchemy_conn)
        df["product_id"] = df["product_id"].astype(float)
        df["images_count"] = df["images_count"].astype(float)
        return df

    @staticmethod
    def trans_mongo_auto_import():
        extract_obj = Extract()

        df = extract_obj.mongo_find(connection=extract_obj.mongo_conn_bill,
                                    db='em--pim--import-service',
                                    collection='auto_imports',
                                    match={},
                                    projection={"companyId": 1})
        df = pd.DataFrame(df)
        df["_id"] = df["_id"].astype(str)
        return df

    @staticmethod
    def trans_sc_attributes():
        extract_obj = Extract()
        sql = """
        select product_id, count(distinct title) filled_attributes
            from (
            -- sc_attribute_select_value
            select s.product_id, si.title, ro.value
            from sc_attribute_select_value s
                left join sc_attribute_i18n si
                    on s.attribute_id = si.attribute_id
                        and si.lang_id = 1
                left join sc_attribute_reference_option_i18n ro
                    on s.reference_option_id = ro.reference_option_id
                        and ro.lang_id = 1
            union
            --sc_attribute_multiselect_value
            select s.product_id, si.title, ro.value
            from sc_attribute_multiselect_value s
                left join sc_attribute_i18n si
                    on s.attribute_id = si.attribute_id
                        and si.lang_id = 1
                left join sc_attribute_reference_option_i18n ro
                    on s.reference_option_id = ro.reference_option_id
                        and ro.lang_id = 1
            union
            -- sc_attribute_text_value
            select s.product_id, si.title, ro.value
            from sc_attribute_text_value s
                left join sc_attribute_i18n si
                    on s.attribute_id = si.attribute_id
                        and si.lang_id = 1
                left join sc_attribute_text_i18n ro
                    on s.id = ro.text_value_id
                        and ro.lang_id = 1
            union
            --sc_attribute_float_value
            select s.product_id, si.title, cast(s.value as varchar)
            from sc_attribute_float_value s
                left join sc_attribute_i18n si
                    on s.attribute_id = si.attribute_id
                        and si.lang_id = 1
            union
            --sc_attribute_array_value
            select s.product_id, si.title, cast(s.value as varchar)
            from sc_attribute_array_value s
                left join sc_attribute_i18n si
                    on s.attribute_id = si.attribute_id
                        and si.lang_id = 1
            ) united
        where value is not null
        group by product_id
        """
        df = extract_obj.read_sql_tmp_file(sql, extract_obj.sc_pim_alchemy_conn)
        df["product_id"] = df["product_id"].astype(float)
        df["filled_attributes"] = df["filled_attributes"].astype(float)
        return df

    @staticmethod
    def trans_mongo_offers():
        extractObj = Extract()
        query = [{
            '$group': {
                '_id': {
                    'availability': '$availability',
                    'companyId': '$companyId',
                    'productId': '$productId',
                    'prices': '$prices',
                    'sku': '$sku'
                },
                'sku_count': {
                    '$sum': 1
                }
            }
        }
        ]

        data = extractObj.mongo_agg(connection=extractObj.mongo_conn, db='em--pim--offers-service',
                                    collection='offer', agg_pipeline=query)
        data_for_frame = []
        for document in data:
            document.update(document["_id"])
            document[document['_id']["prices"][0]["type"]] = document['_id']["prices"][0]["value"]
            document[document['_id']["prices"][1]["type"]] = document['_id']["prices"][1]["value"]
            del document["prices"]
            del document["_id"]
            data_for_frame.append(document)
        df = pd.DataFrame(data_for_frame)
        df = df[['availability', 'companyId', 'productId', 'sku', 'price', 'old_price']]
        return df

    @staticmethod
    def trans_ho_epic_orders():
        extract_obj = Extract()

        orders_sql = """
        select
           DATE_FORMAT(FROM_UNIXTIME(o.created_at), '%Y-%m-%d %T') order_date,
           o.id order_id,
           o.alias marketplace_order_id,
           case when o.alias like '1000%' then 1 else 0 end sc_order,
           o.company_id,
           o.status,
           op.price,
           op.sku,
           op.api_id,
           op.title product_title,
           CASE
                WHEN o.status in (6, 46) THEN 'success' 
                WHEN o.status in (0, 1, 5, 8, 10, 14, 16, 27, 47, 48, 50) THEN 'in progress' 
                WHEN o.status in (3, 4, 7, 11, 12, 13, 15, 17, 18, 20, 24, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 49) THEN 'cancel' 
                WHEN o.status in (25, 99) THEN 'test'
                ELSE 5 # undefined
           END status_type,
           DATE_FORMAT(FROM_UNIXTIME(o.status_at), '%Y-%m-%d %T') status_date,
           os.description status_name,
           op.count * op.price total_price,
           (op.count * op.price) / 100 * op.hubber_commission hubber_com,
           (op.count * op.price) / 100 * op.marketplace_commission marketplace_com,
           op.count product_count,
           o.client_phone
        from `order` o
        left join outgoing o2
            on o.id = o2.order_id
        left join outgoing_product op
            on o2.id = op.outgoing_id
        LEFT JOIN order_status os
            on os.status_id = o.status
        where o.company_id=11162
        and op.sku not in ('PampersEpi_1', 'PampersEpi_2')
        order by order_date desc
        """

        orders = extract_obj.sql_query(orders_sql, extract_obj.mysql_alchemy_conn)

        return orders

    @staticmethod
    def trans_mongo_bill():
        extractObj = Extract()
        data = extractObj.mongo_find(connection=extractObj.mongo_conn_bill,
                                     db='em--billing--processing-service',
                                     collection='invoices', match={}, projection={
                                        "orderId": 1,
                                        "orderNumber": 1,
                                        "orderStatusCode": 1,
                                        "orderCompanyId": 1,
                                        "orderTotal": 1,
                                        "orderCommissionTotal": 1,
                                        "refunded": 1,
                                        'orderCreatedAt': 1,
                                        'createdAt': 1,
                                        'updatedAt': 1})

        data2 = extractObj.mongo_find(connection=extractObj.mongo_conn_bill,
                                      db='em--billing--processing-service',
                                      collection='orderItems', match={},
                                      projection={"appliedTariff": 0, "commission": 0})
        data = pd.DataFrame(data)
        data["refunded"] = data["refunded"].fillna(False)
        data = data[data["refunded"] == False]
        data = data[~data["orderCommissionTotal"].isnull()]
        data2 = pd.DataFrame(data2)

        def title_0(row):
            try:
                a = row["categoryPath"]["0"]["title"]
            except:
                a = None
            return a

        def title_1(row):
            try:
                a = row["categoryPath"]["1"]["title"]
            except:
                a = None
            return a

        def title_2(row):
            try:
                a = row["categoryPath"]["2"]["title"]
            except:
                a = None
            return a

        def title_3(row):
            try:
                a = row["categoryPath"]["3"]["title"]
            except:
                a = None
            return a

        def title_4(row):
            try:
                a = row["categoryPath"]["4"]["title"]
            except:
                a = None
            return a

        data2["title_0"] = data2.apply(title_0, axis=1)
        data2["title_1"] = data2.apply(title_1, axis=1)
        data2["title_2"] = data2.apply(title_2, axis=1)
        data2["title_3"] = data2.apply(title_3, axis=1)
        data2["title_4"] = data2.apply(title_4, axis=1)

        data2.columns = ["items_" + i for i in data2.columns]

        df = data.merge(data2, left_on="_id", right_on="items_invoice", how="left")

        df['C'] = df.groupby(['orderId', 'items_productId']).cumcount() + 1
        df = df[df["C"] == 1]

        df = df[['items_productId',
                 'items_title', 'items_price',
                 'items_quantity', 'items_subtotal', 'items_commissionRate',
                 'items_commissionSubtotal', 'items_createdAt', 'items_updatedAt',
                 'items_title_0', 'items_title_1', 'items_title_2', 'items_title_3',
                 'items_title_4', 'orderId', 'orderNumber', 'orderCompanyId',
                 'orderStatusCode', 'orderCreatedAt', 'orderTotal', 'createdAt',
                 'updatedAt', 'orderCommissionTotal']]
        return df

    @staticmethod
    def trans_mongo_events():
        extractObj = Extract()
        df = extractObj.mongo_find(
            connection=extractObj.mongo_conn_bill,
            db='em--core--activity-stream-service',
            collection='event',
            match={},
            projection={"verb": 1, "publishedAt": 1, "object": 1, "actor.id": 1,
                        "context.product.id": 1, "context.product.createdAt": 1,
                        "context.product.updatedAt": 1, "context.product.publishedAt": 1,
                        "context.product.statusCode": 1})

        t = pd.DataFrame(df)

        t = t[t["verb"] == "product.status.changed"]
        t["product_id"] = t["object"].apply(lambda x: x["id"])
        t["actor_id"] = t["actor"].apply(lambda x: x["id"])
        t["context_createdAt"] = t["context"].apply(lambda x: x["product"]["createdAt"])
        t["context_publishedAt"] = t["context"].apply(lambda x: x["product"]["publishedAt"])
        t["context_updatedAt"] = t["context"].apply(lambda x: x["product"]["updatedAt"])
        t["context_statusCode"] = t["context"].apply(lambda x: x["product"]["statusCode"])

        t = t[['_id', 'verb', 'product_id', 'publishedAt',
               'context_createdAt', 'context_publishedAt', "context_updatedAt",
               'context_statusCode', 'actor_id']]

        t["_id"] = t["_id"].astype(str)
        t["context_createdAt"] = pd.to_datetime(t["context_createdAt"])
        t["context_publishedAt"] = pd.to_datetime(t["context_publishedAt"])
        t["context_updatedAt"] = pd.to_datetime(t["context_updatedAt"])

        t = t.sort_values(["product_id", "publishedAt"])
        return t

    @staticmethod
    def trans_sc_brands():
        extract_obj = Extract()
        sql = """select product_id, value
            from sc_attribute_select_value v
                 join sc_attribute_reference_option_i18n saro
                    on v.reference_option_id = saro.reference_option_id
            where v.attribute_id=5 and lang_id=1"""
        df = extract_obj.read_sql_tmp_file(sql, extract_obj.sc_pim_alchemy_conn)
        return df

    @staticmethod
    def trans_available_products():
        extractObj = Extract()
        query = [
            {
                '$match': {
                    'companyId': {
                        '$nin': [
                            '4314bd2a-5682-4b47-a358-2628dd9c8f52', '462df6bf-e327-4be5-a11b-97c0ca7d18f8',
                            '688fb41b-e697-4e64-b87b-a9e72c93a8c2', '90d56db3-8072-45fa-938f-658b08e423e0'
                        ]
                    }
                }
            }, {
                '$group': {
                    '_id': {
                        'availability': '$availability',
                        'productId': '$productId',
                        'prices': '$prices',
                    },
                    'sku_count': {
                        '$sum': 1
                    }
                }
            }
        ]

        data = extractObj.mongo_agg(connection=extractObj.mongo_conn, db='em--pim--offers-service',
                                    collection='offer', agg_pipeline=query)
        data_for_frame = []
        for document in data:
            document.update(document["_id"])
            document[document['_id']["prices"][0]["type"]] = document['_id']["prices"][0]["value"]
            document[document['_id']["prices"][1]["type"]] = document['_id']["prices"][1]["value"]
            del document["prices"]
            del document["_id"]
            data_for_frame.append(document)
        df = pd.DataFrame(data_for_frame)

        scalium_products_sql = """
            select id, status_id
            from sc_product
            where company_id not in ('4314bd2a-5682-4b47-a358-2628dd9c8f52', '688fb41b-e697-4e64-b87b-a9e72c93a8c2',
                                    '90d56db3-8072-45fa-938f-658b08e423e0', '462df6bf-e327-4be5-a11b-97c0ca7d18f8')
            and deleted <> true
        """
        products = extractObj.read_sql_tmp_file(scalium_products_sql, extractObj.sc_pim_alchemy_conn)
        df["productId"] = df["productId"].astype(int)
        products["id"] = products["id"].astype(int)

        products = products.merge(df[["productId", "availability"]], how="left", left_on="id", right_on="productId")
        products = products[products["availability"] == "in_stock"]
        products = products.groupby(["status_id"])["id"].count().reset_index()
        products = pd.pivot_table(products, values="id", columns="status_id").reset_index()
        products["date"] = pd.to_datetime("today").date()
        products = products[['date', 1, 2, 3, 4, 6, 7]]
        products.columns = ["date", "draft_available", "moderating_available",
                            "published_available", "banned_available",
                            "new_available", "enrich_available"]
        return products

    @staticmethod
    def trans_mongo_import_stat():
        extract_obj = Extract()

        df = extract_obj.mongo_find(connection=extract_obj.mongo_conn_bill,
                                    db='em--pim--import-service',
                                    collection='AbstractImportReport',
                                    match={},
                                    projection={})
        df = pd.DataFrame(df)
        df["_id"] = df["_id"].astype(str)
        df["importId"] = df["importId"].astype(str)
        return df

    @staticmethod
    def trans_products_status_time():
        extract_obj = Extract()

        analytics_sql = """
            select id, created_at, 'created' as context_statuscode
            from sc_sc_product
            where company_id not in (   '4314bd2a-5682-4b47-a358-2628dd9c8f52',
                '462df6bf-e327-4be5-a11b-97c0ca7d18f8',
                 '688fb41b-e697-4e64-b87b-a9e72c93a8c2',
                 '90d56db3-8072-45fa-938f-658b08e423e0')
            union
            select cast(product_id as bigint), publishedat, context_statuscode
            from trans_mongo_events
            where verb='product.status.changed'
        """
        data = extract_obj.read_sql_tmp_file(analytics_sql, extract_obj.pgsql_alchemy_conn)

        data = data.sort_values(["id", "created_at"])

        data["created_at"] = pd.to_datetime(data["created_at"])
        data['time_in_status'] = data.groupby(['id'])['created_at'].diff()
        data["time_in_status"] = data["time_in_status"].shift(-1)
        data["current_date"] = pd.to_datetime("today")
        data["from_current_to_last"] = data["current_date"] - data["created_at"]
        data['time_in_status'].loc[(data['time_in_status'].isnull())] = data['from_current_to_last'].loc[
            (data['time_in_status'].isnull())]

        data = data.groupby(["id", "context_statuscode"])["time_in_status"].sum().reset_index()
        data["time_in_status"] = data["time_in_status"].astype('timedelta64[h]')
        data = data.reset_index().pivot('id', columns='context_statuscode', values='time_in_status').reset_index()
        return data

    @staticmethod
    def trans_mongo_ukrposhta_cities():
        extractObj = Extract()

        data = extractObj.mongo_find(connection=extractObj.mongo_conn_bill,
                                     db='ukrposhta-service',
                                     collection='cities', match={},
                                     projection={"_id": 1, "cityTranslations.value": 1})
        ukrposhta_cities = pd.DataFrame(data)
        ukrposhta_cities["city"] = ukrposhta_cities["cityTranslations"].apply(lambda x: x[1]["value"])
        del ukrposhta_cities["cityTranslations"]
        return ukrposhta_cities

    @staticmethod
    def trans_mongo_novaposhta_cities():
        extractObj = Extract()

        data = extractObj.mongo_find(connection=extractObj.mongo_conn_bill,
                                     db='nova-poshta-service',
                                     collection='settlement', match={},
                                     projection={"_id": 1, "titleTranslations": 1})
        novaposhta_cities = pd.DataFrame(data)
        novaposhta_cities["city"] = novaposhta_cities["titleTranslations"].apply(lambda x: x[1]["value"])
        del novaposhta_cities["titleTranslations"]
        return novaposhta_cities
