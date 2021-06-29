from data_extraction import Extract

extract_obj = Extract()

sql_table_1 = """
DROP TABLE IF EXISTS ga_transaction_data;
CREATE TABLE em_analytics.public.ga_transaction_data(
   ga_transactionId VARCHAR (150) primary key,
   ga_source VARCHAR (150),
   ga_medium VARCHAR (150),
   ga_campaign VARCHAR (150),
   ga_deviceCategory VARCHAR(50),
   ga_date VARCHAR (50),
   ga_transactionRevenue decimal,
   ga_itemQuantity decimal
);
"""

sql_ga_events_data = """
DROP TABLE IF EXISTS ga_events_data;
CREATE TABLE em_analytics.public.ga_events_data(
   ga_date VARCHAR (50),
   ga_eventLabel VARCHAR (50),
   ga_uniqueEvents int
);
"""

sql_table_2 = """
DROP TABLE IF EXISTS ga_test_data;
CREATE TABLE em_analytics.public.ga_test_data(
   ga_date VARCHAR (50),
   ga_channelGrouping VARCHAR (50),
   ga_sessions int
);
"""

sql_table_4 = """
DROP TABLE IF EXISTS ga_by_session;
CREATE TABLE em_analytics.public.ga_by_session(
   ga_source VARCHAR (50),
   ga_medium VARCHAR (50),
   ga_dimension4 VARCHAR (50),
   ga_dimension1 VARCHAR (50),
   ga_dimension14 VARCHAR (50),
   ga_date VARCHAR (50),
   ga_uniquePageviews int
);
"""

sql_table_5 = """
DROP TABLE IF EXISTS epic_static_exel_time;
CREATE TABLE em_analytics.public.epic_static_exel_time(
    time_key	float,
    hour24	float,
    hour24short_string	varchar	(255),
    hour24min_string	varchar	(255),
    hour24full_string	varchar	(255),
    hour12	float,
    hour12short_string	varchar	(255),
    hour12min_string	varchar	(255),
    hour12full_string	varchar	(255),
    am_pm_code	float,
    am_pm_string	varchar	(255),
    minute	float,
    minute_code	float,
    minute_short_string	varchar	(255),
    minute_full_string24	varchar	(255),
    minute_full_string12	varchar	(255),
    half_hour	float,
    half_hour_code	float,
    half_hour_short_string	varchar	(255),
    half_hour_full_string24	varchar	(255),
    half_hour_full_string12	varchar	(255),
    second	float,
    second_short_string	varchar	(255),
    full_time_string24	varchar	(255),
    full_time_string12	varchar	(255),
    full_time	timestamp
);
"""

sql_table_6 = """
DROP TABLE IF EXISTS epic_static_doc_orders;
CREATE TABLE em_analytics.public.epic_static_doc_orders(
art1c	varchar	(11),
good_name	varchar	(100),
doc_number	varchar	(11),
good_qty	decimal,
good_amt	decimal,
storage	varchar	(150),
pay_form	varchar	(50),
delivery_method	varchar	(50),
doc_status	varchar	(50),
delivery_service	varchar	(25),
stock	varchar	(25),
doc_cancel_reason	varchar	(100),
doc_date	timestamp,
date_updated	timestamp,
doc_cancel	boolean,
supplier	varchar	(100),
cancel	boolean,
cancel_reason	varchar	(100),
supply_cancel	boolean,
supply_cancel_reason	varchar	(250),
source	decimal,
doc_storage	varchar	(150),
site_number	varchar	(30),
deletion_mark boolean,
special_order boolean
);
"""

sql_table_7 = """
DROP TABLE IF EXISTS epic_static_contact_orders;
CREATE TABLE em_analytics.public.epic_static_contact_orders(
    date timestamp NULL,
    order_number varchar (11),
    posted boolean,	
    phone_number varchar (150),
    partner_code varchar (10),
    partner_name varchar (100),
    date_updated timestamp NULL,
    delivery_phone_number varchar (150),
    additional_phone_number varchar (150)
);
"""

sql_table_8 = """
DROP TABLE IF EXISTS epic_static_prod_man;
CREATE TABLE em_analytics.public.epic_static_prod_man(
    man_name varchar (50),
    art1c varchar (11),
    good_id	bigint,
    new_group1 varchar (101),
    new_group2 varchar (101),
    new_group3 varchar (101),
    new_group4 varchar (101),
    new_group5 varchar (101),
    dep_last_group varchar (101),
    dep_code varchar (20),
    dep_name varchar (101),
    date_updated timestamp NULL,
    good_name varchar (201),
    date_in_1C varchar (101),
    active varchar (1),
    availability_status	decimal,
    brand varchar (150),
    dep_id varchar (11),
    threshold decimal,
    MarketPlace	decimal,
    ThisContent	decimal,
    NoContentRequired decimal,
    InAssortment decimal,
    RealSupplier varchar (100),
    new_group1_id varchar (11),
    status varchar (50),
    width varchar (50),
    length varchar (50),
    height varchar (50),
    weight varchar (50),
    volume varchar (50),
    multiplicity varchar (50),
    multiplicityPotamus varchar	(50),
    amountInPackage	varchar (50),
    piecesPerPack varchar (50),
    unit varchar (50)
);
"""



sql_table_9 = """
DROP TABLE IF EXISTS ga_sessions_data;
CREATE TABLE em_analytics.public.ga_sessions_data(
   ga_source VARCHAR (450),
   ga_medium VARCHAR (450),
   ga_campaign VARCHAR (450),
   ga_deviceCategory VARCHAR(50),
   ga_date VARCHAR (50),
   ga_city VARCHAR (150),
   ga_sessions integer,
   ga_users integer,
   ga_newUsers integer,
   ga_percentNewSessions decimal,
   ga_transactions integer
);
"""

sql_trans_orders = """
DROP TABLE IF EXISTS trans_orders;
CREATE TABLE em_analytics.public.trans_orders(
    doc_number VARCHAR (150),
    doc_cancel VARCHAR (2),
    doc_date timestamp NULL,
    site_number VARCHAR (150),
    good_amt decimal,
    good_qty decimal, 
    phone_number VARCHAR (150), 
    ga_medium VARCHAR (150), 
    ga_devicecategory VARCHAR (150),
    doc_n integer,
    p_doc_n integer,
    doc_month VARCHAR (20),
    doc_qtr VARCHAR (10),
    doc_year VARCHAR (10),
    doc_month_fo VARCHAR (20),
    doc_qtr_fo VARCHAR (10), 
    doc_year_fo VARCHAR (10), 
    ga_medium_fo VARCHAR (150),
    doc_month_fpo VARCHAR (150),
    doc_qtr_fpo VARCHAR (10),
    doc_year_fpo VARCHAR (10),
    ga_medium_fpo VARCHAR (150),
    active_month integer, 
    active_month_po integer, 
    active_qtr integer, 
    active_qtr_po integer
);
"""

sql_bot_data = """
DROP TABLE IF EXISTS bot_stat;
CREATE TABLE em_analytics.public.bot_stat(
    date DATE,
    ho_orders int,
    ho_revenue decimal,
    ho_cancel_orders int,
    ho_cancel_orders_revenue decimal,
    sc_merchants int,
    sc_revenue decimal,
    sc_orders int,
    sc_order_cancel int,
    sc_revenue_cancel decimal,
    mp_all_products int,
    mp_published int,
    mp_draft int,
    mp_moderating int,
    mp_banned int,
    mp_rework int,
    total_orders int, 
    total_revenue decimal,
    total_cancel_orders int,
    total_cancel_revenue decimal
);
"""



sql_ga_products_traffic = """
DROP TABLE IF EXISTS sql_ga_products_traffic;
CREATE TABLE em_analytics.public.ga_products_traffic(
    ga_dimension6 VARCHAR(50),
    ga_date VARCHAR(50),
    ga_pageviews integer,
    ga_uniquePageviews integer,
    ga_entrances integer
);
"""

sql_sc_order = """
DROP TABLE IF EXISTS sc_order;
CREATE TABLE em_analytics.public.sc_order(
id uuid ,
company_id character varying (64),
customer_id character varying (64),
subtotal double precision ,
created_at timestamp without time zone ,
updated_at timestamp without time zone ,
statuscode character varying (64),
comment character varying (500),
external_id character varying (64),
payed boolean,
status character varying (64),
assignee_id uuid,
checkout_user_language character varying (255),
skip_customer_contact boolean,
call_status character varying (255),
experiments text

);
"""

sql_sc_order_item = """
DROP TABLE IF EXISTS sc_order_item;
CREATE TABLE em_analytics.public.sc_order_item(
id uuid,
order_id uuid ,
offer_id character varying (64),
product_id character varying (64),
title character varying (600),
image character varying (600),
price double precision ,
quantity double precision ,
subtotal double precision ,
created_at timestamp without time zone, 
updated_at timestamp without time zone ,
url character varying (600),
product_external_id character varying (64),
sku character varying (64)
);
"""

sql_sc_status = """
DROP TABLE IF EXISTS sc_status;
CREATE TABLE em_analytics.public.sc_status(
code character varying (64),
title character varying (64),
weight integer 
);
"""

sql_sc_company = """
DROP TABLE IF EXISTS sc_company;
CREATE TABLE em_analytics.public.sc_company(
id uuid ,
name character varying (255),
logo character varying (255),
gov_number character varying (15),
type character varying (3),
iban character varying (29),
owner_id character varying (255),
status_code character varying (32),
legal_info json ,
updated_by character varying (255),
version integer ,
created_at timestamp without time zone ,
updated_at timestamp without time zone ,
address character varying (255),
location character varying (255),
tariff_id character varying (255),
main_account_id uuid,
responsible_id character varying (255),
critical_limit integer,
slug character varying (255)

);
"""


sql_sc_product = """
DROP TABLE IF EXISTS sc_sc_product;
CREATE TABLE em_analytics.public.sc_sc_product(
id bigint primary key,
status_id smallint ,
company_id uuid ,
attribute_set_id bigint ,
version bigint ,
user_last_modified bigint ,
created_at timestamp without time zone ,
updated_at timestamp without time zone ,
deleted boolean ,
external_id character varying (64),
slug character varying (255),
extra jsonb,
published_at timestamp without time zone,
published_by integer,
offer_data jsonb,
deleted_at timestamp without time zone,
deleted_by integer,
source character varying (32)
);
"""

sql_sc_product_category = """
DROP TABLE IF EXISTS sc_sc_product_category;
CREATE TABLE em_analytics.public.sc_sc_product_category(
id integer ,
product_id bigint ,
category_id integer ,
is_main boolean 
);
"""

sql_sc_category = """
DROP TABLE IF EXISTS sc_sc_category;
CREATE TABLE em_analytics.public.sc_sc_category(
id integer ,
parent_id integer ,
deleted boolean ,
user_last_modified bigint ,
path jsonb ,
created_at timestamp without time zone ,
updated_at timestamp without time zone ,
has_child boolean ,
external_id character varying (64),
slug character varying (255),
extra jsonb ,
view_order integer,
image character varying (255),
active boolean
);
"""
sql_sc_product_i18n = """
DROP TABLE IF EXISTS sc_sc_product_i18n;
CREATE TABLE em_analytics.public.sc_sc_product_i18n(
id bigint,
product_id bigint,
lang_id integer,
title character varying (255)
);
"""

sql_sc_product_status = """
DROP TABLE IF EXISTS sc_sc_product_status;
CREATE TABLE em_analytics.public.sc_sc_product_status(
id bigint,
is_initial boolean ,
is_on_sale boolean ,
user_last_modified bigint, 
created_at timestamp without time zone, 
updated_at timestamp without time zone ,
code character varying (64)
);
"""

sql_sc_order_number = """
DROP TABLE IF EXISTS sc_order_number;
CREATE TABLE em_analytics.public.sc_order_number(
number bigint,
order_id uuid
);
"""

sql_trans_ho_epic_orders = """
DROP TABLE IF EXISTS trans_ho_epic_orders;
CREATE TABLE em_analytics.public.trans_ho_epic_orders(
order_date timestamp,
order_id integer,
marketplace_order_id character varying (255),
sc_order integer,
company_id character varying (6),
status integer,
price decimal,
sku character varying (255),
api_id character varying (255),
product_title character varying (455),
status_type character varying (25),
status_date timestamp,
status_name character varying (255),
total_price decimal,
hubber_com decimal,
marketplace_com decimal,
product_count decimal,
client_phone character varying (255)
);
"""

sql_sc_order_address = """
DROP TABLE IF EXISTS sc_order_address;
CREATE TABLE em_analytics.public.sc_order_address(
id uuid,
order_id uuid ,
email character varying (255),
phone character varying (15),
created_at timestamp without time zone ,
updated_at timestamp without time zone ,
shipment_number character varying (255),
shipment_provider character varying (255),
shipment_settlement_id character varying (64),
shipment_office_id character varying (64),
first_name character varying (255),
last_name character varying (255),
patronymic	character varying (255),
shipment_free_experiment character varying (255),
shipment_is_free boolean,
shipment_autogenerated boolean,
shipment_delivery_price double precision
);
"""

sql_trans_supplier_connector = """
DROP TABLE IF EXISTS trans_supplier_connector;
CREATE TABLE em_analytics.public.trans_supplier_connector(
supplier_code character varying (255),
articul_1c character varying (255)
);
"""

sql_sc_category_i18n = """
DROP TABLE IF EXISTS sc_sc_category_i18n;
CREATE TABLE em_analytics.public.sc_sc_category_i18n(
id integer,
category_id integer,
lang_id integer,
title character varying (255)
);
"""

sql_fotos_stat = """
DROP TABLE IF EXISTS fotos_stat;
CREATE TABLE em_analytics.public.fotos_stat(
order_id character varying (1000) primary key,
columns_2 character varying (1000),
columns_3 decimal,
status character varying (1000),
manager character varying (1000),
partner character varying (1000),
columns_7  character varying (1000),
email character varying (1000),
phone character varying (1000),
date timestamp without time zone,
columns_11 character varying (1000),
shipment character varying (1000),
date_2 timestamp without time zone,
comment character varying (1000)
);
"""

sql_trans_mongo_offers = """
DROP TABLE IF EXISTS trans_mongo_offers;
CREATE TABLE em_analytics.public.trans_mongo_offers(
availability character varying (255),
companyId character varying (255),
productId character varying (255),
sku character varying (255),
price decimal,
old_price decimal
);
"""

sql_trans_sc_media = """
DROP TABLE IF EXISTS trans_sc_media;
CREATE TABLE em_analytics.public.trans_sc_media(
product_id	bigint,
images_count integer
);
"""

sql_trans_mongo_auto_import = """
DROP TABLE IF EXISTS trans_mongo_auto_import;
CREATE TABLE em_analytics.public.trans_mongo_auto_import(
_id character varying (255),
companyid character varying (255)
);
"""

sql_trans_sc_atrributes = """
DROP TABLE IF EXISTS trans_sc_attributes;
CREATE TABLE em_analytics.public.trans_sc_attributes(
product_id	bigint,
filled_attributes integer
);
"""

sql_gsh_managers = """
DROP TABLE IF EXISTS gsh_managers;
CREATE TABLE em_analytics.public.gsh_managers(
company_id character varying (255),
company_title character varying (255),
km character varying (255),
mm character varying (255),
sm character varying (255)
);
"""

sql_sc_sc_user = """
DROP TABLE IF EXISTS sc_sc_user;
CREATE TABLE em_analytics.public.sc_sc_user(
id integer,
active boolean,
auth_key character varying (255),
company_id uuid,
created_at timestamp without time zone,
email character varying (255),
first_name character varying (125),
full_name character varying (125),
language character varying (5),
last_name character varying (125),
password_hash character varying (255),
phone character varying (15),
policy_accepted boolean,
verified boolean,
updated_at timestamp without time zone,
onboarding_passed boolean,
email_verification_token_token character varying (255),
email_verification_token_expired_at timestamp without time zone,
password_reset_token_token character varying (255),
password_reset_token_expired_at timestamp without time zone
);
"""


sql_trans_mongo_bill = """
DROP TABLE IF EXISTS trans_mongo_bill;
CREATE TABLE em_analytics.public.trans_mongo_bill(
items_productId character varying (255),
items_title character varying (255), 
items_price decimal,
items_quantity decimal, 
items_subtotal decimal, 
items_commissionRate decimal,
items_commissionSubtotal decimal,  
items_createdAt timestamp, 
items_updatedAt timestamp,
items_title_0 character varying (255), 
items_title_1 character varying (255), 
items_title_2 character varying (255), 
items_title_3 character varying (255),
items_title_4 character varying (255), 
orderId character varying (255), 
orderNumber character varying (255), 
orderCompanyId character varying (255),
orderStatusCode character varying (255), 
orderCreatedAt timestamp, 
orderTotal decimal, 
createdAt timestamp,
updatedAt timestamp, 
orderCommissionTotal decimal
);
"""

sql_company_shipment_options = """
DROP TABLE IF EXISTS sc_company_shipment_options;
CREATE TABLE em_analytics.public.sc_company_shipment_options(
id uuid,
shipment_id uuid,
name character varying (255),
threshold double precision,
enabled boolean
);
"""

sql_sc_company_shipments = """
DROP TABLE IF EXISTS sc_company_shipments;
CREATE TABLE em_analytics.public.sc_company_shipments(
id uuid,
company_id uuid,
provider character varying (255),
enabled boolean
);
"""


sql_sc_history_entries = """
DROP TABLE IF EXISTS sc_history_entries;
CREATE TABLE em_analytics.public.sc_history_entries(
id uuid,
order_id uuid,
type character varying (100),
data text,
created_at timestamp without time zone,
updated_at timestamp without time zone,
initiator_type character varying (50),
initiator text
);
"""

sql_trans_history = """
DROP TABLE IF EXISTS trans_history;
CREATE TABLE em_analytics.public.trans_history(
order_id varchar(100),
from__to_new decimal,
from_canceled_to_new decimal,
from_completed_to_closed decimal,
from_confirmed_to_canceled decimal,
from_confirmed_to_canceled_by_merchant decimal,
from_confirmed_to_closed decimal,
from_confirmed_to_completed decimal,
from_confirmed_to_delivered decimal,
from_confirmed_to_new decimal,
from_confirmed_to_returned decimal,
from_confirmed_to_sent decimal,
from_confirmed_by_merchant_to_canceled decimal,
from_confirmed_by_merchant_to_canceled_by_merchant decimal,
from_confirmed_by_merchant_to_confirmed decimal,
from_confirmed_by_merchant_to_new decimal,
from_delivered_to_canceled decimal,
from_delivered_to_completed decimal,
from_delivered_to_sent decimal,
from_new_to_canceled decimal,
from_new_to_canceled_by_merchant decimal,
from_new_to_confirmed_by_merchant decimal,
from_sent_to_canceled decimal,
from_sent_to_completed decimal,
from_sent_to_delivered decimal,
from_sent_to_new decimal,
from_sent_to_returned decimal,
from_completed_to_returned decimal
);
"""

sql_for_epicentrm_orders = """
DROP TABLE IF EXISTS for_epicentrm_orders;
CREATE TABLE em_analytics.public.for_epicentrm_orders(
feo_id character varying (255) primary key, 
order_id_1c character varying (255),
order_number character varying (255),
client_order character varying (255),
order_date timestamp without time zone,
doc_status character varying (50), 
is_cancel boolean, 
delivery_method character varying (50),
good_id_1c character varying (50),
 art_1c character varying (50) ,
quantity decimal, 
sell_amt decimal
);
"""


sql_for_epicentrm_goods = """
DROP TABLE IF EXISTS for_epicentrm_goods;
CREATE TABLE em_analytics.public.for_epicentrm_goods(
good_id_1c character varying (50), 
art_1c character varying (50), 
good_name character varying (255), 
marketplace boolean, 
deleted boolean
);
"""

sql_fotos_status_history = """
DROP TABLE IF EXISTS fotos_status_history;
CREATE TABLE em_analytics.public.fotos_status_history(
order_id int,
status_date timestamp without time zone,
zfl_id int,
status_from  character varying (50),
status_to  character varying (50)
);
"""

sql_fotos_trans_history = """
DROP TABLE IF EXISTS fotos_trans_history;
CREATE TABLE em_analytics.public.fotos_trans_history(
order_id bigint, 
from_В_работе_to_Закуплен decimal, 
from_В_работе_to_Не_оформлен decimal,
from_В_работе_to_Оплачено decimal, 
from_В_работе_to_Отгружен decimal,
from_В_работе_to_Отменен decimal, 
from_Закуплен_to_В_работе decimal,
from_Закуплен_to_Закуплен decimal,
from_Закуплен_to_Не_оформлен decimal,
from_Закуплен_to_Оплачено decimal,
from_Закуплен_to_Отгружен decimal,
from_Закуплен_to_Отменен decimal,
from_Не_оформлен_to_В_работе decimal,
from_Не_оформлен_to_Закуплен decimal,
from_Не_оформлен_to_Оплачено decimal,
from_Не_оформлен_to_Отгружен decimal,
from_Не_оформлен_to_Отменен decimal,
from_Не_оформлен_to_Резерв decimal,
from_Новый_to_В_работе decimal,
from_Новый_to_Не_оформлен decimal, 
from_Новый_to_Отменен decimal,
from_Новый_to_Резерв decimal,
from_Оплачено_to_Закуплен decimal,
from_Оплачено_to_Не_оформлен decimal,
from_Оплачено_to_Отгружен decimal,
from_Отгружен_to_В_работе decimal,
from_Отгружен_to_Закуплен decimal,
from_Отгружен_to_Не_оформлен decimal,
from_Отгружен_to_Оплачено decimal,
from_Отменен_to_В_работе decimal,
from_Отменен_to_Закуплен decimal,
from_Отменен_to_Не_оформлен decimal,
from_Резерв_to_В_работе decimal,
from_Резерв_to_Не_оформлен decimal,
from_Резерв_to_Отгружен decimal
);
"""


sql_trans_sc_brands = """
DROP TABLE IF EXISTS trans_sc_brands;
CREATE TABLE em_analytics.public.trans_sc_brands(
product_id character varying (50), 
value character varying (255)
);
"""

sql_trans_category_history = """
DROP TABLE IF EXISTS trans_category_history;
CREATE TABLE em_analytics.public.trans_category_history(
date_cat_key character varying (50) primary key,
date date,
id character varying (50),
external_id character varying (50),
уровень1 character varying (255),
уровень2 character varying (255),
уровень3 character varying (255),
уровень4 character varying (255),
уровень5 character varying (255),
название character varying (255),
slug character varying (255),
deleted boolean,
has_child boolean,
epic_all_products integer,
epic_published_products integer,
epic_published_available_products integer,
ho_all_products integer,
ho_published_products integer,
ho_published_available_products integer,
mp_all_products integer,
mp_published_products integer,
mp_published_available_products integer
);
"""


sql_sc_account = """
DROP TABLE IF EXISTS sc_account;
CREATE TABLE em_analytics.public.sc_account(
id uuid,
company_id character varying (36),
type character varying (16),
balance numeric,
hold numeric,
created_at timestamp without time zone,
updated_at timestamp without time zone,
updated_by character varying (255)
);
"""

sql_trans_category_company_history = """
DROP TABLE IF EXISTS trans_category_company_history;
CREATE TABLE em_analytics.public.trans_category_company_history(
date_cat_key character varying (50) primary key,
date date,
id character varying (50),
external_id character varying (50),
уровень1 character varying (255),
уровень2 character varying (255),
уровень3 character varying (255),
уровень4 character varying (255),
уровень5 character varying (255),
название character varying (255),
slug character varying (255),
deleted boolean,
has_child boolean,
company_id character varying (50),
published integer,
published_available integer
);
"""


sql_fotos_stat_model = """
DROP TABLE IF EXISTS fotos_stat_model;
CREATE TABLE em_analytics.public.fotos_stat_model(
order_id character varying (255),
columns_1 character varying (255),
columns_2 decimal,
status character varying (1000),
manager character varying (1000),
partner character varying (1000),
columns_6 character varying (1000),
category character varying (1000),
brand character varying (1000),
product_title character varying (1000),
columns_10 character varying (1000),
columns_11 decimal,
columns_12 decimal,
columns_13 decimal,
email character varying (1000), 
phone character varying (1000),
date timestamp without time zone,
columns_17 character varying (1000),
shipment character varying (1000),
date_2 timestamp without time zone, 
comment character varying (1000)
);
"""


sql_sc_transaction = """
DROP TABLE IF EXISTS sc_transaction;
CREATE TABLE em_analytics.public.sc_transaction(
id uuid,
company_id character varying (36),
account_type character varying (16),
type character varying (16),
object_type character varying (16),
object_id character varying (36),
parent_id character varying (36),
creator_id character varying (36),
creator_type character varying (16),
amount numeric,
comment character varying (255),
completed_at timestamp without time zone,
status character varying (255),
error text,
created_at timestamp without time zone,
updated_at timestamp without time zone,
order_number bigint,
order_created_date timestamp without time zone,
issued_at timestamp without time zone,
last boolean,
total_transactions integer

);
"""

sql_trans_mongo_events = """
DROP TABLE IF EXISTS trans_mongo_events;
CREATE TABLE em_analytics.public.trans_mongo_events(
_id character varying (255),
verb character varying (255),
product_id character varying (255),
publishedAt timestamp without time zone,
context_createdAt timestamp,
context_publishedAt timestamp,
context_updatedAt timestamp,
context_statusCode character varying (25),
actor_id character varying (25)
);
"""


sql_ga_events_category = """
DROP TABLE IF EXISTS ga_events_category;
CREATE TABLE em_analytics.public.ga_events_category(
ga_date  character varying (25),
ga_eventCategory  character varying (255),
ga_uniqueEvents integer, 
ga_totalEvents integer, 
ga_eventsPerSessionWithEvent float
);
"""

sql_trans_available_products = """
DROP TABLE IF EXISTS trans_available_products;
CREATE TABLE em_analytics.public.trans_available_products(
date character varying (25),
draft_available integer,
moderating_available integer, 
published_available integer, 
banned_available integer,
new_available integer,
enrich_available integer
);
"""

sql_trans_mongo_import_stat ="""
DROP TABLE IF EXISTS trans_mongo_import_stat;
CREATE TABLE em_analytics.public.trans_mongo_import_stat(
_id character varying (25), 
total integer,
updated integer,
failed integer,
runtime integer, 
url text,
importId character varying (255),
companyId character varying (255),
mode character varying (255),
sourceType character varying (255),
resourceType character varying (255),
importType character varying (255),
startedAt timestamp without time zone,  
finishedAt timestamp without time zone,
notChanged integer,
createdAt timestamp without time zone,
discriminatorField character varying (255),
created integer
);
"""

sql_trans_products_status_time = """
DROP TABLE IF EXISTS trans_products_status_time;
CREATE TABLE em_analytics.public.trans_products_status_time(
id bigint primary key, 
banned decimal,
draft decimal,
enrich decimal,
moderating decimal,
new decimal,
published decimal,
rework decimal
);
"""

sql_sc_roles = """
DROP TABLE IF EXISTS sc_roles;
CREATE TABLE em_analytics.public.sc_roles(
id integer,
name character varying (128),
default_registration_role boolean,
default_system_role boolean,
super_admin_role boolean,
start_page character varying (128)
);
"""

sql_sc_user_role = """
DROP TABLE IF EXISTS sc_user_role;
CREATE TABLE em_analytics.public.sc_user_role(
user_id integer,
role_id integer
);
"""

sql_trans_mongo_ukrposhta_cities = """
DROP TABLE IF EXISTS trans_mongo_ukrposhta_cities;
CREATE TABLE em_analytics.public.trans_mongo_ukrposhta_cities(
_id character varying (128) primary key,
city character varying (255)
);
"""

sql_trans_mongo_novaposhta_cities = """
DROP TABLE IF EXISTS trans_mongo_novaposhta_cities;
CREATE TABLE em_analytics.public.trans_mongo_novaposhta_cities(
_id character varying (128) primary key,
city character varying (255)
);
"""

# extract_obj.execute_sql_code(sql_sc_sc_user, extract_obj.pgsql_conn)
# extract_obj.execute_sql_code(sql_ga_products_traffic, extract_obj.pgsql_conn)
# extract_obj.execute_sql_code(sql_bot_data, extract_obj.pgsql_conn)
# extract_obj.execute_sql_code(sql_trans_orders, extract_obj.pgsql_conn)
# extract_obj.execute_sql_code(sql_table_1, extract_obj.pgsql_conn)
# extract_obj.execute_sql_code(sql_table_2, extract_obj.pgsql_conn)
# extract_obj.execute_sql_code(sql_table_3, extract_obj.pgsql_conn)
# extract_obj.execute_sql_code(sql_table_4, extract_obj.pgsql_conn)
# extract_obj.execute_sql_code(sql_table_5, extract_obj.pgsql_conn)
# extract_obj.execute_sql_code(sql_table_6, extract_obj.pgsql_conn)
# extract_obj.execute_sql_code(sql_table_7, extract_obj.pgsql_conn)
# extract_obj.execute_sql_code(sql_table_8, extract_obj.pgsql_conn)
# extract_obj.execute_sql_code(sql_table_9, extract_obj.pgsql_conn)
# extract_obj.execute_sql_code(sql_table_9, extract_obj.pgsql_conn)
# extract_obj.execute_sql_code(sql_sc_order, extract_obj.pgsql_conn)
# extract_obj.execute_sql_code(sql_sc_order_item, extract_obj.pgsql_conn)
# extract_obj.execute_sql_code(sql_sc_status, extract_obj.pgsql_conn)
# extract_obj.execute_sql_code(sql_sc_company, extract_obj.pgsql_conn)
# extract_obj.execute_sql_code(sql_sc_product, extract_obj.pgsql_conn)
# extract_obj.execute_sql_code(sql_sc_product_category, extract_obj.pgsql_conn)
# extract_obj.execute_sql_code(sql_sc_category, extract_obj.pgsql_conn)
# extract_obj.execute_sql_code(sql_sc_product_i18n, extract_obj.pgsql_conn)
# extract_obj.execute_sql_code(sql_sc_product_status, extract_obj.pgsql_conn)
# extract_obj.execute_sql_code(sql_sc_order_number, extract_obj.pgsql_conn)
# extract_obj.execute_sql_code(sql_trans_ho_epic_orders, extract_obj.pgsql_conn)
# extract_obj.execute_sql_code(sql_sc_order_address, extract_obj.pgsql_conn)
# extract_obj.execute_sql_code(sql_trans_supplier_connector, extract_obj.pgsql_conn)
# extract_obj.execute_sql_code(sql_sc_category_i18n, extract_obj.pgsql_conn)
# extract_obj.execute_sql_code(sql_fotos_stat, extract_obj.pgsql_conn)
# extract_obj.execute_sql_code(sql_trans_mongo_offers, extract_obj.pgsql_conn)
# extract_obj.execute_sql_code(sql_trans_sc_atrributes, extract_obj.pgsql_conn)
# extract_obj.execute_sql_code(sql_trans_sc_media, extract_obj.pgsql_conn)
# extract_obj.execute_sql_code(sql_ga_events_data, extract_obj.pgsql_conn)
# extract_obj.execute_sql_code(sql_gsh_managers, extract_obj.pgsql_conn)
# extract_obj.execute_sql_code(sql_trans_mongo_bill, extract_obj.pgsql_conn)
# extract_obj.execute_sql_code(sql_trans_mongo_auto_import, extract_obj.pgsql_conn)
# extract_obj.execute_sql_code(sql_company_shipment_options, extract_obj.pgsql_conn)
# extract_obj.execute_sql_code(sql_sc_company_shipments, extract_obj.pgsql_conn)
# extract_obj.execute_sql_code(sql_sc_history_entries, extract_obj.pgsql_conn)
# extract_obj.execute_sql_code(sql_trans_history, extract_obj.pgsql_conn)
# extract_obj.execute_sql_code(sql_for_epicentrm_orders, extract_obj.pgsql_conn)
# extract_obj.execute_sql_code(sql_for_epicentrm_goods, extract_obj.pgsql_conn)
# extract_obj.execute_sql_code(sql_fotos_status_history, extract_obj.pgsql_conn)
# extract_obj.execute_sql_code(sql_fotos_trans_history, extract_obj.pgsql_conn)
# extract_obj.execute_sql_code(sql_trans_sc_brands, extract_obj.pgsql_conn)
# extract_obj.execute_sql_code(sql_trans_category_history, extract_obj.pgsql_conn)
# extract_obj.execute_sql_code(sql_sc_account, extract_obj.pgsql_conn)
# extract_obj.execute_sql_code(sql_trans_category_company_history, extract_obj.pgsql_conn)
# extract_obj.execute_sql_code(sql_fotos_stat_model, extract_obj.pgsql_conn)
# extract_obj.execute_sql_code(sql_sc_transaction, extract_obj.pgsql_conn)
# extract_obj.execute_sql_code(sql_trans_mongo_events, extract_obj.pgsql_conn)
# extract_obj.execute_sql_code(sql_ga_events_category, extract_obj.pgsql_conn)
# extract_obj.execute_sql_code(sql_trans_available_products, extract_obj.pgsql_conn)
extract_obj.execute_sql_code(sql_trans_mongo_import_stat, extract_obj.pgsql_conn)
# extract_obj.execute_sql_code(sql_trans_products_status_time, extract_obj.pgsql_conn)
# extract_obj.execute_sql_code(sql_sc_roles, extract_obj.pgsql_conn)
# extract_obj.execute_sql_code(sql_sc_user_role, extract_obj.pgsql_conn)
# extract_obj.execute_sql_code(sql_trans_mongo_ukrposhta_cities, extract_obj.pgsql_conn)
# extract_obj.execute_sql_code(sql_trans_mongo_novaposhta_cities, extract_obj.pgsql_conn)




# extract_obj.execute_sql_code("delete from ga_transaction_data where ga_date='20200327' ", extract_obj.pgsql_conn)
# extract_obj.execute_sql_code("delete from ga_sessions_data where ga_date='20181214' ", extract_obj.pgsql_conn)
# extract_obj.execute_sql_code("delete from ga_transaction_data")
# extract_obj.execute_sql_code("delete from ga_test_data")
# extract_obj.execute_sql_code("delete from test_insert")
