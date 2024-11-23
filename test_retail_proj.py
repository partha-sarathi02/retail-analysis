# read_customers_df-12435
# read_orders_df -68883
# filter_closed_orders- 7556
# read_app_config

import pytest
from lib.Utils import get_spark_session
from lib.DataReader import read_customers, read_orders
from lib.DataManipulation import filter_closed_orders, count_orders_state, filter_orders_generic
from lib.ConfigReader import get_app_config

@pytest.mark.skip()
def test_customer_records_df(spark):
    customer_records= read_customers(spark, 'LOCAL').count()
    assert customer_records == 12435 

@pytest.mark.skip("work in progress")
def test_order_records_df(spark):
    order_records= read_orders(spark, 'LOCAL').count()
    assert  order_records == 68884

@pytest.mark.skip()
@pytest.mark.slow()
@pytest.mark.transformation()
def test_filter_closed_orders_df(spark):
   read_orders_df= read_orders(spark, 'LOCAL')
   filtered_orders= filter_closed_orders(read_orders_df).count()
   assert filtered_orders == 7556

@pytest.mark.skip()
def test_read_app_config():
    app_conf= get_app_config('LOCAL')
    assert app_conf['customers.file.path'] == 'data/customers.csv'


# testcase: check count of order_state is matching with aggreaged_results.csv data file or not
@pytest.mark.skip()
@pytest.mark.transformation()
def test_count_order_state(spark, expected_result):
    customers_df= read_customers(spark, 'LOCAL')
    customers_state_count= count_orders_state(customers_df)
    assert customers_state_count.collect() == expected_result.collect()

# filter complete order status
@pytest.mark.skip()
def test_order_status_complete(spark):
    orders_df= read_orders(spark, 'LOCAL')
    filtered_count= filter_orders_generic(orders_df, 'COMPLETE').count()
    assert filtered_count == 22900

#filter pending order status
@pytest.mark.skip()
def test_order_status_pending(spark):
    orders_df= read_orders(spark, 'LOCAL')
    filtered_count= filter_orders_generic(orders_df, 'PENDING_PAYMENT').count()
    assert filtered_count == 15030

#generic order status filter
@pytest.mark.latest()
@pytest.mark.parametrize("status, count", [("COMPLETE", 22900), ("PENDING_PAYMENT", 15031)])

def test_order_status_generic(spark, status, count):
    orders_df= read_orders(spark, 'LOCAL')
    filtered_count= filter_orders_generic(orders_df, status).count()
    assert filtered_count == count
