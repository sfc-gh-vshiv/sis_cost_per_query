# Import python packages
import streamlit as st
from snowflake.snowpark.context import get_active_session
from snowflake.snowpark.functions import avg as avg_, sum as sum_, col, lit, datediff, dateadd, date_trunc
from snowflake.snowpark.types import StringType, IntegerType, DateType, FloatType
import datetime
import snowflake.snowpark as sp

# # Get the current credentials
session = get_active_session()

try:
    st.set_page_config(
        page_icon="❄️",
        layout="wide",
        initial_sidebar_state="expanded",
    )
except:
    pass


st.title(" ❄️ Per Query Cost Attribution")
st.caption("This app shows credit usage per query - \
            based on the [query_attribution_history](https://docs.snowflake.com/sql-reference/account-usage/query_attribution_history) view")
st.write("***")

# Set up the layout
date_row = st.columns(2)
top_row = st.columns(3)
bottom_row = st.container()

# Date input
try:
    with date_row[0]:
        start_date, end_date = st.date_input(\
                        'Start Date → End Date :',
                        value=[datetime.date.today() + datetime.timedelta(days=-31), datetime.date.today()],
                        max_value=datetime.date.today()
                    )
        if start_date < end_date:
            pass
        else:
            st.error('Error: End date must fall after start date.')
except:
    st.error("Please select an end date")
    st.stop()

# Account metrics - Credits, # queries and storage

with st.spinner("Getting account metrics.."):
    with top_row[0]:
        # The SQL way
        try:    
            query = f"""SELECT SUM(CREDITS_USED)::FLOAT AS CREDITS 
                        FROM SNOWFLAKE.ACCOUNT_USAGE.METERING_HISTORY 
                        WHERE START_TIME BETWEEN '{start_date}' AND '{end_date}'"""
            df = session.sql(query).to_pandas()
            st.metric('Credits Used','{:,.2f}'.format( df['CREDITS'][0]))
        except Exception as e:
            st.warning(e)
    
    with top_row[1]:
        # The Snowpark way
        try:
            query_history = session.table('SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY')
            query_history = query_history.filter(col("START_TIME").between(start_date, end_date))
            query_history = query_history.count()
            st.metric('Total # Jobs Executed', '{:,.0f}'.format(query_history))
        except Exception as e:
            st.warning(e)
    
    with top_row[2]:
        try:
            storage_usage = session.table('SNOWFLAKE.ACCOUNT_USAGE.STORAGE_USAGE')
            storage_usage = storage_usage.select(avg_((col('STORAGE_BYTES')+col('STAGE_BYTES')+col('FAILSAFE_BYTES'))/(1024*1024*1024*1024)).alias('BILLABLE_TB'))
            storage_usage = storage_usage.filter(col("USAGE_DATE").between(start_date, end_date))
            storage_usage = storage_usage.to_pandas()
            st.metric('Current Storage (TB)', '{:,.3f}'.format(storage_usage['BILLABLE_TB'][0]))
        except Exception as e:
            st.warning(e)

st.write("***")
try:
    # Back to the SQL way
    st.header('Cost per query')
    per_query_cost_sql = f"""
    select 
        qath.query_id,
        qath.credits_attributed_compute,
        qath.start_time,
        qath.end_time,
        qh.query_type,
        qh.database_name || '.' || qh.schema_name as schema,
        qh.user_name || ' (' || qh.role_name || ')' as role_user,
        qh.execution_status,
        vshiv_db.utils.pretty_print_duration(qh.total_elapsed_time) total_elapsed_time,
        qh.rows_produced,
        qath.warehouse_name,
        qh.query_text
    from snowflake.account_usage.query_attribution_history qath
    inner join snowflake.account_usage.query_history qh
        on qath.query_id = qh.query_id
    where qath.start_time::date >= '{start_date}' and qath.end_time::date <= '{end_date}'
    order by credits_attributed_compute desc
    """
    with st.spinner("Getting per query cost.."):
        per_query_cost_df = session.sql(per_query_cost_sql).toPandas()

    st.dataframe(per_query_cost_df)

except Exception as e:
    st.warning(e)



# Credit Usage by Warehouse
with st.spinner("Getting warehouse usage.."):
    try:
        st.write("***")
        st.header('Credit Usage by Warehouse')
        warehouse_metering_history = session.table('SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY').select(col('WAREHOUSE_NAME'),col('CREDITS_USED') ).filter(col("START_TIME").between(start_date, end_date)).group_by("WAREHOUSE_NAME").agg(sum_('CREDITS_USED').cast(FloatType()).alias('TOTAL_CREDITS_USED')).sort('TOTAL_CREDITS_USED', ascending=False).to_pandas()    
        st.vega_lite_chart(warehouse_metering_history,{
            'mark': 'bar',
            'encoding': {
                'x': { "aggregate": "sum", 'field': 'TOTAL_CREDITS_USED'},
                'y': {'field': 'WAREHOUSE_NAME', "sort": "-x"}
            },
        }, use_container_width=True)
    except Exception as e:
        st.warning(e)

