# Import python packages
import streamlit as st
import pandas as pd
from snowflake.snowpark.context import get_active_session

# Write directly to the app
st.title(":open_file_folder: HL7 Data Loader :floppy_disk: ")
st.write(
    """Copy and Paste 'D-' numbers from .csv output from the completeness report 
\n
    https://tableau.dna.corp.adaptivebiotech.com/#/views/ClinicalCompletenessReport/OrdersnotinQuadaxDetail?:iid=1
    """
)

def string_to_rows(df, column_name):
    """Converts items in a string column to separate rows in a DataFrame."""
    
    # Split the string into a list of items
    df[column_name] = df[column_name].str.split(',')

    # Explode the list into separate rows
    df = df.explode(column_name)

    # Reset the index
    df.reset_index(drop=True, inplace=True)

    return df


# Unsupported component error: We removed the component, st.file_uploader, 
# that appeared here in keeping with our security policy.

# uploaded_file = st.file_uploader("Choose a file", type = ["csv"])
# if uploaded_file is not None:
#     # convert file to pandas dataframe
#     df = pd.read_csv(uploaded_file)

#     st.write("Uploaded Data: ")
#     st.write(df)


# enter d-numbers

orders = st.text_area("Enter Orders Here: ")

if orders:
    st.write(orders)

# give user a chance to verify data

# rerun OMG data
# vw_omg_order_list

# Create a DataFrame
# df = pd.DataFrame({'ORDER_NUMBER': [orders]})

# # Convert items in the 'col1' column to separate rows
# df = string_to_rows(df, 'ORDER_NUMBER')

# st.write(df)
    # format the string of orders to something that will work in SQL query
    formatted_orders = ", ".join(f"'{order.strip()}'" for order in orders.split(","))

    # add orders to query
    query = f"""select ord.orderdimkey
    , ord.cora_order_id
    , ord.order_number
    , ord.order_name
    , ord.cora_order_created_date_pt
    , ord.order_status_type
    , ord.order_billing_type
    from modeled.edm.order_dim ord
    where ord.order_number in ({formatted_orders});"""

    # activate Snowflake Session
    session = get_active_session()

    # query db and output results to df
    result = session.sql(query).to_pandas()

    # write results to user to verify information
    st.write(result)

    # button for clicking to push changes through when ready
    clicked = st.button("Click me")
