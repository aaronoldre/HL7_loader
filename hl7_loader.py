# Import python packages
import streamlit as st
import pandas as pd
from snowflake.snowpark.context import get_active_session

# Write directly to the app
st.title(":open_file_folder: HL7 Data Loader :floppy_disk: ")
st.write(
    """Replace this example with your own code!
    **And if you're new to Streamlit,** check
    out our easy-to-follow guides at
    [docs.streamlit.io](https://docs.streamlit.io).
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

orders = st.text_area("Enter Orders Here: ")

st.write(orders)




# Create a DataFrame
df = pd.DataFrame({'ORDER_NUMBER': [orders]})

# Convert items in the 'col1' column to separate rows
df = string_to_rows(df, 'ORDER_NUMBER')

st.write(df)
