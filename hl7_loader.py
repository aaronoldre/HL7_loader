# Import necessary Python packages
import streamlit as st
import pandas as pd
from io import StringIO
from snowflake.snowpark.context import get_active_session

# Set the title of the Streamlit app
st.title(":open_file_folder: HL7 Data Loader :floppy_disk:")

# Write instructions for the user
st.write(
    """
    Open the Completeness Report: 
    [Tableau Clinical Completeness Report](https://tableau.dna.corp.adaptivebiotech.com/#/views/ClinicalCompletenessReport/OrdersnotinQuadaxDetail?:iid=1)
    \n1. Export the report as a .csv file. 
    \n2. Open the .csv file in a text editor.
    \n3. Copy and paste all the content into the text box below.
    """
)

def generate_bulk_insert_statement(df, table_name):
    """
    Generate a single SQL INSERT statement for all rows in the DataFrame.

    Parameters:
        df (pd.DataFrame): The DataFrame containing the data to be inserted.
        table_name (str): The name of the target Snowflake table.

    Returns:
        str: A single SQL INSERT statement.
    """
    # Specify the target table columns
    target_columns = ['order_number', 'message_type']

    # Create the VALUES portion of the SQL statement
    values = ", ".join(
        f"('{row['Order Number']}', 'Missing DFT'), ('{row['Order Number']}', 'Missing OMG')"
        if row['Missing Charges?'] == 'Missing OMG and DFT' 
        else f"('{row['Order Number']}', '{row['Missing Charges?']}')"
        for _, row in df.iterrows()  
    )

    # Build the complete SQL INSERT statement
    statement = f"INSERT INTO {table_name} ({', '.join(target_columns)}) VALUES {values};"

    return statement

# Initialize Snowflake session
session = get_active_session()

# Query to fetch current data from the HL7 exceptions table
e_query = "SELECT * FROM AI_SANDBOX.PUBLIC.HL7_EXCEPTIONS"

# Load the exceptions table into a Pandas DataFrame
e_df = session.sql(e_query).to_pandas()

# Display current orders in the HL7 exceptions table
if not e_df.empty:
    st.write("Current Orders in the HL7 Table:")
    st.write(e_df)

    # SQL statement to clear the HL7 exceptions table
    my_remove_statement = "TRUNCATE TABLE AI_SANDBOX.PUBLIC.HL7_EXCEPTIONS;"

    # Button to clear the table
    remove = st.button("Clear Table")
    if remove:
        session.sql(my_remove_statement).collect()
        st.success("Orders removed from the table.")
else:
    st.write("Nothing is currently loaded in the HL7 Exceptions Table.")

# Text area for user to input orders
orders = st.text_area("Enter Orders Here:")

if orders:
    try:
        # Read the user input as a tab-separated file
        csv_data = StringIO(orders)
        df = pd.read_csv(csv_data, sep="\t")  # Adjust delimiter if needed

        # Display the DataFrame in the app
        st.write("Parsed DataFrame:")
        st.dataframe(df)
    except Exception as e:
        # Display an error message in case of failure
        st.error(f"Error processing the input: {e}")

    # Generate SQL INSERT statement for the DataFrame
    my_insert_stmt = generate_bulk_insert_statement(df, "AI_SANDBOX.PUBLIC.HL7_EXCEPTIONS")

    # Button to upload data to the Snowflake Data Warehouse
    upload = st.button("Load Data to DW")
    if upload:
        session.sql(my_insert_stmt).collect()
        st.success("Orders successfully loaded to the table.")
