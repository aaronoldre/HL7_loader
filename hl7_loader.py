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

uploaded_file = st.file_uploader("Choose a file", type = ["csv"])
if uploaded_file is not None:
    # convert file to pandas dataframe
    df = pd.read_csv(uploaded_file)

    st.write("Uploaded Data: ")
    st.write(df)
