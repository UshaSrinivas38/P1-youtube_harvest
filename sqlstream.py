import streamlit as st
import pandas as pd
from sqlalchemy import create_engine

# Connect to the MySQL database using SQLAlchemy
engine = create_engine('mysql+mysqlconnector://root:1234@localhost/ytubedb')

# Execute SQL query and fetch the data
query = "SELECT * FROM channel_data"

df = pd.read_sql_query(query, engine)

# Close the database connection
engine.dispose()

# Display the data in Streamlit
st.dataframe(df)
