import pandas as pd
import pandas_profiling
import streamlit as st
from streamlit_pandas_profiling import st_profile_report
from pandas_profiling import ProfileReport


df = pd.read_csv("/Users/diakite/Downloads/train.csv")

profile = ProfileReport(df, title="Train Data House Price")   

st.title("Pandas Profiling in Streamlit!")
st.write(df)
st_profile_report(profile)

