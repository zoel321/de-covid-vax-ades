
#import functions

import streamlit as st
import pandas as pd

st.title("Covid-19 Vaccine Adverse Event Statistics")

#get dataframe


summary_df = pd.read_pickle('summary_df1.pkl')

manufacturer = st.radio(
     "Which vaccine?",
     ('Pfizer', 'Moderna', 'Janssen', 'Unknown'))

if manufacturer == 'Pfizer':
    st.dataframe(summary_df[summary_df.vax_manu == 'PFIZER\\BIONTECH'])
elif manufacturer == 'Moderna':
    st.dataframe(summary_df[summary_df.vax_manu == 'MODERNA'])
elif manufacturer == 'Janssen':
    st.dataframe(summary_df[summary_df.vax_manu == 'JANSSEN'])
elif manufacturer == 'Unknown':
     st.dataframe(summary_df[summary_df.vax_manu == 'UNKNOWN MANUFACTURER'])

#mvp writing
st.markdown('These are some initial summary statistics for the adverse events of different Covid-19 vaccines. My pipeline will be for data acquisition and preprocessing then visualizing. I plan to make a dashboard with Dash and Plotly Express.')
