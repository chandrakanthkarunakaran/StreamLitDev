# Import Packages

import streamlit as st
import json
import pandas as pd
import numpy as np
from py2neo import Graph
from boto3 import client
from common_functions import *
from st_aggrid import GridOptionsBuilder, AgGrid, GridUpdateMode, DataReturnMode

st.set_page_config(layout="wide")

# Connections

athena=client("athena",region_name=regionName)

s3=client("s3",region_name=regionName)

neo4j=Graph("bolt://%s:7687"%ec2IP,password="0ne1ntegral")

tenantID="DM00005"

productCode="OneCommerce"

st.title("_DataTwin_- Backend Tables",)

@st.cache
def convert_df(df):
    # IMPORTANT: Cache the conversion to prevent computation on every rerun
    return df.to_csv(index=False).encode('utf-8')

# Source Data

tableSelected="active_bt3"

processCode=tableSelected.split("_")[-1].upper()

q="select * from "+tableSelected.lower()+" limit 100"

print(q)

sourceTable=SQLRun(tenantID,queryFile=None,writeTo="AthenaResults/",mode="Append",
writeFormat=".csv",returnResp=True,qText=q)

print("Table:",sourceTable)

df=sourceTable

gb = GridOptionsBuilder.from_dataframe(df)
gb.configure_default_column(enablePivot=True, enableValue=True, enableRowGroup=True)
gb.configure_selection(selection_mode="multiple", use_checkbox=True)
gb.configure_side_bar()
gridoptions = gb.build()

response = AgGrid(
    df,
    height=200,
    gridOptions=gridoptions,
    enable_enterprise_modules=True,
    update_mode=GridUpdateMode.MODEL_CHANGED,
    data_return_mode=DataReturnMode.FILTERED_AND_SORTED,
    fit_columns_on_grid_load=False,
    header_checkbox_selection_filtered_only=True,
    use_checkbox=True)

# st.write(type(response))
# st.write(response.keys())

v = response['selected_rows']

if v:
    st.write('Selected rows')
    st.dataframe(v)
    dfs = pd.DataFrame(v)
    csv = convert_df(dfs)

    st.download_button(
        label="Download data as CSV",
        data=csv,
        file_name='selected.csv',
        mime='text/csv',
    )