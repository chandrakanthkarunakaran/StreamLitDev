# Packages

#regionName="us-east-1"

import streamlit as st
import json
import pandas as pd
from st_aggrid import  AgGrid, GridUpdateMode, DataReturnMode
from st_aggrid.grid_options_builder import GridOptionsBuilder
from boto3 import client
from common_functions import *
from PIL import Image
import matplotlib.pyplot as plt
import plotly.graph_objects as go


athena=client("athena",region_name=regionName)
s3=client("s3",region_name=regionName)
tenantID="DM00005"



# Functions

st.set_page_config(layout="wide")

st.title("DataTwin Dashboards")

@st.cache
def convert_df(df):
    # IMPORTANT: Cache the conversion to prevent computation on every rerun
    return df.to_csv(index=False).encode('utf-8')

@st.cache
def DataLoad(tableName):

    q="select * from "+tableName.lower()

    print(q)

    sourceTable=SQLRun(tenantID,queryFile=None,writeTo="AthenaResults/",mode="Append",
    writeFormat=".csv",returnResp=True,qText=q)

    sourceTable=sourceTable[[x for x in sourceTable.columns if x not in ["checksum","transactionid","input","output","pk","sk"]]]

    return sourceTable



# # List Of Tables

tableInfo=athena.list_table_metadata(CatalogName='AwsDataCatalog',
                        DatabaseName=tenantID,Expression='')

tables=[x["Name"].upper() for x in tableInfo["TableMetadataList"]]

if "TableSelected" not in st.session_state.keys():

    st.session_state["TableSelected"]=tables[0]

if "Data" not in st.session_state.keys():

    st.session_state["Data"]=DataLoad(tables[0]) 


with st.sidebar:

    image = Image.open('OI.jpg')

    st.image(image, caption='Connecting the dots')

    tableSelected=st.selectbox(label="Select A Table",options=tables) 

    if "TableSelected" in st.session_state.keys() and st.session_state["TableSelected"] !=tableSelected:

        st.session_state["Data"]=DataLoad(tableSelected) 
    
    

    #if "TableSelec"   

    st.session_state["TableSelected"]=tableSelected

    print(tableSelected)



print(st.session_state)

# Grid Option Builder

print("Line Before")

gb=GridOptionsBuilder.from_dataframe(st.session_state["Data"])

gb.configure_pagination(enabled=True)

gb.configure_default_column(groupable=True,editable=True)

gb.configure_selection(selection_mode="multiple",use_checkbox=True,header_checkbox=True)

gridOptions=gb.build()

resp=AgGrid(st.session_state["Data"],gridOptions=gridOptions,height=500,width="100%",update_mode=GridUpdateMode.SELECTION_CHANGED)

print("Sel:",len(resp["selected_rows"]))

v = resp['selected_rows']

if v:
    st.write('Selected ROws')
    dfs = pd.DataFrame(v)
    st.dataframe(dfs[[x for x in dfs.columns if x !="_selectedRowNodeInfo"]])
    csv = convert_df(dfs)

    st.download_button(
        label="Download Selected Rows As CSV",
        data=csv,
        file_name=st.session_state["TableSelected"]+str(pd.Timestamp.now().value)+'.csv',
        mime='text/csv',
    )


if len(resp["selected_rows"])==0:

    contentChart=st.session_state["Data"]

else:

    contentChart=pd.DataFrame(resp["selected_rows"])

    contentChart=contentChart[[x for x in contentChart.columns if x !="_selectedRowNodeInfo"]]

dTypeDF=dict(contentChart.dtypes)

xAxisL=[]
yAxizL=[]

for key,val in dTypeDF.items():

    if "int" in str(val) or "float" in str(val):

        yAxizL.append(key)
    
    else:

        xAxisL.append(key)

xAxisSelected=st.selectbox("X-Axis",xAxisL)

yAxisSelected=st.multiselect("Y-Axis",yAxizL,default=yAxizL[0])


if xAxisSelected:

    print(contentChart)

    print("X:",xAxisSelected)
    # contentChart[xAxisSelected]

    print("Y:",yAxisSelected)
    # print(contentChart[yAxisSelected])


    contentChart[xAxisSelected]=contentChart[xAxisSelected].fillna(value="NULL")

    contDF=pd.pivot_table(contentChart,values=yAxisSelected,index=xAxisSelected,aggfunc=sum)

    contDF=contDF.reset_index()

    print("ContDF:",contDF)

    st.header("Bar Chart")

    st.bar_chart(contDF,x=xAxisSelected,y=yAxisSelected)

    st.header("Spider Chart")

    categories = [*yAxisSelected, yAxisSelected[0]]

    data=contDF[yAxisSelected].to_dict(orient="records")

    print(data)

    dataL2=[list(x.values())+[list(x.values())[0]] for x in data]

    dataL3=[go.Scatterpolar(r=dataL2[i], theta=categories, fill='toself', 
        name=contDF[xAxisSelected][i]) for i in range(len(dataL2))]
    
    fig = go.Figure(
        data=dataL3,
        layout=go.Layout(
            template='plotly_dark',
            title=go.layout.Title(text='Chart'),
            polar={'radialaxis': {'visible': True}},
            showlegend=True
        )
    )

    st.write(fig)










