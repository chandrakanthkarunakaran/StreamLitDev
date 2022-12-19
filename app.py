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

# DisplayTableFunc

def DisplayTable(sourceTable,mapDmnSrt,filters):

    "diplays table based "

    filters=list(set(filters))

    print("Filters:",filters)

    defFilterVals={x:list(set(sourceTable[mapDmnSrt[x]])) for x in filters if mapDmnSrt[x] in sourceTable.columns} 

    print("Def Filters:",defFilterVals)   

    filteredData=sourceTable

    filtersCols=[]

    filterGroups=[filters[n:n+3] for n in range(0, len(filters), 3)]

    filterVals={x:[] for x in filters}    

    for filterGrp in filterGroups:

        filtCols=st.columns(3,gap="large")        

        index=0

        for col in filtCols:

            fieldFilter=filterGrp[index]

            with col:

                filterVals[fieldFilter]=st.multiselect(label=fieldFilter,options=["None"]+defFilterVals[fieldFilter],
                key=fieldFilter)
            
            index+=1    

    for key,val in filterVals.items():

        if len(val)>0:

            filteredData=filteredData.loc[filteredData[mapDmnSrt[key]].isin(val)]
        
    return filteredData

    
# Create App Title

st.title("_DataTwin_- Backend Tables",)



tableInfo=athena.list_table_metadata(CatalogName='AwsDataCatalog',
                        DatabaseName=tenantID,Expression='')

tables=[x["Name"].upper() for x in tableInfo["TableMetadataList"]]

tableSelected=None

with st.sidebar:

    for table in tables:

        if st.button(table):

            tableSelected=table


if tableSelected is not None:

    processCode=tableSelected.split("_")[-1].upper()

    # Fetch details for the table using neo4j

    # processDesc=neo4j.run("match (a:PROCESS:%s{process_code:'%s'})\
    #      return a.short_text as Desc"%(tenantID,processCode)).data()

    # st.header(tableSelected+"->"+processDesc[0]["Desc"])

    # # Process Definition

    # print("ProcessCode:",processCode)

    # print("ProductCode:",productCode)

    # processDefnDF=neo4j.run("match (a:%s:%s_Header{process_code:'%s'})\
    #      return a.short_text as Desc,a.domain as Domain,a.data_type as DataType,\
    #     a.lde as LDES"%(productCode,processCode,processCode)).to_data_frame()
    
    # print("ProcessDefn:",processDefnDF)
    
    # mapDmnSrt={processDefnDF["Desc"][i]:str(processDefnDF["Domain"][i]).lower() for i in range(len(processDefnDF))}
    
    # # Keys -PK

    # keysPK=neo4j.run("match (a:%s{name:'%s_PK'})<--(b)\
    #      return collect(b.short_text) as k1"%(productCode,processCode)).data()
    
    # # Keys -SK
    
    # keysSK=neo4j.run("match (a:%s{name:'%s_SK'})<--(b)\
    #      return collect(b.short_text) as k2"%(productCode,processCode)).data()
    
    # keys=keysPK[0]["k1"]+keysSK[0]["k2"]    
    
    # # LDE Keys

    # ldeFrame=processDefnDF.loc[(processDefnDF["LDES"].notna())&(processDefnDF["LDES"]!="null")]

    # ldeFrame=ldeFrame[["Desc","LDES"]].sort_values(by=["LDES"])

    # ldes=list(ldeFrame["Desc"])

    # keys=[x for x in keys if x not in ldes+["Version","Ver"]]

    # Source Data

    q="select * from "+tableSelected.lower()+" limit 100"

    print(q)

    sourceTable=SQLRun(tenantID,queryFile=None,writeTo="AthenaResults/",mode="Append",
    writeFormat=".csv",returnResp=True,qText=q)

    print("Table:",sourceTable)

    gb = GridOptionsBuilder.from_dataframe(sourceTable)

    gb.configure_pagination(paginationAutoPageSize=True) #Add pagination

    gb.configure_side_bar() #Add a sidebar

#     if "id_row" not in st.session_state:
#         st.session_state["id_row"] = ''
#         selected_rows = []
#     else:
#         selected_rows = (list(range(len(st.session_state["id_row"].get('selectedRows')))))
# #gb.configure_selection(selection_mode="multiple", use_checkbox=True,pre_selected_rows=selected_rows)

    gb.configure_selection('multiple', use_checkbox=False, 
    groupSelectsChildren=True,header_checkbox=True,
    header_checkbox_filtered_only=True) #Enable multi-row selection

    gridOptions = gb.build()

    grid_response = AgGrid(
        sourceTable,
        gridOptions=gridOptions ,
        data_return_mode=DataReturnMode.FILTERED, 
        update_mode=GridUpdateMode.VALUE_CHANGED, 
        fit_columns_on_grid_load=False,
        theme='streamlit', #Add theme color to the table
        enable_enterprise_modules=True,
        height=500, 
        width='100%',
        reload_data=False,
    )



    #print(grid_response)
    

    data = grid_response['data']

    #st.write(data)

    selected = grid_response['selected_rows'] 

    #selected=selected_rows

    if selected:

        df = pd.DataFrame(selected) #Pass the selected rows to a new dataframe df

        st.write(df)

        #print(df)

        @st.cache
        def convert_df(df):
            # IMPORTANT: Cache the conversion to prevent computation on every rerun
            return df.to_csv().encode('utf-8')

        csv = convert_df(df)

        st.download_button(
            label="Download data as CSV",
            data=csv,
            file_name=tableSelected+'.csv',
            mime='text/csv',
        )

        #st.stop()

        # AgGrid(sourceTable.loc[:10,:])

        # Filtering The Data


else:

    st.header("Please Select A Table")

