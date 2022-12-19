# Packages

import streamlit as st
import json
import pandas as pd
import numpy as np
from py2neo import Graph
from boto3 import client
from common_functions import *

# COnfigs

st.set_page_config(layout="wide")

# Functions

athena=client("athena",region_name=regionName)

s3=client("s3",region_name=regionName)

neo4j=Graph("bolt://%s:7687"%ec2IP,password="0ne1ntegral")

tenantID="DM00005"

productCode="OneCommerce"

st.title("_DataTwin_- Dashboards")

# List Of Tables

tableInfo=athena.list_table_metadata(CatalogName='AwsDataCatalog',
                        DatabaseName=tenantID,Expression='')

tables=[x["Name"].upper() for x in tableInfo["TableMetadataList"]]


def FetchAthenaTable(tableName):

    "returns athena table queried output"

    q="select * from "+tableSelected.lower()+" limit 100"

    sourceTable=SQLRun(tenantID,queryFile=None,writeTo="AthenaResults/",mode="Append",
    writeFormat=".csv",returnResp=True,qText=q)

    return sourceTable


if "ProcessCode" not in st.session_state.keys():

    st.session_state["ProcessCode"]=tables[0].split(" ")[-1].upper()

    st.session_state["SourceTable"]=FetchAthenaTable(tables[0])

else:

    





with st.sidebar:

    tableSelected=st.selectbox(label="Select a table to view it.",options=tables,)

    if tableSelected:

        processCode=tableSelected.split("_")[-1].upper()

        # Fetch details for the table using neo4j

        processDefnDF=neo4j.run("match (a:%s:%s_Header{process_code:'%s'})\
         return a.short_text as Desc,a.domain as Domain,a.data_type as DataType,\
        a.lde as LDES"%(productCode,processCode,processCode)).to_data_frame()

        mapDmnSrt={processDefnDF["Desc"][i]:str(processDefnDF["Domain"][i]).lower() for i in range(len(processDefnDF))}

        mapSrtDataType={processDefnDF["Desc"][i]:str(processDefnDF["Domain"][i]).lower() for i in range(len(processDefnDF))}

        dataTypeMap={"VARCHAR":str,"INTEGER":int,"FLOAT":float}

        # Table From AThena

        q="select * from "+tableSelected.lower()+" limit 100"

        sourceTable=SQLRun(tenantID,queryFile=None,writeTo="AthenaResults/",mode="Append",
        writeFormat=".csv",returnResp=True,qText=q)

        mapDmnSrtN={u:v for u,v in mapDmnSrt.items() if v.lower() in sourceTable.columns}

        sourceTable=sourceTable[mapDmnSrtN.values()]

        sourceTable.columns=list(mapDmnSrtN.keys())

        # Select Filtering Criteria

        seletioncriterias=st.multiselect(label="Select Fields For Filtering",options=list(sourceTable.columns))

        print(seletioncriterias)

        print(st.session_state)


        












