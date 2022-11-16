# Packages

from py2neo import Graph
import boto3
import pandas as pd
import io
from copy import deepcopy
import hashlib
import regex as re
import networkx as nx
import random
import string
from itertools import combinations
from boto3 import client
from config import *
import json
import time

# Configs

athena=client("athena",region_name=regionName)

dynamoDB=client("dynamodb",region_name=regionName)

s3=client("s3",region_name=regionName)

stepFunc=boto3.client("stepfunctions",region_name=regionName)

#bucketName="csvoistorage"

neo4j=Graph("bolt://%s:7687"%ec2IP,password="0ne1ntegral")

# Utilities

def AlphaNumericID():
    "Generates an 8 digit random alphanumeric identification code"
    chars=string.ascii_uppercase
    alpha="".join(random.sample(chars,4))
    numeric=str(random.randint(1000,9000))
    alpNumID=alpha+numeric
    alpNumID=list(alpNumID)
    random.shuffle(alpNumID)    
    alpNumID="".join(alpNumID)    
    return alpNumID;

def CheckSumScalar(data,cols):

    'runs checksum on dataframe scalar'

    dataOp=deepcopy(data[cols])

    dataOp.fillna(value='',inplace=True)

    dataOp=dataOp.astype(str)

    checkSum=list(dataOp.apply(lambda x: hashlib.md5("".join(list(x.values)).encode('utf-8')).hexdigest(),axis=1))

    return checkSum

def CheckSum(data,cols):

    "returns checksum column from the data."

    checkSumL=[]

    print("data columns:",data.columns)

    for i in list(data.index):

        cStr=""

        for col in cols:

            print('COlumn:',col)

            colVal=data[col][i]

            print('ColVal:',colVal)

            if pd.isnull(colVal)==True:

                colVal=""
            
            print("Cstr:",cStr)
            
            cStr+=str(colVal)
        
        checkSumStr=hashlib.md5(cStr.encode('utf-8')).hexdigest()

        # print("col val:",cStr)

        # print("Check Sum:",checkSumStr)

        checkSumL.append(checkSumStr)
    
    return checkSumL


# S3 Functions

def KeysS3(bucket_name, prefix='/', delimiter='/'):
    prefix = prefix[1:] if prefix.startswith(delimiter) else prefix
    bucket = boto3.resource('s3').Bucket(bucket_name)
    return (_.key for _ in bucket.objects.filter(Prefix=prefix))



def ReadS3(key,bucket,readTxtTsv=False):

    "read s3 file and return dataframe based on the parameter passed."    

    try:  

        file=s3.get_object(Bucket=bucket,Key=key)

        if ".json" not in key:

            ioObj=io.BytesIO(file['Body'].read())

            #print(ioObj)

        else:

            ioObj=file['Body'].read().decode('utf-8')

            # Special Condition For Json-serde format

            if ioObj[0]!="[" and ioObj[-1]!="]":

                ioObj="["+ioObj+"]"
                
                ioObj=ioObj.replace("\n",",") 

        if ".csv" in key:  

            fileDF=pd.read_csv(ioObj, encoding='utf-8')
        
        elif '.tsv' in key or readTxtTsv==True:

            fileDF=pd.read_csv(ioObj, encoding='utf-8',sep="\t")
        
        elif ".xl" in key:

            if key[-4:]=='.xls':

                engine='xlrd'
            
            else:

                engine='openpyxl'

            fileDF=pd.read_excel(ioObj,engine=engine)
        
        elif ".json" in key:

            fileDF=pd.read_json(ioObj,orient='records')
        
        elif ".txt" in key:

            fileDF=ioObj.read().decode("utf-8")
                
    except UnicodeDecodeError:

        file=s3.get_object(Bucket=bucket,Key=key)

        if ".json" not in key:

            ioObj=io.BytesIO(file['Body'].read())

            print(ioObj)

        else:

            # Special Condition For Json-serde format

            ioObj=file['Body'].read().decode('ISO-8859-1')

            if ioObj[0]!="[" and ioObj[-1]!="]":

                ioObj="["+ioObj+"]"
                
                ioObj=ioObj.replace("\n",",")   

        if ".csv" in key:  

            fileDF=pd.read_csv(ioObj, encoding='ISO-8859-1')
        
        elif '.tsv' in key or readTxtTsv==True:

            fileDF=pd.read_csv(ioObj, encoding='ISO-8859-1',sep="\t")
        
        elif ".xl" in key:

            if key[-4:]=='.xls':

                engine='xlrd'
            
            else:

                engine='openpyxl'

            fileDF=pd.read_excel(ioObj,engine=engine)
        
        elif ".json" in key:

            fileDF=pd.read_json(ioObj,orient='records',encoding='ISO-8859-1') 

        elif ".txt" in key:

            fileDF=ioObj.read().decode('ISO-8859-1')
    
    if ".txt" in key and readTxtTsv ==False:

        return fileDF
    
    else:

        fileDF.rename(columns=lambda x: x.strip(),inplace=True)

        fileDF=fileDF.dropna(subset=fileDF.columns,how='all')       

        return fileDF

def WriteToS3(writeTo,data,bucketName,writeTsvTxt=False):

    "writes file as specified in key"

    in_memory_fp = io.BytesIO()

    dateCols=[x for x in ['InPrdFrom','InPrdTo',"ForPrdFrom","ForPrdTo"] if x in data.columns]

    # for col in dateCols:

    #     if "To" in col:

    #         replVal='2100-12-31'
        
    #     else:

    #         replVal='2001-01-01'

    #     data[dateCols].fillna(value=replVal,inplace=True)

    #     data[col]=data[col].astype(str)

    if ".csv" in writeTo:

        objWrite=data.to_csv(index=False)
    
    elif ".tsv" in writeTo or writeTsvTxt==True:

        objWrite=data.to_csv(index=False,sep="\t")
    
    elif ".json" in writeTo:

        objWrite=data.to_json(orient='records')

        objWrite=objWrite.replace("[","").replace("]","").replace(",{","\n{")
    
    elif ".xl" in writeTo:

        data.to_excel(in_memory_fp,index=False) 

        in_memory_fp.seek(0,0) 

        objWrite=in_memory_fp.read() 


    respWrite=s3.put_object(Body=objWrite,Bucket=bucketName, Key=writeTo)

    return {"Status":"File Uploaded SuccessFully"}

def DeleteS3Obj(key,bucketName):

    "deletes s3 object using key."

    resp=s3.delete_object(Bucket=bucketName,Key=key)

    return resp


# Dynamodb Functions

def Object2DynamoJson(object,operation):

    "converts object into dynamodb json"

    dynJson={}

    if operation in ["PUT","ADD"]:

        for key,val in object.items():

            if type(val) in [int,float]:

                value={"Value":{"N":str(val)},"Action":operation}
            
            elif type(val)==str:

                value={"Value":{"S":str(val)},"Action":"PUT"}
            
            elif type(val) in [dict,list]:

                value={"Value":{"S":str(val)},"Action":"PUT"}
            
            elif pd.isnull(val)==True:

                value={"Value":{"NULL":True},"Action":"PUT"}
            
            dynJson[key]=value

    elif operation=='CREATE':

        for k,v in object.items():

            if type(v) in [int,float]:

                val={"N":str(v)}

            elif type(v)==str:

                val={"S":str(v)}
            
            dynJson[k]=val
    
    return dynJson

def DynamoLogger(txnID,process,tableName,operation,attributes={}):

    "DynamoDB Log Interface"

    # Partition and Sort Keys

    print()

    if process=="PDM":

        pk="TRANSACTION#PDM"
        sk="#PDM#TRANS#"+txnID
    
    elif process=="ETDS":

        pk="TRANSACTION#ETDS"

        sk="ETDS#TRANS#"+txnID

    
    else:

        pk="TRANSACTION"

        sk="#TRANS#"+txnID
    
    # Check Whether A Transaction Is Present In DataBase

    if operation=="StatusCheck":

        resp=dynamoDB.get_item(TableName=tableName,
        Key={"PK":{"S":pk},"SK":{"S":sk}})
    
    # Update Attributes To The DynamoDB Record
    
    elif operation in ["PUT","ADD"]:

        updateJson=Object2DynamoJson(object=attributes,operation=operation)

        resp=dynamoDB.update_item(TableName=tableName,
        Key={"PK":{"S":pk},"SK":{"S":sk}},
        AttributeUpdates=updateJson)
    
    # Create Record

    elif operation=='CREATE':

        attributes["PK"]=pk

        attributes["SK"]=sk   

        item=Object2DynamoJson(object=attributes,operation=operation)             
        
        resp=dynamoDB.put_item(TableName=tableName,
        Item=item)
    

    return resp

def DynamoJsonToDF(jsonD,colmns):

    "converts dynamo json to dataframe."

    dateCols=['ForPrdFrom',"ForPrdTo","InPrdFrom","InPrdTo","EventDt",
    'ReportDate','Date',"ShipmentUpdateDateTime"]

    tableDict={x:[] for x in colmns}

    for item in jsonD:

        for col in list(tableDict.keys()):

            if col in item.keys():

                dataType=list(item[col].keys())[0]

                if dataType=="S":

                    value=item[col]['S'].strip(" ").replace("\n","")

                elif dataType=='N':

                    if col in dateCols:

                        value=pd.Timestamp(int(item[col]["N"]))

                    else:

                        value=float(item[col]["N"])
                else:

                    value=None

                tableDict[col].append(value)

            else:

                tableDict[col].append(None)
    
    tableDF=pd.DataFrame(tableDict)

    if len(tableDF)==0:

        for dateCol in dateCols:

            if dateCol in tableDF.columns:

                tableDF[dateCol]=pd.to_datetime(tableDF[dateCol])    
        
    return tableDF;

def QueryDB(tableName,pk,sk,attributes,qFilter,lastEvalKey):

    "function which queries dynamodb"

    # print("Partition Key:",pk)
    # print("Sort Key:",sk)
    # print("QueryFilter:",qFilter)
    # print('TableName:',tableName)
    # print("Region:",regionName)

    # print(dynamoDB)

    if lastEvalKey is None:
            
        respProcess=dynamoDB.query(TableName=tableName,AttributesToGet=attributes,
                                KeyConditions={"PK":{"AttributeValueList":[{"S":pk}],"ComparisonOperator":"EQ"},
                                "SK":{'AttributeValueList':[{"S":sk}],"ComparisonOperator":"BEGINS_WITH"}},
                                QueryFilter=qFilter)
            
            
    else:
        respProcess=dynamoDB.query(TableName=tableName,AttributesToGet=attributes,
                                KeyConditions={"PK":{"AttributeValueList":[{"S":pk}],"ComparisonOperator":"EQ"},
                                    "SK":{'AttributeValueList':[{"S":sk}],"ComparisonOperator":"BEGINS_WITH"}},
                                QueryFilter=qFilter,ExclusiveStartKey=lastEvalKey)

    
    if "LastEvaluatedKey" in respProcess.keys():
        lastEvalKey=respProcess["LastEvaluatedKey"]
    else:
        lastEvalKey=None
    
    #print(respProcess["Items"])
    

    return respProcess["Items"],lastEvalKey

def DFtoDynamoJson(data):

    'converts dataframe into dynamoDB compatible Json.'

    records=[]

    dateRX=re.compile(r'\d\d\d\d-\d\d-\d\d')

    for id,row in data.iterrows():

        obj={}

        for key,val in row.items():

            if pd.isnull(val)==True:

                obj[key]={"NULL":True}

            elif type(val)==int or type(val)==float:

                obj[key]={"N":str(val)}
            
            elif type(val)==pd.Timestamp:

                obj[key]={"N":str(val.val)}
            
            elif dateRX.search(val) is not None:

                obj[key]={"N":str(pd.Timestamp(val))}
                    
            else:

                obj[key]={"S":str(val)}
        
        objAppend={"PutRequest":{"Item":obj}}
        
        records.append(objAppend)
    
    return records


def WriteToDynamoDB(file,tableName):

    "writes the records from files to dynamoDB"

    size=20

    batchFiles=[file.loc[i:i+size-1,:] for i in range(0, len(file),size)]

    for batchFile in batchFiles:

        dynamoDBJson=DFtoDynamoJson(batchFile)

        dynamoDB.batch_write_item(RequestItems={tableName:dynamoDBJson})
    

    return {"Status":"Successfully Updated To DynamoDB"}
    

# Graph FUnctions

def ATTNSTATUS(timeInVals,timeOutVals):

    "Attendance Status Derivation."

    status=[]

    for i in range(len(timeInVals)):

        timeIn=timeInVals[i]

        timeOut=timeOutVals[i]

        if pd.isnull(timeOut)==False and timeOut !="null":

            status.append("OUT")

        elif (pd.isnull(timeOut)==True or timeOut=='null') and (pd.isnull(timeIn)==False and timeIn!='null'):

            status.append("IN")
        
        else:

            status.append("ATTENDANCE RECORD NOT FOUND")
    

    return status


def DerivationGP(productName,nodesInfo,tenantID,processCode,data):

    "Graph Processing Derivation"

    # DERIVATION INFO

    deriveInfo=neo4j.run("match (a:%s{process_code:$x})-[{derive_type:\"NODE\"}]->(b:%s{process_code:$x}) \
    where  NOT b.domain IN ['PK','SK'] AND b.derive IS NOT NULL\
    return a.short_text as NODE_SRT,a.name as NODE, a.data_type as DATATYPE,\
    b.derive as DERIVE,b.name as PARENT,b.short_text as PARENT_SRT ;"%(productName,productName,),
    x=processCode).to_data_frame()

    if len(deriveInfo)==0:

        deriveInfo=neo4j.run("match (a:%s{process_code:$x})-[{derive_type:\"NODE\"}]->(b:%s{process_code:$x}) \
        where  NOT b.domain IN ['PK','SK'] AND b.derive IS NOT NULL\
        return a.short_text as NODE_SRT,a.name as NODE, a.data_type as DATATYPE,\
        b.derive as DERIVE,b.name as PARENT,b.short_text as PARENT_SRT ;"%(tenantID,tenantID),
        x=processCode).to_data_frame()



    if len(deriveInfo)==0:

        return data

    
    dataDerive=deepcopy(data)

    # Data Rename For Derivation

    mapper={nodesInfo["FIELD"][i]:nodesInfo["NAME"][i] 
    for i in list(nodesInfo.index) if nodesInfo["FIELD"][i] in dataDerive.columns}

    mapperRev={v:u for u,v in mapper.items()}

    print(mapperRev)

    dataDerive.rename(columns=mapper)

    print(dataDerive.columns)

    # Derivation Seq

    nodesDerive=SeqDerived(deriveInfo)

    print("ND:",nodesDerive)

    # Expression Derivation

    deriveExp={deriveInfo["PARENT"][i]:deriveInfo['DERIVE'][i] for i in list(deriveInfo.index)}

    print("DE:",deriveExp)

    nodesInput=list(nodesInfo["NAME"])

    nodesInput.sort(key=len,reverse=True)

    for node in nodesDerive:

        formula=deriveExp[node]

        if node in formula:

            formula=formula.replace(node,"")

        for nodeD in nodesInput:

            if nodeD in mapperRev.keys():

                formula=formula.replace(nodeD,"dataDerive[\"%s\"]"%mapperRev[nodeD])
        

        print(node)

        print(formula)        

        data[mapperRev[node]]=eval(formula)       
    

    return data

def DeriveNodes(nodes,deriveInfo):

    "return list derive nodes derive from nodes passed."

    nodesDerive=list(deriveInfo.loc[deriveInfo["NODE"].isin(nodes),"PARENT"])

    return nodesDerive

def SeqDerived(deriveInfo):

    "returns derivation sequence from nodesInfo."

    print(deriveInfo)

    deriveNodes=[]

    inpNodes=list(deriveInfo.loc[~deriveInfo["NODE"].isin(deriveInfo["PARENT"]),"NODE"])

    inpNodes=DeriveNodes(nodes=inpNodes,deriveInfo=deriveInfo)

    deriveNodes+=inpNodes

    deriveNodes=[deriveNodes[x] for x in range(len(deriveNodes)) if deriveNodes[x] not in deriveNodes[x+1:]]

    return deriveNodes


def WriteMultiFiles(files,fileKeys,bucketName,operation='update',dropPrevTrans=False):

    'writes multiple files to s3 folder'

    i=0

    for file in files:

        file.columns=[x.lower() for x in file.columns]

        key=fileKeys[i]

        if "output" in file.columns and list(set(file['output']))[0]!=key:

            file['input']=[x for x in list(deepcopy(file['output']))]

            file['output']=key

        if key in list(KeysS3(bucket_name=bucketName,prefix=key)) and operation=='append':

            oldFile=ReadS3(key,bucketName)

            if dropPrevTrans==True and "transactionid" in list(file.columns):

                currentTrans=list(set(file['transactionid']))

                oldFile=oldFile.loc[~oldFile['transactionid'].isin(currentTrans)]

            file=pd.concat([oldFile,file],ignore_index=True) 

            #file.drop_duplicates(subset=['pk','sk','checksum'],inplace=True)  

        file.replace({"":None},inplace=True)       

        WriteToS3(key,file,bucketName)

        i+=1
    
    return {'Status':"Success"}


def ExecutionLog(requests):

    "Creates Execution Log for the requests passed and stores it in s3."

    # Finding Iterator Parameters

    execLog={}

    iterationLength=0

    iterationKeys=[]

    iterator=None

    for key,val in requests.items():

        if type(val)==list:

            iterator=key

            iterationKeys=list(val[0].keys())

            iterationLength=len(val)
    
    

    # iteration 

    for key in iterationKeys:

        execLog[key]=[]

        for i in range(iterationLength):

            execLog[key].append(requests[iterator][i][key])
    

    # Header

    for key,val in requests.items():

        if key==iterator:

            continue

        else:

            execLog[key]=[val for i in range(iterationLength)]
    

    execLogDF=pd.DataFrame(execLog)
    

    # Execution ID

    execID=CheckSumScalar(data=deepcopy(execLogDF),cols=list(execLogDF.columns))

    execLogDF['RequestID']=execID

    execLogDF['Status']="Yet To Start"

    return execLogDF

def StepFuncRun(arn,inp,waitForResp=True):

    "invokes stepfunction base on arn and input."

    executionName=str(pd.Timestamp.now())[:10].replace('-',"")+str(pd.Timestamp.now().value)

    payload=json.dumps(inp)

    # Invoke The execution    

    respInvoke=stepFunc.start_execution(
                    stateMachineArn=arn,
                    name=executionName,
                    input=payload
                )
    
    execArn=respInvoke['executionArn']

    # Wait if waitForResp ==True


    if waitForResp==True:

        respReceived=False

        while respReceived==False:

            executionStatus=stepFunc.describe_execution(executionArn=execArn)

            if executionStatus['status'] in ['RUNNING']:

                time.sleep(5)
            
            else:

                respReceived=True

                return {"Status":executionStatus['status']}
    
    else:

        return {"Status":"Success"}


def YM(items):

    "Returns Month of the date passed."

    try:

        month=pd.to_datetime(items).dt.to_period('M').astype(str).str.replace('-','/').replace("NaT",None)
    
    except Exception:

        month=None
    

    return month


# SQL Run


def SQLRun(tenantID,queryFile,writeTo,mode,writeFormat,qParams={},
nullFill='',returnResp=False,fileName=None,qText=None):    

    'executes query from the queryFile and writes the result to its corresponding folder based on mode.'

    paramRX=re.compile(r'{([a-z0-9_]+)}')

    bucketName='dt-'+tenantID.lower()+suffixS3.lower()

    standardBucket='dt-standard'+suffixS3.lower()

    # Deriving URL from writeTo Key

    writeToURL="s3://%s/%s"%(bucketName,writeTo)

    # Unique Token
    
    token="QRY-%s"%(random.randint(100000000000000000000000000000000000000000,
        9800000000000000000000000000000000000000000000)) 
    
    # Reading query from txt file path passed

    if qText is None:
    
        if queryFile in list(KeysS3(bucket_name=bucketName,prefix=queryFile)):

            query=ReadS3(key=queryFile,bucket=bucketName)
        
        else:

            query=ReadS3(key=queryFile,bucket=standardBucket)
    
    else:

        query=qText

    # Parameterised Query

    params=paramRX.findall(query)

    if len(params)>0:

        print(params)

        print(qParams)

        print(query)

        for parameter in params:

            if parameter not in qParams.keys():

                return {"Status":"Parameters Missing: "+parameter}
        
        query=eval('query.format(%s)'%(",".join([x+'='+"'"+v+"'" for x,v in qParams.items() if x in params])))

    # Running SQL Query in Athena

    print(query)

    execResult=athena.start_query_execution(QueryString=query,
                              ClientRequestToken=token,
                              QueryExecutionContext={
                                  "Database":tenantID.lower(),
                                  "Catalog":"AwsDataCatalog"
                              },
                              ResultConfiguration={
                                  "OutputLocation":writeToURL
                              } )
    
    # Fetching Execution ID from the response    

    execID=execResult["QueryExecutionId"]

    writeKey=writeTo+execID+".csv"

    print(execResult)

    fileUpdated=False

    while fileUpdated==False:

        keys=list(KeysS3(bucket_name=bucketName,prefix=writeKey))

        if len(keys)>1:

            fileUpdated=True
    
    if returnResp==True:

        file=ReadS3(key=writeKey,bucket=bucketName)

        return file

    # Deleting MetaData passed along with query output

    DeleteS3Obj(key=writeTo+"%s.csv.metadata"%execID,bucketName=bucketName)

    # Removing all the primitive query results when mode is 'New'

    if mode=="New":

        keys=list(KeysS3(bucket_name=bucketName,prefix=writeTo))

        deleteObjs=[{"Key":x} for x in keys if execID+".csv" not in x ]

        if len(deleteObjs)>0:
            s3.delete_objects(Bucket=bucketName,Delete={"Objects":deleteObjs})
    
    # Renaming if specific filename is given

    if writeFormat!=".csv":

        ConvertS3File(key=writeKey,format='.csv',bucketName=bucketName,toFormat=writeFormat,nullFill=nullFill)
    


    return {"Status":"File Updated Successfully"}


def ConvertS3File(key,format,bucketName,toFormat,nullFill=''):

    "converts s3 file format."

    if ".txt" in key:

        readTxtTsv=True
    
    else:

        readTxtTsv=False

    file=ReadS3(key,bucketName,readTxtTsv)

    DeleteS3Obj(key,bucketName)

    if toFormat==".tsv" or toFormat=='.txt':

        file=file.to_csv(index=False,sep="\t")
    
    elif toFormat==".csv":

        file=file.to_csv(index=False)
    
    elif toFormat==".json":

        if nullFill is not None:

            file.fillna(value=nullFill,inplace=True)

        file=file.to_json(orient='records')
        
        file=file.replace("[","").replace("]","").replace(",{","\n{")
    
    writeKey=key.replace(format,toFormat)

    respWrite=s3.put_object(Body=file,Bucket=bucketName, Key=writeKey)

    return {"Status":"File Format Changed"}



def QueryFormatsCreateTable(format,processCode,stage,mapFields,folderURI):

    'returns query for table creation.'

    if format=='.csv':

        fieldsL=['`'+x+'`'+" "+v for x,v in mapFields.items()]

        fieldsStr=",\n".join(fieldsL)

        query='''CREATE EXTERNAL TABLE `%s_%s`(
            %s
            )
            ROW FORMAT DELIMITED 
            FIELDS TERMINATED BY ',' 
            STORED AS INPUTFORMAT 
            'org.apache.hadoop.mapred.TextInputFormat' 
            OUTPUTFORMAT 
            'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
            LOCATION
            '%s'
            TBLPROPERTIES (
            'has_encrypted_data'='false', 
            'transient_lastDdlTime'='1655561791')'''%(stage,processCode.lower(),fieldsStr,folderURI)
    
    elif format=='.json':

        fieldsL=['`'+x+'`'+" "+v+" COMMENT 'from deserializer'" for x,v in mapFields.items()]

        fieldsStr=",\n".join(fieldsL)

    #     query='''CREATE EXTERNAL TABLE `%s_%s`(
    #       %s
    #     )
    #   ROW FORMAT SERDE 
    #     'org.apache.hive.hcatalog.data.JsonSerDe' 
    #   STORED AS INPUTFORMAT 
    #     'org.apache.hadoop.mapred.TextInputFormat' 
    #   OUTPUTFORMAT 
    #     'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
    #   LOCATION
    #     '%s'
    #   TBLPROPERTIES (
    #     'has_encrypted_data'='false', 
    #     'transient_lastDdlTime'='1655561940')'''%(stage,processCode.lower(),fieldsStr,folderURI)


        query='''CREATE EXTERNAL TABLE IF NOT EXISTS `%s_%s` (
        %s        
        )        
        ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'        
        WITH SERDEPROPERTIES (        
        'serialization.format' = '1'        
        ) LOCATION '%s'        
        TBLPROPERTIES ('has_encrypted_data'='false');'''%(stage,
        processCode.lower(),fieldsStr,folderURI)
          

    return query 



def CreateAthenaTable(tenantID,ddl,bucketName):

    'creates athena table using ddl'

    writeTemp="AthenaPythonResults/"

    # Deriving URL from writeTo Key

    writeToURL="s3://%s/%s"%(bucketName,writeTemp)

    # Unique Token
    
    token="QRY-%s"%(random.randint(100000000000000000000000000000000000000000,
        9800000000000000000000000000000000000000000000)) 

    # Running SQL Query in Athena

    execResult=athena.start_query_execution(QueryString=ddl,
                              ClientRequestToken=token,
                              QueryExecutionContext={
                                  "Database":tenantID.lower(),
                                  "Catalog":"AwsDataCatalog"
                              },
                              ResultConfiguration={
                                  "OutputLocation":writeToURL
                              } )
    
    return execResult

def TableCreateQuery(neo4j,productCode,processCode,tenantID,format,stage,folderURI,addnlFields={}):

    'returns fields and its datatype from neo4j.'

    # Data Type Mapping

    dataTypeMap={"VARCHAR":"string",'INTEGER':"int",'FLOAT':"float",'DATE':'date',"DATETIME":"timestamp"}

    # Querying Fields From Neo4j    

    query="match (a:%s:%s{process_code:$x}) return a.domain as Field,a.data_type as DataType\
    order by a.domain;"%(productCode,processCode+"_Header")

    nodesInfo=neo4j.run(query,x=processCode).to_data_frame()

    if len(nodesInfo)==0:

        query="match (a:%s:%s{process_code:$x}) return a.domain as Field,a.data_type as DataType\
        order by a.domain;"%(tenantID,processCode+"_Header")

        nodesInfo=neo4j.run(query,x=processCode).to_data_frame()
    
    nodesInfo=nodesInfo.loc[~nodesInfo["Field"].isin(['PK','SK','InternalID','Header',
    'Ver','Version','PartitionKey','SortKey'])]

    # Mapping Fields AND Datatype

    mapFields={row['Field']:row['DataType'] for id,row in nodesInfo.iterrows()}

    defFields={x:"VARCHAR" for x in ['pk', 'sk', 'checksum', 'transactionid',
    'input', 'output'] if x not in [x.lower() for x in mapFields.keys()]}

    mapFields.update(addnlFields)

    if processCode not in ['RM2',"CDEL"]:

        mapFields.update(defFields)

    mapFields={x:dataTypeMap[v] for x,v in mapFields.items()}

    print(mapFields)

    # Query format

    query=QueryFormatsCreateTable(format=format,processCode=processCode,
    stage=stage,mapFields=mapFields,folderURI=folderURI)

    print(query)
    

    return query

def LoggerByLevel(obj,level):

    "logs the objects based on its level"

    print("".ljust(120,"#"))

    for l in range(1,int(level)+1):

        logItems=obj[str(l)]        

        for key,val in logItems.items():

            print(key.ljust(20," ")+":",val)

            print()
    

    print("".ljust(120,"#"))
    print()
    print()




    


