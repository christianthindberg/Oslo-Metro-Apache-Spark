# Databricks notebook source exported at Mon, 21 Mar 2016 13:07:02 UTC
# MAGIC %md
# MAGIC ### Workaround for PySpark Streaming Bug
# MAGIC Code below provided by Databricks in response to Sporveien request 3083

# COMMAND ----------

from pyspark.streaming.util import TransformFunctionSerializer

def loads(self, data):
    self.failure = None
    try:
        f, wrap_func, deserializers = self.serializer.loads(bytes(data))
        if f.__module__ is None:
          f.__module__ = "__main__"
        if wrap_func.__module__ is None:
          wrap_func.__module__ = "__main__"
        return TransformFunction(self.ctx, f, *deserializers).rdd_wrapper(wrap_func)
    except:
        self.failure = traceback.format_exc()

__orig = TransformFunctionSerializer.loads
TransformFunctionSerializer.loads = loads

# COMMAND ----------

# MAGIC %md
# MAGIC ### City of Oslo Passenger Counting
# MAGIC Streaming Files from S3. To be moved to Kinesis.
# MAGIC 
# MAGIC Spark reads the files and
# MAGIC   a) Calculate no of passengers in near real-time embarking/disembarking per station
# MAGIC   b) Store passenger count data to permanent storage for later analysis
# MAGIC   c) Output data to Kinesis for visualization and handling by other systems
# MAGIC   
# MAGIC 
# MAGIC ![PassengerCount](https://upload.wikimedia.org/wikipedia/commons/thumb/1/14/Oslo_T-bane_linjekart.svg/350px-Oslo_T-bane_linjekart.svg.png)

# COMMAND ----------

# MAGIC %md
# MAGIC Import Libraries

# COMMAND ----------

import sys
import boto3
from boto3.session import Session
from random import randint
from pyspark.streaming import StreamingContext
from pyspark.streaming.kinesis import KinesisUtils, InitialPositionInStream, StorageLevel
from pyspark.sql.types import *
from pyspark.sql import *
import json

import re, datetime, time, pytz
from pytz import timezone
import urllib
import xml.etree.ElementTree as ET
import pandas as pd

AWS_BUCKET_NAME = "passasjertellingtbane"
SOURCE = "/mnt/%s/tellefiler" %AWS_BUCKET_NAME
CHECKPOINTDIR = "/mnt/%s/checkpoint06" %AWS_BUCKET_NAME
#DBPATH = "/mnt/%s/passenger_data" %AWS_BUCKET_NAME
KINESIS_STREAM = "InfrastructureTest"
KINESIS_LOGSTREAM = "Infrastructure"
TABLENAME_EVERY = "oslopassengers"
TABLENAME_AGGREGATE = "passengers_aggregate"

batchIntervalSeconds = 20

# COMMAND ----------

# MAGIC %md
# MAGIC ### Get AWS Credentials
# MAGIC The AWS keys themeselves are in a separate notebook. Run the Credentials notebook first
# MAGIC 
# MAGIC Note: this cell can _only_ contain the %run statement, no other code (actually not even comments!)

# COMMAND ----------

# MAGIC %run "Users/admin/Credentials"

# COMMAND ----------

# Function GetCredentials() is available since we have now run the Credentials notebook 
ACCESS_KEY, SECRET_KEY = GetCredentials()

# COMMAND ----------

# MAGIC %md
# MAGIC ###Create Kinesis client
# MAGIC Kinesis client can not be serialized which it has to be in order for checkpointing to work.
# MAGIC The solution is to create a wrapper class for the Kinesis client, and use this whenever we need to reference the Kinesis client.
# MAGIC 
# MAGIC Code below provided by Databricks in response to Sporveien request 3083

# COMMAND ----------

def create_client():
  session = Session(aws_access_key_id=ACCESS_KEY,
                  aws_secret_access_key=SECRET_KEY,
                  region_name='eu-west-1')
  kinesisClient = session.client('kinesis')
  response = kinesisClient.list_streams()
  
  bStreamExists = False
  for streamName in response["StreamNames"]:
    if streamName == KINESIS_STREAM:
      bStreamExists = True
      break
  if not bStreamExists:
    kinesisClient.create_stream(StreamName=KINESIS_STREAM, ShardCount=2)
  return kinesisClient

class PickleWrapper(object):
  def __init__(self, creater):
    self.obj = creater()
    self.creater = creater
  def get(self):
    return self.obj
  def __setstate__(self, e):
    self.creater, = e
    self.obj = self.creater()
  def __getstate__(self):
    return (self.creater,)
  
kinesisClientWrapper = PickleWrapper(create_client)


# COMMAND ----------

# MAGIC %md Ensure Kinesis stream is active

# COMMAND ----------

response = kinesisClientWrapper.get().describe_stream(StreamName=KINESIS_STREAM)
while response["StreamDescription"]["StreamStatus"] == "CREATING":
  time.sleep(10)
  response = kinesisClientWrapper.get().describe_stream(StreamName=KINESIS_STREAM)
print response

# COMMAND ----------

# MAGIC %md Routines to send data and log entries to Kinesis

# COMMAND ----------

"""
To be implemented - optimized sendToKinesis
def sendPartition(iter):
    connection = createNewConnection()
    for record in iter:
        connection.send(record)
    connection.close()

dstream.foreachRDD(lambda rdd: rdd.foreachPartition(sendPartition))
"""

# COMMAND ----------

def SendToKinesis(df, topic, frequency):
  try:
    pandas_df = df.toPandas()  #NB: this operation performs a collect()
  except Exception as p_df:
    LogToKinesis("SendToKinesis -- df.toPandas()", "EXCEPTION", str(p_df))
    return    
  try:
    pandas_json = pandas_df.to_json()
  except Exception as p_js:
    LogToKinesis("SendToKinesis -- df.to_json()", "EXCEPTION", str(p_js))
    return
  try:
    kinesisBlob = json.dumps({"topic": topic, "frequency": frequency, "values": pandas_json})
  except Exception as k_blob:
    LogToKinesis("SendToKinesis -- kblob/json.dumps()", "EXCEPTION", str(k_blob))
    return    
  try:
    kinesisClientWrapper.get().put_record(StreamName=KINESIS_STREAM, Data=kinesisBlob, PartitionKey="partition-key" + str(randint(0,9)))
    #kinesisClient.put_record(StreamName=KINESIS_STREAM, Data=kinesisBlob, PartitionKey="partition-key" + str(randint(0,9)))
  except Exception as kc:
    LogToKinesis("SendToKinesis", "EXCEPTION", str(kc))
    
def LogToKinesis(subject, message, *argv):
  list = []
  data = ""
  if argv is not None:
    for arg in argv:
        list.append(str(arg))
  if len(list) > 0:
    data = ", ".join(list)
  kinesisBlob = json.dumps({"topic": "Log", "frequency": 0, "values": {"subject": subject, "time": datetime.datetime.now(timezone('Europe/Oslo')).strftime('%Y-%m-%d %H:%M:%S'),"message": message, "data": data}})
  kinesisClientWrapper.get().put_record(StreamName=KINESIS_LOGSTREAM, Data=kinesisBlob, PartitionKey="LogOutput")
  
""" TODO: check how to write to Kinesis from Worker, ref. serialization issue and PickleWrapper """
def LogToKinesisFromWorker(KINESIS_LOGSTREAM, subject, message, *argv):
  session = Session(aws_access_key_id=ACCESS_KEY,aws_secret_access_key=SECRET_KEY,region_name='eu-west-1')
  kinesisClient = session.client('kinesis')
  list = []
  data = ""
  if argv is not None:
    for arg in argv:
        list.append(str(arg))
  if len(list) > 0:
    data = ", ".join(list)
  kinesisBlob = json.dumps({"topic": "Log", "frequency": 0, "values": {"subject": subject, "time": datetime.datetime.now(timezone('Europe/Oslo')).strftime('%Y-%m-%d %H:%M:%S'),"message": message, "data": data}})
  kinesisClientWrapper.get().put_record(StreamName=KINESIS_LOGSTREAM, Data=kinesisBlob, PartitionKey="LogOutput")

# COMMAND ----------

# MAGIC %md
# MAGIC Create Schema

# COMMAND ----------

DATE_AND_TIME_UNIX = 8
TOG_NUMBER = 9
OWN_MODULE_NO = 10
COUPLED_MODULE_NO = 11
MODULE_CONFIG = 12
LEADING_OR_GUIDED = 13
TOTAL_BOARDING = 14
TOTAL_ALIGHTING = 15
LINE_NUMBER = 18

# COMMAND ----------

schema = StructType([
    StructField("Year", IntegerType(), True),
    StructField("Month", IntegerType(), True),
    StructField("Day", IntegerType(), True),
    StructField("Weekday", StringType(), True),
    StructField("Hour", IntegerType(), True),
    StructField("Minute", IntegerType(), True),
    StructField("Second", IntegerType(), True),
    StructField("Temperature", FloatType(), True),
    StructField("DateAndTimeUnix", IntegerType(), True),
    StructField("TogNumber", IntegerType(), True),
    StructField("OwnModuleNo", IntegerType(), True),
    StructField("CoupledModuleNo", IntegerType(), True),
    StructField("ModuleConfig", IntegerType(), True),
    StructField("LeadingOrGuided", IntegerType(), True),
    StructField("TotalBoarding", IntegerType(), True),
    StructField("TotalAlighting", IntegerType(), True),
    StructField("CurrentStationID", IntegerType(), True),
    StructField("RouteCodeID", IntegerType(), True),
    StructField("LineNumber", IntegerType(), True),
    StructField("StartStationID", IntegerType(), True),
    StructField("EndStationID", IntegerType(), True),
    StructField("SensorDiagnoseError", IntegerType(), True),
    StructField("DataInvalid", IntegerType(), True),
    StructField("Sensor11Boarding", IntegerType(), True),
    StructField("Sensor11Alighting", IntegerType(), True),
    StructField("Sensor12Boarding", IntegerType(), True),
    StructField("Sensor12Alighting", IntegerType(), True),
    StructField("Sensor13Boarding", IntegerType(), True),
    StructField("Sensor13Alighting", IntegerType(), True),
    StructField("Sensor14Boarding", IntegerType(), True),
    StructField("Sensor14Alighting", IntegerType(), True),
    StructField("Sensor15Boarding", IntegerType(), True),
    StructField("Sensor15Alighting", IntegerType(), True),
    StructField("Sensor16Boarding", IntegerType(), True),
    StructField("Sensor16Alighting", IntegerType(), True),
    StructField("Sensor21Boarding", IntegerType(), True),
    StructField("Sensor21Alighting", IntegerType(), True),
    StructField("Sensor22Boarding", IntegerType(), True),
    StructField("Sensor22Alighting", IntegerType(), True),
    StructField("Sensor23Boarding", IntegerType(), True),
    StructField("Sensor23Alighting", IntegerType(), True),
    StructField("Sensor24Boarding", IntegerType(), True),
    StructField("Sensor24Alighting", IntegerType(), True),
    StructField("Sensor25Boarding", IntegerType(), True),
    StructField("Sensor25Alighting", IntegerType(), True),
    StructField("Sensor26Boarding", IntegerType(), True),
    StructField("Sensor26Alighting", IntegerType(), True),
    StructField("Sensor31Boarding", IntegerType(), True),
    StructField("Sensor31Alighting", IntegerType(), True),
    StructField("Sensor32Boarding", IntegerType(), True),
    StructField("Sensor32Alighting", IntegerType(), True),
    StructField("Sensor33Boarding", IntegerType(), True),
    StructField("Sensor33Alighting", IntegerType(), True),
    StructField("Sensor34Boarding", IntegerType(), True),
    StructField("Sensor34Alighting", IntegerType(), True),
    StructField("Sensor35Boarding", IntegerType(), True),
    StructField("Sensor35Alighting", IntegerType(), True),
    StructField("Sensor36Boarding", IntegerType(), True),
    StructField("Sensor36Alighting", IntegerType(), True),
    StructField("DiagnosisSensor11", IntegerType(), True),
    StructField("DiagnosisSensor12", IntegerType(), True),
    StructField("DiagnosisSensor13", IntegerType(), True),
    StructField("DiagnosisSensor14", IntegerType(), True),
    StructField("DiagnosisSensor15", IntegerType(), True),
    StructField("DiagnosisSensor16", IntegerType(), True),
    StructField("DiagnosisSensor21", IntegerType(), True),
    StructField("DiagnosisSensor22", IntegerType(), True),
    StructField("DiagnosisSensor23", IntegerType(), True),
    StructField("DiagnosisSensor24", IntegerType(), True),
    StructField("DiagnosisSensor25", IntegerType(), True),
    StructField("DiagnosisSensor26", IntegerType(), True),
    StructField("DiagnosisSensor31", IntegerType(), True),
    StructField("DiagnosisSensor32", IntegerType(), True),
    StructField("DiagnosisSensor33", IntegerType(), True),
    StructField("DiagnosisSensor34", IntegerType(), True),
    StructField("DiagnosisSensor35", IntegerType(), True),
    StructField("DiagnosisSensor36", IntegerType(), True)])

# COMMAND ----------

schemaAlightBoard = StructType([StructField("OwnModuleNo", IntegerType(), True), StructField("Alighting", IntegerType(), True),StructField("Boarding", IntegerType(), True)])

# COMMAND ----------

# MAGIC %md Get some meteorological data

# COMMAND ----------

"""
TODO 0: Move this away from Spark!
        Reading from external sources should be done _separately_ from stream, f.ex. as a Nodejs kinesis-producer. And then Spark reads from Kinesis
        Doing it in Spark stream introduce complex error and timeout handling if the external service is slow or unavalable
TODO 1: Incorporate Percipiation into the record
TODO 2: Percipitation data for current time is not available. Newest data are 3 hours old. Run a separate job later to enrich the data with precipitation at the time when the passengers boarded the train
"""

# COMMAND ----------

def createDateString(dateObj):
  """
    input parameter: datetime object
    return: string in the form "yyyy-mm-dd"
  """
  return str(dateObj.year)+"-"+str(dateObj.month)+"-"+str(dateObj.day)

def GetPercipitation(datehour):
  """
  Input parameter: datetime object giving the date and time for a passenger boarding/alighting event
  Return: Meteorological observation on 1 hour precipitiation in the last hour before the event for which there is data and the hour during which the obseration was made. 
   Return None if no data exist within 3 hours before the event.
  """
  
  """
  http://eklima.met.no/metdata/MetDataService?invoke=getMetData&timeserietypeID=2&format=&from=2015-09-14&to=2015-09-14&stations=18700&elements=RR_1&hours=8,9,10,11,12&months=&username=   kl11.40 gir det svar fra kl. 8 og kl 9.
  """
  try:
    hour = datetime.timedelta(hours=1)
    ThreeHoursAgo = datehour - 3*hour
    TwoHoursAgo = datehour - 2*hour
    OneHourAgo = datehour - hour

    ThreeHoursAgoString = createDateString(ThreeHoursAgo)
    ObservationDateHourString = createDateString(datehour)
    hours_string = str(ThreeHoursAgo.hour)+","+str(TwoHoursAgo.hour)+","+str(OneHourAgo.hour)+","+str(datehour.hour)

    #URL to get precipitation at Blindern/University of Oslo
    url = "http://eklima.met.no/metdata/MetDataService?invoke=getMetData&timeserietypeID=2&format=&from={0}&to={1}&stations=18700&elements=RR_1&hours={2}&months=&username=".format(ThreeHoursAgoString, ObservationDateHourString, hours_string)
    #print "url: %s" %url  NB: print is only for stand-alone testing NOT for use during streaming!
    metFile = urllib.urlopen(url)
    metXMLString = metFile.read()
    metFile.close()
    #print metXMLString     NB: print is only for stand-alone testing NOT for use during streaming!
    root = ET.fromstring(metXMLString)
    val = None
    values = []
    hours = []
    for v in root.iter('value'):
      values.append(v.text)
    for h in root.iter("from"):
      hours.append(h.text)
    return values, hours  #change to get only last (value, datetime)
  except Exception as e:
    #LogToKinesisFromWorker(KINESIS_LOGSTREAM, "GetPercipitation", "EXCEPTION", str(e))
    return (['0.0', '0.0', '0.0'], ['2016-01-01T01:00:00.000Z', '2016-01-01T01:00:00.000Z', '2016-01-01T01:00:00.000Z']) 

# COMMAND ----------

"""
test = GetPercipitation(datetime.datetime.now(timezone('Europe/Oslo')))
print test
test2 = GetPercipitation(datetime.datetime.fromtimestamp(1336562038))  #09.05.2012 11:13 example time given in Passenger count documentation
print test2
"""

# COMMAND ----------

"""
TODO 0: Move this away from Spark or out in a seprate DSTREAM _not_ keep it part of record-parsing!
        Reading from external sources should be done _separately_ from record_parsing, f.ex. as a Nodejs kinesis-producer. And then Spark reads from Kinesis
        Doing it in Spark stream introduce complex error and timeout handling if the external service is slow or unavalable
TODO 1: Get temperature from the meteorological station closest to the train station based on lat, lng
TODO 2: Get temperature at the time of passenger boarding, not mean temperature
TODO 3: Document the procedure
TODO 4: Get time of the observation, like in Get Precipitation
"""

# COMMAND ----------

NO_TEMP = -100.0

def GetTemperature(date):
  return NO_TEMP
"""
  try:
    prevdate = datetime.datetime.fromordinal(date.toordinal() - 1)  #one day before observation as datetime object
    startdate = str(prevdate.year)+"-"+str(prevdate.month)+"-"+str(prevdate.day)  #one day before as string
    todate = str(date.year)+"-"+str(date.month)+"-"+str(date.day) #observation day

    #URL to get mean temperature at Blindern/University of Oslo
    url = "http://eklima.met.no/metdata/MetDataService?invoke=getMetData&timeserietypeID=0&format=&from={0}&to={1}&stations=18700&elements=tam&hours=&months=&username=".format(startdate, todate)
    #print "url: %s" %url

    metFile = urllib.urlopen(url)
    metXMLString = metFile.read()
    metFile.close()
    #print metXMLString NB: print is only for stand-alone testing NOT for use during streaming!
    temp = None
    root = ET.fromstring(metXMLString)
    for v in root.iter('value'):
      if float(v.text) < -98:
        break
      temp = float(v.text)
    if temp is None:
      LogToKinesisFromWorker(KINESIS_LOGSTREAM,"GetTemperature", "ERROR No temperature returned for date: ", str(date))
      return NO_TEMP  
    #print v.tag, v.attrib, v.text  NB: print is only for stand-alone testing NOT for use during streaming!
    return temp
  except Exception as e:
    LogToKinesisFromWorker(KINESIS_LOGSTREAM,"GetTemperature", "EXCEPTIOM: ", str(e))
    return NO_TEMP
"""

# COMMAND ----------

"""
test = GetTemperature(datetime.datetime.fromtimestamp(1336562038))  #09.05.2012 11:13
print test
test2 = GetTemperature(datetime.datetime.fromtimestamp(time.time()+100000))  Temperature from the future?
print test2
"""

# COMMAND ----------

# MAGIC %md
# MAGIC Convert each line into a list of integers

# COMMAND ----------

"""
TODO 0: Improve error handling, fine tune PASSENGER_RECORD regexp, test with exceptions. Test behaviour if returning nothing rather than "zero" record
TODO 1: tune PASSENGER_RECORD regexp, test LogToKinesis
TODO 2: add GetPrecipitation
"""

# COMMAND ----------

PASSENGER_RECORD = '(\d+);(\d+);(\d+);(\d+);(\d+);(\d+);(\d+);(\d+);(\d+);(\d+);(\d+);(\d+);(\d+);(\d+);(\d+);(\d+);(\d+);(\d+);(\d+);(\d+);(\d+);(\d+);(\d+);(\d+);(\d+);(\d+);(\d+);(\d+);(\d+);(\d+);(\d+);(\d+);(\d+);(\d+);(\d+);(\d+);(\d+);(\d+);(\d+);(\d+);(\d+);(\d+);(\d+);(\d+);(\d+);(\d+);(\d+);(\d+);(\d+);(\d+);(\d+);(\d+);(\d+);(\d+);(\d+);(\d+);(\d+);(\d+);(\d+);(\d+);(\d+);(\d+);(\d+);(\d+);(\d+);(\d+);(\d+);(\d+);(\d+)'

NULL_RECORD = [0,0,0,"",0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0]

def ParsePassengerRecord(record):
  try:
    match = re.search(PASSENGER_RECORD, record)
  except Exception as eir:
    LogToKinesisFromWorker(KINESIS_LOGSTREAM,"ParsePassengerRecord", "EXCEPTION re.search", str(eir), str(record))
    return NULL_RECORD
  if match is None:
    LogToKinesisFromWorker(KINESIS_LOGSTREAM,"ParsePassengerRecord", "ERROR match is None", str(record))
    return NULL_RECORD
  try:  
    t = datetime.datetime.fromtimestamp(float(match.group(1)))
    paxRecord = [int(t.year), int(t.month), int(t.day), t.strftime("%A"), int(t.hour), \
                   int(t.minute), int(t.second), GetTemperature(t)]
    for i in range(1,70):
      paxRecord.append(int(match.group(i)))
    return paxRecord
  except Exception as e:
    LogToKinesisFromWorker(KINESIS_LOGSTREAM,"ParsePassengerRecord", "EXCEPTION parsing passenger record", str(e))
    return NULL_RECORD

# COMMAND ----------

"""
function: updatePassengerCount
used as input function to updateStateByKey, see further below
parameters:
  newAlightBoardValues: List of tuples where each typle contains (alight, board), f.ex [(0, 2), (0, 7), (1, 0), (0, 0), (0, 0)] for a particular train/own_module_no
  prevAlightBoardValues: one tuple (alight, board) containing the sum of previous alightings and boardings for this train/wn_module_no
return
  one tuple containing new (alight, board) aggregate for particular train/own_module_no
NOTE: Calculate elapsed time period allows to: delete a key _permanently_ by returning None or reset value of a key to zero by returning (0,0)
"""
def updatePassengerCount(newAlightBoardValues, prevAlightBoardValues):
  try:
    # trains start running at 05.00 in the morning
    OsloTimeNow = datetime.datetime.now(pytz.timezone('Europe/Oslo'))
    #LogToKinesisFromWorker(KINESIS_LOGSTREAM, "updatePassengerCount OslotimeNow", "hour: ", str(OsloTimeNow.hour), "minute: ", str(OsloTimeNow.minute))
    # reset all passengerCounts to zero every morning
    if OsloTimeNow.hour >= 4 and OsloTimeNow.hour < 5 and OsloTimeNow.minute > 5 and OsloTimeNow.minute < 50: 
      #LogToKinesisFromWorker(KINESIS_LOGSTREAM, "updatePassengerCount - RESET", "hour: ", str(OsloTimeNow.hour), "minute: ", str(OsloTimeNow.minute))
      return (0,0)

    #LogToKinesisFromWorker(KINESIS_LOGSTREAM, "updatePassengerCount", "Worker - parameters", "new: ", str(newAlightBoardValues), "old: ", str(prevAlightBoardValues))
    if prevAlightBoardValues is None or not prevAlightBoardValues:  #test for None or empty list []
       prevAlightBoardValues = (0,0)
    if newAlightBoardValues is None or not newAlightBoardValues:
        newAlightBoardValues = [(0,0)]
    sumNewAlightBoardValues = [sum(alightboardtuple) for alightboardtuple in zip(*newAlightBoardValues)] # add alightings together, add boardings together, return as LIST with two elements: [alightings, boardings]
    #cfLogToKinesisFromWorker(KINESIS_LOGSTREAM, "updatePassengerCount", "Worker - sumNewAlightBoardValues", "new sum: ", str(sumNewAlightBoardValues), "old: ", str(prevAlightBoardValues))
    # next do: [[aNew, bNew] + (aOld,bOld)]
    sumNewAndOldAlightBoardValues = [sumNewAlightBoardValues[0]+prevAlightBoardValues[0], sumNewAlightBoardValues[1]+prevAlightBoardValues[1]] # now we have a list [totAlight, totBoard]
    #LogToKinesisFromWorker(KINESIS_LOGSTREAM, "updatePassengerCount", "Worker - sumNewAndOldAlightBoardValues: ", str(sumNewAndOldAlightBoardValues))
  except Exception as e:
    #LogToKinesisFromWorker(KINESIS_LOGSTREAM, "updatePassengerCount", "EXCEPTION", str(e))
    return (0,0)
  return (sumNewAndOldAlightBoardValues[0], sumNewAndOldAlightBoardValues[1]) # return tuple (a,b)

# COMMAND ----------

# MAGIC %md
# MAGIC ###The Process functions
# MAGIC These functions are called by foreachRDD and used to output data from Spark
# MAGIC - Convert RDD to dataframe
# MAGIC - Save dataframe to temporary memory for passenger visualization in "real-time" map
# MAGIC - Save dataframe to permanent table
# MAGIC - Output to Kinesis for further real-time processing

# COMMAND ----------

""" Output passenger records to Kinesis as dataframe """
def processPax(time, rdd): 
  #LogToKinesis("process", "Entering", str(time))
  if len(rdd.take(1)) != 0:
    try:
      df = rdd.toDF(schema)
      LogToKinesis("process", "rdd.toDF")
      df.registerTempTable("tempPassengerData") #for debug/interactive querying
      SendToKinesis(df, "pax", 0)
    except Exception as e:
      LogToKinesis("process", "EXCEPTION", str(e))
      
def processKinesisPax(time, rdd):
  LogToKinesis("processKinesisPax", "Entering", str(time))
  if len(rdd.take(1)) != 0:
    try:
      #df = rdd.toDF(schema)
      c = rdd.count()
      content = rdd.collect()
      LogToKinesis("processKinesisPax", "content: ", str(content))
      #df.registerTempTable("tempPassengerData") #for debug/interactive querying
      #SendToKinesis(df, "pax", 0)
    except Exception as e:
      LogToKinesis("processKinesisPax", "EXCEPTION", str(e))

""" Output sum of alightings and boardings per line and per station to Kinesis """
def processStationLineWindow(time, rdd):
  #LogToKinesis("processWindow", "Entering", str(time))
  if len(rdd.take(1)) != 0:
    try:
      df = rdd.toDF(schema)
      LogToKinesis("processWindow", "rdd.toDF", str(time))
      df.registerTempTable("tempWinPassengerData") #for debug/interactive querying
      """ Get passengers per Metro Line """
      sumPerLine = df.groupBy("LineNumber").agg({'TotalAlighting': 'sum', "TotalBoarding": "sum"})
      SendToKinesis(sumPerLine, "LineAggregate", 1800)
      """ Get passengers per Station """
      sumPerStation = df.groupBy("CurrentStationID").agg({'TotalAlighting': 'sum', "TotalBoarding": "sum"})
      SendToKinesis(sumPerStation, "StationAggregate", 1800)
    except Exception as e:
      LogToKinesis("processWindow", "EXCEPTION", str(e))

""" Save passenger records periodically to permanent storage for historical processing """
def processTable(time, rdd):
  #LogToKinesis("processTable", "Entering", str(time))
  if len(rdd.take(1)) != 0:
    try:
      df = rdd.toDF(schema)
      LogToKinesis("processTable", "rdd.toDF")
      df.registerTempTable("tempTablePassengerData") #for debug/interactive querying
      df.write.format("parquet").mode("append").saveAsTable(TABLENAME_EVERY)  #NOT WORKING: .partitionBy("LineNumber"), see table "passengersnew"
      #LogToKinesis("processTable", "saveAsTable")
    except Exception as e:
      LogToKinesis("processTable", "EXCEPTION", str(e))

""" Output count of passengers that have alighted and boarded each individual train during current day of operations """
def processOwnModuleState(time, rdd):
  #LogToKinesis("processTotalAlighting", "Entering", str(time))
  if len(rdd.take(1)) != 0:
    try:
      df = rdd.toDF(schemaAlightBoard)
      df.registerTempTable("tempTableAlightingBoarding") #for debug/interactive querying
      #LogToKinesis("OwnModuleAggregate", "df", str(dir(df)))
      SendToKinesis(df, "OwnModuleAggregate", 0)
    except Exception as e:
      LogToKinesis("processTotalAlighting", "EXCEPTION", str(e))

# COMMAND ----------

# MAGIC %md
# MAGIC SETUP STREAM

# COMMAND ----------

"""
kinesisStream = static createStream(ssc, kinesisAppName, streamName, endpointUrl, regionName, initialPositionInStream, checkpointInterval, storageLevel=StorageLevel(True, True, False, True, 2), awsAccessKeyId=None, awsSecretKey=None, decoder=<function utf8_decoder at 0x7f1ab63c29b0>)[source]
"""

# COMMAND ----------

KINESIS_APPNAME = "paxtest2"
KINESIS_ENDPOINT_URL = "kinesis.eu-west-1.amazonaws.com",
KINESIS_REGION = "eu-west-1"

# COMMAND ----------

def creatingfunc():
  # create streaming context
  ssc = StreamingContext(sc, batchIntervalSeconds)
  LogToKinesis("creatingfunc", "StreamingContext", str(dir(ssc)))
  ssc.remember(10*batchIntervalSeconds)
  
  # setup streams
  try: 
    #paxRecords = ssc.textFileStream(SOURCE).map(ParsePassengerRecord)  # parse and enrich pax data
    kinesisStream = KinesisUtils.createStream(ssc, KINESIS_APPNAME, KINESIS_STREAM, KINESIS_ENDPOINT_URL, KINESIS_REGION, InitialPositionInStream.TRIM_HORIZON, 10, StorageLevel.MEMORY_AND_DISK_2, ACCESS_KEY, SECRET_KEY)
    LogToKinesis("kinesisStream", "KinesisUtils.createStream", str(dir(kinesisStream)))
    
    # track total boarding and alighting per train/ownmoduleno
    # Note: rdd returned by updateStateByKey is (ownmoduleno, (alight, board))
    # for easy conversion to dataframe we map this rdd to (ownmoduleno, alight, board). (Not shure why the following did not work: map(lambda k,v: (k,v[0],v[1])) )
    """
    noOfPassengersOwnModuleToday = paxRecords.map(lambda record: (record[OWN_MODULE_NO],(record[TOTAL_ALIGHTING], record[TOTAL_BOARDING])))  \
                              .updateStateByKey(updatePassengerCount) \
                              .map(lambda v: (v[0],v[1][0],v[1][1]))  
        
    paxRecordsWindowStationLine = paxRecords.window(1800,20)  # compute aggregates on a 30 min window updated every 20 sec
    paxRecordsTable = paxRecords.window(900,900) # save to permanent storage every 15 min (how large/small amounts of data is optimal to save at a time?)
    LogToKinesis("creatingfunc", "Streams set up OK")
    """
  except Exception as e:
    LogToKinesis("creatingfunc", "EXCEPTION", str(e))
 
  # output streams
  try: 
    #paxRecords.foreachRDD(processPax)
    #noOfPassengersOwnModuleToday.foreachRDD(processOwnModuleState) # send sum of alightings and boardings and pax present onboard for each train to Kinesis
    #paxRecordsWindowStationLine.foreachRDD(processStationLineWindow) #send aggregates to Kinesis periodically, i.e. last 30 mins updated every 20 secs
    #paxRecordsTable.foreachRDD(processTable) #save to permanent table periodically
    kinesisStream.foreachRDD(processKinesisPax)
  except Exception as e:
    LogToKinesis("mainLoop", "EXCEPTION", str(e))

  ssc.checkpoint(CHECKPOINTDIR)
  return ssc

# COMMAND ----------

# # Start streaming
# try:
#   ssc = StreamingContext.getActiveOrCreate(CHECKPOINTDIR, creatingfunc)
#   ssc.start()
#   ssc.awaitTerminationOrTimeout(2*batchIntervalSeconds)
# except Exception as e:
#   LogToKinesis("MAIN", "EXCEPTION", str(e))

# COMMAND ----------

#dbutils.fs.ls(CHECKPOINTDIR)

# COMMAND ----------

ssc = StreamingContext.getActiveOrCreate(CHECKPOINTDIR, creatingfunc)
ssc.start()
ssc.awaitTerminationOrTimeout(2*batchIntervalSeconds)

# COMMAND ----------

