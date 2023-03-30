from flask import Blueprint,request
import json
from sparkModules.sparksubmit import createSparkApp,getSparkJobDetails
from ociModules.object_store import createPreAuthUrl
import datetime

views = Blueprint('views', __name__)

class Static:
    currTime = datetime.datetime.now().strftime("%H_%M_%S")
    targetDirectory = "adib/dataset"

# print(Static.currTime)
@views.route('/data-accumulation/config', methods=['POST'])
def home():
    if request.method == 'POST':
        if request.json["dataSource"].lower()=="rdbms":
            configJSON = json.dumps(request.json)
            configJSON = configJSON.replace(" ","")
            Static.currTime = datetime.datetime.now().strftime("%H_%M_%S")
            Static.targetDirectory = request.json['targetDirectory']
            res = createSparkApp(
                SparkJobName="SparkJob"+"_"+Static.currTime,
                SparkPoolName="mkplaceSpark",
                # PythonScriptUrl=request.json["pythonscript"],
                PythonScriptUrl="abfss://raw@dlneomadibdataset.dfs.core.windows.net/pythonModules/sparkRDBMSConnector.py",
                config=configJSON
            )
            return res
        elif request.json["dataSource"].lower()=="flatfile":
            return createPreAuthUrl(uploadedFileName=request.json["uploadedFileName"])
        return "Select A Valid Data Source"

@views.route('/data-accumulation/progress', methods=['GET'])
def progress():
    if request.method == 'GET':
        SparkJobName = "SparkJobDefinition_SparkJob"+"_"+Static.currTime+"_submit"
        return json.dumps({
            "progress":getSparkJobDetails(SparkJobName),
            "location":f"abfss://raw@dlneomadibdataset.dfs.core.windows.net/{Static.targetDirectory}"
            })
        