# -*- coding: utf-8 -*-
"""
Created on Tue Oct 23 15:27:43 2018

@author: echeun06
"""

# Copyright 2017 The Trade Desk, Inc.

# This Python code's purpose is to provide a baseline for retrieving MyReports reports via the TTD API.
import requests
import json
import time
import os
import pandas as pd
import io
from datetime import date


# Input Parameters
url = 'https://api.thetradedesk.com/v3/myreports/reportexecution/query/advertisers'
authToken = '2xGhXXXXXXXXXXXX/XXXXX'
payload = {
    'AdvertiserIds': ['XXXXXX'],
    'PageStartIndex': 0,
    'PageSize': 10000
}

#######################


type = 'application/json'

start_time = time.time()

myResponse = requests.post(url, headers={'Content-Type': 'application/json', 'TTD-Auth': authToken}, json=payload)
folder = os.path.abspath(os.path.dirname(__file__)) + "/tmp/"

if (myResponse.ok):

    jData = json.loads(str(myResponse.content))
    print (jData)
    print("{} reports found".format(len(jData['Result'])))
    i=0
    for x in jData["Result"]:
        print(x["ReportScheduleId"])
        i=i+1
        if (x["ReportExecutionState"] == "Complete"):
            if(x["ReportScheduleId"] == 1258768):
                if (x["ReportEndDateExclusive"][0:10].encode('utf-8') == str(date.today())):
                    file = requests.get(x["ReportDeliveries"][-1]["DownloadURL"], headers={'TTD-Auth': authToken})
                    urlData = file.content
                    rawData = pd.read_csv(io.StringIO(urlData.decode('utf-8')))
                    month = rawData['Date'][0][5:7]
                    rawData.to_csv(folder + str(x["ReportScheduleId"]) + "_" + month + ".csv")
                    print (str(x["ReportScheduleId"]) + ".csv is downloaded!")
                    print ("Initiating")
                    os.chdir(folder)
                    os.system("gsutil cp " + str(x["ReportScheduleId"]) + "_" + month + ".csv gs://amnet_ttd_reports")
                    print (str(x["ReportScheduleId"]) + ".csv is uploaded!")

#Upload file to cloud storage

