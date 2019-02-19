#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
Created on Fri Feb  8 21:03:19 2019

@author: echeun06
"""

import pandas as pd
import datetime
from google.cloud import storage
from google.cloud.storage import Blob
import os

folder = os.path.abspath(os.path.dirname(__file__))

def download_blob(blob):
    print("Downloading {name} from GCS".format(name=blob.name))
    blob.download_to_filename(folder + '/tmp/' + blob.name)
    
def list_blobs(bucket):
    print("Retrieving blobs from GCS")
    blobs = bucket.list_blobs()
    blob_list = [blob for blob in blobs]
    return blob_list

def upload_to_gcs(blob, filepath):
    blob.upload_from_filename(filepath)

def clean_dcm(df):
    skiprows = 0
    for j in range(len(df)):
        if df.iloc[j,0] == 'Report Fields' :
            skiprows = j+2  #index+1, header+1
            print ("start_row is %s" %skiprows)
            break
    report_id_dcm = "1258768" + "_" + datetime.datetime.now().strftime("%m") +"_dcm.csv"
    df = pd.read_csv(folder + '/tmp/{filename}'.format(filename = report_id_dcm), delimiter=',', skiprows=skiprows)
    nrow = len(df)
    end_row = len(df)
    for j in range(nrow):
        end_row = end_row -1
        print(df.iloc[end_row,0])
        if df.iloc[end_row,0] == 'Grand Total:' :
            print("end_row is #%d" %(end_row))    #0-indexed
            break
    df = df.iloc[:end_row]  #end_row is in fact the first irrelevant row
    df['Date'] = [datetime.datetime.strptime(d,'%d/%m/%Y').date() for \
                          d in df['Date']]
    return df


#TTD Report
def ttd(df1):
    df = pd.read_csv(folder + '/tmp/{filename}'.format(filename=df1), delimiter=',')
    df = df.groupby(['Creative','Date']).sum().reset_index()
    df = df[['Date','Creative','Clicks','Impressions','Advertiser Cost (Adv Currency)']]
    df['Date'] = [d[:d.find("T")] for d in df['Date']]
    df['Key'] = df['Creative'] + "_" + df['Date']
    return df


#DCM Report
def dcm(df2):
    df = pd.read_csv(folder + '/tmp/{filename}'.format(filename = df2), delimiter=',')       
    df = clean_dcm(df)
    df = df.groupby(['Placement','Date']).sum().reset_index()
    df['Date'] = df['Date'].apply(str)
    df['Key'] = df['Placement'] + "_" + df['Date']
    return df

def main():
    storage_client = storage.Client.from_service_account_json(
    folder + '/amnet-dcm01.json'
    )
    
    #df1 (ttd)
    bucket_name = 'amnet_ttd_reports'
    bucket = storage_client.get_bucket(bucket_name)
    blobs = list_blobs(bucket)
    #Insert TTD Report ID
    report_id = "1258768" + "_" + datetime.datetime.now().strftime("%m") + ".csv" #TTD API report name format
    for blob in blobs:
        if report_id == blob.name:
            download_blob(blob)       
    df1 = ttd(report_id)
    
    #df2 (dcm)
    bucket_name = 'amnet_dcm_reports'
    bucket = storage_client.get_bucket(bucket_name)
    blobs = list_blobs(bucket)
    #Insert DCM Report ID
    report_id_dcm = "1258768" + "_" + datetime.datetime.now().strftime("%m") +"_dcm.csv" #DCM report name format
    for blob in blobs:
        if report_id_dcm == blob.name:
            download_blob(blob)
    df2 = dcm(report_id_dcm)
    df_all = df1.merge(df2, on='Key', how='outer')
    df_all = df_all[['Date_x','Creative','Clicks','Impressions','Advertiser Cost (Adv Currency)', 'Total Conversions']]
    
    #gcs upload
    filename_out = "merge_" + report_id
    df_all.to_csv(folder + '/{filename}'.format(filename=filename_out), sep=",")
    bucket = storage_client.get_bucket("amnet_ttd_reports")
    blob = Blob(filename_out, bucket)
    upload_to_gcs(blob, folder + "/" + filename_out)
    
if __name__ == "__main__":
    main()