#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Dec 18 14:55:13 2020

@author: dabol99
"""

# import packages
import glob
import google.auth
from google.cloud import bigquery
import os

# import functions from auxiliary files
from bigquery_aux import point_google_authentication_as_global_variable
from errors_aux import custom_error

""" 
Define path to Google credentials as global variable if they exists. 
Rais error otherwise.
"""
DIRECTORY_WITH_CREDENTIALS = os.path.join(os.getcwd(), 'Google_credentials')
if not any(fname.endswith('.json') for fname in os.listdir(DIRECTORY_WITH_CREDENTIALS)):
    raise custom_error("No .json file with google credentials in " +\
                       DIRECTORY_WITH_CREDENTIALS + ". Please add one.")
else:
    JSON_FILES_IN_DIRECTORY = os.path.join(DIRECTORY_WITH_CREDENTIALS, '*.json')
    CREDENTIAL_FILE_PATH = glob.glob(JSON_FILES_IN_DIRECTORY)[0] 





def build_bigquery_client():
    """
    This function initialize a bigquery client using the default credentials 
    for the cloud platform in CREDENTIAL_FILE_PATH.
    
    Returns
    -------
    client : bigquery.client object
        A bigquery.client object which uses the default credentials 
        scoped on the google cloud platform.

    """
    # set environment variable to point to google credentials for authentication
    point_google_authentication_as_global_variable(CREDENTIAL_FILE_PATH)
    
    # build authentication object 
    # guidelines at https://cloud.google.com/bigquery/docs/bigquery-storage-python-pandas
    credentials, project_id = google.auth.default(scopes=["https://www.googleapis.com/auth/cloud-platform"])

    # initialize bigquery client with project id and credentials
    client = bigquery.Client(credentials = credentials, project = project_id)
    return client









"""
Fetch credentials from DIRECTORY_WITH_CREDENTIALS and execute build_bigquery_client()
"""
if __name__ == '__main__':
    build_bigquery_client()