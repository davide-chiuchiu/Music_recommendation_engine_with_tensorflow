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

# define custom error class
class custom_error(Exception):
    pass

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






def point_google_authentication_as_global_variable(google_credentials_path):
    """
    This function takes the path google_credentials_path of the .json
    file with the google authentication credentials, and it pass it to 
    the global variable GOOGLE_APPLICATION_CREDENTIALS. Raises error
    if the file ingoogle_credentials_path does not exist
    
    Parameters
    ----------
    google_credentials_path : string 
        A string with the path of the google credentials.
        
    Raises
    ------
    custom_error
        raise error when google_credentials_path does not exist.

    Returns
    -------
    None.

    """
    if os.path.exists(google_credentials_path):
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = google_credentials_path
    else:
        path_head, path_tail = os.path.split(google_credentials_path)
        error_no_credentials = "File " + path_tail + \
                               " with .json credential for project not found in " + \
                               path_head + ". Please add " + path_tail + " to " + path_head
        raise custom_error(error_no_credentials)
    return







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