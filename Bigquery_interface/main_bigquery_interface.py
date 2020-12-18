#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Dec 18 14:55:13 2020

@author: dabol99
"""

# import packages
import os
import glob

# import functions from auxiliary files
from bigquery_aux import point_google_authentication_as_global_variable
from bigquery_aux import initialize_bigquery_client
from errors_aux import custom_error

# define path to Google credentials as global variable if they exists. Rais error otherwise
DIRECTORY_WITH_CREDENTIALS = os.path.join(os.getcwd(), 'Google_credentials')
if not any(fname.endswith('.json') for fname in os.listdir(DIRECTORY_WITH_CREDENTIALS)):
    raise custom_error("No .json file with google credentials in " +\
                       DIRECTORY_WITH_CREDENTIALS + ". Please add one.")
else:
    JSON_FILES_IN_DIRECTORY = os.path.join(DIRECTORY_WITH_CREDENTIALS, '*.json')
    CREDENTIAL_FILE_PATH = glob.glob(JSON_FILES_IN_DIRECTORY)[0] 





def build_bigquery_client():
    # ADD DOCSTRINGS
    
    
    # set environment variable to point to google credentials for authentication
    point_google_authentication_as_global_variable(CREDENTIAL_FILE_PATH)
    return









"""
Fetch credentials from DIRECTORY_WITH_CREDENTIALS and execute build_bigquery_client()
"""
if __name__ == '__main__':
    build_bigquery_client()