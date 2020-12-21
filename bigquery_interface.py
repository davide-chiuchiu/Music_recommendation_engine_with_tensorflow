#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Dec 18 14:55:13 2020

@author: dabol99

This module contains functions that allows to initialize a client for the 
bigquery api using the credentials found in the "Google credentials" directory 
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







def build_bigquery_client_with_default_dataset(default_dataset_id):
    """
    This function uses build_bigquery_client() to initialize a bigquery client
    using the default credentials for the cloud platform in CREDENTIAL_FILE_PATH.
    In addition, it creates the configuration object that allows to send the 
    query to the dataset in default_dataset_id as a default.
    
    Returns
    -------
    client : bigquery.client object
        A bigquery.client object which uses the default credentials 
        scoped on the google cloud platform.
    default_dataset_id: str
        A string that contains the unique bigquery identifier of the dataset
        to use as default for successive queries

    """
    # initialize gibquery client
    client = build_bigquery_client()
    
    # create a configuration object wich points a query to a default dataset
    config_object_default_dataset = bigquery.QueryJobConfig(default_dataset = default_dataset_id)

    
    return client, config_object_default_dataset






def import_sql_query_from_file(query_file_path):
    """
    Parameters
    ----------
    query_file_path : string that contains the path to an .sql file with a 
    query in it.

    Returns
    -------
    returns the query in the file with path query_file_path as a string
    """
    query_file_object = open(query_file_path)
    query = query_file_object.read()
    query_file_object.close()
    
    return query






def send_query_to_database(bigquery_client, job_query_config,  query_file_path):
    """
    This function imports the sql query saved in query_file_path
    and it sends it to the default configurations in job_query_config using
    the bigquery_client object. It returns the result of the query as a pandas
    dataframa

    Parameters
    ----------
    bigquery_client : bigquery.client object
        A bigquery client object to use to interface to bigquery.
    job_query_config : bigquery.QueryJobConfig object
        A bigquer.QueryJobConfig object with the options for the query to perform
    query_file_path : string
        A string that define the path to an .sql file with the query to send 
        to bigquery

    Returns
    -------
    query_as_dataframe : Pandas dataframe
        A pandas dataframe that contains the output of the query in query_file_path.
    """   
    # read query from file in ./folder_of_sql_queries/query_file_name
    query = import_sql_query_from_file(query_file_path)
    
    # send query and return result as pandas dataframe
    query_as_dataframe = bigquery_client.query(query, job_config = job_query_config) \
                         .result() \
                         .to_dataframe()
    return query_as_dataframe







"""
Fetch credentials from DIRECTORY_WITH_CREDENTIALS and execute build_bigquery_client()
"""
if __name__ == '__main__':
    default_dataset = 'bigquery-public-data.google_analytics_sample'    
    client, config_object_default_dataset = build_bigquery_client_with_default_dataset(default_dataset)   
    
    query_path = os.path.join('query_folder', 'snippet_query.SQL')
    df = send_query_to_database(client, config_object_default_dataset,  query_path)