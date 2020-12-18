#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Oct 28 13:30:50 2020

@author: dabol99

This file contain the functions that allows to interface python with a bigquery
database
"""
 
# import packages
import os
from errors_aux import custom_error
from import_sql_query_files import import_sql_query_from_file




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



def send_query_to_database(bigquery_client, job_query_config,  query_file_name, folder_of_sql_queries = "sql_queries"):
    """
    This function imports the sql query saved in ./folder_of_sql_queries/query_file_name
    and it sends it with the configurations in job_query_config via the 
    client bigquery_client. It returns the result of the query as a pandas
    dataframa

    Parameters
    ----------
    bigquery_client : bigquery.client object
    job_query_config : bigquery.QueryJobConfig object
        A bigquer.QueryJobConfig object with the options for the query to perform
    query_file_name : string
        A string that contains the name of the .SQL file with the query to perform
    folder_of_sql_queries : string, optional
        Folder that contains query_file_name. The default is "sql_queries".

    Returns
    -------
    query_as_dataframe : Pandas dataframe
        A pandas dataframe that contains the output of the query in query_file_name.
    """   
    # read query from file in ./folder_of_sql_queries/query_file_name
    query_full_path = os.path.join(os.getcwd(), folder_of_sql_queries, query_file_name)
    schema_query = import_sql_query_from_file(query_full_path)
    
    # send query and return result as pandas dataframe
    query_as_dataframe = bigquery_client.query(schema_query, job_config = job_query_config) \
                         .result() \
                         .to_dataframe()
    return query_as_dataframe