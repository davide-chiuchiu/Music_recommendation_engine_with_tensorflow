#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Oct 26 18:45:10 2020

@author: dabol99

This function contains the function import_sql_query that imports an sql query
stored in an .sql file as a string that can be passed to bigquery.client
"""


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