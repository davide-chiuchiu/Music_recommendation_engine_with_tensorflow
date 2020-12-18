#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Oct 20 18:40:52 2020

@author: dabol99

This script contains is a minimum working example to use python to interface to
a big database with bigquery, perform some simple postprocessing of the 
database with SQL, and then make some exploratory analysis
"""

# import packages
import os
from google.cloud import bigquery
import seaborn
import matplotlib

# set current work directory to the one with this script.
os.chdir(os.path.dirname(os.path.abspath(__file__)))

# import functions from auxiliary files
from bigquery_aux import point_google_authentication_as_global_variable
from bigquery_aux import initialize_bigquery_client
from bigquery_aux import send_query_to_database


'''
Import global variables to point to the google cloud authentication credentials,
initialize bigquery client and create dataset reference as default option for queries
'''
# set environment variable to point to the google credentials. Raise error
# without the .json credential file
google_credentials_path = os.path.join(os.getcwd(), 'Google_credentials', 'bigquery-stackoverflow-DC-fdb49371cf87.json')
point_google_authentication_as_global_variable(google_credentials_path)

# initialize bigquery client 
bigquery_client = initialize_bigquery_client()

# point to stackoverflow dataset as default dataset in the job_query_configurations 
stackoverflow_dataset_id = 'bigquery-public-data.stackoverflow'
job_query_config = bigquery.QueryJobConfig(default_dataset = stackoverflow_dataset_id)


'''
Import info about the dataset structure and save it as .csv 
'''
database_structure = send_query_to_database(bigquery_client, job_query_config, "schema_stack_overflow_query.sql", "sql_queries")
database_structure = database_structure.pivot_table(index = 'column_name', columns = 'table_name', aggfunc='size')
database_structure.to_csv('database_entity_relation_diagram.csv')


'''
Perform query to get the trend of tags in questions with answers
'''
dataframe_cumulative_number_of_tags = send_query_to_database(bigquery_client, job_query_config, "tag_trends.sql", "sql_queries")
# plot how technical questions have changed since 2008
figure, axis = matplotlib.pyplot.subplots(1, 2, gridspec_kw = {'width_ratios': [3, 2]}, figsize = (11, 10))
order_of_lines = dataframe_cumulative_number_of_tags[['unique_tag', 'cumulative_questions']]\
                 .groupby('unique_tag').max()\
                 .sort_values(by = 'cumulative_questions', ascending = False).index
seaborn.lineplot(x = 'creation_quarter',  y = 'cumulative_questions',
                 hue = 'unique_tag', data = dataframe_cumulative_number_of_tags, 
                 palette = 'colorblind', size = 'unique_tag',   size_order = order_of_lines, 
                 ax = axis[0])
axis[0].set(xlabel = "Time [in unit of years]", ylabel = "Total questions posted")
axis[0].legend([], [], frameon = False)

'''
Perform query to get the histogram of waiting time for first answer for the
10 most common tags
'''
distribution_of_first_answers = send_query_to_database(bigquery_client, job_query_config, "distribution_of_first_answer_time_query.sql", "sql_queries")
# plot the frequency to get the best answer to a stackexchange question within
# hours, days, weeks or months.
seaborn.barplot(x = 'frequency', y = 'bucketized_waitining_time', 
                data = distribution_of_first_answers, hue = 'unique_tag', 
                ax = axis[1],
                palette = 'colorblind')
axis[1].set(xlabel = 'Time to get the best answer of a question', ylabel = 'Frequency')
axis[1].legend(title = 'Topic', loc='center right', bbox_to_anchor=(1.7, 0.5))
figure.tight_layout(pad = 3.5)

# close client
bigquery_client.close()
