#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Jan 28 13:02:14 2021

@author: dabol99
"""
# import modules
import sklearn.preprocessing




def encode_sequential_labels(dataframe, variable_to_encode):
    """
    This function takes a dataframe and it encodes the categorical
    variable in variable_to_encode using sklearn.preprocessing.LabelEncoder
    Inputs: 
        dataframe:
          pandas dataframe with a categorical variable to encode
        variable_to_encode: 
          string with the name of the variable to encode
    Outputs:
        encoder:
          sklearn.preprocessing.LabelEncoder object trained to encode
          the categorical variable variable_to_encode
        dataframe: 
          the original pandas dataframe in input with an extra column
          containing the encoded values of variable_to_encode
    """
    # set name of encoded variable in dataframe 
    encoded_variable_name = 'encoded_' + variable_to_encode
    # initialize encoder
    encoder = sklearn.preprocessing.LabelEncoder()
    # encode variable and add it to dataframe
    dataframe[encoded_variable_name] = encoder.fit_transform(dataframe[variable_to_encode].values)
    return encoder, dataframe





def format_inputs(dataframe, x_labels, y_label):
    """
    This function takes the data in dataframe from the labels in x_labels and y_label,
    and it formats them so that they can be properly passed to keras.
    Inputs:
        dataframe:
          A pandas dataframe
        x_labels:
          A list of strings containing the name of dataframe columns that have to 
          be formatted as an array of arrays.
        y_label:
          String with the name of the variable that will be used a target in the keras model
    Outputs:
        x_array:
          Array of arrays where each array corresponds to the data from dataset
          in the columns with names in x_labels.
        y_array: 
          Array with the data from the columns in dataset with name y_label
    """
    x_array = [dataframe.loc[:, i] for i in x_labels];
    y_array = dataframe[y_label]
    
    return x_array, y_array