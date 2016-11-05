# -*- coding: utf-8 -*-
"""
Created on Sat Nov  5 10:35:35 2016

@author: chyam
Purpose: prepare sturctured-data for azure ml. load from blob, clean, i) write to blob ii) pass to a azure ml learner
"""

from __future__ import print_function
from azure.storage.blob import BlockBlobService
import configparser
import pandas as pd
from io import StringIO

def main():
    
    # Get credential
    parser = configparser.ConfigParser()
    parser.read('config.ini')
    STORAGE_ACCOUNT_NAME = parser.get('credential', 'STORAGE_ACCOUNT_NAME')    
    STORAGE_ACCOUNT_KEY = parser.get('credential', 'STORAGE_ACCOUNT_KEY')
    CONTAINER_NAME_STRUCTUREDDATA = parser.get('credential', 'CONTAINER_NAME_STRUCTUREDDATA')

    # access to blob storage
    block_blob_service = BlockBlobService(account_name=STORAGE_ACCOUNT_NAME, account_key=STORAGE_ACCOUNT_KEY)
    generator = block_blob_service.list_blobs(CONTAINER_NAME_STRUCTUREDDATA)  # only one file 'dataframe.tsv'

    for blob in generator: #only one file 'dataframe.tsv'
        # load structured data from blob
        print(blob.name)
        blob_text = block_blob_service.get_blob_to_text(CONTAINER_NAME_STRUCTUREDDATA, blob.name);  #print(blob_text.content)
        # clean df
        df = pd.DataFrame.from_csv(StringIO(blob_text.content), index_col=None, sep='\t'); print(df.shape)
        df_clean = df.dropna(); print(df_clean.shape)
        
    # write cleaned dataframe to blob
    print("-----------write cleaned dataframe to blob------------")
    df_clean_str = df_clean.to_csv(sep='\t', index=False); 
    
    dfblobname = 'dataframe_cleaned.tsv' 
    block_blob_service.create_blob_from_text(CONTAINER_NAME_STRUCTUREDDATA, dfblobname, df_clean_str)

    return

if __name__ == '__main__':
    main() 