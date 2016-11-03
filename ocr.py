# -*- coding: utf-8 -*-
"""
Created on Wed Nov  2 10:28:18 2016
@author: chyam
purpose: load images from blob storage, post to Microsoft OCR, save results, populate 'text' to .tsv file.
"""
from __future__ import print_function
from azure.storage.blob import BlockBlobService
import configparser
import time
import requests
import json
import pandas as pd
from io import StringIO

_url = 'https://api.projectoxford.ai/vision/v1.0/ocr'
_maxNumRetries = 10

def main():
    # Get credential

    parser = configparser.ConfigParser()
    parser.read('config.ini')
    
    # access to blob storage
    block_blob_service = BlockBlobService(account_name=parser.get('credential', 'STORAGE_ACCOUNT_NAME_2'), account_key=parser.get('credential', 'STORAGE_ACCOUNT_KEY_2'))
    generator = block_blob_service.list_blobs(parser.get('credential', 'CONTAINER_NAME_2'))
   
    # empty dataframe
    df = pd.DataFrame({'Text' : [], 'Category' : [], 'ReceiptID' : []})
    
    # index
    index = 0
    for blob in generator:
        print(blob.name)
#        if blob.name == 'receipt_00000.JPG': # just for testing, save allowance, remove this later
        #imageurl = "https://atstrdtmuswdmo.blob.core.windows.net:443/ml-hackaton-receipts" + "/" + blob.name; print(imageurl)
        
        imageurl = "https://" + parser.get('credential', 'STORAGE_ACCOUNT_NAME_2') + ".blob.core.windows.net/" + parser.get('credential', 'CONTAINER_NAME_2') + "/" + blob.name; print(imageurl)
        
        # OCR parameters
        params = { 'language': 'en', 'detectOrientation ': 'true'} 
        
        headers = dict()
        headers['Ocp-Apim-Subscription-Key'] = parser.get('credential', 'VISION_API_KEY') 
        headers['Content-Type'] = 'application/json' 
        
        image_url = { 'url': imageurl } ; 
        image_file = None
        result = processRequest( image_url, image_file, headers, params )
        
        if result is not None:
            #print(result)
            result_str = json.dumps(result); #print(result_str)
            
            # write result into blob
            ocrblobname = blob.name[:-3] + 'json'
            block_blob_service.create_blob_from_text(parser.get('credential', 'CONTAINER_NAME_3'), ocrblobname, result_str)

            # extract text
            text = extractText(result); #print (text)
            
            # populate dataframe
            df.loc[index,'Text'] = text
        else:
            # populate dataframe
            df.loc[index,'Text'] = None
                        
        df.loc[index,'Category'] = 'catogory' ## !! need to get this from excel file
        df.loc[index,'ReceiptID'] = blob.name
 
        index = index + 1
            
    # write dataframe to blob
    print("-----------------------")
    df_str = df.to_csv(sep='\t', index=False); 
    
    # NEED THIS LATER TO READ INTO DATAFRAME
    #df_read = pd.DataFrame.from_csv(StringIO(df_str), index_col=None, sep='\t'); 
    dfblobname = 'dataframe.tsv' ## !! need to turn to string?
    block_blob_service.create_blob_from_text(parser.get('credential', 'CONTAINER_NAME_4'), dfblobname, df_str) # !! Might have problem

    return
    
def extractText(result):
    text = ""
    for region in result['regions']:
        for line in region['lines']:
            for word in line['words']:
                #print (word.get('text'))
                text = text + " " + word.get('text')
    return text
    
def processRequest( image_url, image_file, headers, params ):

    """
    Ref: https://github.com/Microsoft/Cognitive-Vision-Python/blob/master/Jupyter%20Notebook/Computer%20Vision%20API%20Example.ipynb
    Helper function to process the request to Project Oxford
    Parameters:
    json: Used when processing images from its URL. See API Documentation
    data: Used when processing image read from disk. See API Documentation
    headers: Used to pass the key information and the data type request
    """

    retries = 0
    result = None

    while True:

        response = requests.request( 'post', _url, json = image_url, data = image_file, headers = headers, params = params )
        
        if response.status_code == 429: 

            print( "Message: %s" % ( response.json()['message'] ) )

            if retries <= _maxNumRetries: 
                time.sleep(1) 
                retries += 1
                continue
            else: 
                print( 'Error: failed after retrying!' )
                break

        elif response.status_code == 200 or response.status_code == 201:

            if 'content-length' in response.headers and int(response.headers['content-length']) == 0: 
                result = None 
            elif 'content-type' in response.headers and isinstance(response.headers['content-type'], str): 
                if 'application/json' in response.headers['content-type'].lower(): 
                    result = response.json() if response.content else None 
                elif 'image' in response.headers['content-type'].lower(): 
                    result = response.content
        else:
            print(response.json()) 
            print( "Error code: %d" % ( response.status_code ) ); 
            print( "Message: %s" % ( response.json()['message'] ) ); 

        break
        
    return result
    
if __name__ == '__main__':
    main() 