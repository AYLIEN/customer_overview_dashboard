#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Apr 15 12:26:07 2020

@author: floyd
"""

import pandas as pd
import requests
import time
from tqdm import tqdm
import datetime as dt
import multiprocessing as multip
import numpy as np
import os

def calc_day(date_diff=0):
    today = dt.datetime.today()
    day = today-dt.timedelta(days=date_diff)
    day_string = dt.datetime.strftime(day, '%Y-%m-%dT00:00:00Z')
    return day_string

def trends(src_id, param):
    """
    A function that takes the a list of query parameter values and runs a trends
    query on the Aylien API according to the input parameters.


    """
    today = calc_day(1)
    first_day = calc_day(8)
    headers = {'X-AYLIEN-NewsAPI-Application-ID': 'ihs',
               'X-AYLIEN-NewsAPI-Application-Key': 'sgf4ZTtEuBkGSLpc'
              }
    parameters = {'source.id': src_id
                 , 'published_at.start': first_day
                 , 'published_at.end': today
                 , 'categories.level': '1'
                 , 'categories.taxonomy': 'iptc-subjectcode'
                 , 'field': param
                  }
    url = "http://api.newsapi-dd01.aylien.com/api/v1/trends"
    timeout_flag = 0
    while timeout_flag == 0:
        try:
            response = requests.get(url=url, headers=headers, params=parameters)
            timeout_flag += 1
        except TimeoutError:
            print('Connection times out. Resting for a few seconds')
            time.sleep(5)
            pass
    if response.status_code != 200:
        status = str(response.status_code)
        if response.status_code == 429:
            time.sleep(30)
            print("API Error: {}".format(status))
        result = 'No data found'
    else:
        result = response.json()
        return result

def timeSeries(src_id):
    """
    A function that takes a sorce id and runs a time
    series query on the Aylien API according to the input id.
    """
    today = calc_day(0)
    first_day = calc_day(7)
    headers = {'X-AYLIEN-NewsAPI-Application-ID': 'ihs',
               'X-AYLIEN-NewsAPI-Application-Key': 'sgf4ZTtEuBkGSLpc'
              }
    parameters = {'source.id': src_id
                  ,'published_at.start': first_day
                  ,'published_at.end': today
                  ,'period': '+7DAYS'
                  }
    url = "http://api.newsapi-dd01.aylien.com/api/v1/time_series"
    response = requests.get(url=url, headers=headers, params=parameters)
    if response.status_code != 200:
        status = str(response.status_code)
        if response.status_code == 429:
            time.sleep(30)
            print("API Error: {}".format(status))
        result = 'No data found'
    else:
        result = response.json()
        return(result)

def data_framing(data, sources, header, trend):
    """
    A function that takes in json data and formats it to a pandas data frame.
    """
    trend = [x[trend] for x in data]
    trends_cat = []
    trends_counts = []
    sources_final = []
    for i, tr in tqdm(enumerate(trend)):
        if tr:
            for t in tr:
                sources_final.append(sources[i])
                trends_cat.append(t[header])
                trends_counts.append(t['count'])
    result_frame = pd.DataFrame(list(zip(sources_final, trends_cat, trends_counts)),
                                columns=['source_id', header, 'counts'])
    return result_frame
        

def main():
    source_ids = list(pd.read_csv('../source ids.csv')['ID'])
    #first, obtain category trends
    cat_trends = []
    for src in tqdm(source_ids):
        trend = trends(src, 'categories.id')
        cat_trends.append(trend)
    cat_frame = data_framing(cat_trends, source_ids, 'value', 'trends')
    #map the category codes to category names
    categories_all = pd.read_csv('../categories.csv')
    categories_all['category']=[x.replace("'", '').replace('’', '') for x in categories_all['category']]
    cat_frame_new = cat_frame.merge(categories_all, how='left', left_on='value'
                                    , right_on='category')
    cat_frame_final = cat_frame_new[['source_id', 'category_name', 'counts']]
    cat_frame_final.to_csv('ihs_cat_facts.csv', header=None, index=None)
    #next, grab the language trends
    lang_trends = []
    for src in tqdm(source_ids):
        trend = trends(src, 'language')
        lang_trends.append(trend)
    lang_frame = data_framing(lang_trends, source_ids, 'value', 'trends')
    lang_frame.to_csv('ihs_lang_facts.csv', header=None, index=None)
    #finally, grab the overall story counts sources
    story_trends = []
    for src in tqdm(source_ids):
        trend = timeSeries(src)
        story_trends.append(trend)
    story_frame = data_framing(story_trends, source_ids, 'published_at', 'time_series')
    story_frame = story_frame[['source_id', 'counts']]
    story_frame.to_csv('ihs_story_counts.csv', header=None, index=None)
    
    #Upload the data to BQ, replacing what is already there with the new data
    bq_loc = '/home/floyd/Documents/Projects/google_cloud/google-cloud-sdk/bin/bq'
    os.system(bq_loc+" query --use_legacy_sql=false 'DELETE FROM `aylien-production`.customer_report.ihs_facts_category WHERE 1=1'")
    os.system(bq_loc+' load --source_format=CSV customer_report.ihs_facts_category ihs_cat_facts.csv')
    
    os.system(bq_loc+" query --use_legacy_sql=false 'DELETE FROM `aylien-production`.customer_report.ihs_facts_language WHERE 1=1'")
    os.system(bq_loc+' load --source_format=CSV customer_report.ihs_facts_language ihs_lang_facts.csv')

    os.system(bq_loc+" query --use_legacy_sql=false 'DELETE FROM `aylien-production`.customer_report.ihs_facts_stories WHERE 1=1'")
    os.system(bq_loc+' load --source_format=CSV customer_report.ihs_facts_stories ihs_story_counts.csv')

main()  
    
    
