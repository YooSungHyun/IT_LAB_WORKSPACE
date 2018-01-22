# -*- coding: utf-8 -*-
"""
Created on Wed Jan 17 17:54:26 2018

@author: Nikodemos
"""

import courses_data

import math
import os
import argparse

import logging
import logging.config
import json

def load_logger():
    with open('logging.json', 'rt') as f:
        config = json.load(f)
        
        logging.config.dictConfig(config)
        lg = logging.getLogger()
    return lg
    
def make_csv_files(save_dir='./data/',
                   rows_per_file = 50000):
    logger.info("Loading a DB Table")
    df = courses_data.load_courses()
    total_rows = df['SEQLEN'].count() #df 총 count.
    logger.info(str(total_rows)+" rows data was loaded")
    
    loop_cnt = math.ceil(total_rows / rows_per_file) # 반복카운터(파일갯수,올) = 총 Data 카운트 / 파일당 row 수
    
    if not os.path.exists(save_dir):
        os.mkdir(save_dir)
    
    file_prefix = save_dir + 'course_data_'
    
    start_pos = end_pos = 0
    for i in range(loop_cnt):
        start_pos = rows_per_file*i
        end_pos = start_pos + rows_per_file

        file_path = file_prefix + str(i) + '.bsv'
        df[start_pos:end_pos].to_csv(file_path,sep='|',index=False,header=False)

    logger.info(str(loop_cnt)+" files created.")
    
if __name__ == "__main__":
    
    # get a number of epochs by argument
    parser = argparse.ArgumentParser()
    parser.add_argument("rows_per_file", 
                        help="[integer]number of loading data rows", 
                        type=int)
    args = parser.parse_args()
    
    logger = load_logger()
    
    make_csv_files(rows_per_file=args.rows_per_file)
    