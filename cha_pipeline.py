#imports 
import os
from os import listdir, chdir, getcwd, popen
from os.path import isfile, join
import time
import math
from random import shuffle
import argparse
import sys
import configparser
import multiprocessing as mp
import concurrent.futures
import gcsfs
import re
import pandas as pd
import importlib
from datetime import datetime
from dateutil import parser
from bs4 import BeautifulSoup as BS  # Can parse xml or html docs
#custom imports
from src.xbrl_scraper.requests_scraper import XbrlScraper
from src.data_processing.xbrl_pd_methods import XbrlExtraction
from src.data_processing.xbrl_parser import XbrlParser
from src.data_processing.cst_data_processing import DataProcessing
from src.data_processing.xbrl_csv_cleaner import XbrlCSVCleaner
from src.data_processing.combine_csvfiles import XbrlCsvAppender
from src.validators.xbrl_validator_methods import XbrlValidatorMethods
from src.classifier.cst_classifier import Classifier

pd.set_option("display.max_columns", 500)

# Reads from the config file
cfg = configparser.ConfigParser()
cfg.read("cha_pipeline.cfg")

# Calling pipeline (based on booleans in config file)
# -------------------------------------------------
def main():
    print("-" * 50)
    #set GCP enviroment 
    os.environ["PROJECT_ID"] = cfg['gcsfs_setup']["project_id"]
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = cfg['gcsfs_setup']['key']
    fs = gcsfs.GCSFileSystem(project=cfg['gcsfs_setup']['project_id'], token=cfg['gcsfs_setup']['key'], cache_timeout=0)

    # Execute module xbrl_web_scraper
    if cfg['cha_workflow']['xbrl_web_scraper'] == str(True):
        print("XBRL web scraper running...")
        print("Scraping XBRL data to:", cfg['xbrl_web_scraper_args']['x_scraper_dir_to_save'])

        scraper = XbrlScraper()
        scraper.scrape_webpage(cfg['xbrl_web_scraper_args']['x_scraper_url'],
                               cfg['xbrl_web_scraper_args']['x_scraper_base_url'],
                               cfg['xbrl_web_scraper_args']['x_scraper_dir_to_save'])

    # Validate xbrl data
    if cfg['cha_workflow']['xbrl_validator'] == str(True):
        print("Validating xbrl web scraped data...")
        validator = XbrlValidatorMethods(fs)
        validator.validate_compressed_files(cfg['xbrl_validator_args']['x_validator_scraped_dir'])

    # Execute module xbrl_unpacker
    if cfg['cha_workflow']['xbrl_unpacker'] == str(True):
        print("XBRL unpacker running...")
        print("Reading zip file from directory: ", cfg['xbrl_unpacker_args']['x_unpacker_file_source_dir'])
        print("Unpacking zip file to directory: ", cfg['xbrl_unpacker_args']['x_unpacker_dir_to_save'])
        unpacker = DataProcessing(fs)
        unpacker.extract_compressed_files(cfg['xbrl_unpacker_args']['x_unpacker_file_source_dir'],
                                          cfg['xbrl_unpacker_args']['x_unpacker_dir_to_save'])

    # Execute module xbrl_parser
    if cfg['cha_workflow']['xbrl_parser'] == str(True):
        print("XBRL parser running...")
        parser_executer = XbrlParser(fs)
        parser_executer.parse_files(cfg['xbrl_parser_args']['x_parser_process_quarter'],
                                    cfg['xbrl_parser_args']['x_parser_process_year'],
                                    cfg['xbrl_parser_args']['x_unpacked_data'],
                                    cfg['xbrl_parser_args']['x_parser_custom_input'],
                                    cfg['xbrl_parser_args']['x_parser_bq_location'],
                                    cfg['xbrl_parser_args']['x_processed_csv'],
                                    cfg['xbrl_parser_args']['x_parser_no_of_cores'])

    # Execute module xbrl_csv_cleaner
    if cfg['cha_workflow']['xbrl_csv_cleaner'] == str(True):
        print("XBRL CSV cleaner running...")
        XbrlCSVCleaner.clean_parsed_files(cfg['xbrl_csv_cleaner_args']['x_csv_cleaner_indir'],
                                          cfg['xbrl_csv_cleaner_args']['x_csv_cleaner_outdir'])

    # Append XBRL data on an annual or quarterly basis
    if cfg['cha_workflow']['xbrl_file_appender'] == str(True):
        print("XBRL appender running...")
        appender = XbrlCsvAppender(fs)
        appender.merge_files_by_year(cfg['xbrl_file_appender_args']['x_file_appender_indir'],
                                     cfg['xbrl_file_appender_args']['x_file_appender_outdir'],
                                     cfg['xbrl_file_appender_args']['x_file_appender_year'],
                                     cfg['xbrl_file_appender_args']['x_file_appender_quarter'])

if __name__ == "__main__":
    #do we want to incorperate time metrics in some of our print statements within modules?
    process_start = time.time()

    main()

    print("-" * 50)
    print("Process Complete")
