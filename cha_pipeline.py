from os import listdir, chdir, getcwd, popen
from os.path import isfile, join
import time
import math
from random import shuffle
import argparse
import sys
# import cv2
import configparser
import multiprocessing as mp
import concurrent.futures

import os
import re
import numpy as np
import pandas as pd
import importlib

from datetime import datetime
from dateutil import parser
from bs4 import BeautifulSoup as BS  # Can parse xml or html docs
pd.set_option("display.max_columns",500)
config = configparser.ConfigParser()
config.read("cha_pipeline.cfg")

xbrl_web_scraper = config.get('cha_workflow', 'xbrl_web_scraper')
xbrl_web_scraper_validator = config.get('cha_workflow', 'xbrl_validator')
xbrl_unpacker = config.get('cha_workflow', 'xbrl_unpacker')
xbrl_parser = config.get('cha_workflow', 'xbrl_parser')
xbrl_file_appender = config.get('cha_workflow', 'xbrl_file_appender')
pdf_web_scraper = config.get('cha_workflow', 'pdf_web_scraper')
pdfs_to_images = config.get('cha_workflow', 'pdfs_to_images')
train_classifier_model = config.get('cha_workflow', 'train_classifier_model')
binary_classifier = config.get('cha_workflow', 'binary_classifier')
ocr_functions = config.get('cha_workflow', 'ocr_functions')
nlp_functions = config.get('cha_workflow', 'nlp_functions')
merge_xbrl_to_pdf_data = config.get('cha_workflow', 'merge_xbrl_to_pdf_data')

# Arguments for the XBRL web scraper
scraped_dir = config.get('xbrl_web_scraper_args', 'scraped_dir')
xbrl_scraper = config.get('xbrl_web_scraper_args', 'xbrl_scraper')

# Arguments for the XBRL web scraper validator
validator_scraped_dir = config.get('xbrl_validator_args', 'scraped_dir')

# Arguments for the XBRL unpacker
unpacker_source_dir = config.get('xbrl_unpacker_args', 'xbrl_unpacker_file_source_dir')
unpacker_destination_dir = config.get('xbrl_unpacker_args', 'xbrl_unpacker_file_destination_dir')

# Arguments for the XBRL parser
xbrl_unpacked_data = config.get('xbrl_parser_args', 'xbrl_parser_data_dir')
xbrl_processed_csv = config.get('xbrl_parser_args', 'xbrl_parser_processed_csv_dir')
xbrl_tag_frequencies = config.get('xbrl_parser_args', 'xbrl_parser_tag_frequencies')
xbrl_tag_list = config.get('xbrl_parser_args', 'xbrl_parser_tag_list')
xbrl_parser_process_year = config.get('xbrl_parser_args', 'xbrl_parser_process_year')
xbrl_parser_process_quarter = config.get('xbrl_parser_args', 'xbrl_parser_process_quarter')
xbrl_parser_custom_input = config.get('xbrl_parser_args', 'xbrl_parser_custom_input')

# Arguments for xbrl appender
xbrl_file_appender_indir = config.get('xbrl_file_appender_args', 'xbrl_file_appender_indir')
xbrl_file_appender_outdir = config.get('xbrl_file_appender_args', 'xbrl_file_appender_outdir')
xbrl_file_appender_year = config.get('xbrl_file_appender_args', 'xbrl_file_appender_year')
xbrl_file_appender_quarter = config.get('xbrl_file_appender_args', 'xbrl_file_appender_quarter')

# Arguments for xbrl melt to pivot table

# Arguments for xbrl subsets

# Arguments for the filing_fetcher scraper
filed_accounts_scraped_dir = config.get('pdf_web_scraper_args', 'filed_accounts_scraped_dir')
filed_accounts_scraper = config.get('pdf_web_scraper_args', 'filed_accounts_scraper')

# Arguments for pdfs_to_images

# Arguments for train_classifier_model

# Arguments for binary_classifier

# Arguments for binary_classifier_accuracy

# Arguments for ocr_functions

# Arguments for nlp_functions

# Arguments for merge_xbrl_to_pdf_data

from src.data_processing.cst_data_processing import DataProcessing
from src.classifier.cst_classifier import Classifier
from src.performance_metrics.binary_classifier_metrics import BinaryClassifierMetrics
from src.data_processing.xbrl_pd_methods import XbrlExtraction
from src.data_processing.xbrl_parser import XbrlParser
from src.validators.xbrl_validator_methods import XbrlValidatorMethods
from src.data_processing.combine_csvfiles import XbrlCsvAppender

def main():
    print("-" * 50)

    # Execute module xbrl_web_scraper
    if xbrl_web_scraper == str(True):
        print("XBRL web scraper running...")
        print("Scraping XBRL data to:", scraped_dir)
        print("Running crawler from:", xbrl_scraper)
        chdir(xbrl_scraper)
        print(getcwd())
        cmdlinestr = "scrapy crawl xbrl_scraper"
        popen(cmdlinestr).read()

    # Validate xbrl data
    if xbrl_web_scraper_validator == str(True):
        validator = XbrlValidatorMethods()
        print("Validating xbrl web scraped data...")
        validator.validate_compressed_files(validator_scraped_dir)

    # Execute module xbrl_unpacker
    if xbrl_unpacker == str(True):
        print("XBRL unpacker running...")
        print("Unpacking zip files...")
        print("Reading from directory: ", unpacker_source_dir)
        print("Writing to directory: ", unpacker_destination_dir)
        unpacker = DataProcessing()
        unpacker.extract_compressed_files(unpacker_source_dir, unpacker_destination_dir)

    # Execute module xbrl_parser
    if xbrl_parser == str(True):
        print("XBRL parser running...")
        
        extractor = XbrlExtraction()
        parser = XbrlParser()

        # Create a list of months based on what quarter in the year has been specified
        if xbrl_parser_process_quarter == "1":
            month_list = ['January', 'February', 'March']
        elif xbrl_parser_process_quarter == "2":
            month_list = ['April', 'May', 'June']
        elif xbrl_parser_process_quarter == "3":
            month_list = ['July', 'August', 'September']
        elif xbrl_parser_process_quarter == "4":
            month_list = ['October', 'November', 'December']
        else:
            month_list = ['January', 'February', 'March', 'April', 'May', 'June',
                          'July', 'August', 'September', 'October', 'November', 'December']
            if xbrl_parser_process_quarter != "None":
                print("Invalid quarter specified...processing one year of data!")

        # Create a list of directories from each month present in the month list
        directory_list = []
        if xbrl_parser_custom_input == "None":
            for month in month_list:
                directory_list.append(xbrl_unpacked_data
                                      + "/Accounts_Monthly_Data-"
                                      + month
                                      + xbrl_parser_process_year)

        # If a custom list has been specified as a comma separated string, use this instead
        else:
            folder_list = xbrl_parser_custom_input.split(",")
            for folder in folder_list:
                directory_list.append(xbrl_unpacked_data + "/" + folder)

        for directory in directory_list:

            print("Parsing " + directory + "...")

            # Get all the filenames from the example folder
            files, folder_month, folder_year = extractor.get_filepaths(directory)

            print(len(files))

            '''
            ### Targets largest files
            #limit to moderate amount of files
            files = files[0:50000]
            # Here you can splice/truncate the number of files you want to process for testing
            # TO BE COMMENTED OUT AFTER TESTING
            filesize = []
            for i in range(len(files)):
                filesize.append((files[i],os.path.getsize(files[i])))

            filesize.sort(key = lambda filesize: filesize[1],reverse=True)
            
            for i in range(len(filesize)):
                files[i] = filesize[i][0]
                
            total_files = len(files)
            shuffle(files)
            files = files
            '''
            files = files[0:30]

            # TO BE COMMENTED OUT AFTER TESTING
            print(folder_month, folder_year)

            # Code needed to split files by the number of cores before passing in as an argument
            num_processes = 3
            chunk_len = math.ceil(len(files) / num_processes)
            files = [files[i:i + chunk_len] for i in range(0, len(files), chunk_len)]

            #define number of processors
            pool = mp.Pool(processes=num_processes)
            # Finally, build a table of all variables from all example (digital) documents
            # splitting the load between cpu cores = num_processes
            # This can take a while (hopefully not anymore!!!)
            r = pool.map(extractor.build_month_table, files)
            pool.close()
            pool.join()
            #combine resultant list of lists
            r = [item for sublist in r for item in sublist]

            #combine data and convert into dataframe
            results = parser.flatten_data(r)
            print(results.shape)

            #save to csv
            extractor.output_xbrl_month(results, xbrl_processed_csv, folder_month, folder_year)

            # Find list of all unique tags in dataset
            list_of_tags = results["name"].tolist()
            list_of_tags_unique = list(set(list_of_tags))

            print("Longest tag: ", len(max(list_of_tags_unique, key=len)))

            # Output all unique tags to a txt file

            ## Commented out while testing parser changes
            
            extractor.retrieve_list_of_tags(
                results,
                "name",
                xbrl_tag_list,
                folder_month,
                folder_year
            )

            # Output all unique tags and their relative frequencies to a txt file
            extractor.get_tag_counts(
                results,
                "name",
                xbrl_tag_frequencies,
                folder_month,
                folder_year
            )

            # print(results.shape)

        #tempcsv = pd.read_csv("/shares/xbrl_parsed_data/2020-April_xbrl_data.csv", lineterminator='\n')
        #print(tempcsv.head(5000000))
        #print(tempcsv.shape)

    # Append XBRL data on an annual or quarterly basis
    if xbrl_file_appender == str(True):
        appender = XbrlCsvAppender()
        print("XBRL appender running...")
        appender.merge_files_by_year(xbrl_file_appender_indir,
                                     xbrl_file_appender_outdir,
                                     xbrl_file_appender_year,
                                     xbrl_file_appender_quarter)
    """
    # Execute PDF web scraper
    if pdf_web_scraper == str(True):
        print("PDF web scraper running...")
        print("Scraping filed accounts as PDF data to:", filed_accounts_scraped_dir)
        print("Running crawler from:", filed_accounts_scraper)
        chdir(filed_accounts_scraper)
        print(getcwd())
        paper_filing_cmdlinestr = "scrapy crawl latest_paper_filing"
        popen(paper_filing_cmdlinestr).read()

    # Convert PDF files to images
    if pdfs_to_images == str(True):
        print("Converting all PDFs to images...")

    # Train the Classifier model
    if train_classifier_model == str(True):
        print("Training classifier model...")

    # Execute binary Classifier
    if binary_classifier == str(True):
        print("Executing binary classifier...")

    # Execute OCR
    if ocr_functions == str(True):
        print("Running all OCR functions...")
        # instance to class
        
    # Execute NLP
    if nlp_functions == str(True):
        print("Running all NLP functions...")

    # Merge xbrl and PDF file data
    if merge_xbrl_to_pdf_data == str(True):
        print("Merging XBRL and PDF data...")

 
    # construct the argument parse and parse the arguments
    ap = argparse.ArgumentParser()
    ap.add_argument("-d", "--input_imgs", required = True,
        help = "path to a directory containing images to be used as input data")
    ap.add_argument("-c", "--cascade_file", required = True,
        help = "path to cascade classifier.")
    ap.add_argument("-s", "--scale_factor", required = True,
        help = "cascade classifier scale factor.")
    ap.add_argument("-m", "--min_neighbors", required = True,
        help = "cascade classifier min neighbors.")
    ap.add_argument("-x", "--cascade_width", required = True,
        help = "cascade classifier starting sample width.")
    ap.add_argument("-y", "--cascade_height", required = True,
        help = "cascade classifier starting sample height.")
    ap.add_argument("-o", "--classifier_output_dir", required = True,
        help = "path to classifier output.")
    ap.add_argument("-p", "--processed_images_dir", required = True,
        help = "path to images processed by the classifier.")
    ap.add_argument("-v", "--show_classifier_output", required = False,
        help = "display the output of the classifier to the user.")
    args = vars(ap.parse_args())

    detector = cv2.CascadeClassifier(args["cascade_file"])

    # load the input image and convert it to grayscale
    for image in listdir(args["input_imgs"]):
        img = Classifier.classifier_input(join(args["input_imgs"], image))
        grey = Classifier.imgs_to_grey(img)

        rects = Classifier.ensemble(detector,
                           grey,
                           float(args["scale_factor"]),
                           int(args["min_neighbors"]),
                           int(args["cascade_width"]),
                           int(args["cascade_height"]))

        print("\n[INFO] Found " + str(Classifier.count_classifier_output(rects)) + " companies house stamps.")

        Classifier.classifier_output(rects, img, args["classifier_output_dir"])

        Classifier.classifier_process(img, join(args["processed_images_dir"], image))

        if args["show_classifier_output"] == "True": 
            Classifier.display_classifier_output(image, img)
            cv2.waitKey(0)
            cv2.destroyAllWindows()
        else:
            pass
    """
if __name__ == "__main__":
    process_start = time.time()

    main()

    print("-" * 50)
    print("Process Complete")
    print("The time taken to process an image is: ", "{}".format((time.time() - process_start) / 60, 2), " minutes")
