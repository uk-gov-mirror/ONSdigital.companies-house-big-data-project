from os import listdir, chdir, getcwd, popen
from os.path import isfile, join
import time
import argparse
import sys
# import cv2
import configparser

import os
import re
import numpy as np
import pandas as pd
import importlib

from datetime import datetime
from dateutil import parser
from bs4 import BeautifulSoup as BS  # Can parse xml or html docs

config = configparser.ConfigParser()
config.read("cha_pipeline.cfg")

xbrl_web_scraper = config.get('cha_workflow', 'xbrl_web_scraper')
xbrl_unpacker = config.get('cha_workflow', 'xbrl_unpacker')
xbrl_parser = config.get('cha_workflow', 'xbrl_parser')
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

# Arguments for the XBRL unpacker
unpacker_source_dir = config.get('xbrl_unpacker_args', 'xbrl_unpacker_file_source_dir')
unpacker_destination_dir = config.get('xbrl_unpacker_args', 'xbrl_unpacker_file_destination_dir')

# Arguments for the XBRL parser
xbrl_unpacked_data = config.get('xbrl_parser_args', 'xbrl_data_dir')
xbrl_processed_csv = config.get('xbrl_parser_args', 'xbrl_processed_csv_dir')
xbrl_tag_frequencies = config.get('xbrl_parser_args', 'xbrl_tag_frequencies')
xbrl_tag_list = config.get('xbrl_parser_args', 'xbrl_tag_list')

# Arguments for OCR runner
ocr_image_dir = config.get('ocr_args', 'ocr_image_dir')
ocr_text_dir = config.get('ocr_args', 'ocr_text_dir')
ocr_image_dir_cs = config.get('ocr_args', 'ocr_image_dir_cs')
ocr_text_dir_cs = config.get('ocr_args', 'ocr_text_dir_cs')
ocr_im_type = config.get('ocr_args', 'ocr_im_type')
ocr_im_type_cs = config.get('ocr_args', 'ocr_im_type_cs')
tessdata_prefix = config.get('ocr_args', 'tessdata_prefix')

from src.data_processing.cst_data_processing import DataProcessing
from src.classifier.cst_classifier import Classifier
from src.performance_metrics.binary_classifier_metrics import BinaryClassifierMetrics
from src.data_processing.xbrl_pd_methods import XbrlExtraction
from src.ocr.ocr_runner import RunOCR

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
        
        
        # Table of variables and values that indicate consolidated status
        consolidation_var_table = {
            "includedinconsolidationsubsidiary": True,
            "investmententityrequiredtoapplyexceptionfromconsolidationtruefalse": True,
            "subsidiaryunconsolidatedtruefalse": False,
            "descriptionreasonwhyentityhasnotpreparedconsolidatedfinancialstatements": "exist",
            "consolidationpolicy": "exist"
        }

        def clean_value(string):
            """
            Take a value that's stored as a string,
            clean it and convert to numeric.

            If it's just a dash, it's taken to mean
            zero.
            """
            if string.strip() == "-":
                return (0.0)

            try:
                return float(string.strip().replace(",", "").replace(" ", ""))
            except:
                pass

            return (string)

        def retrieve_from_context(soup, contextref):
            """
            Used where an element of the document contained no data, only a
            reference to a context element.
            Finds the relevant context element and retrieves the relevant data.

            Returns a text string

            Keyword arguments:
            soup -- BeautifulSoup souped html/xml object
            contextref -- the id of the context element to be raided
            """
            try:
                context = soup.find("xbrli:context", id=contextref)
                contents = context.find("xbrldi:explicitmember").get_text().split(":")[-1].strip()

            except:
                contents = ""

            return (contents)

        def retrieve_accounting_standard(soup):
            """
            Gets the account reporting standard in use in a document by hunting
            down the link to the schema reference sheet that always appears to
            be in the document, and extracting the format and standard date from
            the string of the url itself.
            WARNING - That means that there's a lot of implicity hardcoded info
            on the way these links are formated and referenced, within this
            function.  Might need changing someday.

            Returns a 3-tuple (standard, date, original url)

            Keyword arguments:
            soup -- BeautifulSoup souped html/xml object
            """

            # Find the relevant link by its unique attribute
            link_obj = soup.find("link:schemaref")

            # If we didn't find anything it's an xml doc using a different
            # element name:
            if link_obj == None:
                link_obj = soup.find("schemaref")

            # extract the name of the .xsd schema file, which contains format
            # and date information
            text = link_obj['xlink:href'].split("/")[-1].split(".")[0]

            # Split the extracted text into format and date, return values
            return (text[:-10].strip("-"), text[-10:], link_obj['xlink:href'])







        def retrieve_unit(soup, each):
            """
            Gets the reporting unit by trying to chase a unitref to
            its source, alternatively uses element attribute unitref
            if it's not a reference to another element.

            Returns the unit

            Keyword arguments:
            soup -- BeautifulSoup souped html/xml object
            each -- element of BeautifulSoup souped object
            """

            # If not, try to discover the unit string in the
            # soup object
            try:
                unit_str = soup.find(id=each['unitref']).get_text()

            except:
                # Or if not, in the attributes of the element
                try:
                    unit_str = each.attrs['unitref']

                except:
                    return ("NA")

            return (unit_str.strip())

        def retrieve_date(soup, each):
            """
            Gets the reporting date by trying to chase a contextref
            to its source and extract its period, alternatively uses
            element attribute contextref if it's not a reference
            to another element.
            Returns the date

            Keyword arguments:
            soup -- BeautifulSoup souped html/xml object
            each -- element of BeautifulSoup souped object
            """

            # Try to find a date tag within the contextref element,
            # starting with the most specific tags, and starting with
            # those for ixbrl docs as it's the most common file.
            date_tag_list = ["xbrli:enddate",
                             "xbrli:instant",
                             "xbrli:period",
                             "enddate",
                             "instant",
                             "period"]

            for tag in date_tag_list:
                try:
                    date_str = each['contextref']
                    date_val = parser.parse(soup.find(id=each['contextref']).find(tag).get_text()). \
                        date(). \
                        isoformat()
                    return (date_val)
                except:
                    pass

            try:
                date_str = each.attrs['contextref']
                date_val = parser.parse(each.attrs['contextref']). \
                    date(). \
                    isoformat()
                return (date_val)
            except:
                pass

            return ("NA")

        def parse_element(soup, element):
            """
            For a discovered XBRL tagged element, go through, retrieve its name
            and value and associated metadata.

            Keyword arguments:
            soup -- BeautifulSoup object of accounts document
            element -- soup object of discovered tagged element
            """

            if "contextref" not in element.attrs:
                return ({})

            element_dict = {}

            # Basic name and value
            try:
                # Method for XBRLi docs first
                element_dict['name'] = element.attrs['name'].lower().split(":")[-1]
            except:
                # Method for XBRL docs second
                element_dict['name'] = element.name.lower().split(":")[-1]

            element_dict['value'] = element.get_text()
            element_dict['unit'] = retrieve_unit(soup, element)
            element_dict['date'] = retrieve_date(soup, element)

            # If there's no value retrieved, try raiding the associated context data
            if element_dict['value'] == "":
                element_dict['value'] = retrieve_from_context(soup, element.attrs['contextref'])

            # If the value has a defined unit (eg a currency) convert to numeric
            if element_dict['unit'] != "NA":
                element_dict['value'] = clean_value(element_dict['value'])

            # Retrieve sign of element if exists
            try:
                element_dict['sign'] = element.attrs['sign']

                # if it's negative, convert the value then and there
                if element_dict['sign'].strip() == "-":
                    element_dict['value'] = 0.0 - element_dict['value']
            except:
                pass

            return (element_dict)

        def parse_elements(element_set, soup):
            """
            For a set of discovered elements within a document, try to parse
            them.  Only keep valid results (test is whether field "name"
            exists).

            Keyword arguments:
            element_set -- BeautifulSoup iterable search result object
            soup -- BeautifulSoup object of accounts document
            """
            elements = []
            for each in element_set:
                element_dict = parse_element(soup, each)
                if 'name' in element_dict:
                    elements.append(element_dict)
            return (elements)

        def summarise_by_sum(doc, variable_names):
            """
            Takes a document (dict) after extraction, and tries to extract
            a summary variable relating to the financial state of the enterprise
            by summing all those named that exist.  Returns dict.

            Keyword arguments:
            doc -- an extracted document dict, with "elements" entry as created
                   by the 'scrape_clean_elements' functions.
            variable_names - variables to find and sum if they exist
            """

            # Convert elements to pandas df
            df = pd.DataFrame(doc['elements'])

            # Subset to most recent (latest dated)
            df = df[df['date'] == doc['doc_balancesheetdate']]

            total_assets = 0.0
            unit = "NA"

            # Find the total assets by summing components
            for each in variable_names:

                # Fault-tolerant, will skip whatever isn't numeric
                try:
                    total_assets = total_assets + df[df['name'] == each].iloc[0]['value']

                    # Retrieve reporting unit if exists
                    unit = df[df['name'] == each].iloc[0]['unit']

                except:
                    pass

            return ({"total_assets": total_assets, "unit": unit})

        def summarise_by_priority(doc, variable_names):
            """
            Takes a document (dict) after extraction, and tries to extract
            a summary variable relating to the financial state of the enterprise
            by looking for each named, in order.  Returns dict.

            Keyword arguments:
            doc -- an extracted document dict, with "elements" entry as created
                   by the 'scrape_clean_elements' functions.
            variable_names - variables to find and check if they exist.
            """

            # Convert elements to pandas df
            df = pd.DataFrame(doc['elements'])

            # Subset to most recent (latest dated)
            df = df[df['date'] == doc['doc_balancesheetdate']]

            primary_assets = 0.0
            unit = "NA"

            # Find the net asset/liability variable by hunting names in order
            for each in variable_names:
                try:

                    # Fault tolerant, will skip whatever isn't numeric
                    primary_assets = df[df['name'] == each].iloc[0]['value']

                    # Retrieve reporting unit if it exists
                    unit = df[df['name'] == each].iloc[0]['unit']
                    break

                except:
                    pass

            return ({"primary_assets": primary_assets, "unit": unit})

        def summarise_set(doc, variable_names):
            """
            Takes a document (dict) after extraction, and tries to extract
            summary variables relating to the financial state of the enterprise
            by returning all those named that exist.  Returns dict.

            Keyword arguments:
            doc -- an extracted document dict, with "elements" entry as created
                   by the 'scrape_clean_elements' functions.
            variable_names - variables to find and return if they exist.
            """
            results = {}

            # Convert elements to pandas df
            df = pd.DataFrame(doc['elements'])

            # Subset to most recent (latest dated)
            df = df[df['date'] == doc['doc_balancesheetdate']]

            # Find all the variables of interest should they exist
            for each in variable_names:
                try:
                    results[each] = df[df['name'] == each].iloc[0]['value']

                except:
                    pass

            # Send the variables back to be appended
            return (results)

        def scrape_elements(soup, filepath):
            """
            Parses an XBRL (xml) company accounts file
            for all labelled content and extracts the
            content (and metadata, eg; unitref) of each
            element found to a dictionary

            params: filepath (str)
            output: list of dicts
            """

            # Try multiple methods of retrieving data, I think only the first is
            # now needed though.  The rest will be removed after testing this
            # but should not affect execution speed.
            try:
                element_set = soup.find_all()
                elements = parse_elements(element_set, soup)
                if len(elements) <= 5:
                    raise Exception("Elements should be gte 5, was {}".format(len(elements)))
                return (elements)
            except:
                pass

            return (0)

        def flatten_data(doc):
            """
            Takes the data returned by process account, with its tree-like
            structure and reorganises it into a long-thin format table structure
            suitable for SQL applications.
            """

            # Need to drop components later, so need copy in function
            doc2 = doc.copy()
            doc_df = pd.DataFrame()

            # Pandas should create series, then columns, from dicts when called
            # like this
            for element in doc2['elements']:
                doc_df = doc_df.append(element, ignore_index=True)

            # Dump the "elements" entry in the doc dict
            doc2.pop("elements")

            # Create uniform columns for all other properties
            for key in doc2:
                doc_df[key] = doc2[key]

            return (doc_df)

        def process_account(filepath):
            """
            Scrape all of the relevant information from
            an iXBRL (html) file, upload the elements
            and some metadata to a mongodb.

            Named arguments:
            filepath -- complete filepath (string) from drive root
            """
            doc = {}

            # Some metadata, doc name, upload date/time, archive file it came from
            doc['doc_name'] = filepath.split("/")[-1]
            doc['doc_type'] = filepath.split(".")[-1].lower()
            doc['doc_upload_date'] = str(datetime.now())
            doc['arc_name'] = filepath.split("/")[-2]
            doc['parsed'] = True

            # Complicated ones
            sheet_date = filepath.split("/")[-1].split(".")[0].split("_")[-1]
            doc['doc_balancesheetdate'] = datetime.strptime(sheet_date, "%Y%m%d").date().isoformat()

            doc['doc_companieshouseregisterednumber'] = filepath.split("/")[-1].split(".")[0].split("_")[-2]

        #     print(filepath)

            try:
                soup = BS(open(filepath, "rb"), "html.parser")
            except:
                print("Failed to open: " + filepath)
                return (1)

            # Get metadata about the accounting standard used
            try:
                doc['doc_standard_type'], doc['doc_standard_date'], doc['doc_standard_link'] = retrieve_accounting_standard(
                    soup)
            except:
                doc['doc_standard_type'], doc['doc_standard_date'], doc['doc_standard_link'] = (0, 0, 0)

            # Fetch all the marked elements of the document
            try:
                doc['elements'] = scrape_elements(soup, filepath)
            except Exception as e:
                doc['parsed'] = False
                doc['Error'] = e

            try:
                return (doc)
            except Exception as e:
                return (e)

        def get_filepaths(directory):

            """ Helper function -
            Get all of the filenames in a directory that
            end in htm* or xml.
            Under the assumption that all files within
            the folder are financial records. """

            files = [directory + "/" + filename
                        for filename in os.listdir(directory)
                            if (("htm" in filename.lower()) or ("xml" in filename.lower()))]

            month_and_year = ('').join(directory.split('-')[-1:])
            month, year = month_and_year[:-4], month_and_year[-4:]

            return files, month, year

        def progressBar(name, value, endvalue, bar_length = 50, width = 20):
                percent = float(value) / endvalue
                arrow = '-' * int(round(percent*bar_length) - 1) + '>'
                spaces = ' ' * (bar_length - len(arrow))
                sys.stdout.write(
                    "\r{0: <{1}} : [{2}]{3}%   ({4} / {5})".format(
                        name,
                        width,
                        arrow + spaces,
                        int(round(percent*100)),
                        value,
                        endvalue
                    )
                )
                sys.stdout.flush()
                if value == endvalue:
                    sys.stdout.write('\n\n')

        def retrieve_list_of_tags(dataframe, column, output_folder):
            """
            Save dataframe containing all unique tags to txt format in specified directory.

            Arguements:
                dataframe:     tabular data
                column:        location of xbrl tags
                output_folder: user specified file location
            Returns:
                None
                Raises:
                None
            """
            list_of_tags = dataframe['name'].tolist()
            list_of_tags_unique = list(set(list_of_tags))

            print(
                "Number of tags in total: {}\nOf which are unique: {}".format(len(list_of_tags), len(list_of_tags_unique))
            )

            with open(output_folder + "/" + folder_year + "-" + folder_month +     "_list_of_tags.txt", 'w') as f:
                for item in list_of_tags_unique:
                    f.write("%s\n" % item)

        def get_tag_counts(dataframe, column, output_folder):
            """
            Save dataframe containing all unique tags to txt format in specified directory.

            Arguements:
                dataframe:     tabular data
                column:        location of xbrl tags
                output_folder: user specified file location
            Returns:
                None
            Raises:
                None
            """
            cache = dataframe
            cache["count"] = cache.groupby(by = column)[column].transform("count")
            cache.sort_values("count", inplace = True, ascending = False)
            cache.drop_duplicates(subset = [column, "count"], keep = "first", inplace = True)
            cache = cache[[column, "count"]]

            print(cache.shape)

            cache.to_csv(
                output_folder + "/" + folder_year + "-" + folder_month + "_unique_tag_frequencies.csv",
                header = None,
                index = True,
                sep = "\t",
                mode = "a"
            )

        def build_month_table(list_of_files):
            """
            """
            process_start = time.time()

            # Empty table awaiting results
            results = pd.DataFrame()

            COUNT = 0

            # For every file
            for file in list_of_files:

                COUNT += 1

                # Read the file
                doc = process_account(file)

                # tabulate the results
                doc_df = flatten_data(doc)

                # append to table
                results = results.append(doc_df)

                XbrlExtraction.progressBar("XBRL Accounts Parsed", COUNT, len(list_of_files), bar_length = 50, width = 20)

            print("Average time to process an XBRL file: \x1b[31m{:0f}\x1b[0m".format((time.time() - process_start) / 60, 2), "seconds")

            return results

        def output_xbrl_month(dataframe, output_folder, file_type = "csv"):
            """
            Save dataframe to csv format in specified directory, with particular naming scheme "YYYY-MM_xbrl_data.csv".

            Arguments:
                dataframe:     tabular data
                output_folder: user specified file destination
            Returns:
                None
            Raises:
                None
            """
            if file_type == "csv":
                dataframe.to_csv(
                    output_folder
                        + "/"
                        + folder_year
                        + "-"
                        + folder_month
                        + "_xbrl_data.csv",
                    index = False,
                    header = True
                )
            else:
                print("I need a CSV for now...")

        # Get all the filenames from the example folder
        files, folder_month, folder_year = get_filepaths(xbrl_unpacked_data)

        print(len(files))

        # Here you can splice/truncate the number of files you want to process for testing
        files = files[0:30]

        print(folder_month, folder_year)

        # Finally, build a table of all variables from all example (digital) documents
        # This can take a while
        results = build_month_table(files)

        print(results.shape)

        results.head(10)

        output_xbrl_month(results, xbrl_processed_csv)

        # Find list of all unique tags in dataset
        list_of_tags = results["name"].tolist()
        list_of_tags_unique = list(set(list_of_tags))

        print("Longest tag: ", len(max(list_of_tags_unique, key=len)))

        # Output all unique tags to a txt file
        retrieve_list_of_tags(
            results,
            "name",
            xbrl_tag_list
        )

        # Output all unique tags and their relative frequencies to a txt file
        get_tag_counts(
            results,
            "name",
            xbrl_tag_frequencies
        )

        # print(results.shape)

    # Execute PDF web scraper
    if pdf_web_scraper == str(True):
        print("PDF web scraper running...")

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
        os.environ["TESSDATA_PREFIX"] = tessdata_prefix
        ocr_runner = RunOCR()
        ocr_runner.image_to_data(ocr_image_dir_cs, ocr_text_dir_cs, ocr_im_type_cs, data=False)
        ocr_runner.image_to_data(ocr_image_dir, ocr_text_dir, ocr_im_type)
        
    # Execute NLP
    if nlp_functions == str(True):
        print("Running all NLP functions...")

    # Merge xbrl and PDF file data
    if merge_xbrl_to_pdf_data == str(True):
        print("Merging XBRL and PDF data...")

    """
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
