from bs4 import BeautifulSoup as BS  # Can parse xml or html docs
from datetime import datetime
from dateutil import parser
from src.data_processing.xbrl_pd_methods import XbrlExtraction
from google.cloud import bigquery
from google.oauth2 import service_account
from google.cloud import storage
from functools import partial
import pandas as pd
import os
import csv
import time
import sys
import math
import time
import multiprocessing as mp
import numpy as np
import gcsfs
import pytz
import psutil
import gc




class XbrlParser:
    """ This is a class for parsing the XBRL data."""

    def __init__(self, fs):
        """
        Constructs all the necessary attributes for the XbrlParser object of
        which there are none.
        """
        self.__init__
        self.fs = fs

    # Table of variables and values that indicate consolidated status
    consolidation_var_table = {
        "includedinconsolidationsubsidiary": True,
        "investmententityrequiredto\
        applyexceptionfromconsolidationtruefalse": True,
        "subsidiaryunconsolidatedtruefalse": False,
        "descriptionreasonwhyentityhasnot\
        preparedconsolidatedfinancialstatements": "exist",
        "consolidationpolicy": "exist"
    }

    @staticmethod
    def retrieve_accounting_standard(soup):
        """
        Gets the account reporting standard in use in a document by hunting
        down the link to the schema reference sheet that always appears to
        be in the document, and extracting the format and standard date from
        the string of the url itself.
        WARNING - That means that there's a lot of implicit hardcoded info
        on the way these links are formatted and referenced within this
        function.  Might need changing someday.

        Arguments:
            soup: BeautifulSoup souped html/xml object (BeautifulSoup object)
        Returns:
            standard:   The standard for the object (string)
            date:       The date for the object (string)
            original_url: The original url of the object (string)
        Raises:
            None
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
        standard, date, original_url = \
            text[:-10].strip("-"), text[-10:], link_obj['xlink:href']

        return standard, date, original_url

    @staticmethod
    def parse_element(soup, element):
        """
        For a discovered XBRL tagged element, go through, retrieve its name
        and value and associated metadata.

        Arguments:
            soup:     BeautifulSoup object of accounts document (BeautifulSoup object)
            element:  soup object of discovered tagged element
        Returns:
            element_dict: A dictionary containing the elements name value and
                          metadata (dict)
        Raises:
            None
        """
        if "contextref" not in element.attrs:
            return {}

        element_dict = []

        # Basic name and value
        try:
            # Method for XBRLi docs first
            element_dict['name'] = element.attrs['name'].lower().split(":")[-1]
        except:
            # Method for XBRL docs second
            element_dict['name'] = element.name.lower().split(":")[-1]

        element_dict['value'] = element.get_text()
        element_dict['unit'] = XbrlParser.retrieve_unit(soup, element)
        element_dict['date'] = XbrlParser.retrieve_date(soup, element)

        # If there's no value retrieved, try raiding the associated context
        # data
        if element_dict['value'] == "":
            element_dict['value'] = XbrlParser.retrieve_from_context(
                soup, element.attrs['contextref'])

        # If the value has a defined unit (eg a currency) convert to numeric
        if element_dict['unit'] != "NA":
            element_dict['value'] = XbrlParser.clean_value(
                element_dict['value'])

        # Retrieve sign of element if exists
        try:
            element_dict['sign'] = element.attrs['sign']

            # if it's negative, convert the value then and there
            if element_dict['sign'].strip() == "-":
                element_dict['value'] = 0.0 - element_dict['value']
        except:
            pass

        return element_dict

    @staticmethod
    def summarise_by_sum(doc, variable_names):
        """
        Takes a document (dict) after extraction, and tries to extract
        a summary variable relating to the financial state of the enterprise
        by summing all those named that exist.

        Arguments:
            doc:            an extracted document dict, with "elements" entry
                            as created by the 'scrape_clean_elements' functions
                            (dict)
            variable_names: variables to find and sum (of all) if they exist
        Returns (as a dict):
            total_assets:   the totals of the given values
            units:          the units corresponding to the given sum
        """
        # Check arguments are of correct types
        if not isinstance(doc, dict):
            raise TypeError(
                "'doc' argument must be a dictionary"
            )
        if isinstance(variable_names, list):
            if not all(isinstance(name, str) for name in variable_names):
                raise TypeError (
                    "Variable names must be passed as strings"
                )
        else:
            if not isinstance(variable_names, str):
                raise TypeError(
                    "Variable name must be passed as a string"
                )

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
                total_assets = total_assets + df[df['name'] == each]\
                    .iloc[0]['value']
                # Retrieve reporting unit if exists
                unit = df[df['name'] == each].iloc[0]['unit']
            except:
                pass

        return {"total_assets": total_assets, "unit": unit}

    @staticmethod
    def summarise_by_priority(doc, variable_names):
        """
        Takes a document (dict) after extraction, and tries to extract
        a summary variable relating to the financial state of the enterprise
        by looking for each named, in order.

        Arguments:
            doc:            an extracted document dict, with "elements" entry
                            as created by the 'scrape_clean_elements' functions
                            (dict)
            variable_names: variables to find and check if they exist
        Returns (as a dict):
            primary_assets: total assets from given variables
            unit:           units for corresponding assets
        Raises:
            None
        """
        # Check arguments are of correct types
        if not isinstance(doc, dict):
            raise TypeError(
                "'doc' argument must be a dictionary"
            )
        if isinstance(variable_names, list):
            if not all(isinstance(name, str) for name in variable_names):
                raise TypeError(
                    "Variable names must be passed as strings"
                )
        else:
            if not isinstance(variable_names, str):
                raise TypeError(
                    "Variable name must be passed as a string"
                )

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

        return {"primary_assets": primary_assets, "unit": unit}

    @staticmethod
    def summarise_set(doc, variable_names):
        """
        Takes a document (dict) after extraction, and tries to extract
        summary variables relating to the financial state of the enterprise
        by returning all those named that exist.
        
        Arguments:
            doc:            an extracted document dict, with "elements" entry
                            as created by the 'scrape_clean_elements' functions
                            (dict)
            variable_names: variables to find and return if they exist.
        Returns:
            results: a dictionary of all the values for each in variable_names
                     (dict)
        Raises:
            None
        """
        # Check arguments are of correct types
        if not isinstance(doc, dict):
            raise TypeError(
                "'doc' argument must be a dictionary"
            )
        if isinstance(variable_names, list):
            if not all(isinstance(name, str) for name in variable_names):
                raise TypeError(
                    "Variable names must be passed as strings"
                )
        else:
            if not isinstance(variable_names, str):
                raise TypeError(
                    "Variable name must be passed as a string"
                )

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
        return results

    @staticmethod
    def flatten_dict(doc):
        """
        Takes in a list of dictionaries and combines them into a
        single dictionary - assumes dictionaries all have the same keys.

        Argument:
            doc: a list of dictionaries (list)
        Returns:
            doc_dict: a dictionary formed by combing the list of dictionaries
                      (dict)
        Raises:
            None
        """
        # Check arguments are of correct types
        if not all(isinstance(el, dict) for el in doc):
            raise TypeError(
                "'doc' argument must be a list of dictionaries"
            )

        # combines list of dictionaries into one dictionary based on common
        # keys
        doc_dict = {}
        for k in doc[0].keys():
            doc_dict[k] = [d[k] for d in doc]

        return doc_dict
