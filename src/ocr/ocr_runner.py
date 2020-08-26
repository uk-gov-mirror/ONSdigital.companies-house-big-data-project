import os
import pytesseract as ts
import multiprocessing
import functools
import numpy as np


NUM_PROCESSES = multiprocessing.cpu_count()

class RunOCR:

    def __init__(self):
        self.__init__


    @staticmethod
    def run_ocr_string(path):
        """ calls pytesseract to extract text from single image"""
        ocr_config = r"--psm 6"
        return ts.image_to_string(path, config=ocr_config)


    @staticmethod
    def save_string_to_file(text, path):
        """ saves text string to text file"""
        with open(path, 'w') as file:
            file.write(text)


    @staticmethod
    def run_ocr_data(path):
        """ calls pytesseract to return pandas dataframe of info from single image"""
        ocr_config = r"--psm 6"
        return ts.image_to_data(path, config=ocr_config, output_type = 'data.frame')


    @staticmethod
    def reduce_statement(df, text):
        """ searches for consecutive words in text within dataframe to match phrase """

        text = text.split()
        add_text = [df.text.shift(-i).str.lower() == s for i, s in enumerate(text)]

        return functools.reduce(lambda x, y: x & y, add_text)


    @staticmethod
    def save_data_to_csv(df, path):
        "saves dataframe to csv file"
        df.to_csv(path, index=False, header=True)


    def identify_page_data(self, df):
        """ returns label based on identifying key phrase within df"""

        if any(self.reduce_statement(df, "balance sheet")): label = "bal"
        elif any(self.reduce_statement(df, "statement of financial position")): label = "fin"
        else: label = "na"
        return label


    @staticmethod
    def convert_data_to_string(df):
        """ takes the data output and converts to a single string"""


        lines = ["par_num", "line_num"]
        df = df.groupby(lines).agg(lambda x:" ".join(x))
        return "\n".join(df["text"].tolist())


    def process_text_data(self, im_dir, data_dir, files, im_type):
        """
        takes a path and filename, calls tesseract to convert to
        dataframe, identifies page, extracts columns and saves as a csv
        """

        for file in files:

            df = self.run_ocr_data(os.path.join(im_dir, file))

            label = self.identify_page_data(self, df)

            outfile = file[:-len(im_type)-1] + "_" + label
            self.save_data_to_csv(df, os.path.join(data_dir, outfile+".csv"))


    def process_text(self, im_dir, data_dir, files, im_type):
        """
        takes a path and filename, calls tesseract to convert to
        dataframe, identifies page, extracts columns and saves as a csv
        """

        for file in files:
            text = self.run_ocr_string(os.path.join(im_dir, file))
            self.save_string_to_file(text, os.path.join(data_dir, file[:-len(im_type)]+"txt"))


    @classmethod
    def image_to_data(self, im_dir, txt_dir, im_type, data='True'):
        """
        """
        if not os.path.isdir(im_dir):
            raise Exception(f'{im_dir} not found')

        if not os.path.isdir(txt_dir):
            os.mkdir(txt_dir)

        os.environ["OMP_THREAD_LIMIT"] = "1"

        files = [f for f in os.listdir(im_dir) if f[-len(im_type):] == im_type]

        split_files = np.array_split(files, NUM_PROCESSES)

        chunks = [(self, im_dir, txt_dir, f.tolist(), im_type) for f in split_files]

        pool = multiprocessing.Pool(processes=NUM_PROCESSES)
        func = self.process_text_data if data else self.process_text
        output = pool.starmap(func, chunks)

        pool.close()
        pool.join()




        