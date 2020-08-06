import os
import csv
import itertools
import pandas as pd
from typing import List, Tuple

def combine_csv(files: List[str], outfile: str):
    '''Combines all csv files listed in files into a single cs file using csv.writer.
    Unsuitable if files do not have the same format. Faster than the pandas version.'''

    files.sort()
    print("Processing " + str(len(files)) + " files...")
    with open(outfile, "w") as w:
        writer = csv.writer(w, delimiter=',')
        for n, file in enumerate(files):
            print("Processing " + file + "...")
            with open(file, "r") as f:
                reader = csv.reader(f, delimiter=',')
                if n > 0: next(reader)
                for row in reader:
                 writer.writerow(row)

def combine_csv_pd(files: List[str], outfile: str):
    '''Combines all csv files listed in files into a single csv file using pandas'''

    files.sort()
    combined_csv = pd.concat([pd.read_csv(f, engine = 'python') for f in files])
    combined_csv.to_csv(outfile, index = False)

def _add_path(indir: str, files: str):
    '''returns list with paths appended to file names'''
    return [indir+i for i in files]

def merge_files_by_year(indir: str, outdir: str, years: Tuple[int]):
    ''' Finds all csv files with directory indir, groups files into
    individual years and calls combine_csv function to merge into
    single file given that the years falls within the range specified
    by the user.'''

    if os.path.exists(indir):

        if not os.path.exists(outdir):
            os.mkdir(outdir)

        files = os.listdir(indir)
        files = ([i for i in files if i[-4:] == '.csv'
                  and i[:4].isnumeric() and int(i[:4]) in range(*years)])
        years = [[k,_add_path(indir, i)] for k, i in itertools.groupby(files, lambda x: x[:4])]

        [combine_csv(files, outdir+year+'_xbrl.csv') for year, files in years]
    else:
        print("Input file path does not exist")

if __name__ == '__main__':

    indir = '/shares/data/20200519_companies_house_accounts/xbrl_parsed_data/'
    outdir = '/home/peterd/test/'
    years = (2008, 2011)

    merge_files_by_year(indir, outdir, years)
