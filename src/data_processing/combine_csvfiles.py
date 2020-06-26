import os
import csv
import itertools
import pandas as pd
from typing import List, Tuple

def _combine_csv(year: str, files: List[str], outdir: str):
    '''Combines all csv files listed in files into a single cs file using csv.writer.
    Unsuitable if files do not have the same format. Faster than the pandas version.'''

    files.sort()
    new_file = outdir + year + '_xbrl.csv'
    with open(new_file, "w") as w:
        writer = csv.writer(w, delimiter=',')
        for n, file in enumerate(files):
            with open(file, "r") as f:
                reader = csv.reader(f, delimiter=',')
                if n > 0: next(reader)
                for row in reader:
                 writer.writerow(row)

def _combine_csv_pd(year: str, files: List[str], outdir: str):
    '''Combines all csv files listed in files into a single csv file using pandas'''

    files.sort()
    new_file = outdir+year+'_xbrl.csv'
    combined_csv = pd.concat([pd.read_csv(f, engine = 'python') for f in files])
    combined_csv.to_csv(new_file, index = False)

def _add_path(indir: str, files: str):
    '''returns list with paths appended to file names'''
    return [indir+i for i in files]

def merge_files_by_year(indir: str, outdir: str, years: Tuple[int]):
    ''' Finds all csv files with directory indir, groups files into
    individual years and calls combine_csv function to merge into
    single file given that the years falls within the range specified
    by the user.'''

    if not os.path.exists(outdir):
        os.mkdir(outdir)

    files = os.listdir(indir)
    files = ([i for i in files if i[-4:] == '.csv'
              and i[:4].isnumeric() and int(i[:4]) in range(*years)])
    years = [[k,_add_path(indir, i)] for k, i in itertools.groupby(files, lambda x: x[:4])]

    [_combine_csv(year, files, outdir) for year, files in years]

if __name__ == '__main__':

    indir = '/shares/data/20200519_companies_house_accounts/xbrl_parsed_data/'
    outdir = '/home/emmas/test/'
    years = (2008, 2011)

    merge_files_by_year(indir, outdir, years)
