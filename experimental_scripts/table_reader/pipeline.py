import gcsfs
import pandas as pd
import numpy as np
import statistics as stats

from line_reader import LineReader
from table_identifier import TableIdentifier
from table_fitter import TableFitter
from pdf_annotator import PDFAnnotator

# df = pd.read_csv("gs://ons-companies-house-dev-parsed-pdf-data/doc_ai_token_dfs/top_10/bhp_bs_tokens.csv")
df = pd.read_csv("gs://ons-companies-house-dev-parsed-pdf-data/doc_ai_token_dfs/active/01961626_acitve_bs_tokens.csv")

# Implements the line reader module
lines_data = LineReader(df)
lines_data.add_first_vertex()
lines_data.get_line_nums()
lines_data.group_within_line()

# Implement the table identifier module
structs_data = TableIdentifier(lines_data.data)
structs_data.detect_table()

# Implement the table fitter module
table_data = TableFitter(structs_data.data)
table_data.clean_values()
table_data.get_first_col()
table_data.get_header_row()
table_data.get_other_columns()

