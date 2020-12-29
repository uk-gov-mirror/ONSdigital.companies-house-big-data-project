import gcsfs
import pandas as pd
import numpy as np
import statistics as stats

from line_reader import LineReader
from table_identifier import TableIdentifier
from table_fitter import TableFitter
from pdf_annotator import PDFAnnotator

fs = gcsfs.GCSFileSystem("ons-companies-house-dev")

tokens = fs.ls("ons-companies-house-dev-scraped-pdf-data/doc_ai_outputs/doc_ai_token_dfs/")[1:]
names = [((t.split("/")[-1]).split(".")[0])[:-7] for t in tokens]

fails = []
for i in range(len(tokens)):
    try:
        df = pd.read_csv("gs://"+tokens[i])

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

        # Create an annotated pdf
        annotator = PDFAnnotator("ons-companies-house-dev-scraped-pdf-data/doc_ai_outputs/bs_pdfs/"
                                 + names[i] + ".pdf", gcp=True)
        annotator.pdf_to_png()
        annotator.annotate_table("ons-companies-house-dev-scraped-pdf-data/doc_ai_outputs/visual_outputs/" + names[i]+"_table.jpg",
                                 table_data)
    except:
        fails.append(names[i])
