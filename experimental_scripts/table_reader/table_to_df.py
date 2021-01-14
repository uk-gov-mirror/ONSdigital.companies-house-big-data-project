import gcsfs
import pandas as pd

class Table2Df:

    def __init__(self, table_fit, fs):
        self.table = table_fit
        self.fs = fs
        self.df = None
        self.data = table_fit.data

    def reconstruct_table(self):
        headers = self.headers_to_strings(self.table.header_groups)
        table_data = self.table.data.drop(self.table.header_indices)

        new_df = pd.DataFrame(columns=headers)
        for index, row in table_data.iterrows():
            new_df.loc[row["line_num"], headers[int(row["column"])]] = row["value"]
        return new_df


    def headers_to_strings(self, headers):
        str_headers = ["Entities"]
        for h in headers:
            k = 0
            new_string = ""
            while k < len(h):
                new_string+= (self.data.loc[h[k], "value"] + "\n")
                k+=1
            str_headers.append(new_string)
        return str_headers



