import gcsfs
import pandas as pd
import regex

class Table2Df:

    def __init__(self, table_fit, fs):
        self.table = table_fit
        self.fs = fs
        self.df = None
        self.data = table_fit.data
        self.table_data = self.table.data.drop(self.table.header_indices)

    def reconstruct_table(self):
        headers = self.headers_to_strings(self.table.header_groups)

        new_df = pd.DataFrame(columns=headers)
        for index, row in self.table_data.iterrows():
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

    def get_info_headers(self, years = range(1999,2020)):
        currency_indexes = [i for i in self.table.header_indices if
                            len(regex.findall(r"\p{Sc}", self.data.loc[i, "value"]))]
        self.unit_headers = currency_indexes
        
        date_indexes = []
        for i in self.table.header_indices:
            print([(str(y) in self.data.loc[i, "value"]) for y in years])
            contains_year = any([str(y) in self.data.loc[i, "value"] for y in years])
            if contains_year:
                date_indexes.append(i)
        self.date_headers = date_indexes

        header_data = pd.DataFrame(columns=["columns", "dates", "units"])
        relevant_cols = []
        for i, g in enumerate(self.table.header_groups):
            unit_col = [j for j in g if j in self.unit_headers]
            date_col = [j for j in g if j in self.date_headers]
            if len(unit_col)==1  or len(date_col)==1:
                relevant_cols.append(i+1)
                if not unit_col:
                    date = self.data.loc[date_col[0], "value"]
                    unit = 0
                elif not date_col:
                    pass

