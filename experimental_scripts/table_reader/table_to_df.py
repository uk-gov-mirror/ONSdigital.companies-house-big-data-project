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
            contains_year = any([str(y) in self.data.loc[i, "value"] for y in years])
            if contains_year:
                date_indexes.append(i)
        self.date_headers = date_indexes

        header_data = pd.DataFrame(columns=["column", "date", "unit"])
        for i, g in enumerate(self.table.header_groups):
            unit_col = [j for j in g if j in self.unit_headers]
            date_col = [j for j in g if j in self.date_headers]
            if len(unit_col)==1  or len(date_col)==1:
                header_data.loc[i, "column"] = i+1
                if not unit_col:
                    header_data.loc[i, "date"] = self.data.loc[date_col[0], "value"]
                    header_data.loc[i, "unit"] = self.get_closest_el(self.data, date_col[0], self.unit_headers)
                elif not date_col:
                    header_data.loc[i, "date"] = self.get_closest_el(self.data, unit_col[0], self.date_headers)
                    header_data.loc[i, "unit"] = self.data.loc[unit_col[0], "value"]
                else:
                    header_data.loc[i, "date"] = self.data.loc[date_col[0], "value"]
                    header_data.loc[i, "unit"] = self.data.loc[unit_col[0], "value"]
        return header_data

    def get_final_df(self):
        self.df = self.table_data.merge(self.get_info_headers(), on="column")
        for index, row in self.df.iterrows():
            l = row["line_num"]
            try:            
                # print(self.df[(self.df["line_num"]==l)&(self.df["column"]==0)].iloc[0]["value"])
                self.df.loc[index, "name"] = self.data[(self.data["line_num"]==l)&(self.data["column"]==0)].iloc[0]["value"]
            except:
                self.df.loc[index, "name"] = None
        self.df = self.df[["name", "value", "date", "unit"]]

    @staticmethod
    def get_central_coord(data, i):
        return 0.5*(eval(data.loc[i, "normed_vertices"])[3][0] + eval(data.loc[i, "normed_vertices"])[2][0])
    
    @staticmethod
    def get_closest_el(data, x, elements):
        dists = []
        for el in elements:
            dists.append(abs(Table2Df.get_central_coord(data, el) - Table2Df.get_central_coord(data, x)))
        return data.loc[elements[dists.index(min(dists))], "value"]
        

