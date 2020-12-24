
import pandas as pd
import numpy as np
import statistics as stats

from line_reader import LineReader
from table_identifier import TableIdentifier

class TableFitter(TableIdentifier):
    def __init__(self, df):
        TableIdentifier.__init__(self, df)
        self.columns = []
        
    def clean_values(self, chars = ["\n"]):
        """
        Removes all unwanted characters (default is just newline) from
        the value column of the class data.
        
        Arguments:
            chars: List of strings of characters to remove
        Returns:
            None
        Raises:
            None
        """
        for char in chars:
            self.data["value"] = self.data["value"].str\
                        .replace(char, '')
        self.notes_row = [i for i in self.data.index if self.data.loc[i, "value"].lower() in ["note", "notes"]]
        self.assets_row = [i for i in self.data.index if "asset" in self.data.loc[i, "value"].lower()]
    
    def get_first_col(self):
        """
        Gets the indices of the first column of a table and saves
        as a class atrribute. First column selected by prescence of the
        string "assets"
        
        Arguments:
            None
        Returns:
            None
        Raises:
            None
        """
        assets_dist = self.find_alignment(self.data, self.assets_row)
        aligned_dict = self.find_aliged_indices(self.data, assets_dist)
        self.columns.append(aligned_dict["indices"])

    @staticmethod
    def find_alignment(df, indices):
        """
        Returns the average alignment of the given indices. Returns a list
        of the average x coordinate of the left edge, right edge and center
        of the boxes around the elements.
        
        Arguments:
            df:         DataFrame for which indices can be used
            indices:    Indices of elements to be considered
        Returns:
            assets_summary: List of the average left, right and central
                             x coordinate for the elements
        Raises:
            None
        """
        assets_dist = [[],[],[]]
        if type(indices) == int:
            indices = [indices]
        for i in indices:
            assets_dist[0].append(eval(df.loc[i, "normed_vertices"])[0][0])
            assets_dist[1].append(eval(df.loc[i, "normed_vertices"])[1][0])
            assets_dist[2].append(0.5*(eval(df.loc[i, "normed_vertices"])[1][0] + eval(df.loc[i, "normed_vertices"])[0][0]))
        assets_summary = [stats.mean(assets_dist[0]),stats.mean(assets_dist[1]),stats.mean(assets_dist[2])]
        return assets_summary


    def find_aliged_indices(self, df, alignment):
        """
        Finds the indices of elements in the DataFrame aligned with
        the three x coordinates in the alignment object (the result of
        find_alignment). Decides which way to align based on the number
        of elements that correspond to each alignment.

        Arguments:
            df:         DataFrame to be considered
            alignment:  list of 3 x coordinates from find_alignment
        Returns (as dict):
            indices: The indices of the dataframe that align to the given 
                     alignment object
            alignment: Number corresponding to left right or central alignment
        Raises:
            None
        """
        indices_close = [[], [], []]
        print(alignment)
        for i in df.index:
            print(df.loc[i, "normed_vertices"])
            if self.is_close(alignment[0], eval(df.loc[i, "normed_vertices"])[0][0], dist = 0.01):
                indices_close[0].append(i)
            elif self.is_close(alignment[1], eval(df.loc[i, "normed_vertices"])[1][0], dist = 0.01):
                indices_close[1].append(i)
            elif self.is_close(alignment[2], 0.5*(eval(df.loc[i, "normed_vertices"])[1][0] + eval(df.loc[i, "normed_vertices"])[0][0]), dist = 0.01):
                indices_close[2].append(i)
        aligned = [j for j,k in enumerate(indices_close) if len(k) == max([len(i) for i in indices_close])]
        print(aligned)
        return {"indices": indices_close[aligned[0]], "alignment":aligned[0]}
   
    def get_header_row(self):
        l = self.data.loc[self.notes_row[0], "line_num"]
        header_lines = [l]
        header_indices = list(self.data[self.data["line_num"] == l].index)
        while l < self.total_lines:
            l+=1
            print([(i in self.columns[0]) for i in self.data[self.data["line_num"] == l].index])
            if any([(i in self.columns[0]) for i in self.data[self.data["line_num"] == l].index]):
                print("yes:", l)
                break
            else:
                header_lines.append(l)
                header_indices += list(self.data[self.data["line_num"] == l].index)

                
        l = self.data.loc[self.notes_row[0], "line_num"]
        while l > min(self.data.index):
            l-=1
            if any([(i in self.columns[0]) for i in self.data[self.data["line_num"] == l].index]):
                break
            else:
                header_lines.append(l)
                header_indices += list(self.data[self.data["line_num"] == l].index)
        self.header_lines = header_lines
        self.header_indices = header_indices
        bottom_line = max(self.header_lines)
        self.header_coords = [self.find_alignment(self.data, i) for i in self.data[self.data["line_num"] == bottom_line].index] 
        print(len(header_lines) ," header lines have been detected")

    def get_other_columns(self):
        other_cols_df = self.data.drop(self.columns[0])
        for i in range(len(self.header_coords)):
            self.columns.append([])
        for i in other_cols_df.index:
            col_to_fit = self.find_closest_col(other_cols_df, self.header_coords, i)
            self.columns[col_to_fit].append(i)
        
    @staticmethod
    def find_closest_col(df,  columns, index):
        dists = []
        for col in columns:
            dists.append(abs(col[2] - (0.5*(eval(df.loc[index, "normed_vertices"])[1][0] + eval(df.loc[index, "normed_vertices"])[0][0]))))
        fitted_col = dists.index(min(dists)) + 1
        return fitted_col
        

