import pandas as pd
import numpy as np
import statistics as stats

from line_reader import LineReader
from table_identifier import TableIdentifier


class TableFitter(TableIdentifier):
    def __init__(self, df):
        TableIdentifier.__init__(self, df)
        self.columns = []
        self.notes_row = []
        self.assets_row = []
        self.header_indices = []
        self.header_lines = []
        self.header_coords = []
        
    def clean_values(self, chars = ["\n"]):
        """
        Removes all unwanted characters (default is just newline) from
        the value column of the class data. Also sets attributes for the
        indices where we see specific values.
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

        # Set class attributes for the indices where we find specific values
        self.notes_row = [i for i in self.data.index
                          if self.data.loc[i, "value"].lower()
                          in ["note", "notes"]]
        self.assets_row = [i for i in self.data.index
                           if "asset" in self.data.loc[i, "value"].lower()]
    
    def get_first_col(self):
        """
        Gets the indices of the first column of a table and saves
        as a class attribute. First column selected by presence of the
        string "assets".
        
        Arguments:
            None
        Returns:
            None
        Raises:
            None
        """
        # Find the distribution of coordinates in assets_row
        assets_dist = self.find_alignment(self.data, self.assets_row)

        # Find all other elements that align with the assets coordinates
        aligned_dict = self.find_aligned_indices(self.data, assets_dist)
        self.columns.append(aligned_dict["indices"])

    @staticmethod
    def find_alignment(df, indices):
        """
        Returns the average alignment of the given indices. Returns a list
        of the average x coordinate of the left edge, right edge and center
        (of the boxes around the elements).
        
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

        # For each of the indices, append the x coordinate of the left, centre
        # and right of the bounding polygon
        for i in indices:
            assets_dist[0].append(eval(df.loc[i, "normed_vertices"])[3][0])
            assets_dist[1].append(eval(df.loc[i, "normed_vertices"])[2][0])
            assets_dist[2].append(
                0.5*(eval(df.loc[i, "normed_vertices"])[3][0]
                     + eval(df.loc[i, "normed_vertices"])[2][0]))

        # Compute the average for each of these coordinates
        assets_summary = [stats.mean(assets_dist[0]),
                          stats.mean(assets_dist[1]),
                          stats.mean(assets_dist[2])]
        return assets_summary

    def find_aligned_indices(self, df, alignment):
        """
        Finds the indices of elements in the DataFrame aligned with
        the three x coordinates in the alignment object (the result of
        find_alignment). Decides which way to align based on the number
        of elements that correspond to each alignment.

        Arguments:
            df:         DataFrame to be considered
            alignment:  list of 3 x coordinates from find_alignment
        Returns (as dict):
            indices:    The indices of the dataframe that align to the given
                        alignment object
            alignment:  Number corresponding to left right or central alignment
        Raises:
            None
        """
        # List of indices 'close' to each of the three coordinates
        indices_close = [[], [], []]

        # Loop over the specified indices
        for i in df.index:
            if self.is_close(alignment[0],
                             eval(df.loc[i, "normed_vertices"])[3][0],
                             dist = 0.01):
                indices_close[0].append(i)
            elif self.is_close(alignment[1],
                               eval(df.loc[i, "normed_vertices"])[2][0],
                               dist = 0.01):
                indices_close[1].append(i)
            elif self.is_close(alignment[2],
                               0.5*(eval(df.loc[i, "normed_vertices"])[2][0]
                                    + eval(df.loc[i, "normed_vertices"])[3][0]),
                               dist=0.01):
                indices_close[2].append(i)

        # Select only the indices in the list with the most elements
        aligned = [j for j, k in enumerate(indices_close)
                   if len(k) == max([len(i) for i in indices_close])]
        return {"indices": indices_close[aligned[0]], "alignment":aligned[0]}
   
    def get_header_row(self):
        """
        Obtains a list of indices corresponding to the header row. This is
        based on looking for lines near to the line containing the 'notes'
        tag which don't have elements in the first column.

        Arguments:
            None
        Returns:
            None
        Raises:
            None
        """

        # Set the current line we are considering - start with notes line
        l = self.data.loc[self.notes_row[0], "line_num"]

        # Add the notes line to the relevant variables
        header_lines = [l]
        header_indices = list(self.data[self.data["line_num"] == l].index)

        # Look for other header rows below the 'notes' row
        while l < self.total_lines:
            l += 1
            # Stop at the first line with an element in the first column
            if any([(i in self.columns[0])
                    for i in self.data[self.data["line_num"] == l].index]):
                break
            else:
                # Add the relevant lines and indices if it is a header row
                header_lines.append(l)
                header_indices += \
                    list(self.data[self.data["line_num"] == l].index)

        # Start from the 'notes' line and look for header rows above
        l = self.data.loc[self.notes_row[0], "line_num"]
        while l > min(self.data.index):
            l -= 1
            if any([(i in self.columns[0])
                    for i in self.data[self.data["line_num"] == l].index]):
                break
            else:
                header_lines.append(l)
                header_indices += \
                    list(self.data[self.data["line_num"] == l].index)
        self.header_lines = header_lines
        self.header_indices = header_indices
        bottom_line = max(self.header_lines)
        self.header_coords = \
            [self.find_alignment(self.data, i)
             for i in self.data[self.data["line_num"] == bottom_line].index]
        print(len(header_lines), " header lines have been detected")

    def get_other_columns(self):
        """
        Function to group the indices of columns other than the first, based
        on elements which are aligned with the elements in the header row.

        Arguments:
            None
        Returns:
            None
        Raises:
            None
        """

        # Don't consider elements in the first column
        other_cols_df = self.data.drop(self.columns[0])

        # Add a list to the columns attribute for each in the header row
        for i in range(len(self.header_coords)):
            self.columns.append([])

        # For each element not in a column add the index to a column
        for i in other_cols_df.index:
            col_to_fit = \
                self.find_closest_col(other_cols_df, self.header_coords, i)
            self.columns[col_to_fit].append(i)
        
    @staticmethod
    def find_closest_col(df, columns, index):
        """
        Finds which column a given element (index) should be assigned to by
        finding which header element it is closest to.

        Arguments:
            df:         DataFrame to reference from
            columns:    List of the x coordinates of the alignments of each of
                        the columns
            index:      The index of the element to be classified
        Returns:
            fitted_col: The list index of the column to add the index to
        Raises:
            None
        """
        dists = []
        # For each of the columns, append the distance between the centre of
        # the element and the column
        for col in columns:
            dists.append(
                abs(col[2] -
                    (0.5*(eval(df.loc[index, "normed_vertices"])[3][0]
                          + eval(df.loc[index, "normed_vertices"])[2][0]))))

        # Find the index of the one with the smallest distance (+1 since can't
        # be the first column
        fitted_col = dists.index(min(dists)) + 1
        return fitted_col
        

