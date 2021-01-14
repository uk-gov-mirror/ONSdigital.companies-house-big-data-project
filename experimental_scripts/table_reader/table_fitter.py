import pandas as pd
import numpy as np
import statistics as stats
import regex

from line_reader import LineReader
from table_identifier import TableIdentifier


class TableFitter(TableIdentifier):
    def __init__(self, df):
        TableIdentifier.__init__(self, df)
        self.columns = []
        self.notes_row = []
        self.assets_row = []

        # Header properties
        self.header_indices = []
        self.header_lines = []
        self.header_coords = []
        self.header_groups = []

        # Date/Unit properties
        self.date_headers = []
        self.unit_headers = []

        
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
        x_dist = self.find_alignment(self.data, self.assets_row)

        # Find all other elements that align with the assets coordinates
        aligned_dict = self.find_aligned_indices(self.data, x_dist)
        for i in aligned_dict["indices"]:
            self.data.loc[i, "column"] = int(0)
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
        x_dist = [[],[],[]]
        if type(indices) != list:
            indices = [indices]

        # For each of the indices, append the x coordinate of the left, centre
        # and right of the bounding polygon
        for i in indices:
            x_dist[0].append(eval(df.loc[i, "normed_vertices"])[3][0])
            x_dist[1].append(eval(df.loc[i, "normed_vertices"])[2][0])
            x_dist[2].append(
                0.5*(eval(df.loc[i, "normed_vertices"])[3][0]
                     + eval(df.loc[i, "normed_vertices"])[2][0]))


        aligned_medians = [stats.median(x_dist[0]),
                        stats.median(x_dist[1]),
                        stats.median(x_dist[2])]
        aligned_index = None
        aligned_x_point = None
        # Compute the variance for each of these coordinates
        if len(x_dist[0]) >= 2:
            aligned_vars = [stats.variance(x_dist[0]),
                            stats.variance(x_dist[1]),
                            stats.variance(x_dist[2])]
            
            # Find the alignment by taking the index where variance is minimised
            aligned_index = aligned_vars.index(min(aligned_vars))
            aligned_x_point = stats.median(x_dist[aligned_index])
        # Return a dict of the aligned index and the median x point of the alignment
        assets_summary = {"aligned_index": aligned_index,
                            "aligned_x_point": aligned_x_point,
                            "median_points": aligned_medians}
        return assets_summary
            

    @staticmethod
    def find_aligned_indices(df, alignment, d=0.01):
        """
        Finds the indices of elements in the DataFrame aligned with
        the three x coordinates in the alignment object (the result of
        find_alignment). Decides which way to align based on the number
        of elements that correspond to each alignment.

        Arguments:
            df:         DataFrame to be considered
            alignment:  dict of alignment from find_alignment
            d:          Constraint on how close elements need to be to be
                        considered aligned
        Returns (as dict):
            indices:    The indices of the dataframe that align to the given
                        alignment object
            alignment:  Number corresponding to left right or central alignment
        Raises:
            None
        """
        # List of indices 'close' to each of the three coordinates
        indices_close = []

        # Cases for the 3 alignment options
        if alignment["aligned_index"] == 0:
            for i in df.index:
                if TableFitter.is_close(alignment["aligned_x_point"],
                                eval(df.loc[i, "normed_vertices"])[3][0],
                                dist=d):
                    indices_close.append(i)

        elif alignment["aligned_index"] == 1:
            for i in df.index:
                if TableFitter.is_close(alignment["aligned_x_point"],
                                eval(df.loc[i, "normed_vertices"])[2][0],
                                dist=d):
                    indices_close.append(i)

        elif alignment["aligned_index"] == 2:
            for i in df.index:
                if TableFitter.is_close(alignment["aligned_x_point"],
                                0.5*(eval(df.loc[i, "normed_vertices"])[2][0]
                                    + eval(df.loc[i, "normed_vertices"])[3][0]),
                                dist=d):
                    indices_close.append(i)
        elif alignment["aligned_index"] == None:
            indices_close = [[],[],[]]
            for i in df.index:
                if TableFitter.is_close(alignment["median_points"][0],
                                eval(df.loc[i, "normed_vertices"])[3][0],
                                dist=d):
                    indices_close[0].append(i)
                elif TableFitter.is_close(alignment["median_points"][1],
                                eval(df.loc[i, "normed_vertices"])[2][0],
                                dist=d):
                    indices_close[1].append(i)
                elif TableFitter.is_close(alignment["median_points"][2],
                                0.5*(eval(df.loc[i, "normed_vertices"])[2][0]
                                        + eval(df.loc[i, "normed_vertices"])[3][0]),
                                dist=d):
                    indices_close[2].append(i)
            aligned = [j for j, k in enumerate(indices_close)
                   if len(k) == max([len(i) for i in indices_close])]
            alignment["aligned_index"] = aligned[0]
            indices_close = indices_close[aligned[0]]

        return {"indices": indices_close, "alignment":alignment["aligned_index"]}
   
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
        while l > min(self.data["line_num"]):
            l -= 1
            if any([(i in self.columns[0])
                    for i in self.data[self.data["line_num"] == l].index]):
                print(f"line number is{l}")
                break
            else:
                header_lines.append(l)
                header_indices += \
                    list(self.data[self.data["line_num"] == l].index)
        self.header_lines = header_lines
        self.header_indices = header_indices
        self.header_groups = self.group_header_points(self.data,
                                                      self.header_indices)
        self.header_coords = \
            [self.find_alignment(self.data, i)["median_points"]
             for i in self.header_groups]
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
            self.data.loc[i, "column"] = int(col_to_fit)
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

    @staticmethod
    def group_header_points(df, header_indices):
        """
        Takes a set of indices and groups them using the find_alignment
        function to put close indices into groups.

        Arguments:
            df:             DataFrame to use the indices to reference from
            header_indices: Indices which we want to group
        Returns:
            header_groups:  List of indices grouped into lists according to
                            which are aligned to each other.
        Raises:
            None
        """

        # Creates a new DataFrame to use and a list to add groups to
        header_df = df.loc[header_indices, :]
        header_groups = []

        # While we still have ungrouped indices
        while header_df.shape[0] > 0:
            # Find the first elements of the dataframe
            first_ind = header_df.index[0]

            # Find the alignment object for that element
            aligner = TableFitter.find_alignment(header_df, first_ind)

            # Find the indices aligned to that object
            grouped_inds = TableFitter.find_aligned_indices(header_df,
                                                            aligner,
                                                            d=0.04)

            # Add the group of indices to the list and remove them from the df
            header_groups.append(grouped_inds["indices"])
            header_df = header_df.drop(grouped_inds["indices"])

        return header_groups

    def get_info_headers(self, years = range(1999,2020)):
        currency_indexes = [i for i in self.header_indices if
                            len(regex.findall(r"\p{Sc}", self.data.loc[i, "value"]))]
        self.unit_headers = currency_indexes
        
        date_indexes = []
        for i in self.header_indices:
            print([(str(y) in self.data.loc[i, "value"]) for y in years])
            contains_year = any([str(y) in self.data.loc[i, "value"] for y in years])
            if contains_year:
                date_indexes.append(i)
        self.date_headers = date_indexes

        relevant_cols = []
        add_unit = []
        add_date = []
        for i, g in enumerate(self.header_groups):
            unit_col = [j for j in g if j in self.unit_headers]
            date_col = [j for j in g if j in self.date_headers]
            if len(unit_col)==1  or len(date_col)==1:
                if not unit_col:
                    date = self.data.loc[date_col[0], "value"]
                    unit = 0
                elif not date_col:
                    add_date.append(i)
    
    def find_closest_ind(self, el, elements):
        pass
            

