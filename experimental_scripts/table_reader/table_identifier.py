from line_reader import LineReader


class TableIdentifier(LineReader):

    def __init__(self, df):
        LineReader.__init__(self, df)
        self.total_lines = max(df["line_num"])
    
    def detect_table(self, c=0.4, page_prop=0.4):
        """
        Uses the result of group_by_widths to infer which lines of the data
        could correspond to a table - any line containing

        Arguments:
            c:          Constant to consider if a line is part of a table
                        (passed in group_by_widths)
            page_prop:  Minimum proportion of total elements we need to
                        consider a set of elements to be part of a table
        Returns:
            None
        Raises:
            None
        """
        # Get the groups of indices where the elements have sufficiently small
        # width
        indices = self.group_by_widths(self.data, const=c)

        # Only take the groups where we have a sufficient proportion of lines
        # on the page
        table_indices = [g for g in indices if len(g) > 0.4*self.data.shape[0]]

        # Case for multiple tables - needs improving
        if len(table_indices) != 1:
            print("More than one table has been detected")
        else:
            self.data = self.data.loc[table_indices[0], :]

    @staticmethod
    def group_by_widths(df, const=0.4):
        """
        Groups the line elements into groups of consecutive elements with
        widths less than the specified constant.

        Arguments:
            df:     DataFrame that has been grouped into line elements
            const:  Constraint on the max width of an element to include
        Returns:
            width_groups:   List of lists of grouped consecutive dataframe
                            indices where the corresponding elements have
                            width less than const.
        Raises:
            None
        """
        lines_df = df.copy()
        width_groups = [[]]
        # Set a counter for the group in width_groups we are considering
        k = 0

        # Loop over the elements in the dataframe
        for i in lines_df.index:
            # If the width is small enough, add the index to the current group
            if lines_df.loc[i, "width"] < const:
                width_groups[k].append(i)

            # Set behaviour for long first line
            elif i == 0:
                continue

            # If this line is long but the previous one is short, make a new
            # group and set k to point to the new group
            elif lines_df.loc[i-1, "width"] < const:
                width_groups.append([])
                k += 1

        return width_groups
