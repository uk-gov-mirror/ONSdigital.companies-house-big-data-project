from line_reader import LineReader

class TableIdentifier(LineReader):
    def __init__(self, df):
        LineReader.__init__(self, df)
        self.total_lines = max(df["line_num"])
    
    def detect_table(self, c = 0.4):
        """
        Uses the result of group_by_widths to infer which lines of the data
        could correspond to a table

        Arguments:
            None
        Returns:
            None
        Raises:
            None
        """
        indices = self.group_by_widths(self.data, const = c)
        table_indices = [g for g in indices if len(g)> 0.4*self.total_lines]
        if len(table_indices) != 1:
            print("More than one table has been detected")
        else:
            self.data = self.data.loc[table_indices[0], :]


    @staticmethod
    def group_by_widths(df, const = 0.4):
        """
        Groups the line elements into groups of consecutive elements with
        widths less than the specified constant.

        Arguments:
            df:     DataFrame that has been grouped into line elements
            const:  Constraint on the max width of an element to include
        Returns:
            width_groups:   List of lists of grouped the dataframe indices of conscutive
                            elements less than the specified width
        Raises:
            None
        """
        lines_df = df.copy()
        width_groups = [[]]
        k = 0
        for i in lines_df.index:
            if lines_df.loc[i, "width"] < const:
                width_groups[k].append(i)
            elif i == 0:
                continue
            elif lines_df.loc[i-1, "width"] < const:
                width_groups.append([])
                k+=1
        return width_groups
