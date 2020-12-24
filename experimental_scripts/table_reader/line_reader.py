import pandas as pd
import numpy as np

class LineReader():

    def __init__(self, df):
        self.data = df
        self.original_data = df
    
    def get_line_nums(self):
        """
        Takes a token dataframe output of the doc ai API and adds line numbers by grouping tokens
        with similar y coordinates. Tokens are determined to be on the same line if their
        y coordinates differ by less than half the height of the first token.
        
        Arguments:
            df: token dataframe output from the doc ai API
        Returns:
            fdf:    final dataframe after line numbers have been added to the original
        Raises:
            None
        """
        fdf = self.data.copy()
        fdf  = fdf.sort_values(["first_y_vertex"], ascending = ["True"])
        fdf = fdf.reset_index()
        
        # Use n to count which line number we are on
        n = 0

        # Set up empty lists to add the results to and extract vertices
        odd_ones = []
        vertices = fdf["normed_vertices"]
        # fdf["line_num"] = [np.nan]*len(fdf["normed_vertices"])
        fdf.loc[0,"line_num"] = 0

        # Add a line number for each of the identified tokens
        for i in range(1, len(vertices)):
            # Added incase 'odd tokens' have been skipped
            k = i -1
            while k in odd_ones:
                k-=1
                print(k, fdf.loc[i, "value"])

            # Assign variables for the bottom left vertex of tokens and set y distance 
            # according to the height of the first token
            v2,v1 = eval(vertices[i])[0], eval(vertices[k])[0]        
            d = 0.5*(eval(vertices[k])[3][1] - eval(vertices[k])[0][1])
            
            # If the next token is on the line above, skip and come back later
            if v2[1] < (v1[1] - d):
                odd_ones.append(i)
                continue

            # If the next token differs enough in y coordinate, increase the line number
            elif (not self.is_close(v2[1], v1[1], dist = d)):
                n += 1
            fdf.loc[i,"line_num"] = n
        print(odd_ones)
        # Deal with the skipped tokens
        for i in odd_ones:
            d = 0.1*(eval(fdf["normed_vertices"][i])[3][1]-eval(fdf["normed_vertices"][i])[0][1])
            v1 = eval(fdf.loc[i,"normed_vertices"])[0][1]
            j = 0
            # Find the vertex with already assigned line number which has a close y
            # coordinate
            while not self.is_close(v1, eval(fdf.loc[j,"normed_vertices"])[0][1], dist=d):
                j+=1
            fdf.loc[i,"line_num"] = fdf.loc[j,"line_num"]
        
        self.data = fdf
        # return fdf

    def group_within_line(self):
        """
        Takes a token data frame with line numbers, and returns a data frame where the text
        on each line has been grouped by tokens which are closest to eachother. This is 
        effectively identifying different bodies of text on each line.

        Arguments:
            df: data frame of the token output, with line numbers included
        Returns:
            new_df: data frame where the 'value' column contains groups of text by line

        """
        # Sort the input dataframe by line and x coordinate
        fdf  = self.data.sort_values(["line_num", "first_x_vertex"], ascending = ["True", "True"])

        fdf["line_num"] = fdf["line_num"].astype('int64')

        # Create new dict to store new info
        new_dict = {"value" : [], "confidence" : [], "normed_vertices" : [], "line_num":[], "white_space":[], "width":[]}

        # Loop over the tokens on each line
        for i in range(max(fdf['line_num'])):
            line_df = fdf[fdf['line_num']==i].reset_index()

            # Set the relevant values for the first token
            new_value = line_df["value"][0]
            new_confidence = line_df["confidence"][0]
            new_vertices = eval(line_df["normed_vertices"][0])
            new_ws = 0

            # Loop over tokens on the line, considering pairs of conscutive tokens
            for j in range(1, line_df.shape[0]):
                # Assign variables for the adjacent corners of consective tokens
                v1,v2 = eval(line_df["normed_vertices"][j-1])[1][0], eval(line_df["normed_vertices"][j])[0][0]

                # Set h to be the 'height' of the previous token
                h = eval(line_df["normed_vertices"][j])[3][1] - eval(line_df["normed_vertices"][j])[0][1]

                # Condition if two tokens are sufficiently close (as a proportion of the height)
                if v2 - v1 < h:
                    # Concetenate the relevant values for consecutive tokens
                    new_value = new_value + line_df["value"][j]
                    new_confidence = new_confidence*line_df["confidence"][j]
                    new_vertices[1:3] = eval(line_df["normed_vertices"][j])[1:3]
                    new_ws += self.get_ws(eval(line_df["normed_vertices"][j-1]), eval(line_df["normed_vertices"][j]))["area"]
                
                else:
                    # Add the concatenated values to the dict
                    new_dict["value"].append(new_value)
                    new_dict["confidence"].append(new_confidence)
                    new_dict["normed_vertices"].append(new_vertices)
                    new_dict["line_num"].append(i)
                    new_dict["white_space"].append(new_ws)
                    new_dict["width"].append(new_vertices[1][0] - new_vertices[0][0])

                    # Reset the values according to the current token
                    new_value = line_df["value"][j]
                    new_confidence = line_df["confidence"][j]
                    new_vertices = eval(line_df["normed_vertices"][j])
                    new_ws = 0
            
            # Add on the final values
            new_dict["value"].append(new_value)
            new_dict["confidence"].append(new_confidence)
            new_dict["normed_vertices"].append(new_vertices)
            new_dict["line_num"].append(i)
            new_dict["white_space"].append(new_ws)
            new_dict["width"].append(new_vertices[1][0] - new_vertices[0][0])


        grouped_df = pd.DataFrame.from_dict(new_dict)
        grouped_df["normed_vertices"] = grouped_df["normed_vertices"].astype("str")

        self.data = grouped_df
        # return grouped_df

    def add_first_vertex(self):
        """
        Add columns to the dataframe for the x and y coordinate of the first (bottom left)
        vertex for each token

        Arguments:
            df: token dataframe with "line_num" column
        Returns:
            fdf: final data frame with 'first_x_vertex' and 'first_y_vertex' columns
                added
        Raises:
            None
        """
        fdf = self.data.copy()
        xs, ys = [], []
        for vertices in fdf["normed_vertices"]:
            xs.append(eval(vertices)[0][0])
            ys.append(eval(vertices)[0][1])
        fdf["first_x_vertex"] = xs
        fdf["first_y_vertex"] = ys
        self.data = fdf
        return fdf

    @staticmethod
    def get_ws(p1, p2):
        """
        Get the dimensions of the whitespace between two elements.
        
        Arguments:
            p1: The normed vertices of the polygon bounding the first element
            p2: The normed vertices of the polygon bounding the first element
        Returns (as a dict):
            width:  (max) x distance between the end of p1 and the start of p2
            height: (max) y distance between the top and bottom of the two elements
            area:   width*height as computed above
        Raises:
            None
        """
        h = max([p1[2][1] - p1[1][1], p2[3][1] - p2[0][1]])
        w = max([p2[0][0] - p1[1][0], p2[3][0] - p1[2][0]])
        return {"width":w, "height":h, "area":w*h}

    @staticmethod
    def is_close(x, y, dist = 1e-7, show = False):
        """
        Returns bool of whether two values are sufficiently close.
        Warning - could be prone to float errors.
        """
        if show:
            print(abs(x-y), dist)
        if abs(x-y) < dist:
            return True
        else:
            return False





    

    