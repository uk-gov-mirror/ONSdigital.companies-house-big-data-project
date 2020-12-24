import gcsfs
import pandas as pd
import numpy as np
import statistics as stats
from pdf_annotator import PDFAnnotator

df = pd.read_csv("gs://ons-companies-house-dev-parsed-pdf-data/doc_ai_token_dfs/top_10/bhp_bs_tokens.csv")

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

def get_line_nums(df):
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
    fdf = df.copy()
    # Use n to count which line number we are on
    n = 0

    # Set up empty lists to add the results to and extract vertices
    odd_ones = []
    vertices = fdf["normed_vertices"]
    fdf["line_num"] = [np.nan]*len(fdf["normed_vertices"])
    fdf["line_num"][0] = 0

    # Add a line number for each of the identified tokens
    for i in range(1, len(vertices)):
        # Added incase 'odd tokens' have been skipped
        k = i -1
        while k in odd_ones:
            k-=1

        # Assign variables for the bottom left vertex of tokens and set y distance 
        # according to the height of the first token
        v2,v1 = eval(vertices[i])[0], eval(vertices[k])[0]        
        d = 0.5*(eval(vertices[k])[3][1] - eval(vertices[k])[0][1])
        
        # If the next token is on the line above, skip and come back later
        if v2[1] < (v1[1] - d):
            odd_ones.append(i)

        # If the next token differs enough in y coordinate, increase the line number
        elif (not is_close(v2[1], v1[1], dist = d)):
            n += 1
        fdf["line_num"][i] = n
    
    # Deal with the skipped tokens
    for i in odd_ones:
        d = 0.5*(eval(fdf["normed_vertices"][i])[3][1]-eval(fdf["normed_vertices"][i])[0][1])
        v1 = eval(fdf["normed_vertices"][i])[0][1]
        j = 0
        # Find the vertex with already assigned line number which has a close y
        # coordinate
        while not is_close(v1, eval(fdf["normed_vertices"][j])[0][1], dist=d):
            j+=1
        fdf["line_num"][i] = fdf["line_num"][j]
    
    return fdf

def add_first_vertex(df):
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

    fdf = df.copy()
    xs, ys = [], []
    for vertices in df["normed_vertices"]:
        xs.append(eval(vertices)[0][0])
        ys.append(eval(vertices)[0][1])
    fdf["first_x_vertex"] = xs
    fdf["first_y_vertex"] = ys

    return fdf

def group_within_line(df):
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
    fdf  = df.sort_values(["line_num", "first_x_vertex"], ascending = ["True", "True"])

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
                new_ws += get_ws(eval(line_df["normed_vertices"][j-1]), eval(line_df["normed_vertices"][j]))["area"]
            
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

    return grouped_df

def get_ws(v1, v2):
    h = max([v1[2][1] - v1[1][1], v2[3][1] - v2[0][1]])
    w = max([v2[0][0] - v1[1][0], v2[3][0] - v1[2][0]])
    return {"width":w, "height":h, "area":w*h}

def get_vertex_dist(tokens_df):
    xsl = []
    xsr = []
    ysb = []
    yst = []
    for i,vertices in enumerate(tokens_df["normed_vertices"]):
        xsl.append([eval(vertices)[0][0],i])
        xsr.append([eval(vertices)[1][0],i])
        ysb.append([eval(vertices)[0][1],i])
        yst.append([eval(vertices)[3][1],i])

    return pd.DataFrame(data = {"left_x_points":xsl, "right_x_points":xsr, "bottom_y_points":ysb, "top_y_points":yst})

def add_av_point(points, point):
    t = [i for i in range(len(points)) if is_close(points[i][0][0], point[0], dist=0.01)]
    new_points = points.copy()
    print(t)
    if len(t) == 0:
        new_points.append([point])
        return(new_points)
    else:
        new_points[t[0]].append(point)
        return new_points


def group_points(points):
    new_points = [[points[0]]]
    for i in range(1, len(points)):
        new_points = add_av_point(new_points, points[i])
    return new_points

def filter_points(grouped_points, k):
    avs = [g for g in grouped_points if len(g)>=k]
    return avs

def get_token_index(grouped_points, k):
    indices = []
    for g in grouped_points:
        print(g)
        if len(g) >= k:
            indices += [x[1] for x in g]
    return indices

def group_by_widths(df, const = 0.5):
    lines_df = df.copy()
    width_groups = [[]]
    k = 0
    for i, w in enumerate(lines_df["width"]):
        if w < const:
            width_groups[k].append(i)
        elif lines_df["width"][i-1] < const:
            width_groups.append([])
            k+=1
    return width_groups


def make_line_df(token_df):
    fdf  = token_df.sort_values(["line_num", "first_x_vertex"], ascending = ["True", "True"])
    fdf["line_num"] = fdf["line_num"].astype('int64')

    line_dict = {"line_num":[],"num_elements":[], "line_vertices":[],"width":[], "height":[], "ws_area":[], "ws_percent":[]}
    for i in range(max(fdf['line_num'])):
        line_dict["line_num"].append(i)
        line_df = fdf[fdf['line_num']==i].reset_index()





new_df = get_line_nums(df)
new_df = add_first_vertex(new_df)
lines_df = group_within_line(new_df)

annotator = PDFAnnotator("bs_pdfs/bhp_bs.pdf")

vdf = get_vertex_dist(lines_df)
x = group_points(vdf["left_x_points"])

# enter = input("Continue?")
# i = 0
# while(enter != "q") and (i <= 82):
#     print(lines_df["value"][i])
#     i+=1
#     enter = input("Continue?")