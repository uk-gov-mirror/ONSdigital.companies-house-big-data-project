def tag_extraction(df, tag_col, wanted_tag):
    """
    Inputs:
        :param df: DataFrame
        :param tag_col: name of column that contains tag name (string)
        :param wanted_tag: name of extracted tag(s) (string or list)

    Description:
        Function filters DataFrame to extract xbrl tags, returning the filtered DataFrame.
        Throws error if the wanted tags are not a string or list

    Output: 
        :return: DataFrame filtered to contain only the required tag(s)
    """

    if isinstance(wanted_tag, str):
        t = 1
        tag = [wanted_tag]

    elif isinstance(wanted_tag, list):
        t = 1
        tag = wanted_tag

    if not isinstance(wanted_tag, (str, list)):
        t = 0

    if t == 1:
        output = df[df[tag_col].isin(tag)]

    if t == 0:
        raise TypeError("The wanted_tag needs to be a string or list")

    return output