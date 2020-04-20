import pyspark.sql.functions as F

class XbrlDataProcessing:
	'''
	'''
	def unique_tag_count(dataframe, name_column, crn_column, count_column):
		"""
		Groups by 'name' column to ascertain the values, 'tags'.
		Counts of unique company records are summed and ordered by descending frequency
		'distinct_count'.

		Args:
			dataframe:    a spark dataFrame
			name_column:  column containing 'name' variable
			crn_column:   column containing CRN variable
			count_column: new column alias definition for frequency counts
		Returns:
			A dataframe ordered by new column, frequencies of distinct CRN values.
		Raises:
			None
		"""
		object = (
			dataframe\
			.groupBy(name_column)\
			.agg(
				F.countDistinct(crn_column).alias(count_column)
			)\
			.orderBy(F.col(count_column), ascending=False)
		)
		
		return (object)

	def principal_activity_filter(df, tag_name = 'descriptionprincipalactivities',
                              matching_string, has_tag_name,
                              has_matching_string):
	    """
	    This function will filter the data frame on whether a company record
	    contains a specified tag (default is ‘descriptionprincipalactivites’) in
	    the name column. If the record contains the tag (the string is present),
	    its value will be searched for the string specified (the matching_string
	    argument) and filtered out. The aim is to leave only the company
	    records that may have a principal activity in the data frame.

	    Args:

		df:                  A Spark DataFrame
		tag_name:            The tag within the name column (column_name)
		matching_string:     The string to filter on
		has_tag_name:        1 means the df has the specified tag
		has_matching_string: 1 means the df has the specified string in the 
				     'value' column

	    Returns:
		A Spark DataFrame

	    Raises:
		None
	    """

	    tag_name = 'descriptionprincipalactivities'          # tag name to filter on
	    matching_string = 'No description of principal activity' # value to filter on
	    column_name = 'doc_companieshouseregisterednumber'    # col to filter on 

	    df = df.withColumn('principal_activity', 
			       F.when(F.col('name') == tag_name, 1).otherwise(0))\
		   .withColumn('no_principal_activity',
		    	       F.when(F.col('value') == matching_string, 1).otherwise(0))\
		   .orderBy([column_name, 'principal_activity'], ascending = False)\
       		   .groupBy([column_name, 'doc_balancesheetdate', 'value'])\
       		   .agg(F.first('principal_activity').alias('principal_activity'),
		    	F.first('no_principal_activity').alias('no_principal_activity'))\
    		   .filter(F.col('principal_activity') == has_tag_name)\
		   .filter(F.col('no_principal_activity') == has_matching_string))
	
	    return(df)
