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
        
	def xbrl_import(fp, year, month):
	    """
	    This function reads in the data for the chosen year and month from a
	    parquet file  and creates a new spark data frame.

	    Args:
  		  fp: The file path for the data source
		    year: The year of the data
		    month: The month of the data

	    Returns: 
		A Spark Data frame

	    Raises:
    		None
	    """
      
      import_fp = fp+'/'+str(year)+'_'+str(month).lower()+'_parsed.parquet'
	    df = spark.read.parquet(import_fp)

	    return(df)

	def tag_count(df):
	    """
	    This function takes a spark data frame as an argument and does a groupby
	    to get all the values within the column headed ‘name’. These values are
	    referred to as ‘tags’. The total number of tags are counted for the whole
	    dataset and ordered by the count from high to low.

	    Args:
    		df: A Spark DataFrame

	    Returns:
		A function object
        
            Raises:
    		None
	    """
      
	    object = (df
		     .groupBy('name')
		     .agg(
		     F.count(
		      'doc_companieshouseregisterednumber').alias('tag_count')
		     )
		     .orderBy(F.col('tag_count'),ascending=False)
		     )

	    return(object)
	
	def tag_distribution(dataframe, tag_contains, tag_col, crn_col):
	    """
	    Provision of distribution of values (referred to as 'tags') attributed to column 'name'.
	    A tag (string type) is specified, filtered by, and counted across unique company records to determine its relative
	    dataset coverage.

	    Args:
		dataframe:    a spark dataframe 
		tag_contains: the value to filter 
		tag_col:      the column the value is attributed to
		crn_col:      the column name for crn values
		
	    Returns:
		A spark dataFrame
		
	    Raises:
		None
	    """

	    dataframe = (
		dataframe
		.filter(
		    F.col(tag_col).contains(tag_contains))
		.groupBy(tag_col)
		.agg(
		    F.count(crn_col).alias('count'),
		    F.countDistinct(crn_col).alias('dist_count')
		)
		.withColumn(
		    '%_coverage', (
		    F.col('dist_count') / dataframe.select(crn_col).dropDuplicates(
		    ).count()) * 100
		)
		.withColumn('duplicate_frac',
		    F.col('count') / F.col('dist_count'))
		.orderBy('%_coverage', ascending=False)
	    )

	    return dataframe

	def cleaning_df(dataframe):
	    """
	    This function lowercases and removes specified punctuation for data in the
	    ‘description’ column.

	    Args:
		dataframe: A Spark DataFrame

	    Returns: 
		dataframe: A Spark DataFrame

	    Raises:
		None
	    """

	    cleaned_df = (
		dataframe
		.withColumn("Description",F.lower(F.col("Description")))
		.withColumn('Description', regexp_replace(
		    'Description', '[()\;#,.]', ''))
	    )

	    return(cleaned_df)
