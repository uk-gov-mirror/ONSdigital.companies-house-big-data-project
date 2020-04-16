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
