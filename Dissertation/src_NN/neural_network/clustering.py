import gcsfs
import os
import pandas as pd

#project_id = 'ons-companies-house-dev'

# Read in the data
df = pd.read_csv("gs://basic_company_data/diss_basic_comp_data.csv")
print(df.head())


