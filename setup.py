from setuptools import setup, find_packages

setup(
    name = "companies_house_accounts",
    version = "1.0.7",
    description = "The aim of this project is to extract all the balance sheet variables from statutory company accounts filed at Companies House, both in pdf and iXBRL format.  These will then be provided in a standardised format to users in MDR and ESG. MDR will use these to establish which variables are best correlated with the variables being collected in the financial and “capital stocks” surveys. As a result of this MDR work, they will then recommend which of the variables should be included in the statistical business register. ESG will also use the data directly in the National Accounts and Balance of Payments, to improve various historical factors and fixed percentages.",
    url = "https://github.com/spudinator/companies_house_accounts.git",
    author = "Robert Stone",
    author_email = "robert.stone@ons.gov.uk",
    license = "MIT",
    packages = find_packages(exclude=["companies_house_accounts.tests"]),
    install_requires = ["opencv-python"]        
)
