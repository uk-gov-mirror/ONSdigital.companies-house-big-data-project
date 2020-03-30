from setuptools import setup, find_packages

setup(
    name = "companies_house_accounts",
    version = "1.0.1",
    description = "ons companies house project repos",
    url = "https://github.com/spudinator/companies_house_accounts.git",
    author = "Robert Stone",
    author_email = "robert.stone@ons.gov.uk",
    license = "MIT",
    packages = find_packages(exclude=["companies_house_accounts.tests"]),
    install_requires = ["numpy", "scikit-image", "scikit-learn", "opencv-python"]
    
    
)