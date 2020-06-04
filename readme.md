# Companies House Accounts

This project aims to independently source XBRL and PDF data from the Companies House accounts website, and produce a merged data
set of processed data for build and archive. The high-level processes are:

1.	Web-scrape all XBRL data from the companies house accounts website. 
2.	Process web-scraped XBRL data by unzipping it, and then converting all data into their csv equivalent.
3.	Web-scrape filled accounts from companies house as pdf data.
4.	Convert each page of the pdf into separate images.
5.	Create a model of a Cascade Classifier 
6.	Apply Classifier to identify and extract the cover page and the balance sheet from converted filled accounts data.
7.	Implement Classifier performance metrics to determine the accuracy and precision of the Classifier.
8.	Apply Optical Character Recognition (OCR) to images that have been classified in step 5 to convert into text data.
9.	Apply Natural Language Processes (NLP) to text data extracted in step 7 to extract patterns from the raw text data.
10.	Merge processed XBRL data from step 2 with data generated from step 8.

## Installation

Use the package manager [pip](https://pip.pypa.io/en/stable/) to install this project's required modules and dependencies.

```bash
pip3 install {module}
```

## Usage

Load main pipeline, and call subsidary modules.

```python
from src.data_processing.cst_data_processing import DataProcessing
from src.classifier.cst_classifier import Classifier
from src.performance_metrics.binary_classifier_metrics import BinaryClassifierMetrics
```

## Contributing
Pull requests are welcome. For major changes, please open an issue first to discuss what you would like to change.

Please make sure to update tests as appropriate.

## License
[MIT](https://choosealicense.com/licenses/mit/)
