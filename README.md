# SEC-EDGAR-Scraper
##### by Sean Cao
A python web scraper used to pull statistics out of 13F-HR and 13F-HR/A information tables by date on the 
[SEC website](https://searchwww.sec.gov/EDGARFSClient/jsp/EDGAR_MainAccess.jsp) and outputs the data to an xml file.
## Installation
- pip install requests
- pip install beautifulsoup4
- pip install multiprocessing
- pip install lxml
## Usage
- Update the start_date, end_day, month, and year parameters to the time period to be searched
- run **python funds.py**
