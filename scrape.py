#!/usr/bin/python3
import requests
from bs4 import BeautifulSoup
import csv
import lxml
import time
import re
import multiprocessing
from requests.adapters import HTTPAdapter

start_time = time.time()

start_day = '01'
end_day = '31'
month = '05'
year = '2019'
params = {
    'search_text': 'INFORMATION TABLE',
    'sort': 'Date',
    'startDoc': '1',
    'formType': '',
    'isAdv': 'true',
    'stemming': 'true',
    'numResults': '10',
    'fromDate': month+'/'+start_day+'/'+year,
    'toDate': month+'/'+end_day+'/'+year
}
headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/74.0.3729.169 Safari/537.36'}

all_urls = []
records = []

def sort_second(val):
    return val[1]

def scrape_page(url):
    time.sleep(5)
    data = []
    try:
        response = requests_session.get(url, timeout=10)
        soup = BeautifulSoup(response.text, 'lxml')

        # testing limitations to number requests that can be made
        if 'blocked' in response.text:
            print('we have been blocked')
        # if response of ok
        if response.status_code == 200:
            # for every filing on page
            for filing in soup.find(attrs={'xmlns:autn': 'http://schemas.autonomy.com/aci/'}).findAll('tr', attrs={'class': None}):
                        # if filing is an information table (what we are looking for)
                        if 'INFORMATION TABLE' in filing.find(attrs={'class': 'filing'}).text:
                            date = filing.find(attrs={'class': 'blue'}).text
                            text = filing.find(attrs={'class': 'filing'}).text
                            name = text[text.index('for')+4:]
                            href = filing.find(attrs={'class': 'filing'})['href']
                            # fixing special cases where xml files were named incorrectly
                            if 'xml' in href:
                                link = href[href.index('http'):href.index(".xml'")+4]
                                # print(link)
                            else:
                                continue

                            try:
                                # get information table for scraping
                                info_table_response = requests_session.get(link, timeout=11)
                                time.sleep(5)
                                info_table = BeautifulSoup(
                                    info_table_response.text, 'lxml-xml')
                                
                                # test for blocked by server status
                                if 'blocked' in info_table_response.text:
                                    print('we have been blocked')
                                # print(info_table_response.status_code)

                                # if response of ok
                                if info_table_response.status_code == 200:
                                    total = 0
                                    companies = []
                                    try:
                                        # get data for company
                                        for row in info_table.find(attrs={'summary': 'Form 13F-NT Header Information'}).findAll('tr'):
                                            if row.find(attrs={'class': 'FormData'}):
                                                company =   row.find(attrs={'class': 'FormData'}).text
                                                company = company.strip()
                                                value = row.find(
                                                    attrs={'class': 'FormDataR'}).text.replace(',', '')
                                                total += int(value)
                                                companies.append([company, int(value)])
                                                companies.sort(key=sort_second, reverse=True)
                                        for i in range(1, 11):
                                            if len(companies) < i:
                                                companies.append(['', ''])
                                        output = [name, date, total]
                                        for i in range(0, 10):
                                            output.append(companies[i][0])
                                        for i in range(0, 10):
                                            output.append(companies[i][1])
                                        data.append(output)
                                    except:
                                        # if error finding information table
                                        data.append([name, date, total, 'Information Table Not Found'])
                            except requests.exceptions.ConnectionError as e:
                                data.append([name, date, '', 'Information Table Not Found: Connection error, make sure you are connected to the internet'])
                                print(e)
                            except requests.exceptions.Timeout as e:
                                data.append([name, date, '', 'Information Table Not Found: Timeout Error'])
                                print(e)
                            except requests.exceptions.RequestException as e:
                                data.append([name, date, '', 'Information Table Not Found: Error'])
                                print(e)
                            except KeyboardInterrupt:
                                data.append([name, date, '', 'Information Table Not Found: Someone stopped the program'])
    except requests.exceptions.ConnectionError as e:
        print('ERROR:', url)
        print(e)
    except requests.exceptions.Timeout as e:
        print('ERROR: ', url)
        print(e)
    except requests.exceptions.RequestException as e:
        print('ERROR: ', url)
        print(e)
    except KeyboardInterrupt as e:
        print('ERROR: ', url)
        print(e)    
    return data

# generate all the urls to scrape based on how many pages of results there are
def generate_urls(form_type):
    params['formType'] = form_type
    FHR_response = requests_session.get(
        'https://searchwww.sec.gov/EDGARFSClient/jsp/EDGAR_MainAccess.jsp', params=params)
    FHR = BeautifulSoup(FHR_response.text, 'lxml')

    if FHR_response.status_code == 200:
        # print(FHR.find('table', id='result'))
        # print(FHR.find('table', id='result').find(text=re.compile('results')))
        if(FHR.find('table', id='result').find(text=re.compile('results'))):
            total_results = int(FHR.find('table', id='result').find(text=re.compile('results')).parent.previous_sibling.text)
            for i in range(1,total_results,10):
                all_urls.append('https://searchwww.sec.gov/EDGARFSClient/jsp/EDGAR_MainAccess.jsp?search_text=INFORMATION+TABLE&sort=Date&startDoc='+str(i)+'&numResults=10&isAdv=true&formType='+form_type+'&fromDate='+month+'/'+start_day+'/'+year+'&toDate='+month+'/'+end_day+'/'+year+'&stemming=true')

# max the number of attempts made at 5
requests_session = requests.Session()
requests_session.mount('www.sec.gov', HTTPAdapter(max_retries=5))
requests_session.mount('http://', HTTPAdapter(max_retries=5))
requests_session.mount('https://', HTTPAdapter(max_retries=5))

# find all urls to scrape for each form type
generate_urls('Form13FHR')
generate_urls('Form13FHRAD')

# multiprocessing threads = 5
if __name__ == '__main__':
    with multiprocessing.Pool(5) as p:
        records = p.map(scrape_page, all_urls)

# write out data to funds.csv
with open('funds.csv', 'w+', newline='') as csv_file:
    csv_writer = csv.writer(csv_file)
    headers = ['Name', 'Filing Date', 'Total Assets', '1 Name', '2 Name', '3 Name', '4 Name', '5 Name', '6 Name', '7 Name', '8 Name',
               '9 Name', '10 Name', '1 Value', '2 Value', '3 Value', '4 Value', '5 Value', '6 Value', '7 Value', '8 Value', '9 Value', '10 Value']
    csv_writer.writerow(headers)
    for page in records:
        csv_writer.writerows(page)

print(multiprocessing.current_process().name)
print("--- %s seconds ---" % (time.time() - start_time))