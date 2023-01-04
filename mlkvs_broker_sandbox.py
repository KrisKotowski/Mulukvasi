import requests
import pandas as pd
import mlkvs_scrap_tools as s
from bs4 import BeautifulSoup
import json
import sys

C_URL: str = 'https://www.bankmillennium.pl/portal-apps/getMainFxRates'
C_URL_SUCCESS = 200
C_TIMEOUT = 1
C_HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/102.0.0.0 Safari/537.36'}
G_OUTPUT_COLUMNS = ['pair', 'buy', 'sell', 'broker', 'rate_type']


def get_soup_table(a_html_in):
    def convert_html_to_table(table):
        def row_get_data_text(tr, coltag='td'):  # td (data) or th (header)
            return [td.get_text(strip=True) for td in tr.find_all(coltag)]

        rows = []
        trs = table.find_all('tr')
        headerow = row_get_data_text(trs[0], 'th')
        if headerow:  # if there is a header row include first
            rows.append(headerow)
            trs = trs[1:]
        for tr in trs:  # for every table row
            rows.append(row_get_data_text(tr, 'td'))  # data row
        return rows

    i_soup = BeautifulSoup(a_html_in.text, "html.parser")
    i_htmltable = i_soup.find('table')
    i_list_table = convert_html_to_table(i_htmltable)
    i_htmltable.decompose()

    return pd.DataFrame(i_list_table[0:])

def get_soup_table2(a_html_in):
    def convert_html_to_table(table):
        def row_get_data_text(tr, coltag='td'):  # td (data) or th (header)
            return [td.get_text(strip=True) for td in tr.find_all(coltag)]

        rows = []
        trs = table.find_all('tr')
        headerow = row_get_data_text(trs[0], 'th')
        if headerow:  # if there is a header row include first
            rows.append(headerow)
            trs = trs[1:]
        for tr in trs:  # for every table row
            rows.append(row_get_data_text(tr, 'th'))  # data row
        return rows

    i_soup = BeautifulSoup(a_html_in.text, "html.parser")
    i_htmltable = i_soup.find('table')
    i_list_table = convert_html_to_table(i_htmltable)
    i_htmltable.decompose()

    return pd.DataFrame(i_list_table[0:])

def get_json_table(a_json_in):
    return pd.DataFrame(pd.json_normalize(a_json_in))

try:

    cookies = {
        'sid': 'ba2191a1863fdc256fc3158c49ebb92cb50d3d67',
        'fxID': 'msugb0qvtvvgsjurt5t29ajnk1',
        'ln_or': 'eyI0ODUzOTI5IjoiZCJ9',
        'cookie_consent_user_consent_token': 'IUdyfMyfKQNm',
        'cookie_consent_user_accepted': 'true',
        '_gcl_au': '1.1.1682682648.1672697396',
        '_fbp': 'fb.1.1672697396304.1010728710',
        '_gid': 'GA1.2.481276116.1672697396',
        'cookie_consent_level': '%7B%22strictly-necessary%22%3Atrue%2C%22functionality%22%3Atrue%2C%22tracking%22%3Atrue%2C%22targeting%22%3Atrue%7D',
        '_ga': 'GA1.2.1867842534.1672697377',
        '_ga_E339RWGCP4': 'GS1.1.1672697376.1.1.1672697491.0.0.0',
    }

    headers = {
        'Accept': 'application/json, text/javascript, */*; q=0.01',
        'Accept-Language': 'pl,en-US;q=0.9,en;q=0.8,ru;q=0.7',
        'Connection': 'keep-alive',
        # 'Cookie': 'sid=ba2191a1863fdc256fc3158c49ebb92cb50d3d67; fxID=msugb0qvtvvgsjurt5t29ajnk1; ln_or=eyI0ODUzOTI5IjoiZCJ9; cookie_consent_user_consent_token=IUdyfMyfKQNm; cookie_consent_user_accepted=true; _gcl_au=1.1.1682682648.1672697396; _fbp=fb.1.1672697396304.1010728710; _gid=GA1.2.481276116.1672697396; cookie_consent_level=%7B%22strictly-necessary%22%3Atrue%2C%22functionality%22%3Atrue%2C%22tracking%22%3Atrue%2C%22targeting%22%3Atrue%7D; _ga=GA1.2.1867842534.1672697377; _ga_E339RWGCP4=GS1.1.1672697376.1.1.1672697491.0.0.0',
        'Origin': 'https://www.kantor.pl',
        'Referer': 'https://www.kantor.pl/kursy-walut',
        'Sec-Fetch-Dest': 'empty',
        'Sec-Fetch-Mode': 'cors',
        'Sec-Fetch-Site': 'same-origin',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36',
        'X-Requested-With': 'XMLHttpRequest',
        'sec-ch-ua': '"Not?A_Brand";v="8", "Chromium";v="108", "Google Chrome";v="108"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
    }

    data = {
        'getExchageRates': '1',
    }

    response = requests.post('https://www.kantor.pl/ajax', cookies=cookies, headers=headers, data=data)

    print(response.text)

    print('done downloading URL status code={0} "{1}"'.format(i_url_content.status_code, C_URL))
except Exception as e:
    print('error downloading URL "{0}" {1}'.format(e, C_URL))


