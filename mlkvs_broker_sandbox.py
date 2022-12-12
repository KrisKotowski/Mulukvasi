import requests
import pandas as pd
import mlkvs_scrap_tools as s
from bs4 import BeautifulSoup
import json

C_URL: str = 'https://www.revolut.com/api/exchange/quote/?amount=100000&country=GB&fromCurrency=USD&isRecipientAmount=false&toCurrency=PLN'
C_URL_SUCCESS = 200
C_TIMEOUT = 1
C_HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/102.0.0.0 Safari/537.36'}


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
        'cookieBannerNewClosed': 'true',
        'isAnalyticsTargetingCookiesEnabled': 'false',
        'rev_geo_country_code': 'PL',
        '_ga_NC0XSL7JGN': 'GS1.1.1670873196.1.0.1670873196.0.0.0',
        '_ga': 'GA1.1.1568213650.1670873197',
    }

    headers = {
        'authority': 'www.revolut.com',
        'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
        'accept-language': 'pl,en-US;q=0.9,en;q=0.8,ru;q=0.7',
        'cache-control': 'max-age=0',
        # 'cookie': 'cookieBannerNewClosed=true; isAnalyticsTargetingCookiesEnabled=false; rev_geo_country_code=PL; _ga_NC0XSL7JGN=GS1.1.1670873196.1.0.1670873196.0.0.0; _ga=GA1.1.1568213650.1670873197',
        'sec-ch-ua': '"Not?A_Brand";v="8", "Chromium";v="108", "Google Chrome";v="108"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'sec-fetch-dest': 'document',
        'sec-fetch-mode': 'navigate',
        'sec-fetch-site': 'none',
        'sec-fetch-user': '?1',
        'upgrade-insecure-requests': '1',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36',
    }

    params = {
        'amount': '100000',
        'country': 'GB',
        'fromCurrency': 'USD',
        'isRecipientAmount': 'false',
        'toCurrency': 'PLN',
    }

    i_url_content = requests.get('https://www.revolut.com/api/exchange/quote/', params=params, cookies=cookies,
                            headers=headers)


    #i_url_content = requests.get(C_URL, headers=C_HEADERS, timeout=C_TIMEOUT)
    print(i_url_content.text)
    y = json.loads(i_url_content.text)

    print(y["rate"]["rate"])
    print('done downloading URL status code={0} "{1}"'.format(i_url_content.status_code, C_URL))
except Exception as e:
    print('error downloading URL "{0}" {1}'.format(e, C_URL))


