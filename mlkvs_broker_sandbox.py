import requests
import pandas as pd
import mlkvs_scrap_tools as s
from bs4 import BeautifulSoup

C_URL: str = 'https://tradingeconomics.com/currencies?quote=pln'
C_URL_SUCCESS = 200
C_TIMEOUT = 1
C_HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/102.0.0.0 Safari/537.36'}


def get_soup_table(a_html_in):
    i_soup = BeautifulSoup(a_html_in.text, "html.parser")
    i_htmltable = i_soup.find('table')
    i_list_table = s.convert_html_to_table(i_htmltable)
    i_htmltable.decompose()

    return pd.DataFrame(i_list_table[0:])

try:


    i_url_content = requests.get(C_URL, headers=C_HEADERS, timeout=C_TIMEOUT)
    a = get_soup_table(i_url_content)
    a = a.drop([0])
    print(a[0])
    print(a[1])
    print(a['1'])


    print('done downloading URL status code={0} "{1}"'.format(i_url_content.status_code, C_URL))
except Exception as e:
    print('error downloading URL "{0}" {1}'.format(e, C_URL))


