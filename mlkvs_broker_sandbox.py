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
        'appToken': 'dad99d7d8e52c2c8aaf9fda788d8acdc',
        'gid': 'b498fa9c-adda-4935-957b-a50cf9d3c4e6',
        '__cf_bm': 'DhH2VDXjJL_B7VZmiL3MkeQf8CS9AesqmSzh0smGppA-1671207227-0-ASWaoT2qx/kYIBC2kN38s4I6gZgrcxHzcUc0RkeRanyqq91xnptW/c2sRHWfuX/q26ouxAz0Ubtv8vb+C/N8/gJAQJ6ymH+Gm141/nPsFt7j',
        'twCookieConsentGTM': 'true',
        '_gcl_au': '1.1.1968403973.1671207195',
        'gid': 'b498fa9c-adda-4935-957b-a50cf9d3c4e6',
        'lux_uid': '167120719553634467',
        '__pdst': '7664f14f7383421b814cd50a3337d74c',
        '_gid': 'GA1.2.1409014136.1671207196',
        '_rdt_uuid': '1671207195601.2ff5261a-e2ba-44af-bd4f-5d3266b2e787',
        '_scid': 'db41ba03-0ec2-4f78-8d9e-0103c8303a57',
        '_pk_ses.984.649b': '1',
        'FPLC': 'CM7Sra2wTbTpUZ3ddOxEze2sI6SEXq0JhWvUwxKPEf%2Fh7x9HomI%2F9JPoQ0cXZROHElSBBKVUB%2FjBMjkndaIPy8lV5Bjfq9UDlO%2FKH8KFIfq9fZXL4GbXpTowi7dCKQ%3D%3D',
        'FPID': 'FPID2.2.QhXCI%2BQY5viCrN4s0R58hlNX0xN%2BP9MVhSnBiqQh2u8%3D.1671207196',
        'FPAU': '1.1.1968403973.1671207195',
        '_ts_yjad': '1671207196480',
        '_fbp': 'fb.1.1671207229780.2071022871',
        'tatari-session-cookie': '5003a6dd-cc96-ae17-72fd-22dbc69837ff',
        'localeData': 'en',
        'twCookieConsent': '%7B%22policyId%22%3A%222020-01-31%22%2C%22expiry%22%3A1686932229964%2C%22isEu%22%3Afalse%2C%22status%22%3A%22accepted%22%7D',
        '_tq_id.TV-7290902709-1.649b': 'c8b5c17942e264a6.1671207196.0.1671207431..',
        '_pk_id.984.649b': '9936de1fd3b16efd.1671207196.1.1671207431.1671207196.',
        '_ga': 'GA1.1.840754893.1671207196',
        'mp_e605c449bdf99389fa3ba674d4f5d919_mixpanel': '%7B%22distinct_id%22%3A%20%221851bb610f84bf-0e9114e330f9f8-c457526-1fa400-1851bb610fab01%22%2C%22%24device_id%22%3A%20%221851bb610f84bf-0e9114e330f9f8-c457526-1fa400-1851bb610fab01%22%2C%22%24initial_referrer%22%3A%20%22%24direct%22%2C%22%24initial_referring_domain%22%3A%20%22%24direct%22%7D',
        '_uetsid': '8bebce807d5c11ed8a76bf053a078265',
        '_uetvid': '8bebf4307d5c11eda3983b617da30a5d',
        'tatari-cookie-test': '80201575',
        '_ga_MFT2R11DFX': 'GS1.1.1671207195.1.1.1671208326.0.0.0',
    }

    headers = {
        'authority': 'wise.com',
        'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
        'accept-language': 'pl,en-US;q=0.9,en;q=0.8,ru;q=0.7',
        'cache-control': 'max-age=0',
        # 'cookie': 'appToken=dad99d7d8e52c2c8aaf9fda788d8acdc; gid=b498fa9c-adda-4935-957b-a50cf9d3c4e6; __cf_bm=DhH2VDXjJL_B7VZmiL3MkeQf8CS9AesqmSzh0smGppA-1671207227-0-ASWaoT2qx/kYIBC2kN38s4I6gZgrcxHzcUc0RkeRanyqq91xnptW/c2sRHWfuX/q26ouxAz0Ubtv8vb+C/N8/gJAQJ6ymH+Gm141/nPsFt7j; twCookieConsentGTM=true; _gcl_au=1.1.1968403973.1671207195; gid=b498fa9c-adda-4935-957b-a50cf9d3c4e6; lux_uid=167120719553634467; __pdst=7664f14f7383421b814cd50a3337d74c; _gid=GA1.2.1409014136.1671207196; _rdt_uuid=1671207195601.2ff5261a-e2ba-44af-bd4f-5d3266b2e787; _scid=db41ba03-0ec2-4f78-8d9e-0103c8303a57; _pk_ses.984.649b=1; FPLC=CM7Sra2wTbTpUZ3ddOxEze2sI6SEXq0JhWvUwxKPEf%2Fh7x9HomI%2F9JPoQ0cXZROHElSBBKVUB%2FjBMjkndaIPy8lV5Bjfq9UDlO%2FKH8KFIfq9fZXL4GbXpTowi7dCKQ%3D%3D; FPID=FPID2.2.QhXCI%2BQY5viCrN4s0R58hlNX0xN%2BP9MVhSnBiqQh2u8%3D.1671207196; FPAU=1.1.1968403973.1671207195; _ts_yjad=1671207196480; _fbp=fb.1.1671207229780.2071022871; tatari-session-cookie=5003a6dd-cc96-ae17-72fd-22dbc69837ff; localeData=en; twCookieConsent=%7B%22policyId%22%3A%222020-01-31%22%2C%22expiry%22%3A1686932229964%2C%22isEu%22%3Afalse%2C%22status%22%3A%22accepted%22%7D; _tq_id.TV-7290902709-1.649b=c8b5c17942e264a6.1671207196.0.1671207431..; _pk_id.984.649b=9936de1fd3b16efd.1671207196.1.1671207431.1671207196.; _ga=GA1.1.840754893.1671207196; mp_e605c449bdf99389fa3ba674d4f5d919_mixpanel=%7B%22distinct_id%22%3A%20%221851bb610f84bf-0e9114e330f9f8-c457526-1fa400-1851bb610fab01%22%2C%22%24device_id%22%3A%20%221851bb610f84bf-0e9114e330f9f8-c457526-1fa400-1851bb610fab01%22%2C%22%24initial_referrer%22%3A%20%22%24direct%22%2C%22%24initial_referring_domain%22%3A%20%22%24direct%22%7D; _uetsid=8bebce807d5c11ed8a76bf053a078265; _uetvid=8bebf4307d5c11eda3983b617da30a5d; tatari-cookie-test=80201575; _ga_MFT2R11DFX=GS1.1.1671207195.1.1.1671208326.0.0.0',
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
        'source': 'EUR',
        'target': 'PLN',
        'length': '1',
        'unit': 'day',
    }

    i_url_content = requests.get('https://wise.com/gateway/v3/price?sourceAmount=100000&sourceCurrency=GBP&targetCurrency=USD',
                            headers={'accept-language': 'pl,en-US;q=0.9,en;q=0.8,ru;q=0.7'}, cookies={}, params={}, timeout=2)

    i_json = json.loads(i_url_content.text)
    a = s.get_json_table(i_json)
    a = a.sort_values(by='targetAmount', ascending=False)
    #a['rate'] = a['sourceAmount'] / a['targetAmount']
    a_rate = a['sourceAmount'][1] / a['targetAmount'][1]
    print(a_rate)
    sys.exit()
    print(i_json["rate"]["from"])
    i_indexes = [1]
    dftable = pd.DataFrame(columns=G_OUTPUT_COLUMNS, index=i_indexes)


    for x in range(1, len(i_indexes)+1):
        dftable['pair'][x] = i_json[x-1]["source"] + i_json[x-1]["target"]
        dftable['buy'][x] = int(i_json[x-1]["value"] * 10000)
        dftable['sell'][x] = int(i_json[x-1]["value"] * 10000)
        dftable['broker'][x] = 9
        dftable['rate_type'][x] = 1

    print(dftable)

    print('done downloading URL status code={0} "{1}"'.format(i_url_content.status_code, C_URL))
except Exception as e:
    print('error downloading URL "{0}" {1}'.format(e, C_URL))


