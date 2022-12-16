import pandas as pd
import mlkvs_scrap_tools as s
import time
import global_vars as gv
import tradermade as tm
from bs4 import BeautifulSoup
import json


class ScrapBroker:
    C_BROKER_ID = 0
    C_BROKER_NAME = 'parent broker'
    C_URLS = [["", "1"]]
    C_PRICE_TYPE = 1
    C_API_KEY = ''
    C_COOKIES = ''
    C_HEADERS = ''
    C_PARAMS = ''
    C_PAIR = ''

    def __init__(self):
        self.c_df_urls = pd.DataFrame(self.C_URLS, columns=['url', 'price_type'])

    def read_single_file(self, a_url, a_rate_type, a_headers='', a_cookies='', a_params=''):
        gv.G_LOGGER.info('{0} start file download "{1}"'.format(self.C_BROKER_NAME, a_url))

        i_start_time = time.time()
        i_url_content = gv.G_URL.get_url_content(a_url, a_headers, a_cookies, a_params)
        i_end_time = time.time()

        if i_url_content is None:
            gv.G_LOGGER.error('{0} file download failed, timeout "{1}"'.format(self.C_BROKER_NAME, a_url))
            return None
        else:
            if i_url_content.status_code != gv.G_URL.C_URL_SUCCESS:
                gv.G_LOGGER.error('{0} file download failed, status code: {1} "{2}"'.format(self.C_BROKER_NAME,
                                                                                            i_url_content.status_code,
                                                                                            a_url))
                return None
            else:
                gv.G_LOGGER.info('{0} done file download in {1} sec. "{2}"'.format(self.C_BROKER_NAME, '{:.2f}'.format(
                    i_end_time - i_start_time), a_url))

        return i_url_content

    def read_all_files(self):

        i_threads = list()
        i_dftable_final = pd.DataFrame()

        for index, row in self.c_df_urls.iterrows():
            x = s.ThreadWithReturnValue(target=self.read_single_file, args=(row['url'], row['price_type']))
            if x is not None:
                i_threads.append(x)
                x.start()

        if len(i_threads) > 0:
            for index, thread in enumerate(i_threads):
                i_df_output = thread.join()
                i_frames = [i_df_output, i_dftable_final]
                i_dftable_final = pd.concat(i_frames, ignore_index=True)

            return i_dftable_final
        else:
            return None



class ScrapeWise(ScrapBroker):

    def __init__(self):
        self.C_URLS = [["https://wise.com/rates/history?source=EUR&target=PLN&length=1&unit=day", "1"],
                       ["https://wise.com/rates/history?source=USD&target=PLN&length=1&unit=day", "1"],
                       #["https://wise.com/rates/history?source=CHF&target=PLN&length=1&unit=day", "1"],
                       #["https://wise.com/rates/history?source=GBP&target=PLN&length=1&unit=day", "1"]
                       ]

        ScrapBroker.__init__(self)

    def read_single_file(self, a_url, a_rate_type):

        i_url_content = ScrapBroker.read_single_file(self, a_url, a_rate_type, {}, {}, {})

        if i_url_content is None:
            return None

        i_json = json.loads(i_url_content.text)

        i_indexes = [1]
        dftable = pd.DataFrame(columns=gv.G_OUTPUT_COLUMNS, index=i_indexes)

        for x in range(1, len(i_indexes) + 1):
            dftable['pair'][x] = i_json[x - 1]["source"] + i_json[x - 1]["target"]
            dftable['buy'][x] = int(i_json[x - 1]["value"] * 10000)
            dftable['sell'][x] = int(i_json[x - 1]["value"] * 10000)
            dftable['broker'][x] = 9
            dftable['rate_type'][x] = 1

        return dftable

class ScrapMillenium(ScrapBroker):

    def __init__(self):
        self.C_URLS = [["https://www.bankmillennium.pl/portal-apps/getMainFxRates", "1"]]
        ScrapBroker.__init__(self)

    def read_single_file(self, a_url, a_rate_type):

        i_url_content = ScrapBroker.read_single_file(self, a_url, a_rate_type)

        if i_url_content is None:
            return None

        i_json = json.loads(i_url_content.text)

        i_indexes = [1, 2, 3, 4]
        dftable = pd.DataFrame(columns=gv.G_OUTPUT_COLUMNS, index=i_indexes)

        for x in range(1, 5):
            dftable['pair'][x] = i_json["items"][x - 1]["currency"] + "PLN"
            dftable['buy'][x] = int(i_json["items"][x - 1]["foreignExchangeBuy"] * 10000)
            dftable['sell'][x] = int(i_json["items"][x - 1]["foreignExchangeSale"] * 10000)
            dftable['broker'][x] = self.C_BROKER_ID
            dftable['rate_type'][x] = a_rate_type

        return dftable


class ScrapRevolutEUR(ScrapBroker):

    def __init__(self):
        self.C_URLS = [["https://www.revolut.com/api/exchange/quote/", "1"]]

        self.C_PAIR = 'EURPLN'

        self.C_COOKIES = {'cookieBannerNewClosed': 'true', 'isAnalyticsTargetingCookiesEnabled': 'false',
                          'rev_geo_country_code': 'PL', '_ga_NC0XSL7JGN': 'GS1.1.1670873196.1.0.1670873196.0.0.0',
                          '_ga': 'GA1.1.1568213650.1670873197', }

        self.C_HEADERS = {'authority': 'www.revolut.com',
                          'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
                          'accept-language': 'pl,en-US;q=0.9,en;q=0.8,ru;q=0.7', 'cache-control': 'max-age=0',
                          # 'cookie': 'cookieBannerNewClosed=true; isAnalyticsTargetingCookiesEnabled=false; rev_geo_country_code=PL; _ga_NC0XSL7JGN=GS1.1.1670873196.1.0.1670873196.0.0.0; _ga=GA1.1.1568213650.1670873197',
                          'sec-ch-ua': '"Not?A_Brand";v="8", "Chromium";v="108", "Google Chrome";v="108"',
                          'sec-ch-ua-mobile': '?0', 'sec-ch-ua-platform': '"Windows"', 'sec-fetch-dest': 'document',
                          'sec-fetch-mode': 'navigate', 'sec-fetch-site': 'none', 'sec-fetch-user': '?1',
                          'upgrade-insecure-requests': '1',
                          'user-agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36', }

        self.C_PARAMS = {'amount': '100000', 'country': 'GB', 'fromCurrency': 'EUR', 'isRecipientAmount': 'false',
                         'toCurrency': 'PLN', }

        ScrapBroker.__init__(self)

    def read_single_file(self, a_url, a_rate_type):
        i_url_content = ScrapBroker.read_single_file(self, a_url, a_rate_type, self.C_HEADERS, self.C_COOKIES,
                                                     self.C_PARAMS)

        if i_url_content is None:
            return None

        i_json = json.loads(i_url_content.text)

        dftable = pd.DataFrame(columns=gv.G_OUTPUT_COLUMNS, index=[1])
        dftable['pair'] = self.C_PAIR
        dftable['buy'] = int(i_json["rate"]["rate"] * 10000)
        dftable['sell'] = int(i_json["rate"]["rate"] * 10000)
        dftable['broker'] = self.C_BROKER_ID
        dftable['rate_type'] = a_rate_type

        return dftable


class ScrapRevolutUSD(ScrapBroker):

    def __init__(self):
        self.C_URLS = [["https://www.revolut.com/api/exchange/quote/", "1"]]

        self.C_PAIR = 'USDPLN'

        self.C_COOKIES = {'cookieBannerNewClosed': 'true', 'isAnalyticsTargetingCookiesEnabled': 'false',
                          'rev_geo_country_code': 'PL', '_ga_NC0XSL7JGN': 'GS1.1.1670873196.1.0.1670873196.0.0.0',
                          '_ga': 'GA1.1.1568213650.1670873197', }

        self.C_HEADERS = {'authority': 'www.revolut.com',
                          'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
                          'accept-language': 'pl,en-US;q=0.9,en;q=0.8,ru;q=0.7', 'cache-control': 'max-age=0',
                          # 'cookie': 'cookieBannerNewClosed=true; isAnalyticsTargetingCookiesEnabled=false; rev_geo_country_code=PL; _ga_NC0XSL7JGN=GS1.1.1670873196.1.0.1670873196.0.0.0; _ga=GA1.1.1568213650.1670873197',
                          'sec-ch-ua': '"Not?A_Brand";v="8", "Chromium";v="108", "Google Chrome";v="108"',
                          'sec-ch-ua-mobile': '?0', 'sec-ch-ua-platform': '"Windows"', 'sec-fetch-dest': 'document',
                          'sec-fetch-mode': 'navigate', 'sec-fetch-site': 'none', 'sec-fetch-user': '?1',
                          'upgrade-insecure-requests': '1',
                          'user-agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36', }

        self.C_PARAMS = {'amount': '100000', 'country': 'GB', 'fromCurrency': 'USD', 'isRecipientAmount': 'false',
                         'toCurrency': 'PLN', }

        ScrapBroker.__init__(self)

    def read_single_file(self, a_url, a_rate_type):
        i_url_content = ScrapBroker.read_single_file(self, a_url, a_rate_type, self.C_HEADERS, self.C_COOKIES,
                                                     self.C_PARAMS)

        if i_url_content is None:
            return None

        i_json = json.loads(i_url_content.text)

        dftable = pd.DataFrame(columns=gv.G_OUTPUT_COLUMNS, index=[1])
        dftable['pair'] = self.C_PAIR
        dftable['buy'] = int(i_json["rate"]["rate"] * 10000)
        dftable['sell'] = int(i_json["rate"]["rate"] * 10000)
        dftable['broker'] = self.C_BROKER_ID
        dftable['rate_type'] = a_rate_type

        return dftable


class ScrapBloomberg(ScrapBroker):

    def __init__(self):
        self.C_URLS = [["https://www.bloomberg.com/markets/currencies/europe-africa-middle-east", "1"]]

        self.C_COOKIES = {'afUserId': 'a8e62d73-1dd2-4acd-9105-15afb187238d-p', }

        self.C_HEADERS = {'authority': 'www.bloomberg.com',
                          'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
                          'accept-language': 'pl,en-US;q=0.9,en;q=0.8,ru;q=0.7', 'cache-control': 'max-age=0',
                          # 'cookie': 'afUserId=a8e62d73-1dd2-4acd-9105-15afb187238d-p',
                          'sec-ch-ua': '"Not?A_Brand";v="8", "Chromium";v="108", "Google Chrome";v="108"',
                          'sec-ch-ua-mobile': '?0', 'sec-ch-ua-platform': '"Windows"', 'sec-fetch-dest': 'document',
                          'sec-fetch-mode': 'navigate', 'sec-fetch-site': 'none', 'sec-fetch-user': '?1',
                          'upgrade-insecure-requests': '1',
                          'user-agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36', }

        ScrapBroker.__init__(self)

    def read_single_file(self, a_url, a_rate_type):
        def get_soup_table(a_html_in):
            i_soup = BeautifulSoup(a_html_in.text, "html.parser")
            i_htmltable = i_soup.find('table')
            i_list_table = s.convert_html_to_table(i_htmltable)
            i_htmltable.decompose()

            return pd.DataFrame(i_list_table[0:])

        def get_soup_table2(a_html_in):
            i_soup = BeautifulSoup(a_html_in.text, "html.parser")
            i_htmltable = i_soup.find('table')
            i_list_table = s.convert_html_to_table2(i_htmltable)
            i_htmltable.decompose()

            return pd.DataFrame(i_list_table[0:])

        i_url_content = ScrapBroker.read_single_file(self, a_url, a_rate_type)

        if i_url_content is None:
            return None

        # html to table
        dftable1 = get_soup_table(i_url_content)
        dftable2 = get_soup_table2(i_url_content)

        dftable1 = dftable1.drop([0])
        dftable2 = dftable2.drop([0])

        dftable = pd.DataFrame()
        dftable['pair'] = dftable2[0].str.replace('-', '')
        dftable['buy'] = ((dftable1[0]).astype(float) * 10000).apply(int)
        dftable['sell'] = ((dftable1[0]).astype(float) * 10000).apply(int)
        dftable['broker'] = self.C_BROKER_ID
        dftable['rate_type'] = a_rate_type

        return dftable


class ScrapTradingEconomics(ScrapBroker):

    def __init__(self):
        self.C_URLS = [["https://tradingeconomics.com/currencies?quote=pln", "1"],
                       ["https://tradingeconomics.com/currencies?quote=eur", "1"]]

        ScrapBroker.__init__(self)

    def read_single_file(self, a_url, a_rate_type):
        def get_soup_table(a_html_in):
            i_soup = BeautifulSoup(a_html_in.text, "html.parser")
            i_htmltable = i_soup.find('table')
            i_list_table = s.convert_html_to_table(i_htmltable)
            i_htmltable.decompose()

            return pd.DataFrame(i_list_table[0:])

        i_url_content = ScrapBroker.read_single_file(self, a_url, a_rate_type)

        if i_url_content is None:
            return None

        # html to table
        dftable2 = get_soup_table(i_url_content)
        dftable2 = dftable2.drop([0])
        dftable = pd.DataFrame()
        dftable['pair'] = dftable2[0]
        dftable['buy'] = ((dftable2[1]).astype(float) * 10000).apply(int)
        dftable['sell'] = ((dftable2[1]).astype(float) * 10000).apply(int)
        dftable['broker'] = self.C_BROKER_ID
        dftable['rate_type'] = a_rate_type

        return dftable


class ScrapTraderMade(ScrapBroker):

    def __init__(self):
        # self.C_API_KEY = 'DlZmo2SbGyCjzcXyaaPw'
        ScrapBroker.__init__(self)

    def read_single_file(self):

        try:
            gv.G_LOGGER.info('{0} start api data download'.format(self.C_BROKER_NAME))
            i_start_time = time.time()
            tm.set_rest_api_key(self.C_API_KEY)
            i_result = tm.live(currency='USDPLN,EURPLN', fields=["bid", "ask"])
            i_end_time = time.time()
            gv.G_LOGGER.info('done downloading api data {0} sec.'.format(self.C_BROKER_NAME,
                                                                         '{:.2f}'.format(i_end_time - i_start_time)))
        except Exception as e:
            gv.G_LOGGER.error('error downloading api data "{0}" {1}'.format(e, self.C_BROKER_NAME), exc_info=False)
            return None

        dftable = pd.DataFrame(i_result, columns=['instrument', 'bid', 'ask'])
        dftable['pair'] = dftable['instrument']
        dftable['buy'] = ((dftable['bid']).astype(float) * 10000).apply(int)
        dftable['sell'] = ((dftable['ask']).astype(float) * 10000).apply(int)
        dftable['broker'] = self.C_BROKER_ID
        dftable['rate_type'] = self.C_PRICE_TYPE

        dftable.drop('instrument', axis=1, inplace=True)
        dftable.drop('bid', axis=1, inplace=True)
        dftable.drop('ask', axis=1, inplace=True)

        return dftable

    def read_all_files(self):
        i_dftable_final = self.read_single_file()
        return i_dftable_final


class ScrapCinkciarz(ScrapBroker):

    def __init__(self):
        self.C_URLS = [["https://cinkciarz.pl/wa/pe/transactional?subscriptionId=PLN&unit=10", "1"],
                       # ["https://cinkciarz.pl/wa/pe/transactional?subscriptionId=PLN&unit=50000", "2"],
                       # ["https://cinkciarz.pl/wa/pe/transactional?subscriptionId=EUR&unit=10", "unit price"],
                       # ["https://cinkciarz.pl/wa/pe/transactional?subscriptionId=EUR&unit=50000", "50K price"],
                       # ["https://cinkciarz.pl/wa/pe/transactional?subscriptionId=USD&unit=10", "unit price"],
                       # ["https://cinkciarz.pl/wa/pe/transactional?subscriptionId=USD&unit=50000", "50K price"],
                       # ["https://cinkciarz.pl/wa/pe/transactional?subscriptionId=GBP&unit=10", "unit price"],
                       # ["https://cinkciarz.pl/wa/pe/transactional?subscriptionId=GBP&unit=50000", "50K price"],
                       # ["https://cinkciarz.pl/wa/pe/transactional?subscriptionId=CHF&unit=10", "unit price"],

                       ]
        ScrapBroker.__init__(self)

    def read_single_file(self, a_url, a_rate_type):
        def get_soup_table(a_html_in):
            i_soup = BeautifulSoup(a_html_in.text, "html.parser")
            i_htmltable = i_soup.find('table')
            i_list_table = s.convert_html_to_table(i_htmltable)
            i_htmltable.decompose()

            return pd.DataFrame(i_list_table[0:])

        i_url_content = ScrapBroker.read_single_file(self, a_url, a_rate_type)

        if i_url_content is None:
            return None

        # html to table
        dftable = get_soup_table(i_url_content)
        dftable = dftable.drop([0])
        dftable[1] = 1
        dftable[5] = 1
        dftable[6] = self.C_BROKER_ID
        dftable[7] = a_rate_type
        dftable = dftable[[5, 6, 0, 7, 1, 3, 4]]
        dftable = dftable.rename({0: 'pair', 1: 'qty', 3: 'buy', 4: 'sell', 5: 'scan_id', 6: 'broker', 7: 'rate_type'},
                                 axis=1)
        dftable['buy'] = ((dftable['buy'].str.replace(',', '.')).astype(float) * 10000).apply(int)
        dftable['sell'] = ((dftable['sell'].str.replace(',', '.')).astype(float) * 10000).apply(int)

        dftable.drop('scan_id', axis=1, inplace=True)
        dftable.drop('qty', axis=1, inplace=True)

        return dftable


class ScrapIK(ScrapBroker):

    def __init__(self):
        self.C_URLS = [["https://klient.internetowykantor.pl/api/public/marketBrief", "1"]]
        ScrapBroker.__init__(self)

    def read_single_file(self, a_url, a_rate_type):
        i_url_content = ScrapBroker.read_single_file(self, a_url, a_rate_type)

        if i_url_content is None:
            return None

        # json to table
        dftable = s.get_json_table(i_url_content.json())

        dftable = dftable.drop(columns=['ts', 'directExchangeOffers.forexOld', 'directExchangeOffers.buyOld',
                                        'directExchangeOffers.sellOld', 'directExchangeOffers.forexNow'])
        dftable = dftable.rename({'directExchangeOffers.buyNow': 'buy', 'directExchangeOffers.sellNow': 'sell'}, axis=1)
        dftable['pair'] = dftable['pair'].str.replace('_', '')
        dftable['qty'] = 1
        dftable['broker'] = self.C_BROKER_ID
        dftable['scan_id'] = 1
        dftable['rate_type'] = a_rate_type
        dftable['buy'] = (dftable['buy'] * 10000).apply(int)
        dftable['sell'] = (dftable['sell'] * 10000).apply(int)

        dftable = dftable[['scan_id', 'broker', 'pair', 'rate_type', 'qty', 'buy', 'sell']]

        dftable.drop('scan_id', axis=1, inplace=True)
        dftable.drop('qty', axis=1, inplace=True)

        return dftable
