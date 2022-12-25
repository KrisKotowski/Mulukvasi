import pandas as pd
import mlkvs_scrap_tools as s
import time
import global_vars as gv
import tradermade as tm
from bs4 import BeautifulSoup
import json
import requests


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
        self.C_URLS = [
            ["https://wise.com/gateway/v3/price?sourceAmount=50000&sourceCurrency=PLN&targetCurrency=EUR", "1"],
            ["https://wise.com/gateway/v3/price?sourceAmount=50000&sourceCurrency=PLN&targetCurrency=USD", "1"],
            ["https://wise.com/gateway/v3/price?sourceAmount=50000&sourceCurrency=EUR&targetCurrency=PLN", "1"],
            ["https://wise.com/gateway/v3/price?sourceAmount=50000&sourceCurrency=USD&targetCurrency=PLN", "1"],
            ["https://wise.com/gateway/v3/price?sourceAmount=50000&sourceCurrency=PLN&targetCurrency=GBP", "1"],
            ["https://wise.com/gateway/v3/price?sourceAmount=50000&sourceCurrency=PLN&targetCurrency=CHF", "1"],
            ["https://wise.com/gateway/v3/price?sourceAmount=50000&sourceCurrency=GBP&targetCurrency=PLN", "1"],
            ["https://wise.com/gateway/v3/price?sourceAmount=50000&sourceCurrency=CHF&targetCurrency=PLN", "1"],
            ["https://wise.com/gateway/v3/price?sourceAmount=50000&sourceCurrency=PLN&targetCurrency=NOK", "1"],
            ["https://wise.com/gateway/v3/price?sourceAmount=50000&sourceCurrency=NOK&targetCurrency=PLN", "1"], ]

        self.C_HEADERS = {}
        self.C_COOKIES = {}
        self.C_PARAMS = {}

        ScrapBroker.__init__(self)

    def read_single_file(self, a_url, a_rate_type):

        i_url_content = ScrapBroker.read_single_file(self, a_url, a_rate_type, self.C_HEADERS, self.C_COOKIES,
                                                     self.C_PARAMS)

        if i_url_content is None:
            return None

        i_json = json.loads(i_url_content.text)

        i_indexes = [1]
        dftable = pd.DataFrame(columns=gv.G_OUTPUT_COLUMNS, index=i_indexes)

        i_json_df = s.get_json_table(i_json)
        i_json_df = i_json_df.sort_values(by='targetAmount', ascending=False)

        for x in range(1, len(i_indexes) + 1):
            dftable['pair'][x] = i_json_df["sourceCcy"][1] + i_json_df['targetCcy'][1]
            dftable['sell'][x] = int(i_json_df['sourceAmount'][1])
            dftable['buy'][x] = int(i_json_df['targetAmount'][1])
            dftable['broker'][x] = self.C_BROKER_ID
            dftable['rate_type'][x] = a_rate_type

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

        # reverse
        dftable2 = dftable.copy()

        dftable2['pair'] = dftable2['pair'].str.slice(3, 6) + dftable2['pair'].str.slice(0, 3)
        dftable2['buy'] = ((1 / (dftable2['sell'] / 10000)) * 10000).apply(int)
        dftable2['sell'] = ((1 / (dftable2['buy'] / 10000)) * 10000).apply(int)

        return pd.concat([dftable, dftable2])


class ScrapeRevolut(ScrapBroker):

    def __init__(self):
        self.C_URLS = [[
            "https://www.revolut.com/api/exchange/quote/?amount=50000&country=GB&fromCurrency=EUR&isRecipientAmount=false&toCurrency=PLN",
            "1"], [
            "https://www.revolut.com/api/exchange/quote/?amount=50000&country=GB&fromCurrency=USD&isRecipientAmount=false&toCurrency=PLN",
            "1"], [
            "https://www.revolut.com/api/exchange/quote/?amount=50000&country=GB&fromCurrency=PLN&isRecipientAmount=false&toCurrency=USD",
            "1"], [
            "https://www.revolut.com/api/exchange/quote/?amount=50000&country=GB&fromCurrency=PLN&isRecipientAmount=false&toCurrency=EUR",
            "1"], [
            "https://www.revolut.com/api/exchange/quote/?amount=50000&country=GB&fromCurrency=GBP&isRecipientAmount=false&toCurrency=PLN",
            "1"], [
            "https://www.revolut.com/api/exchange/quote/?amount=50000&country=GB&fromCurrency=CHF&isRecipientAmount=false&toCurrency=PLN",
            "1"], [
            "https://www.revolut.com/api/exchange/quote/?amount=50000&country=GB&fromCurrency=PLN&isRecipientAmount=false&toCurrency=GBP",
            "1"], [
            "https://www.revolut.com/api/exchange/quote/?amount=50000&country=GB&fromCurrency=PLN&isRecipientAmount=false&toCurrency=CHF",
            "1"], [
            "https://www.revolut.com/api/exchange/quote/?amount=50000&country=GB&fromCurrency=NOK&isRecipientAmount=false&toCurrency=PLN",
            "1"], [
            "https://www.revolut.com/api/exchange/quote/?amount=50000&country=GB&fromCurrency=PLN&isRecipientAmount=false&toCurrency=NOK",
            "1"]]

        self.C_HEADERS = {'accept-language': 'pl,en-US;q=0.9,en;q=0.8,ru;q=0.7'}
        self.C_COOKIES = {}
        self.C_PARAMS = {}

        ScrapBroker.__init__(self)

    def read_single_file(self, a_url, a_rate_type):

        i_url_content = ScrapBroker.read_single_file(self, a_url, a_rate_type, self.C_HEADERS, self.C_COOKIES,
                                                     self.C_PARAMS)

        if i_url_content is None:
            return None

        i_json = json.loads(i_url_content.text)

        i_indexes = [1]
        dftable = pd.DataFrame(columns=gv.G_OUTPUT_COLUMNS, index=i_indexes)

        for x in range(1, len(i_indexes) + 1):
            dftable['pair'][x] = i_json["sender"]["currency"] + i_json["recipient"]["currency"]
            dftable['sell'][x] = int(i_json["sender"]["amount"])
            dftable['buy'][x] = int(i_json["recipient"]["amount"])
            dftable['broker'][x] = self.C_BROKER_ID
            dftable['rate_type'][x] = a_rate_type

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

        # reverse
        dftable2 = dftable.copy()

        dftable2['pair'] = dftable2['pair'].str.slice(3, 6) + dftable2['pair'].str.slice(0, 3)
        dftable2['buy'] = ((1 / (dftable2['buy'] / 10000)) * 10000).apply(int)
        dftable2['sell'] = ((1 / (dftable2['sell'] / 10000)) * 10000).apply(int)

        return pd.concat([dftable, dftable2])


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

        # reverse
        dftable2 = dftable.copy()

        dftable2['pair'] = dftable2['pair'].str.slice(3, 6) + dftable2['pair'].str.slice(0, 3)
        dftable2['buy'] = ((1 / (dftable2['buy'] / 10000)) * 10000).apply(int)
        dftable2['sell'] = ((1 / (dftable2['sell'] / 10000)) * 10000).apply(int)

        return pd.concat([dftable, dftable2])


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

        # reverse
        dftable2 = dftable.copy()

        dftable2['pair'] = dftable2['pair'].str.slice(3, 6) + dftable2['pair'].str.slice(0, 3)
        dftable2['buy'] = ((1 / (dftable2['buy'] / 10000)) * 10000).apply(int)
        dftable2['sell'] = ((1 / (dftable2['sell'] / 10000)) * 10000).apply(int)

        return pd.concat([dftable, dftable2])

    def read_all_files(self):
        i_dftable_final = self.read_single_file()
        return i_dftable_final


class ScrapCinkciarz(ScrapBroker):

    def __init__(self):
        self.C_URLS = [["https://cinkciarz.pl/wa/home-rates", "1"]]

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

        i_json = json.loads(i_url_content.text)

        i_indexes = [1,2,3,4,5,6,7,8]
        i_indexes2 = [1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16]

        dftable = pd.DataFrame(columns=gv.G_OUTPUT_COLUMNS, index=i_indexes2)

        for x in range(1, len(i_indexes) + 1):
            # buy
            dftable['pair'][x] = i_json["currenciesRate"][x-1]["currenciesPair"]["from"] + i_json["currenciesRate"][x-1]["currenciesPair"]["to"]
            dftable['sell'][x] = i_json["numberOfUnits"]
            dftable['buy'][x] = int(float(i_json["currenciesRate"][x-1]["sell"]["rate"]) * i_json["numberOfUnits"])
            dftable['broker'][x] = self.C_BROKER_ID
            dftable['rate_type'][x] = a_rate_type

        for x in range(1, len(i_indexes) + 1):
            # sell
            dftable['pair'][x + 8] = i_json["currenciesRate"][x-1]["currenciesPair"]["to"] + i_json["currenciesRate"][x-1]["currenciesPair"]["from"]
            dftable['sell'][x + 8] = i_json["numberOfUnits"]
            dftable['buy'][x + 8] = int(float(i_json["currenciesRate"][x-1]["buy"]["rate"]) * i_json["numberOfUnits"])
            dftable['broker'][x + 8] = self.C_BROKER_ID
            dftable['rate_type'][x + 8] = a_rate_type

        return dftable


class ScrapIK(ScrapBroker):

    def __init__(self):
        self.C_URLS = [["https://klient.internetowykantor.pl/api/public/directExchangeCompare/BUY/50000/PLN/EUR/", "1"],
                       ["https://klient.internetowykantor.pl/api/public/directExchangeCompare/BUY/50000/EUR/PLN/", "1"],
                       ["https://klient.internetowykantor.pl/api/public/directExchangeCompare/BUY/50000/PLN/USD/", "1"],
                       ["https://klient.internetowykantor.pl/api/public/directExchangeCompare/BUY/50000/USD/PLN/", "1"],
                       ["https://klient.internetowykantor.pl/api/public/directExchangeCompare/BUY/50000/PLN/CHF/", "1"],
                       ["https://klient.internetowykantor.pl/api/public/directExchangeCompare/BUY/50000/CHF/PLN/", "1"],
                       ["https://klient.internetowykantor.pl/api/public/directExchangeCompare/BUY/50000/PLN/GBP/", "1"],
                       ["https://klient.internetowykantor.pl/api/public/directExchangeCompare/BUY/50000/GBP/PLN/", "1"],
                       ["https://klient.internetowykantor.pl/api/public/directExchangeCompare/BUY/50000/PLN/NOK/", "1"],
                       ["https://klient.internetowykantor.pl/api/public/directExchangeCompare/BUY/50000/NOK/PLN/", "1"]]
        ScrapBroker.__init__(self)

    def read_single_file(self, a_url, a_rate_type):
        i_url_content = ScrapBroker.read_single_file(self, a_url, a_rate_type)

        if i_url_content is None:
            return None

        # json to table
        i_json = json.loads(i_url_content.text)

        i_indexes = [1]
        dftable = pd.DataFrame(columns=gv.G_OUTPUT_COLUMNS, index=i_indexes)

        for x in range(1, len(i_indexes) + 1):
            dftable['pair'][x] = a_url[-8:].replace('/', '')
            dftable['sell'][x] = 50000
            dftable['buy'][x] = int(float(i_json["result"]["exchangeAmount"]))
            dftable['broker'][x] = self.C_BROKER_ID
            dftable['rate_type'][x] = a_rate_type

        return dftable
