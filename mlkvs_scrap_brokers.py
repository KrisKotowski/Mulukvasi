import pandas as pd
import mlkvs_scrap_tools as s
import time
import global_vars as gv


class ScrapCinkciarz:
    C_BROKER_ID = 1
    C_BROKER_NAME = 'Cinkciarz'
    C_URLS = [["https://cinkciarz.pl/wa/pe/transactional?subscriptionId=PLN&unit=10", "1"],
              ["https://cinkciarz.pl/wa/pe/transactional?subscriptionId=PLN&unit=50000", "2"],
              # ["https://cinkciarz.pl/wa/pe/transactional?subscriptionId=EUR&unit=10", "unit price"],
              # ["https://cinkciarz.pl/wa/pe/transactional?subscriptionId=EUR&unit=50000", "50K price"],
              # ["https://cinkciarz.pl/wa/pe/transactional?subscriptionId=USD&unit=10", "unit price"],
              # ["https://cinkciarz.pl/wa/pe/transactional?subscriptionId=USD&unit=50000", "50K price"],
              # ["https://cinkciarz.pl/wa/pe/transactional?subscriptionId=GBP&unit=10", "unit price"],
              # ["https://cinkciarz.pl/wa/pe/transactional?subscriptionId=GBP&unit=50000", "50K price"],
              # ["https://cinkciarz.pl/wa/pe/transactional?subscriptionId=CHF&unit=10", "unit price"],

              ]

    def __init__(self):
        self.c_df_urls = pd.DataFrame(self.C_URLS, columns=['url', 'price_type'])

    def read_single_file(self, a_url, a_rate_type):

        gv.G_LOGGER.info('{0} start file download "{1}"'.format(self.C_BROKER_NAME, a_url))

        i_start_time = time.time()
        i_url_content = gv.G_URL.get_url_content(a_url)
        i_end_time = time.time()

        if i_url_content.status_code != gv.G_URL.C_URL_SUCCESS:
            gv.G_LOGGER.info(
                '{0} file download failed, status code: {1} "{2}"'.format(self.C_BROKER_NAME, i_url_content.status_code,
                                                                          a_url))
            return None
        else:
            gv.G_LOGGER.info('{0} done file download in {1} sec. "{2}"'.format(self.C_BROKER_NAME, '{:.2f}'.format(
                i_end_time - i_start_time), a_url))

        # html to table
        dftable = s.get_soup_table(i_url_content)
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

    def read_all_files(self):

        i_threads = list()
        i_dftable_final = pd.DataFrame()

        for index, row in self.c_df_urls.iterrows():
            x = s.ThreadWithReturnValue(target=self.read_single_file, args=(row['url'], row['price_type']))
            i_threads.append(x)
            x.start()

        for index, thread in enumerate(i_threads):
            i_df_output = thread.join()
            i_frames = [i_df_output, i_dftable_final]
            i_dftable_final = pd.concat(i_frames, ignore_index=True)

        return i_dftable_final


class ScrapIK:
    C_BROKER_ID = 2
    C_BROKER_NAME = 'InternetowyKantor'

    C_URLS = [['https://klient.internetowykantor.pl/api/public/marketBrief', '1']]

    def __init__(self):
        self.c_df_urls = pd.DataFrame(self.C_URLS, columns=['url', 'price_type'])

    def read_single_file(self, a_url, a_rate_type):
        gv.G_LOGGER.info('{0} start file download "{1}"'.format(self.C_BROKER_NAME, a_url))

        i_start_time = time.time()
        i_url_content = gv.G_URL.get_url_content(a_url)
        i_end_time = time.time()

        if i_url_content.status_code != gv.G_URL.C_URL_SUCCESS:
            gv.G_LOGGER.info(
                '{0} file download failed, status code: {1} "{2}"'.format(self.C_BROKER_NAME, i_url_content.status_code,
                                                                          a_url))
            return None
        else:
            gv.G_LOGGER.info('{0} done file download in {1} sec. "{2}"'.format(self.C_BROKER_NAME, '{:.2f}'.format(
                i_end_time - i_start_time), a_url))

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

    def read_all_files(self):

        i_threads = list()
        i_dftable_final = pd.DataFrame()

        for index, row in self.c_df_urls.iterrows():
            x = s.ThreadWithReturnValue(target=self.read_single_file, args=(row['url'], row['price_type'],))
            i_threads.append(x)
            x.start()

        for index, thread in enumerate(i_threads):
            i_df_output = thread.join()
            i_frames = [i_df_output, i_dftable_final]
            i_dftable_final = pd.concat(i_frames, ignore_index=True)

        return i_dftable_final
