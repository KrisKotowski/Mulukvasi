# webscrapping tools

import threading

import pandas as pd


class ThreadWithReturnValue(threading.Thread):

    def __init__(self, group=None, target=None, name=None, args=(), kwargs={}, Verbose=None):
        threading.Thread.__init__(self, group, target, name, args, kwargs)
        self._return = None

    def run(self):
        if self._target is not None:
            self._return = self._target(*self._args, **self._kwargs)

    def join(self, *args):
        threading.Thread.join(self, *args)
        return self._return


def convert_html_to_table(table):
    """Parses a html segment started with tag <table> followed 
    by multiple <tr> (table rows) and inner <td> (table data) tags. 
    It returns a list of rows with inner columns. 
    Accepts only one <th> (table header/data) in the first row.
    """

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
def convert_html_to_table2(table):
    """Parses a html segment started with tag <table> followed
    by multiple <tr> (table rows) and inner <td> (table data) tags.
    It returns a list of rows with inner columns.
    Accepts only one <th> (table header/data) in the first row.
    """

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


def get_json_table(a_json_in):
    return pd.DataFrame(pd.json_normalize(a_json_in))
