# global variables

import pathlib
import mlkvs_db_tools as d
import mlkvs_url_tools as u
from datetime import datetime
import logging

G_LOGGER = 0
G_DB = 0
G_URL = 0
G_STEPS = 0
G_DELAY_SECONDS = 0
G_SCAN_ID = 0
G_PAIRS = 0
G_QUOTES = 0
G_DB_INI_FILE = 0
G_PROGRAM_MODE = 0
G_OUTPUT_COLUMNS = ['pair', 'buy_qty', 'sell_qty', 'broker']
G_CONST_MODE_PROD = 0
G_CONST_MODE_TEST_DB = 1
G_CONST_MODE_TEST_NODB = 2


def init(a_mode=0):
    # logger init
    i_log_file = pathlib.Path(pathlib.Path(__file__).parent, 'log\\', datetime.now().strftime('%d-%m-%Y__%H-%M-%S.txt'))

    global G_PROGRAM_MODE
    G_PROGRAM_MODE = a_mode

    global G_LOGGER
    G_LOGGER = logging.getLogger()
    G_LOGGER.setLevel(logging.INFO)

    i_formatter = logging.Formatter('%(asctime)s|%(levelname)s|thread:%(thread)d|%(message)s')

    i_file_handler = logging.FileHandler(i_log_file)
    i_file_handler.setFormatter(i_formatter)

    i_console_handler = logging.StreamHandler()
    i_console_handler.setFormatter(i_formatter)

    G_LOGGER.addHandler(i_file_handler)
    G_LOGGER.addHandler(i_console_handler)

    # get settings from DB
    global G_DB_INI_FILE
    G_DB_INI_FILE = pathlib.Path(pathlib.Path(__file__).parent / 'database.ini')

    global G_DB
    G_DB = d.DatabaseTools()

    i_df = G_DB.sql_to_df('select steps, delay_seconds, log_level_file, log_level_console from scrap_param')

    global G_SCAN_ID
    G_SCAN_ID = G_DB.sql_to_single_value('SELECT last_value from scan_scan_id_seq')

    global G_PAIRS
    G_PAIRS = G_DB.sql_to_df(
        'select currency1||currency2 pair, currency_pair_id pair_id from currency_pair where status=%s', '1')

    global G_QUOTES
    G_QUOTES = G_DB.sql_to_df(
        'SELECT currency_quote from currency_quote where status=%s', '1')

    global G_STEPS
    G_STEPS = i_df['steps'].iloc[0]

    global G_DELAY_SECONDS
    G_DELAY_SECONDS = i_df['delay_seconds'].iloc[0]

    i_log_level_file = int(i_df['log_level_file'].iloc[0])
    i_log_level_console = int(i_df['log_level_console'].iloc[0])

    # TEST MODE settings
    if G_PROGRAM_MODE != G_CONST_MODE_PROD:
        G_STEPS = 2
        G_DELAY_SECONDS = 3
        i_log_level_file = logging.DEBUG
        i_log_level_console = logging.INFO

    i_file_handler.setLevel(i_log_level_file)
    i_console_handler.setLevel(i_log_level_console)

    global G_URL
    G_URL = u.URLTools()
