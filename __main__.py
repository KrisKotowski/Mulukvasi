# main project file

import mlkvs_scrap_tools as s
import pandas as pd
import time
import global_vars as gv
import sys
import importlib

try:
    # initialize global vars

    if len(sys.argv) > 1:
        if str(sys.argv[1]) == '0':
            print('PROGRAM running in production mode.')
        elif str(sys.argv[1]) == '1':
            print('PROGRAM running in test mode with saving to DB. 2 steps, 3 seconds delay.')
        elif str(sys.argv[1]) == '2':
            print('PROGRAM running in test mode without saving to DB. 2 steps, 3 seconds delay.')
        else:
            print('Unknown argument passed [{0}] !!!'.format(str(sys.argv[1])))
            sys.exit()
        gv.init(int(sys.argv[1]))
    else:
        gv.init()

    # initialize scraper objects
    i_scraps = list()
    i_module = importlib.import_module("mlkvs_scrap_brokers")

    i_cursor = gv.G_DB.execute_sql(
        'select broker_id, descr, class_name, api_key from public.broker where status=1::text', 0)
    i_rows = i_cursor.fetchall()

    for i_row in i_rows:
        i_classname = i_row[2]
        i_class = getattr(i_module, i_classname)
        i_instance = i_class()
        i_instance.C_BROKER_ID = i_row[0]
        i_instance.C_BROKER_NAME = i_row[1]
        i_instance.C_API_KEY = i_row[3]
        i_scraps.append(i_instance)

    if len(i_scraps) == 0:
        raise Exception('no active brokers to scrap')

    #sys.exit()
    gv.G_LOGGER.info('PROGRAM START: initialization success in mode [{0}]'.format(gv.G_PROGRAM_MODE))

except Exception as e:
    gv.G_LOGGER.error('initialization failed "{0}"'.format(e), exc_info=True)
    gv.G_LOGGER.error('program halted')
    sys.exit()

try:
    i_threads = list()

    # start scraping
    gv.G_LOGGER.info('start scraping')

    for i_step in range(1, gv.G_STEPS + 1):

        gv.G_LOGGER.info('starting step {0}...'.format(i_step))

        # clear lists before next iteration
        i_dftable_final = pd.DataFrame()
        i_df_output = pd.DataFrame()
        i_threads.clear()

        # multithread function call
        for i_object in i_scraps:
            i_thread_scrap = s.ThreadWithReturnValue(target=i_object.read_all_files)
            if i_thread_scrap is not None:
                i_threads.append(i_thread_scrap)
                i_thread_scrap.start()

        # multithread function join
        if len(i_threads) > 0:
            for index, thread in enumerate(i_threads):
                i_df_output = thread.join()
                i_frames = [i_df_output, i_dftable_final]
                i_dftable_final = pd.concat(i_frames, ignore_index=True)

            # add pair id and join to eliminate 'unmonitored' pairs
            i_dftable_final = pd.merge(i_dftable_final, gv.G_PAIRS, on='pair', how='right')

            if gv.G_PROGRAM_MODE in [gv.G_CONST_MODE_TEST_NODB, gv.G_CONST_MODE_TEST_DB]:
                print(i_dftable_final)

            # save to DB
            if gv.G_PROGRAM_MODE in [gv.G_CONST_MODE_PROD, gv.G_CONST_MODE_TEST_DB]:
                i_thread_DB = s.ThreadWithReturnValue(target=gv.G_DB.save_hist_db(i_dftable_final))
                i_thread_DB.start()

        gv.G_LOGGER.info('done step {0}...'.format(i_step))

        if i_step < gv.G_STEPS:
            gv.G_LOGGER.info('sleeping for {0} seconds...'.format(gv.G_DELAY_SECONDS))
            time.sleep(gv.G_DELAY_SECONDS)
            if gv.G_PROGRAM_MODE in [gv.G_CONST_MODE_PROD, gv.G_CONST_MODE_TEST_DB]:
                i_thread_DB.join()

    gv.G_LOGGER.info('done scraping')

except Exception as e:
    gv.G_LOGGER.error('error during scraping'.format(e), exc_info=True)
    gv.G_LOGGER.error('program halted')
    sys.exit()

# program end
gv.G_LOGGER.info('PROGRAM END')
