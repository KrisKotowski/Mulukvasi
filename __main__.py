# main project file

import mlkvs_scrap_brokers as p
import mlkvs_scrap_tools as s
import pandas as pd
import time
import global_vars as gv
import sys

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

    # initialize objects
    i_scraps = list()
    i_scraps.append(p.ScrapCinkciarz())
    i_scraps.append(p.ScrapIK())
    i_scraps.append(p.ScrapRevolut1())
    i_scraps.append(p.ScrapRevolut2())
    i_scraps.append(p.ScrapMillenium())
#    i_scraps.append(p.ScrapTraderMade())
#    i_scraps.append(p.ScrapTradingEconomics())
#    i_scraps.append(p.ScrapBloomberg())

    i_threads = list()

    gv.G_LOGGER.info('PROGRAM START: initialization success in mode [{0}]'.format(gv.G_PROGRAM_MODE))
except Exception as e:
    gv.G_LOGGER.error('initialization failed "{0}"'.format(e), exc_info=True)
    gv.G_LOGGER.error('program halted')
    sys.exit()

try:
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
            i_threads.append(i_thread_scrap)
            i_thread_scrap.start()

        # multithread function join
        for index, thread in enumerate(i_threads):
            i_df_output = thread.join()
            i_frames = [i_df_output, i_dftable_final]
            i_dftable_final = pd.concat(i_frames, ignore_index=True)

        # add pair id and join to eliminate 'unmonitored' pairs

        i_dftable_final = pd.merge(i_dftable_final, gv.G_PAIRS, on='pair', how='right')

        if gv.G_PROGRAM_MODE in [gv.G_CONST_MODE_TEST_NODB, gv.G_CONST_MODE_TEST_DB]:
            gv.G_LOGGER.info(i_dftable_final)

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
