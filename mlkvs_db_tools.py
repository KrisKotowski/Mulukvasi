import pandas as pd
import psycopg2
import global_vars as gv
from configparser import ConfigParser


class DatabaseTools:

    def __init__(self):
        try:
            gv.G_LOGGER.debug('start openning DB')
            i_params = self.config_ini(gv.G_DB_INI_FILE)
            self.CI_CONNECTION = psycopg2.connect(**i_params)
            gv.G_LOGGER.debug('done openning DB')
        except Exception as e:
            gv.G_LOGGER.error('error openning DB {0}'.format(e), exc_info=True)
            raise

    def __del__(self):
        try:
            gv.G_LOGGER.debug('start closing DB')
            self.CI_CONNECTION.close()
            gv.G_LOGGER.debug('done closing DB')
        except Exception as e:
            gv.G_LOGGER.error('error closing DB {0}'.format(e), exc_info=True)
            raise

    # !/usr/bin/python

    def config_ini(self, a_filename='database.ini', a_section='postgresql'):
        try:
            gv.G_LOGGER.debug('start reading ini file')

            # create a parser
            i_parser = ConfigParser()

            # read config file
            i_parser.read(a_filename)

            # get section, default to postgresql
            i_db = {}
            if i_parser.has_section(a_section):
                i_params = i_parser.items(a_section)
                for i_param in i_params:
                    i_db[i_param[0]] = i_param[1]
            else:
                raise Exception('Section {0} not found in the {1} file'.format(a_section, a_filename))
            gv.G_LOGGER.debug('done reading ini file')
            return i_db
        except Exception as e:
            gv.G_LOGGER.error('error reading ini file {0}'.format(e), exc_info=True)
            raise

    def sql_to_df(self, a_sql, args=()):
        try:
            gv.G_LOGGER.debug('start fetching dataframe from SQL: "{0}"'.format(a_sql))
            i_cursor = self.CI_CONNECTION.cursor()
            i_cursor.execute(a_sql, args)
            gv.G_LOGGER.debug('done fetching dataframe from SQL: "{0}"'.format(a_sql))
            return pd.DataFrame(i_cursor.fetchall(), columns=[desc[0] for desc in i_cursor.description])
        except Exception as e:
            gv.G_LOGGER.error('error fetching dataframe from SQL: "{0}" "{1}"'.format(a_sql, e), exc_info=True)
            raise

    def sql_to_single_value(self, a_sql, args=()):
        try:
            gv.G_LOGGER.debug('start fetching single value from SQL: "{0}"'.format(a_sql))
            i_cursor = self.CI_CONNECTION.cursor()
            i_cursor.execute(a_sql, args)
            gv.G_LOGGER.debug('done fetching single value from SQL: "{0}"'.format(a_sql))
            return i_cursor.fetchall()[0][0]
        except Exception as e:
            gv.G_LOGGER.error('error fetching single value from SQL: "{0}" "{1}"'.format(a_sql, e), exc_info=True)
            raise

    def commit_sql(self, a_cursor):
        try:
            gv.G_LOGGER.debug('start commiting SQL: "{0}"'.format(a_cursor.query))
            a_cursor.connection.commit()
            gv.G_LOGGER.debug('done commiting SQL: "{0}"'.format(a_cursor.query))
        except Exception as e:
            gv.G_LOGGER.error('error commiting SQL: "{0}" "{1}"'.format(a_cursor.query, e), exc_info=True)
            gv.G_LOGGER.error('sending rollback')
            a_cursor.connection.rollback()
            raise

    def execute_sql(self, a_sql, a_commit=1, args=()):
        try:
            gv.G_LOGGER.debug('start executing SQL: "{0}"'.format(a_sql))
            i_cursor = self.CI_CONNECTION.cursor()
            i_cursor.execute(a_sql, args)
            gv.G_LOGGER.debug('done executing SQL: "{0}"'.format(a_sql))
            if a_commit == 1:
                DatabaseTools.commit_sql(self, i_cursor)
            return i_cursor
        except Exception as e:
            gv.G_LOGGER.error('error executing SQL: "{0}" "{1}"'.format(i_cursor.query, e), exc_info=True)
            gv.G_LOGGER.error('sending rollback')
            i_cursor.connection.rollback()
            raise

    def save_hist_db(self, a_dftable_final):
        try:
            gv.G_LOGGER.debug('start saving hist in DB')

            i_cursor = DatabaseTools.execute_sql(self, 'insert into public.scan(datetime) values(CURRENT_TIMESTAMP)')
            gv.G_SCAN_ID = DatabaseTools.sql_to_single_value(self, 'SELECT last_value from scan_scan_id_seq')

            for i_row in range(0, a_dftable_final.shape[0]):
                i_sql = 'insert into hist (scan_id, broker_id, currency_pair_id, rate_id, buy_rate, sell_rate) values ({0}, {1}, {2}, {3}, {4}, {5})'.format(
                    gv.G_SCAN_ID, a_dftable_final['broker'].iloc[i_row], a_dftable_final['pair_id'].iloc[i_row],
                    a_dftable_final['rate_type'].iloc[i_row], a_dftable_final['buy'].iloc[i_row],
                    a_dftable_final['sell'].iloc[i_row])
                i_cursor = DatabaseTools.execute_sql(self, i_sql, 0)

            DatabaseTools.commit_sql(self, i_cursor)

            gv.G_LOGGER.debug('done saving hist in DB')
        except Exception as e:
            gv.G_LOGGER.error('error saving hist in DB: "{0}"'.format(e), exc_info=True)
            raise
