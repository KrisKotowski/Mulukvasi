import requests
import global_vars as gv


class URLTools:
    C_URL_SUCCESS = 200
    C_TIMEOUT = 10
    C_HEADERS = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/102.0.0.0 Safari/537.36'}

    def get_url_content(self, a_url):
        try:
            gv.G_LOGGER.debug('start downloading URL {0}'.format(a_url))
            i_url_content = requests.get(a_url, headers=URLTools.C_HEADERS, timeout=URLTools.C_TIMEOUT)
            gv.G_LOGGER.debug('done downloading URL {0}'.format(a_url))
            return i_url_content
        except Exception as e:
            gv.G_LOGGER.error('error downloading URL "{0}" {1}'.format(e, a_url), exc_info=True)
