import logging
import requests
from requests.exceptions import HTTPError, Timeout, ConnectionError, RequestException
from utils.errors import ApplicationRequestError


logger = logging.getLogger(__name__)


class APIClient:

    def __init__(self, base_url, api_key, retries, timeout):
        self.base_url = base_url
        self.api_key = api_key
        self.headers = {
            'Content-Type': 'application/json'
        }
        self.retries = retries
        self.timeout = timeout

    def get(self, endpoint, params=None):
        url = f"{self.base_url}/{endpoint}"
        for i in range(self.retries):
            try:
                response = requests.get(url, headers=self.headers, params=params,
                                        timeout=self.timeout)
                response.raise_for_status()

                break

            except HTTPError as http_err:
                logger.error(f'Erro HTTP - Status {response.status_code}: {response.text}')
                if i == self.retries - 1:
                    raise ApplicationRequestError(
                        f'Interrompendo. Erro HTTP ao acessar {url}: {http_err}'
                    ) from http_err
                else:
                    logger.info('Tentando novamente...')

            except Timeout as timeout_err:
                logger.error(f'Erro de timeout: {timeout_err}')
                if i == self.retries - 1:
                    raise ApplicationRequestError(
                        f'Interrompendo. Timeout ao acessar {url}: {timeout_err}'
                    ) from timeout_err
                else:
                    logger.info('Tentando novamente...')

            except ConnectionError as conn_err:
                logger.error(f'Erro de Conexão: {conn_err}')
                if i == self.retries - 1:
                    raise ApplicationRequestError(
                        f'Interrompendo. Erro de conexão ao acessar {url}: {conn_err}'
                    ) from conn_err
                else:
                    logger.info('Tentando novamente...')

            except RequestException as req_err:
                logger.error(f'Erro na requisicao: {req_err}')
                if i == self.retries - 1:
                    raise ApplicationRequestError(
                        f'Interrompendo. Erro na requisição ao acessar {url}: {req_err}'
                    ) from req_err
                else:
                    logger.info('Tentando novamente...')

            except Exception as exc:
                logger.error('Um erro inesperado aconteceu.')
                if i == self.retries - 1:
                    raise ApplicationRequestError() from exc
                else:
                    logger.info('Tentando novamente...')

        return response.json()
