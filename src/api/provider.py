import logging
from enum import Enum
from api.client import APIClient
from config import (
    URL, API_KEY, ENDPOINT_REGISTROS, TIMEOUT_IN_SECONDS, RETRIES
)

logger = logging.getLogger(__name__)


class ProviderEnum(Enum):
    """Classe de enumeração para encapsular parâmetro de provedores de serviço de GPS
    """
    CONECTA = "conecta"


class Provider(APIClient):
    """Classe que encapsula os métodos de requisição de dados da API,
    agnóstico em relação ao provedor.
    """

    def __init__(self, provider):
        """Construtor da classe Provider

        Args:
            - provider (ProviderEnum): Enum do provedor de serviços.
        """
        self.provider_name = provider
        self.url = URL
        self.api_key = API_KEY
        self.registros = ENDPOINT_REGISTROS
        super().__init__(base_url=self.url, api_key=self.api_key,
                         timeout=TIMEOUT_IN_SECONDS, retries=RETRIES)

    def get_registros(self, data_hora_inicio, data_hora_fim):
        """Método que recupera os registros de GPS.

        Args:
            data_hora_inicio (str): Timestamp de início da captura dos dados.
            data_hora_fim (str): Timestamp de fim da captura dos dados.

        Returns:
            list: Lista dos registros de GPS dos ônibus naquele período especificado
        """

        params = {
            "guidIdentificacao": self.api_key,
            "dataInicial": data_hora_inicio,
            "dataFinal": data_hora_fim
        }

        response = self.get(endpoint=self.registros, params=params)

        print(f"Total de registros retornados da API do enpoint REGISTROS: {len(response)}")

        return response
