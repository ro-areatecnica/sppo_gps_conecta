import logging
from enum import Enum
from api.client import APIClient
from config import (
    URL, API_KEY, ENDPOINT_REGISTROS, ENDPOINT_REALOCACAO, ENDPOINT_VIAGENS_CONSOLIDADAS,
    TIMEOUT_IN_SECONDS, RETRIES
)

logger = logging.getLogger(__name__)


class ProviderEnum(Enum):
    """Classe de enumeração para encapsular parâmetro de provedores de serviço de GPS
    """
    CONECTA = "conecta"
    ZIRIX = "zirix"


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
        self.realocacao = ENDPOINT_REALOCACAO
        self.consolidadas = ENDPOINT_VIAGENS_CONSOLIDADAS
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

        return response

    def get_realocacao(self, data_hora_inicio, data_hora_fim):
        """Método que recupera as realocações de linhas

        Args:
            data_hora_inicio (str): Timestamp de início da captura dos dados. (formato YYYY-MM-DD HH:mm:SS)
            data_hora_fim (str): Timestamp de fim da captura dos dados. (formato YYYY-MM-DD HH:mm:SS)

        Returns:
            list: Lista dos registros de realocações no período especificado
        """

        params = {
            "guidIdentificacao": self.api_key,
            "dataInicial": data_hora_inicio,
            "dataFinal": data_hora_fim
        }

        response = self.get(endpoint=self.realocacao, params=params)

        return response

    def get_viagens_consolidadas(self, data_hora_inicio, data_hora_fim):
        """Método que recupera as viagens consolidadas de um dia

        Args:
            data_hora_início (string): Timestamp de início da captura dos dados. (formato YYYY-MM-DD HH:mm:SS)
            data_hora_fim (string): Timestamp de fim da captura dos dados. (formato YYYY-MM-DD HH:mm:SS)

        Returns:
            list: Lista das viagens consolidadas para o dia selecionado
        """

        params = {
            "guidIdentificacao": self.api_key,
            "datetime_processamento_inicio": data_hora_inicio,
            "datetime_processamento_fim": data_hora_fim
        }

        response = self.get(endpoint=self.consolidadas, params=params)

        return response
