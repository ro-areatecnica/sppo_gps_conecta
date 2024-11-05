import logging 

logger = logging.getLogger(__name__)

class ApplicationError(Exception):
    """Classe base para outras exceções na aplicação."""

class ConversionError(ApplicationError):
    """Exceção para tratar erro de conversão do JSON para Dataframe do Pandas."""

    def __init__(self, message='Erro na conversão de JSON para Dataframe.'):
        self.message = message
        logger.error(self.message, exc_info=True)
        super().__init__(self.message)

class ApplicationRequestError(ApplicationError):
    """Exceção para tratar erros de requisição da API"""

    def __init__(self, message='Erro na requisição da API'):
        self.message = message
        logger.error(self.message, exc_info=True)
        super().__init__(self.message)

class ProviderNameError(ApplicationError):
    """Exceção para tratar erro no nome do provedor de serviço. 
    Ex.: "conecta" e "zirix" são nomes válidos. 
    """

    def __init__(self, provider='', message='Provedor desconhecido'):
        self.message = message + f': {provider}' if provider else message
        logger.error(self.message, exc_info=True)
        super().__init__(self.message)

class GoogleCloudError(ApplicationError):
    """Exceção para tratar falhas na comunicação com a plataforma Google Cloud.
    """
    def __init__(self, message='Erro na comunicação com o Google Cloud'):
        self.message = message
        logger.error(self.message, exc_info=True)
        super().__init__(self.message)

class UnknownParameterError(ApplicationError):
    """Exceção para tratar erros de parâmetros de execução incorretos

    """
    def __init__(self, message='Erro na interpretação dos parâmetros'):
        self.message = message
        logger.error(self.message, exc_info=True)
        super().__init__(self.message)
