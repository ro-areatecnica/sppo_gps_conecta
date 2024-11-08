from logger import logger
from datetime import datetime, timezone, timedelta
import functions_framework
from api.provider import Provider, ProviderEnum
from config import (
    GOOGLE_CLOUD_PROJECT, GOOGLE_CLOUD_DATASET, GOOGLE_CLOUD_CONTROL_TABLE,
    START_DATE, END_DATE
)
from cloud.bigquery import GoogleCloudClient
from utils.data_utils import parse_date
from utils.helpers import json_to_df

@functions_framework.http
def main(request):
    try:
        logger.info('====== INÍCIO ======')
        client = GoogleCloudClient(project_id=GOOGLE_CLOUD_PROJECT)

        client.create_control_table_if_not_exists(
            dataset_id=GOOGLE_CLOUD_DATASET,
            control_table_id=GOOGLE_CLOUD_CONTROL_TABLE
        )

        endpoints_to_run = client.get_failed_success_endpoints(
            dataset_id=GOOGLE_CLOUD_DATASET,
            control_table_id=GOOGLE_CLOUD_CONTROL_TABLE,
            api=ProviderEnum.CONECTA.value
        )

        if not endpoints_to_run:
            logger.info("Nenhum endpoint falho ou sucesso recente encontrado na tabela de controle.")
            return "Nenhum endpoint encontrado", 200

        logger.info(f"Endpoints encontrados: {endpoints_to_run}")

        gps_provider = Provider(ProviderEnum.CONECTA.value)

        now = datetime.now(timezone.utc)
        last_execution = client.get_last_execution(GOOGLE_CLOUD_DATASET, GOOGLE_CLOUD_CONTROL_TABLE)

        for endpoint in endpoints_to_run:
            start_date, end_date = define_dates(endpoint, last_execution, now)

            if start_date is None or end_date is None:
                logger.info(f"Pulando o processamento do endpoint {endpoint}. Intervalo de tempo ainda não atingido.")
                continue

            logger.info(f"Processando endpoint: {endpoint}, Start date: {start_date}, End date: {end_date}")
            try:
                process_data(gps_provider, client, endpoint, logger, start_date, end_date)
            except Exception as e:
                logger.error(f"Erro ao processar endpoint {endpoint}: {str(e)}")
                client.update_control_table(
                    GOOGLE_CLOUD_DATASET, GOOGLE_CLOUD_CONTROL_TABLE,
                    ProviderEnum.CONECTA.value, endpoint, "failed"
                )

    except Exception as e:
        logger.error(f"Erro durante a execução: {str(e)}")
        return f"Erro durante a execução: {str(e)}", 500
    logger.info('====== PROCESSO ENCERRADO ======')

    return "Dados processados com sucesso", 200

def define_dates(endpoint, last_execution, now):
    if START_DATE and END_DATE:
        start_date = parse_date(START_DATE)
        end_date = parse_date(END_DATE)
    else:
        if last_execution and last_execution.get('last_extraction'):
            start_date = parse_date(last_execution['last_extraction'])
        else:
            start_date = now

        end_date = now

        if endpoint in ['envioViagensSMTR', 'EnvioViagensRetroativasSMTR']:
            if now - start_date > timedelta(hours=1):
                end_date = start_date + timedelta(hours=1)
            else:
                return None, None
        elif endpoint == 'EnvioSMTR':
            if now - start_date > timedelta(minutes=5):
                end_date = start_date + timedelta(minutes=5)
            else:
                return None, None

    return start_date, end_date


def process_data(gps_provider, client, endpoint, logger, start_date, end_date):
    """Executa a lógica de processamento para um endpoint específico."""
    logger.info(f'Start date: {start_date}')
    logger.info(f'End date: {end_date}')

    results = None
    if endpoint == 'EnvioSMTR':
        results = gps_provider.get_registros(data_hora_inicio=start_date, data_hora_fim=end_date)
    elif endpoint == 'EnvioViagensRetroativasSMTR':
        results = gps_provider.get_realocacao(data_hora_inicio=start_date, data_hora_fim=end_date)
    elif endpoint == 'envioViagensSMTR':
        results = gps_provider.get_viagens_consolidadas(data_hora_inicio=start_date, data_hora_fim=end_date)
    else:
        raise ValueError(f'Endpoint desconhecido: {endpoint}')

    if results:
        df_results = json_to_df(results)
        if not df_results.empty:
            df_results['ro_extraction_ts'] = datetime.now(timezone.utc)
            table_name = client.get_table_name(endpoint)
            client.load_df_to_bigquery(df_results, GOOGLE_CLOUD_DATASET, table_name)
            client.update_control_table(GOOGLE_CLOUD_DATASET, GOOGLE_CLOUD_CONTROL_TABLE,
                                        ProviderEnum.CONECTA.value, endpoint, 'success',
                                        last_extraction=datetime.now(timezone.utc).isoformat())
        else:
            client.update_control_table(GOOGLE_CLOUD_DATASET, GOOGLE_CLOUD_CONTROL_TABLE,
                                        ProviderEnum.CONECTA.value, endpoint, 'failed')

