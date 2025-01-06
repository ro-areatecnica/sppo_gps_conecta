from logger import logger
from datetime import datetime, timezone, timedelta
import functions_framework
from api.provider import Provider, ProviderEnum
from config import (
    GOOGLE_CLOUD_PROJECT, GOOGLE_CLOUD_DATASET, GOOGLE_CLOUD_CONTROL_TABLE,
    START_DATE, END_DATE
)
from cloud.bigquery import GoogleCloudClient
from utils.helpers import json_to_df

@functions_framework.http
def main(request):
    try:
        logger.info('====== INÍCIO ======')
        client = GoogleCloudClient(project_id=GOOGLE_CLOUD_PROJECT)

        client.create_control_table_if_not_exists(
            dataset_id=GOOGLE_CLOUD_DATASET,
            control_table_id=GOOGLE_CLOUD_CONTROL_TABLE,
            partition_field = "data"
        )

        endpoints_to_run = client.get_failed_success_endpoints(
            dataset_id=GOOGLE_CLOUD_DATASET,
            control_table_id=GOOGLE_CLOUD_CONTROL_TABLE,
            api=ProviderEnum.CONECTA.value
        )

        if not endpoints_to_run:
            logger.info("Nenhum endpoint falho ou sucesso recente encontrado na tabela de controle.")
            return "Nenhum endpoint encontrado"

        logger.info(f"Endpoints encontrados: {endpoints_to_run}"), 200

        gps_provider = Provider(ProviderEnum.CONECTA.value)

        now = datetime.now(timezone.utc)
        last_execution = client.get_last_execution(GOOGLE_CLOUD_DATASET, GOOGLE_CLOUD_CONTROL_TABLE)

        for endpoint in endpoints_to_run:
            start_date, end_date = define_dates(endpoint, last_execution, now)

            if start_date is None or end_date is None:
                logger.info(f"Pulando o processamento do endpoint {endpoint}. Intervalo de tempo ainda não atingido.")
                continue

            logger.info(f"Processando endpoint: {endpoint}, Start date: {start_date}, End date: {end_date}")

            process_data(gps_provider, client, endpoint, logger, start_date, end_date)


    except Exception as e:
        logger.error(f"Erro durante a execução: {str(e)}"), 500
        return f"Erro durante a execução: {str(e)}"
    logger.info('====== PROCESSO ENCERRADO ======')

    return "Dados processados com sucesso", 200

def define_dates(endpoint, last_execution, now):
    if START_DATE and END_DATE:
        start_date = START_DATE
        end_date = END_DATE
    else:
        start_date_dt = last_execution['last_extraction'] or now - timedelta(minutes=5)
        end_date_dt = start_date_dt + timedelta(minutes=5)
        if endpoint == 'envioSMTR':
            if now < end_date_dt:
                return None, None

        start_date = start_date_dt.strftime('%Y-%m-%d %H:%M:%S')
        end_date = end_date_dt.strftime('%Y-%m-%d %H:%M:%S')

    return start_date, end_date

def process_data(gps_provider, client, endpoint, logger, start_date, end_date):
    logger.info(f'Start date: {start_date}')
    logger.info(f'End date: {end_date}')

    try:

        results = gps_provider.get_registros(data_hora_inicio=start_date, data_hora_fim=end_date)
        df_results = json_to_df(results)

        if not results or df_results.empty:
            message = f"Erro: Nenhum dado retornado para o intervalo {start_date} - {end_date}"
            client.insert_control_table(
                GOOGLE_CLOUD_DATASET, GOOGLE_CLOUD_CONTROL_TABLE,
                ProviderEnum.CONECTA.value, endpoint, 'failed', datetime.now().date(),
                last_extraction=end_date, total_extracted=len(results), total_stored=len(df_results), message=message
            )
            return
        else:
            if not df_results.empty:
                df_results['data_extraction'] = datetime.now(timezone.utc).date()
                table_name = client.get_table_name(endpoint)

                client.load_df_to_bigquery(df_results, GOOGLE_CLOUD_DATASET, table_name,
                                           partition_field="data_extraction")
                client.insert_control_table(
                    GOOGLE_CLOUD_DATASET, GOOGLE_CLOUD_CONTROL_TABLE,
                    ProviderEnum.CONECTA.value, endpoint, 'success', datetime.now().date(),
                    last_extraction=end_date, total_extracted=len(results), total_stored=len(df_results),
                    message=f"Data inserted successfully."
                )

    except Exception as e:
        client.insert_control_table(
            GOOGLE_CLOUD_DATASET, GOOGLE_CLOUD_CONTROL_TABLE,
            ProviderEnum.CONECTA.value, endpoint, 'failed', datetime.now().date(),
            last_extraction=end_date, total_extracted=0, total_stored=0,
            message=f"Erro ao processar endpoint {endpoint}: {type(e).__name__} - {str(e)}"
        )
