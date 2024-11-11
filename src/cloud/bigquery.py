from google.cloud import bigquery
import logging
from google.api_core.exceptions import NotFound
from datetime import datetime
from utils.table_mapping import table_name_mapping
from config import BACKOFF_MINUTES


class GoogleCloudClient:
    def __init__(self, project_id):
        self.client = bigquery.Client(project=project_id)
        logging.debug(f"BigQuery client inicializado para o projeto: {project_id}")

    def get_table_name(self, endpoint):
        return table_name_mapping.get(endpoint, f'table_{endpoint.lower()}')

    def create_control_table_if_not_exists(self, dataset_id, control_table_id):
        dataset_ref = self.client.dataset(dataset_id)
        table_ref = dataset_ref.table(control_table_id)
        try:
            self.client.get_table(table_ref)
            logging.debug(f"Tabela de controle '{control_table_id}' já existe.")
        except NotFound:
            schema = [
                bigquery.SchemaField("api", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("endpoint", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("last_extraction", "TIMESTAMP", mode="NULLABLE"),
                bigquery.SchemaField("status", "STRING", mode="REQUIRED"),
            ]
            table = bigquery.Table(table_ref, schema=schema)
            self.client.create_table(table)
            logging.info(f"Tabela de controle '{control_table_id}' criada com sucesso.")
            # Inserir um registro inicial para evitar problemas de fluxo
            query = f"""
                INSERT INTO `{self.client.project}.{dataset_id}.{control_table_id}` (api, endpoint, last_extraction, status)
                VALUES 
                    ('conecta', 'envioSMTR', '{datetime.utcnow()}', 'success'),
                    ('conecta', 'EnvioViagensSMTR', '{datetime.utcnow()}', 'success'),
                    ('conecta', 'EnvioRealocacoesSMTR', '{datetime.utcnow()}', 'success')
            """
            self.client.query(query).result()
            logging.info(f"Registro inicial inserido na tabela de controle '{control_table_id}'.")

    def get_failed_success_endpoints(self, dataset_id, control_table_id, api):
        query = f"""
            SELECT endpoint, last_extraction, status
            FROM `{dataset_id}.{control_table_id}`
            WHERE api = '{api}'
            AND status IN ('failed', 'success')  -- Considera tanto falhos quanto sucesso
            AND TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), last_extraction, MINUTE) > {BACKOFF_MINUTES}
        """

        results = self.client.query(query).result()
        # Copia os resultados para evitar reiteração
        results_list = list(results)

        # Log de debug para cada linha
        for row in results_list:
            logging.debug(
                f"Endpoint: {row['endpoint']}, Last Extraction: {row['last_extraction']}, Status: {row['status']}")

        return [row["endpoint"] for row in results_list]

    def get_last_execution(self, dataset_id, control_table_id):
        query = f"""
            SELECT last_extraction
            FROM `{self.client.project}.{dataset_id}.{control_table_id}`
            WHERE api = 'conecta'
            AND status IN ('failed', 'success')
        """
        logging.debug(f"Executando query para obter a última extração: {query}")
        result = self.client.query(query).result()
        row = next(result, None)
        return {'last_extraction': row["last_extraction"]} if row and row["last_extraction"] else None

    def load_df_to_bigquery(self, dataframe, dataset_id, table_id):
        table_ref = self.client.dataset(dataset_id).table(table_id)
        job_config = bigquery.LoadJobConfig(
            write_disposition="WRITE_APPEND"  # Ajuste conforme necessário: "WRITE_TRUNCATE" ou "WRITE_APPEND"
        )
        try:
            job = self.client.load_table_from_dataframe(dataframe, table_ref, job_config=job_config)
            job.result()  # Aguarda até que o job seja concluído
            logging.info(f"Carregamento para BigQuery concluído: {table_id}")
        except Exception as e:
            logging.error(f"Erro ao carregar dados para a tabela {table_id}: {e}")
            raise  # Relevante para propagar o erro, caso precise de tratamento adicional em outro lugar

    def update_control_table(self, dataset_id, control_table_id, api, endpoint, status, last_extraction=None):
        # Define o valor padrão para last_extraction se não for fornecido
        last_extraction_value = last_extraction if last_extraction is not None else datetime.utcnow()
        query = f"""
            MERGE `{self.client.project}.{dataset_id}.{control_table_id}` T
            USING (SELECT @api AS api, @endpoint AS endpoint, @last_extraction AS last_extraction, @status AS status) S
            ON T.api = S.api AND T.endpoint = S.endpoint
            WHEN MATCHED THEN
            UPDATE SET last_extraction = S.last_extraction, status = S.status
            WHEN NOT MATCHED THEN
            INSERT (api, endpoint, last_extraction, status)
            VALUES(S.api, S.endpoint, S.last_extraction, S.status)
        """
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("api", "STRING", api),
                bigquery.ScalarQueryParameter("endpoint", "STRING", endpoint),
                bigquery.ScalarQueryParameter("last_extraction", "TIMESTAMP", last_extraction_value),
                bigquery.ScalarQueryParameter("status", "STRING", status),
            ]
        )
        try:
            self.client.query(query, job_config=job_config).result()
            logging.info(f"Status de controle atualizado para '{endpoint}': {status}")
        except Exception as e:
            logging.error(f"Erro ao atualizar a tabela de controle para o endpoint '{endpoint}': {e}")
            raise

    def count_records(self, dataset_id, table_id):
        """Conta o número de registros em uma tabela BigQuery."""
        query = f"SELECT COUNT(*) as total FROM `{self.client.project}.{dataset_id}.{table_id}`"
        query_job = self.client.query(query)
        results = query_job.result()
        for row in results:
            return row.total
