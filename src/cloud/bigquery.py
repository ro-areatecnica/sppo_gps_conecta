from google.cloud import bigquery
import logging
from google.api_core.exceptions import NotFound
from datetime import datetime, timedelta, timezone
from utils.table_mapping import table_name_mapping


class GoogleCloudClient:
    def __init__(self, project_id):
        self.client = bigquery.Client(project=project_id)
        logging.debug(f"BigQuery client inicializado para o projeto: {project_id}")

    def get_table_name(self, endpoint):
        return table_name_mapping.get(endpoint, f'table_{endpoint.lower()}')

    def create_control_table_if_not_exists(self, dataset_id, control_table_id, partition_field=None):
        dataset_ref = self.client.dataset(dataset_id)
        table_ref = dataset_ref.table(control_table_id)
        time_partitioning = bigquery.TimePartitioning(field=partition_field) if partition_field else None

        try:
            self.client.get_table(table_ref)
            logging.debug(f"Tabela de controle '{control_table_id}' já existe.")
        except NotFound:
            schema = [
                bigquery.SchemaField("data", "DATE", mode="REQUIRED", description="Data da extração."),
                bigquery.SchemaField("api", "STRING", mode="REQUIRED", description="Campo relacionado a API especifica."),
                bigquery.SchemaField("endpoint", "STRING", mode="REQUIRED", description="Endpoint relacionado a API."),
                bigquery.SchemaField("status", "STRING", mode="REQUIRED", description="STATUS[failed/success]."),
                bigquery.SchemaField("last_extraction", "TIMESTAMP", mode="NULLABLE", description="Data da ultima extração."),
                bigquery.SchemaField("total_extracted", "INT64", mode="REQUIRED", description="Total de registros extraídos da API."),
                bigquery.SchemaField("total_stored", "INT64", mode="REQUIRED", description="Total de registros armazenados no BigQuery."),
                bigquery.SchemaField("message", "STRING", mode="REQUIRED", description="Mensagem de erro ou sucesso.")
            ]
            table = bigquery.Table(table_ref, schema=schema)

            if time_partitioning:
                table.time_partitioning = time_partitioning

            self.client.create_table(table)
            logging.info(f"Tabela de controle '{control_table_id}' criada com sucesso.")
            query = f"""
                INSERT INTO `{self.client.project}.{dataset_id}.{control_table_id}` (data, api, endpoint, status, last_extraction, total_extracted, total_stored, message)
                VALUES 
                    ('{datetime.today().date()}', 'conecta', 'envioSMTR', 'success', '{(datetime.now(timezone.utc) - timedelta(minutes=5)).strftime("%Y-%m-%d %H:%M:%S")}', 0, 0, 'Data inserted successfully.')
            """
            self.client.query(query).result()
            logging.info(f"Registro inicial inserido na tabela de controle '{control_table_id}'.")

    def get_failed_success_endpoints(self, dataset_id, control_table_id, api):
        query = f"""
            SELECT endpoint, MAX(last_extraction) AS last_extraction
            FROM `{dataset_id}.{control_table_id}`
            WHERE api = '{api}'
            AND status IN ('failed', 'success')
            GROUP BY endpoint
        """

        results = self.client.query(query).result()

        row = next(results, None)
        if row is not None:
            logging.debug(
                f"Endpoint: {row['endpoint']}, Last Extraction: {row['last_extraction']}"
            )
            return [row["endpoint"]]
        else:
            logging.debug("Nenhum endpoint encontrado.")
            return []

    def get_last_execution(self, dataset_id, control_table_id):
        query = f"""
            SELECT 
            *
            FROM
            `{self.client.project}.{dataset_id}.{control_table_id}`
            WHERE
            last_extraction = (
              SELECT
                MAX(last_extraction)
              FROM
                `{self.client.project}.{dataset_id}.{control_table_id}`
            )
            ORDER BY last_extraction DESC
            LIMIT 1
        """
        logging.debug(f"Executando query para obter a última extração: {query}")
        result = self.client.query(query).result()
        row = next(result, None)
        return {'last_extraction': row["last_extraction"]} if row and row["last_extraction"] else None

    def load_df_to_bigquery(self, dataframe, dataset_id, table_id, partition_field=None):
        table_ref = self.client.dataset(dataset_id).table(table_id)
        job_config = bigquery.LoadJobConfig(
            write_disposition="WRITE_APPEND",
            time_partitioning=bigquery.TimePartitioning(field=partition_field) if partition_field else None
        )
        try:
            job = self.client.load_table_from_dataframe(dataframe, table_ref, job_config=job_config)
            job.result()
            logging.info(f"Carregamento para BigQuery concluído: {table_id}")
        except Exception as e:
            logging.error(f"Erro ao carregar dados para a tabela {table_id}: {e}")
            raise

    def insert_control_table(self, dataset_id, control_table_id, api, endpoint, status, data, total_extracted, total_stored, message, last_extraction=None):
        query = f"""
            INSERT INTO `{self.client.project}.{dataset_id}.{control_table_id}`
            (data, api, endpoint, status, last_extraction, total_extracted, total_stored, message)
            VALUES (@data, @api, @endpoint, @status, @last_extraction, @total_extracted, @total_stored, @message)
        """
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("api", "STRING", api),
                bigquery.ScalarQueryParameter("data", "DATE", data),
                bigquery.ScalarQueryParameter("endpoint", "STRING", endpoint),
                bigquery.ScalarQueryParameter("status", "STRING", status),
                bigquery.ScalarQueryParameter("last_extraction", "TIMESTAMP", last_extraction),
                bigquery.ScalarQueryParameter("total_extracted", "INT64", total_extracted),
                bigquery.ScalarQueryParameter("total_stored", "INT64", total_stored),
                bigquery.ScalarQueryParameter("message", "STRING", message)
            ]
        )
        try:
            self.client.query(query, job_config=job_config).result()
            logging.info(f"Novo registro de controle inserido para '{endpoint}': {status} em {last_extraction}")
        except Exception as e:
            logging.error(f"Erro ao inserir o histórico na tabela de controle para o endpoint '{endpoint}': {e}")
            raise
