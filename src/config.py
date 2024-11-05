from google.cloud import secretmanager
from decouple import config

def get_secret_key(secret_id):
    client = secretmanager.SecretManagerServiceClient()
    name = f'projects/{GOOGLE_CLOUD_PROJECT}/secrets/{secret_id}/versions/latest'
    response = client.access_secret_version(request={'name': name})
    return response.payload.data.decode("UTF-8")

GOOGLE_CLOUD_PROJECT = config('GOOGLE_CLOUD_PROJECT')
URL = config('URL')
ENDPOINT_REGISTROS = config('ENDPOINT_REGISTROS', default='')
ENDPOINT_REALOCACAO = config('ENDPOINT_REALOCACAO', default='')
ENDPOINT_VIAGENS_CONSOLIDADAS = config('ENDPOINT_VIAGENS_CONSOLIDADAS', default='')

API_KEY = get_secret_key('api_key_conecta')  # Atualizado para usar apenas o secret_id
GOOGLE_CLOUD_DATASET = config('GOOGLE_CLOUD_DATASET')
GOOGLE_CLOUD_CONTROL_TABLE = config('GOOGLE_CLOUD_CONTROL_TABLE', default='control_table')
PROVIDER = config('PROVIDER')
START_DATE = config('START_DATE', default='')
END_DATE = config('END_DATE', default='')
BACKOFF_MINUTES = config('BACKOFF_MINUTES', default=5, cast=int)
TIMEOUT_IN_SECONDS = config('TIMEOUT_IN_SECONDS', default=300, cast=int)
RETRIES = config('RETRIES', default=3, cast=int)
