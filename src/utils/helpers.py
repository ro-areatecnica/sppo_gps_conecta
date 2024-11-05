import pandas as pd


def json_to_df(json):
    """Função auxiliar de conversão de JSON para pandas Dataframe

    Args:
        json (dict): dados no formato JSON para serem transformados

    Returns:
        Dataframe: Pandas Dataframe com os dados do JSON
    """
    return pd.json_normalize(json)
