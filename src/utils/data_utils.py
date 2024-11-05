from datetime import datetime, timezone

def parse_date(date_input):
    """Converte string ou objeto datetime para um objeto datetime."""
    if isinstance(date_input, datetime):
        return date_input.replace(tzinfo=timezone.utc)
    try:
        return datetime.strptime(date_input, '%Y-%m-%d %H:%M:%S').replace(tzinfo=timezone.utc)
    except ValueError:
        raise ValueError(f"Formato inválido para a data: {date_input}")
