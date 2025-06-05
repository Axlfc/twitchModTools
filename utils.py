from datetime import datetime
from typing import Dict
import hashlib
from dateutil.parser import parse as parse_date  # mover al inicio si prefieres

def generate_message_id(message: Dict) -> str:
    """
    🔧 Generate a unique message ID using username, timestamp and text hash.
    Falls back to 'bot_or_unknown' if username is missing or invalid.
    """
    # Get timestamp
    if isinstance(message.get('timestamp'), datetime):
        timestamp_str = message['timestamp'].isoformat()
    elif 'timestamp_str' in message and message['timestamp_str']:
        timestamp_str = message['timestamp_str']
    elif 'timestamp' in message and isinstance(message['timestamp'], str):
        timestamp_str = message['timestamp']
    else:
        timestamp_str = datetime.now().isoformat()

    # Hash of the message text
    text_hash = hashlib.md5(message.get('text', '').encode('utf-8')).hexdigest()[:8]

    # Determine username
    raw_username = str(message.get('username', '')).strip().lower()
    if not raw_username or raw_username in ['unknown', 'none', 'null']:
        username = "bot_or_unknown"
    else:
        username = raw_username

    return f"{username}_{timestamp_str}_{text_hash}"


def enrich_message(message: Dict, file_source: str = "unknown") -> Dict:
    """
    Asegura que un mensaje tenga todos los campos esperados,
    incluyendo timestamp como datetime, timestamp_str e ID.
    """
    # Normalizar timestamp
    if 'timestamp' not in message or not message['timestamp']:
        message['timestamp'] = datetime.now()
    elif isinstance(message['timestamp'], str):
        try:
            message['timestamp'] = parse_date(message['timestamp'])
        except Exception:
            message['timestamp'] = datetime.now()

    # Asegurar string legible de timestamp
    message['timestamp_str'] = message['timestamp'].isoformat()

    # Username
    if not message.get('username'):
        message['username'] = "unknown"

    # File source
    message['file_source'] = file_source or "unknown"

    # Generar ID único
    message['message_id'] = generate_message_id(message)

    return message
