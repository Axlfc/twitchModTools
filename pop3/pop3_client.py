#!/usr/bin/env python3
"""
EmailClient - Cliente POP3/SMTP completo con backup integrado
Evolución de EmailBackupClient
"""

import poplib
import smtplib
import email
import os
import json
import datetime
import hashlib
import base64
import re
from pathlib import Path
from email.header import decode_header
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email import encoders
from spam_detector import SpamDetector


class EnvLoader:
    """Cargador de archivos .env sin dependencias externas"""

    @staticmethod
    def load_env_file(filepath='.env'):
        """Cargar variables de entorno desde archivo .env"""
        if not os.path.exists(filepath):
            return {}

        env_vars = {}
        try:
            with open(filepath, 'r', encoding='utf-8') as file:
                for line in file:
                    line = line.strip()
                    if not line or line.startswith('#'):
                        continue
                    if '=' not in line:
                        continue
                    key, value = line.split('=', 1)
                    key = key.strip()
                    value = value.strip()
                    if (value.startswith('"') and value.endswith('"')) or \
                            (value.startswith("'") and value.endswith("'")):
                        value = value[1:-1]
                    env_vars[key] = value
                    os.environ[key] = value
            return env_vars
        except Exception:
            return {}

    @staticmethod
    def get_env_var(key, default=None):
        """Obtener variable de entorno con valor por defecto"""
        return os.getenv(key, default)


class SecureCredentialManager:
    """Gestor ultra-seguro de credenciales"""

    def __init__(self, config_file=".secure_config"):
        self.config_file = Path(config_file)
        self.env_loader = EnvLoader()

    def _simple_encrypt(self, text, key):
        key_hash = hashlib.sha256(key.encode()).digest()
        encrypted = bytearray()
        for i, char in enumerate(text.encode('utf-8')):
            encrypted.append(char ^ key_hash[i % len(key_hash)])
        return base64.b64encode(bytes(encrypted)).decode('ascii')

    def _simple_decrypt(self, encrypted_text, key):
        try:
            encrypted_bytes = base64.b64decode(encrypted_text.encode('ascii'))
            key_hash = hashlib.sha256(key.encode()).digest()
            decrypted = bytearray()
            for i, byte in enumerate(encrypted_bytes):
                decrypted.append(byte ^ key_hash[i % len(key_hash)])
            return decrypted.decode('utf-8')
        except Exception:
            return None

    def load_from_env(self):
        self.env_loader.load_env_file('.env')
        username = self.env_loader.get_env_var('EMAIL_USERNAME') or self.env_loader.get_env_var('TINET_USERNAME')
        password = self.env_loader.get_env_var('EMAIL_PASSWORD') or self.env_loader.get_env_var('TINET_PASSWORD')
        display_name = self.env_loader.get_env_var('DISPLAY_NAME', '')

        if username and password:
            return username.strip(), password.strip(), display_name.strip()
        return None, None, display_name.strip()

    def save_credentials(self, username, password, display_name="", master_key=None):
        if not master_key or len(master_key) < 8:
            return False

        try:
            encrypted_username = self._simple_encrypt(username, master_key)
            encrypted_password = self._simple_encrypt(password, master_key)
            key_verify = hashlib.sha256(master_key.encode()).hexdigest()[:16]

            config_data = {
                'key_verify': key_verify,
                'username': encrypted_username,
                'password': encrypted_password,
                'display_name': display_name,
                'created': datetime.datetime.now().isoformat()
            }

            with open(self.config_file, 'w') as f:
                json.dump(config_data, f, indent=2)

            try:
                os.chmod(self.config_file, 0o600)
            except:
                pass
            return True
        except Exception:
            return False

    def load_credentials(self, master_key=None):
        if not self.config_file.exists() or not master_key:
            return None, None, ""

        try:
            with open(self.config_file, 'r') as f:
                config_data = json.load(f)

            key_verify = hashlib.sha256(master_key.encode()).hexdigest()[:16]

            if key_verify != config_data.get('key_verify'):
                return None, None, ""

            username = self._simple_decrypt(config_data['username'], master_key)
            password = self._simple_decrypt(config_data['password'], master_key)
            display_name = config_data.get('display_name', "")

            return username.strip(), password.strip(), display_name
        except Exception:
            return None, None, ""


class EmailClient:
    PROVIDERS = {
        'gmail.com': {
            'pop_server': 'pop.gmail.com',
            'pop_port': 995,
            'smtp_server': 'smtp.gmail.com',
            'smtp_port': 465
        },
        'outlook.com': {
            'pop_server': 'outlook.office365.com',
            'pop_port': 995,
            'smtp_server': 'smtp.office365.com',
            'smtp_port': 587
        },
        'hotmail.com': {
            'pop_server': 'outlook.office365.com',
            'pop_port': 995,
            'smtp_server': 'smtp.office365.com',
            'smtp_port': 587
        },
        'yahoo.com': {
            'pop_server': 'pop.mail.yahoo.com',
            'pop_port': 995,
            'smtp_server': 'smtp.mail.yahoo.com',
            'smtp_port': 465
        },
        'tinet.cat': {
            'pop_server': 'pop3.tinet.cat',
            'pop_port': 995,
            'smtp_server': 'smtp.tinet.cat',
            'smtp_port': 465
        }
    }

    def __init__(self):
        self.credential_manager = SecureCredentialManager()
        self.username = None
        self.password = None
        self.display_name = ""

        env_loader = EnvLoader()
        env_loader.load_env_file('.env')

        self.pop_server = env_loader.get_env_var('POP_SERVER', 'pop3.tinet.cat')
        self.pop_port = int(env_loader.get_env_var('POP_PORT', '995'))
        self.smtp_server = env_loader.get_env_var('SMTP_SERVER', 'smtp.tinet.cat')
        self.smtp_port = int(env_loader.get_env_var('SMTP_PORT', '465'))
        self.backup_dir = Path(env_loader.get_env_var('BACKUP_DIR', 'email_backup'))

        try:
            self.backup_dir.mkdir(parents=True, exist_ok=True)
        except:
            pass

    def autodetect_settings(self, email_address):
        """Autodetecta la configuración basada en el dominio del email"""
        if '@' not in email_address:
            return False

        domain = email_address.split('@')[1].lower()
        if domain in self.PROVIDERS:
            settings = self.PROVIDERS[domain]
            self.pop_server = settings['pop_server']
            self.pop_port = settings['pop_port']
            self.smtp_server = settings['smtp_server']
            self.smtp_port = settings['smtp_port']
            return True
        return False

    def load_credentials(self):
        u, p, d = self.credential_manager.load_from_env()
        if u and p:
            self.username, self.password, self.display_name = u, p, d
            return True

        u, p, d = self.credential_manager.load_credentials()
        if u and p:
            self.username, self.password, self.display_name = u, p, d
            return True
        return False

    def connect_pop3(self):
        try:
            server = poplib.POP3_SSL(self.pop_server, self.pop_port, timeout=10)
            server.user(self.username)
            server.pass_(self.password)
            return server
        except Exception as e:
            print(f"❌ Error POP3: {e}")
            return None

    def test_smtp_auth(self):
        """Prueba la conexión y autenticación SMTP sin enviar correo"""
        try:
            if self.smtp_port == 465:
                server = smtplib.SMTP_SSL(self.smtp_server, self.smtp_port, timeout=10)
            else:
                server = smtplib.SMTP(self.smtp_server, self.smtp_port, timeout=10)
                server.starttls()

            server.login(self.username, self.password)
            server.quit()
            return True, "OK"
        except Exception as e:
            return False, str(e)

    def send_email(self, to_address, subject, body, attachments=None):
        """Enviar un email vía SMTP"""
        try:
            msg = MIMEMultipart()
            msg['From'] = f"{self.display_name} <{self.username}>" if self.display_name else self.username
            msg['To'] = to_address
            msg['Subject'] = subject
            msg.attach(MIMEText(body, 'plain'))

            if attachments:
                for filepath in attachments:
                    path = Path(filepath)
                    if not path.exists():
                        continue
                    part = MIMEBase('application', 'octet-stream')
                    with open(path, 'rb') as f:
                        part.set_payload(f.read())
                    encoders.encode_base64(part)
                    part.add_header('Content-Disposition', f'attachment; filename="{path.name}"')
                    msg.attach(part)

            # Conexión SMTP
            if self.smtp_port == 465:
                server = smtplib.SMTP_SSL(self.smtp_server, self.smtp_port, timeout=30)
            else:
                server = smtplib.SMTP(self.smtp_server, self.smtp_port, timeout=30)
                server.starttls()

            server.login(self.username, self.password)
            server.send_message(msg)
            server.quit()
            return True
        except Exception as e:
            print(f"❌ Error SMTP: {e}")
            return False

    def decode_mime_words(self, s):
        if not s: return ""
        try:
            decoded_parts = decode_header(s)
            return "".join([
                part.decode(encoding or 'utf-8', errors='ignore') if isinstance(part, bytes) else str(part)
                for part, encoding in decoded_parts
            ])
        except:
            return str(s)

    def save_email(self, email_obj, email_id, uid=None):
        try:
            subject = self.decode_mime_words(email_obj.get('Subject', 'Sin asunto'))
            sender = self.decode_mime_words(email_obj.get('From', 'Desconocido'))

            email_data = {
                'id': email_id,
                'uid': uid or str(email_id),
                'subject': subject,
                'from': sender,
                'to': self.decode_mime_words(email_obj.get('To', '')),
                'date': email_obj.get('Date', ''),
                'headers': {k: self.decode_mime_words(str(v)) for k, v in email_obj.items()},
                'body_text': '',
                'body_html': '',
                'attachments': []
            }

            safe_subject = "".join(c for c in subject if c.isalnum() or c in (' ', '-', '_')).rstrip()[:50]
            # Usar UID para el nombre de la carpeta si está disponible, si no el ID
            folder_id = uid if uid else f"{email_id:04d}"
            email_folder = self.backup_dir / f"email_{folder_id}"
            email_folder.mkdir(parents=True, exist_ok=True)

            if email_obj.is_multipart():
                for part in email_obj.walk():
                    content_type = part.get_content_type()
                    content_disposition = str(part.get("Content-Disposition", ""))
                    if content_type == "text/plain" and "attachment" not in content_disposition:
                        payload = part.get_payload(decode=True)
                        if payload: email_data['body_text'] = payload.decode('utf-8', errors='ignore')
                    elif content_type == "text/html" and "attachment" not in content_disposition:
                        payload = part.get_payload(decode=True)
                        if payload: email_data['body_html'] = payload.decode('utf-8', errors='ignore')
                    elif "attachment" in content_disposition:
                        filename = self.decode_mime_words(part.get_filename())
                        if filename:
                            payload = part.get_payload(decode=True)
                            if payload:
                                with open(email_folder / filename, 'wb') as f:
                                    f.write(payload)
                                email_data['attachments'].append({'filename': filename, 'size': len(payload)})
            else:
                payload = email_obj.get_payload(decode=True)
                if payload: email_data['body_text'] = payload.decode('utf-8', errors='ignore')

            json_filename = f"email_{email_id:04d}.json" if not uid else "email_data.json"
            with open(email_folder / json_filename, 'w', encoding='utf-8') as f:
                json.dump(email_data, f, ensure_ascii=False, indent=2)
            return True
        except Exception as e:
            print(f"❌ Error guardando email {email_id}: {e}")
            return False

    def backup_all_emails(self, limit=None):
        server = self.connect_pop3()
        if not server: return False
        try:
            # Obtener UIDLs
            resp, items = server.uidl()
            # items es una lista de strings 'id uid'
            uidl_map = {}
            for item in items:
                msg_id, msg_uid = item.decode().split()
                uidl_map[int(msg_id)] = msg_uid

            num_messages = len(uidl_map)
            if limit: num_messages = min(num_messages, limit)

            successful = 0
            # Iterar sobre los IDs disponibles de forma segura
            for i in sorted(uidl_map.keys()):
                if limit and successful >= limit:
                    break
                try:
                    uid = uidl_map[i]
                    raw_email = b"\n".join(server.retr(i)[1])
                    email_obj = email.message_from_bytes(raw_email)
                    if self.save_email(email_obj, i, uid=uid):
                        successful += 1
                except Exception as e:
                    print(f"Error descargando email {i}: {e}")
                    continue
            return True
        finally:
            server.quit()

    def get_local_emails(self):
        """Cargar emails guardados localmente"""
        emails = []
        if not self.backup_dir.exists():
            return emails

        for folder in sorted(self.backup_dir.glob('email_*'), reverse=True):
            if folder.is_dir():
                json_files = list(folder.glob('*.json'))
                if json_files:
                    try:
                        with open(json_files[0], 'r', encoding='utf-8') as f:
                            data = json.load(f)
                            # Inyectar análisis de spam al cargar
                            analysis = SpamDetector.analyze(data)
                            data['spam_score'] = analysis['score']
                            data['spam_level'] = analysis['level']
                            data['spam_reasons'] = analysis['reasons']
                            emails.append(data)
                    except:
                        continue
        return emails


if __name__ == "__main__":
    # Test simple
    client = EmailClient()
    if client.load_credentials():
        print(f"✅ Cliente cargado para: {client.username}")
        # client.backup_all_emails(limit=5)
    else:
        print("❌ No se pudieron cargar credenciales")
