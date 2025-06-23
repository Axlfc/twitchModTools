#!/usr/bin/env python3
"""
Cliente POP3 para backup de correos electrónicos
Sistema de credenciales ultra-seguro sin dependencias externas
Ahora con soporte para archivos .env sin librerías externas
VERSIÓN CORREGIDA - Problemas de autenticación solucionados
"""

import poplib
import email
import os
import json
import datetime
import getpass
import hashlib
import base64
from pathlib import Path
from email.header import decode_header


class EnvLoader:
    """Cargador de archivos .env sin dependencias externas"""

    @staticmethod
    def load_env_file(filepath='.env'):
        """Cargar variables de entorno desde archivo .env"""
        if not os.path.exists(filepath):
            print(f"⚠️  Archivo {filepath} no encontrado")
            return {}

        env_vars = {}
        try:
            with open(filepath, 'r', encoding='utf-8') as file:
                for line_num, line in enumerate(file, 1):
                    line = line.strip()

                    # Saltar líneas vacías y comentarios
                    if not line or line.startswith('#'):
                        continue

                    # Debe contener '='
                    if '=' not in line:
                        print(f"⚠️  Línea {line_num} en {filepath} no válida: {line}")
                        continue

                    # Dividir en clave y valor (solo en el primer '=')
                    key, value = line.split('=', 1)
                    key = key.strip()
                    value = value.strip()

                    # Remover comillas si están presentes
                    if (value.startswith('"') and value.endswith('"')) or \
                            (value.startswith("'") and value.endswith("'")):
                        value = value[1:-1]

                    # Guardar en diccionario y establecer en os.environ
                    env_vars[key] = value
                    os.environ[key] = value

            print(f"✅ Cargadas {len(env_vars)} variables desde {filepath}")
            return env_vars

        except Exception as e:
            print(f"❌ Error leyendo {filepath}: {e}")
            return {}

    @staticmethod
    def get_env_var(key, default=None):
        """Obtener variable de entorno con valor por defecto"""
        return os.getenv(key, default)


class SecureCredentialManager:
    """Gestor ultra-seguro de credenciales sin dependencias externas"""

    def __init__(self, config_file=".secure_config"):
        self.config_file = Path(config_file)
        self.credentials = {}
        self.env_loader = EnvLoader()

    def _simple_encrypt(self, text, key):
        """Cifrado simple basado en XOR con clave derivada"""
        # Generar clave derivada usando hash
        key_hash = hashlib.sha256(key.encode()).digest()
        # Cifrar usando XOR
        encrypted = bytearray()
        for i, char in enumerate(text.encode('utf-8')):
            encrypted.append(char ^ key_hash[i % len(key_hash)])
        # Codificar en base64 para almacenamiento
        return base64.b64encode(bytes(encrypted)).decode('ascii')

    def _simple_decrypt(self, encrypted_text, key):
        """Descifrado simple"""
        try:
            # Decodificar de base64
            encrypted_bytes = base64.b64decode(encrypted_text.encode('ascii'))
            # Generar misma clave derivada
            key_hash = hashlib.sha256(key.encode()).digest()
            # Descifrar usando XOR
            decrypted = bytearray()
            for i, byte in enumerate(encrypted_bytes):
                decrypted.append(byte ^ key_hash[i % len(key_hash)])
            return decrypted.decode('utf-8')
        except Exception:
            return None

    def load_from_env(self):
        """Cargar credenciales desde archivo .env"""
        print("\n=== CARGANDO CREDENCIALES DESDE .env ===")

        # Cargar archivo .env
        env_vars = self.env_loader.load_env_file('.env')

        # Buscar credenciales en variables de entorno
        username = self.env_loader.get_env_var('TINET_USERNAME') or self.env_loader.get_env_var('EMAIL_USERNAME')
        password = self.env_loader.get_env_var('TINET_PASSWORD') or self.env_loader.get_env_var('EMAIL_PASSWORD')

        if username and password:
            print("✅ Credenciales cargadas desde .env")
            return username.strip(), password.strip()  # STRIP ESPACIOS EXTRA
        else:
            missing = []
            if not username:
                missing.append('TINET_USERNAME (o EMAIL_USERNAME)')
            if not password:
                missing.append('TINET_PASSWORD (o EMAIL_PASSWORD)')
            print(f"❌ Variables faltantes en .env: {', '.join(missing)}")
            return None, None

    def save_credentials(self, username, password):
        print("\n=== GUARDADO ULTRA-SEGURO DE CREDENCIALES ===")
        # Limpiar espacios de las credenciales
        username = username.strip()
        password = password.strip()

        # Solicitar clave maestra
        master_key = getpass.getpass("Introduce una clave maestra (no se mostrará): ")
        confirm_key = getpass.getpass("Confirma la clave maestra: ")

        if master_key != confirm_key:
            print("❌ Las claves no coinciden")
            return False
        if len(master_key) < 8:
            print("❌ La clave debe tener al menos 8 caracteres")
            return False

        try:
            # Cifrar credenciales
            encrypted_username = self._simple_encrypt(username, master_key)
            encrypted_password = self._simple_encrypt(password, master_key)

            # Crear hash de verificación de la clave maestra
            key_verify = hashlib.sha256(master_key.encode()).hexdigest()[:16]

            # Guardar en archivo
            config_data = {
                'key_verify': key_verify,
                'username': encrypted_username,
                'password': encrypted_password,
                'created': datetime.datetime.now().isoformat()
            }

            with open(self.config_file, 'w') as f:
                json.dump(config_data, f, indent=2)

            # Establecer permisos restrictivos (solo en sistemas Unix)
            try:
                os.chmod(self.config_file, 0o600)  # Solo lectura/escritura para el propietario
            except:
                pass

            print("✅ Credenciales guardadas de forma segura")
            return True

        except Exception as e:
            print(f"❌ Error guardando credenciales: {e}")
            return False

    def load_credentials(self):
        """Cargar credenciales de forma segura"""
        if not self.config_file.exists():
            print("❌ No existe archivo de configuración")
            return None, None

        try:
            with open(self.config_file, 'r') as f:
                config_data = json.load(f)

            # Solicitar clave maestra
            master_key = getpass.getpass("Introduce tu clave maestra: ")

            # Verificar clave maestra
            key_verify = hashlib.sha256(master_key.encode()).hexdigest()[:16]
            if key_verify != config_data.get('key_verify'):
                print("❌ Clave maestra incorrecta")
                return None, None

            # Descifrar credenciales
            username = self._simple_decrypt(config_data['username'], master_key)
            password = self._simple_decrypt(config_data['password'], master_key)

            if username is None or password is None:
                print("❌ Error descifrando credenciales")
                return None, None

            print("✅ Credenciales cargadas correctamente")
            return username.strip(), password.strip()  # STRIP ESPACIOS EXTRA

        except Exception as e:
            print(f"❌ Error cargando credenciales: {e}")
            return None, None

    def delete_credentials(self):
        """Eliminar archivo de credenciales"""
        try:
            if self.config_file.exists():
                # Sobrescribir con datos aleatorios antes de eliminar
                with open(self.config_file, 'wb') as f:
                    f.write(os.urandom(1024))
                self.config_file.unlink()
                print("✅ Credenciales eliminadas de forma segura")
                return True
        except Exception as e:
            print(f"❌ Error eliminando credenciales: {e}")
        return False


class EmailBackupClient:
    def __init__(self):
        self.mail_server = None
        self.credential_manager = SecureCredentialManager()
        self.username = None
        self.password = None

        # Cargar configuración del servidor desde .env o usar defaults
        env_loader = EnvLoader()
        env_loader.load_env_file('.env')

        # CORREGIDO: Usar pop3.tinet.cat (no .org)
        self.pop_server = env_loader.get_env_var('POP_SERVER', 'pop3.tinet.cat')
        self.pop_port = int(env_loader.get_env_var('POP_PORT', '995'))

        backup_dir_name = env_loader.get_env_var('BACKUP_DIR', 'email_backup')
        self.backup_dir = Path(backup_dir_name)

        # Crear el directorio si no existe, incluyendo cualquier carpeta intermedia
        try:
            self.backup_dir.mkdir(parents=True, exist_ok=True)
            print(f"✅ Directorio de backup asegurado: {self.backup_dir.resolve()}")
        except Exception as e:
            print(f"❌ No se pudo crear el directorio de backup: {e}")

    def setup_credentials(self):
        """Configurar credenciales por primera vez"""
        print("\n=== CONFIGURACIÓN INICIAL DE CREDENCIALES ===")
        print("Este proceso solo se hace una vez y las credenciales se guardan cifradas")

        username = input("Usuario de email: ").strip()
        password = getpass.getpass("Contraseña de email (no se mostrará): ").strip()

        if not username or not password:
            print("❌ Usuario y contraseña son obligatorios")
            return False

        return self.credential_manager.save_credentials(username, password)

    def load_credentials(self):
        """Cargar credenciales desde archivo seguro o .env"""
        # Primero intentar cargar desde .env
        env_username, env_password = self.credential_manager.load_from_env()
        if env_username and env_password:
            self.username = env_username
            self.password = env_password
            return True

        # Si no hay .env, intentar cargar desde archivo cifrado
        print("🔐 No se encontraron credenciales en .env, intentando archivo cifrado...")
        self.username, self.password = self.credential_manager.load_credentials()
        return self.username is not None and self.password is not None

    def connect(self):
        """Conectar al servidor POP3"""
        if not self.username or not self.password:
            print("❌ No hay credenciales cargadas")
            return False

        try:
            print(f"Conectando a {self.pop_server}:{self.pop_port}...")
            print(f"🔍 Usuario: {self.username}")
            print(f"🔍 Password: {'*' * len(self.password)} ({len(self.password)} caracteres)")

            self.mail_server = poplib.POP3_SSL(self.pop_server, self.pop_port)
            self.mail_server.user(self.username)
            self.mail_server.pass_(self.password)
            print("✅ Connexió i autenticació correcta!")
            num_messages = len(self.mail_server.list()[1])
            print("NUMERO DE MISSATGES:\t", num_messages)
            # self.mail_server.quit()

            return True

        except poplib.error_proto as e:
            error_msg = str(e).lower()
            print(f"❌ Error de autenticación POP3: {e}")

            if "authentication failed" in error_msg or "invalid" in error_msg:
                print("🔍 DIAGNÓSTICO DE AUTENTICACIÓN:")
                print("   - Verifica que el usuario y contraseña sean correctos")
                print("   - Comprueba que la cuenta tenga acceso POP3 habilitado")
                print("   - Algunos proveedores requieren contraseñas de aplicación")
                print("   - Verifica que no haya espacios extra en las credenciales")
                print("   - Para TINET, verifica que uses el dominio correcto (@tinet.cat)")

            # Limpiar conexión fallida
            try:
                if hasattr(self, 'mail_server'):
                    self.mail_server.quit()
            except:
                pass
            return False

        except Exception as e:
            print(f"❌ Error de conexión: {e}")

            # Información adicional de diagnóstico
            if "timed out" in str(e).lower():
                print("🔍 DIAGNÓSTICO:")
                print("   - Problema de conectividad de red")
                print("   - Firewall bloqueando conexión")
                print("   - Servidor temporalmente no disponible")
            elif "connection refused" in str(e).lower():
                print("🔍 DIAGNÓSTICO:")
                print("   - Servidor o puerto incorrectos")
                print("   - Servicio POP3 no disponible")

            return False

    def get_email_count(self):
        """Obtener número de emails en el servidor"""
        try:
            num_messages = len(self.mail_server.list()[1])
            print(f"📧 Emails disponibles: {num_messages}")
            return num_messages
        except Exception as e:
            print(f"❌ Error obteniendo número de emails: {e}")
            return 0

    def decode_mime_words(self, s):
        """Decodificar headers de email"""
        if not s:
            return ""
        try:
            decoded_parts = decode_header(s)
            decoded_string = ""
            for part, encoding in decoded_parts:
                if isinstance(part, bytes):
                    decoded_string += part.decode(encoding or 'utf-8', errors='ignore')
                else:
                    decoded_string += str(part)
            return decoded_string
        except Exception:
            return str(s)

    def save_email(self, email_obj, email_id):
        """Guardar email individual junto con sus archivos adjuntos"""
        try:
            # Extraer información básica
            subject = self.decode_mime_words(email_obj.get('Subject', 'Sin asunto'))
            sender = self.decode_mime_words(email_obj.get('From', 'Desconocido'))
            date = email_obj.get('Date', 'Fecha desconocida')

            # Crear estructura de datos del email
            email_data = {
                'id': email_id,
                'subject': subject,
                'from': sender,
                'to': self.decode_mime_words(email_obj.get('To', '')),
                'date': date,
                'headers': dict(email_obj.items()),
                'body_text': '',
                'body_html': '',
                'attachments': []
            }

            # Crear carpeta específica para este email
            safe_subject = "".join(c for c in subject if c.isalnum() or c in (' ', '-', '_')).rstrip()[:50]

            email_folder = self.backup_dir / f"email_{email_id:04d}_{safe_subject}"

            email_folder.mkdir(parents=True, exist_ok=True)

            # Procesar partes del mensaje
            if email_obj.is_multipart():
                for part in email_obj.walk():
                    content_type = part.get_content_type()
                    content_disposition = str(part.get("Content-Disposition", ""))

                    # Guardar cuerpo de texto plano
                    if content_type == "text/plain" and "attachment" not in content_disposition:
                        body = part.get_payload(decode=True)
                        if body:
                            email_data['body_text'] = body.decode('utf-8', errors='ignore')

                    # Guardar cuerpo HTML
                    elif content_type == "text/html" and "attachment" not in content_disposition:
                        body = part.get_payload(decode=True)
                        if body:
                            email_data['body_html'] = body.decode('utf-8', errors='ignore')

                    # Guardar archivo adjunto
                    elif "attachment" in content_disposition:
                        filename = part.get_filename()
                        if filename:
                            decoded_filename = self.decode_mime_words(filename)
                            payload = part.get_payload(decode=True)
                            if payload:
                                # Guardar el archivo en la carpeta correspondiente
                                filepath = email_folder / decoded_filename
                                with open(filepath, 'wb') as f:
                                    f.write(payload)
                                print(f"📎 Guardado adjunto: {decoded_filename}")
                                # Registrar en JSON
                                email_data['attachments'].append({
                                    'filename': decoded_filename,
                                    'content_type': content_type,
                                    'size': len(payload)
                                })

            else:
                # Email simple (no multipart)
                body = email_obj.get_payload(decode=True)
                if body:
                    email_data['body_text'] = body.decode('utf-8', errors='ignore')

            # Guardar metadata del email en formato JSON
            filename = f"email_{email_id:04d}_{safe_subject}.json"
            filepath = email_folder / filename

            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(email_data, f, ensure_ascii=False, indent=2)

            print(f"📄 Email guardado en: {filepath}")

            return True

        except Exception as e:
            print(f"❌ Error guardando email {email_id}: {e}")
            return False

    def save_attachments(self, email_obj, email_folder):
        """Guardar los archivos adjuntos en una carpeta"""
        attachments = []
        if email_obj.is_multipart():
            for part in email_obj.walk():
                content_disposition = str(part.get("Content-Disposition", ""))
                if "attachment" in content_disposition:
                    filename = part.get_filename()
                    if filename:
                        decoded_filename = self.decode_mime_words(filename)
                        # Crear archivo en la carpeta del email
                        filepath = email_folder / decoded_filename
                        payload = part.get_payload(decode=True)
                        if payload:
                            with open(filepath, 'wb') as f:
                                f.write(payload)
                            attachments.append(decoded_filename)
                            print(f"📎 Guardado adjunto: {decoded_filename}")
        return attachments

    def backup_all_emails(self, limit=None):
        """Hacer backup de todos los emails"""
        if not self.connect():
            return False
        try:
            num_messages = self.get_email_count()
            if num_messages == 0:
                print("ℹ️  No hay emails para hacer backup")
                return True

            # Limitar número de emails si se especifica
            if limit:
                num_messages = min(num_messages, limit)

            print(f"🔄 Iniciando backup de {num_messages} emails...")
            successful_backups = 0

            for i in range(1, num_messages + 1):
                print(f"📧 Procesando email {i}/{num_messages}...", end=" ")
                try:
                    # Obtener email completo
                    raw_email = b"\n".join(self.mail_server.retr(i)[1])
                    email_obj = email.message_from_bytes(raw_email)
                    if self.save_email(email_obj, i):
                        successful_backups += 1
                        print("✅")
                    else:
                        print("❌")
                except Exception as e:
                    print(f"❌ Error: {e}")

            print(f"\n🎯 Backup completado: {successful_backups}/{num_messages} emails guardados")

            # Guardar resumen del backup
            summary = {
                'timestamp': datetime.datetime.now().isoformat(),
                'total_emails': num_messages,
                'successful_backups': successful_backups,
                'server': self.pop_server,
                'username': self.username
            }

            with open(self.backup_dir / 'backup_summary.json', 'w') as f:
                json.dump(summary, f, indent=2)

            return True
        except Exception as e:
            print(f"❌ Error durante el backup: {e}")
            return False
        finally:
            self.disconnect()

    def list_emails_preview(self, count=10):
        """Mostrar preview de los últimos emails"""
        if not self.connect():
            return
        try:
            num_messages = self.get_email_count()
            if num_messages == 0:
                print("ℹ️  No hay emails")
                return
            start = max(1, num_messages - count + 1)
            print(f"\n📋 Últimos {min(count, num_messages)} emails:")
            print("=" * 100)
            for i in range(start, num_messages + 1):
                try:
                    # Obtener solo headers para preview
                    headers = self.mail_server.top(i, 0)[1]
                    email_obj = email.message_from_bytes(b"\n".join(headers))
                    subject = self.decode_mime_words(email_obj.get('Subject', 'Sin asunto'))[:50]
                    sender = self.decode_mime_words(email_obj.get('From', 'Desconocido'))[:30]
                    date = email_obj.get('Date', 'Fecha desconocida')[:20]
                    print(f"{i:3d} | {date:<20} | {sender:<30} | {subject}")
                except Exception as e:
                    print(f"{i:3d} | ❌ Error obteniendo preview: {e}")
            print("=" * 100)
        except Exception as e:
            print(f"❌ Error listando emails: {e}")
        finally:
            self.disconnect()

    def disconnect(self):
        """Cerrar conexión"""
        try:
            if hasattr(self, 'mail_server'):
                self.mail_server.quit()
                print("🔌 Desconectado del servidor")
        except Exception as e:
            print(f"⚠️  Error desconectando: {e}")

    def create_env_template(self):
        """Crear plantilla de archivo .env"""
        template = """# Configuración TINET POP3
# Credenciales de email
TINET_USERNAME=tu_usuario@tinet.cat
TINET_PASSWORD=tu_contraseña_segura

# Configuración del servidor (opcional)
POP_SERVER=pop3.tinet.cat
POP_PORT=995

# Directorio de backup (opcional)
BACKUP_DIR=email_backup

# Ejemplo de variables alternativas
# EMAIL_USERNAME=tu_usuario@tinet.cat
# EMAIL_PASSWORD=tu_contraseña_segura
"""

        env_file = Path('.env')
        if env_file.exists():
            print("⚠️  El archivo .env ya existe")
            overwrite = input("¿Deseas sobrescribir el archivo existente? (s/N): ").strip().lower()
            if overwrite not in ['s', 'si', 'sí', 'y', 'yes']:
                print("❌ Operación cancelada")
                return False

        try:
            with open('.env', 'w', encoding='utf-8') as f:
                f.write(template)

            # Establecer permisos restrictivos
            try:
                os.chmod('.env', 0o600)
            except:
                pass

            print("✅ Archivo .env creado exitosamente")
            print("🔧 Edita el archivo .env con tus credenciales reales")
            return True
        except Exception as e:
            print(f"❌ Error creando .env: {e}")
            return False

    def show_current_config(self):
        """Mostrar configuración actual"""
        print("\n📋 === CONFIGURACIÓN ACTUAL ===")
        print(f"🌐 Servidor POP3: {self.pop_server}")
        print(f"🔌 Puerto: {self.pop_port}")
        print(f"👤 Usuario: {self.username if self.username else 'No configurado'}")
        print(
            f"🔑 Password: {'Configurado (' + str(len(self.password)) + ' caracteres)' if self.password else 'No configurado'}")
        print(f"📁 Directorio backup: {self.backup_dir}")

        # Mostrar variables de entorno relevantes
        print("\n🔧 Variables de entorno detectadas:")
        env_vars = ['TINET_USERNAME', 'TINET_PASSWORD', 'EMAIL_USERNAME', 'EMAIL_PASSWORD',
                    'POP_SERVER', 'POP_PORT', 'BACKUP_DIR']
        for var in env_vars:
            value = os.getenv(var)
            if value:
                if 'PASSWORD' in var:
                    print(f"   {var}: {'*' * len(value)} ({len(value)} caracteres)")
                else:
                    print(f"   {var}: {value}")
            else:
                print(f"   {var}: No definida")

    def diagnose_connection(self):
        """Diagnosticar problemas de conexión"""
        print("\n🔍 === DIAGNÓSTICO DE CONEXIÓN ===")

        # Verificar credenciales
        if not self.username or not self.password:
            print("❌ No hay credenciales cargadas")
            return

        print(f"✅ Servidor: {self.pop_server}")
        print(f"✅ Puerto: {self.pop_port}")
        print(f"✅ Usuario: {self.username}")
        print(f"✅ Password: Configurado ({len(self.password)} caracteres)")

        # Verificar conectividad básica
        import socket
        print(f"\n🌐 Probando conectividad a {self.pop_server}:{self.pop_port}...")
        try:
            sock = socket.create_connection((self.pop_server, self.pop_port), timeout=10)
            sock.close()
            print("✅ Conectividad TCP exitosa")
        except Exception as e:
            print(f"❌ Error de conectividad: {e}")
            return

        # Probar conexión POP3
        print("\n🔐 Probando conexión POP3...")
        try:
            test_server = poplib.POP3_SSL(self.pop_server, self.pop_port)
            print("✅ Conexión SSL establecida")

            # Probar usuario
            try:
                response = test_server.user(self.username)
                print(f"✅ Usuario aceptado: {response}")
            except Exception as e:
                print(f"❌ Error con usuario: {e}")
                test_server.quit()
                return

            # Probar contraseña
            try:
                response = test_server.pass_(self.password)
                print(f"✅ Contraseña aceptada: {response}")
                print("🎉 ¡Autenticación exitosa!")
            except Exception as e:
                print(f"❌ Error con contraseña: {e}")
                print("\n💡 POSIBLES SOLUCIONES:")
                print("   1. Verifica que la contraseña sea correcta")
                print("   2. Comprueba si necesitas una 'contraseña de aplicación'")
                print("   3. Verifica que POP3 esté habilitado en tu cuenta")
                print("   4. Revisa si hay espacios extra en las credenciales")
                print("   5. Algunos proveedores requieren autenticación de 2 factores")

            test_server.quit()

        except Exception as e:
            print(f"❌ Error de conexión POP3: {e}")
            print("\n💡 VERIFICA:")
            print("   - Servidor y puerto correctos")
            print("   - Firewall o proxy corporativo")
            print("   - Certificados SSL válidos")


def main():
    """Función principal"""
    print("🔐 === CLIENTE POP3 ULTRA-SEGURO PARA BACKUP DE CORREOS ===")
    print("📁 Versión corregida - Problemas de autenticación solucionados\n")

    try:
        client = EmailBackupClient()

        # Mostrar configuración actual al inicio
        print("📋 Configuración detectada:")
        print(f"   Servidor: {client.pop_server}:{client.pop_port}")

        # Intentar cargar credenciales automáticamente
        if not client.load_credentials():
            print("❌ No se pudieron cargar las credenciales")
            print("💡 Opciones disponibles:")
            print("   1. Crear archivo .env con tus credenciales")
            print("   2. Usar sistema de credenciales cifradas")

            choice = input("¿Qué deseas hacer? (1=.env, 2=cifrado): ").strip()
            if choice == '1':
                if client.create_env_template():
                    print("🔄 Reinicia el programa después de editar .env")
                return
            elif choice == '2':
                if not client.setup_credentials():
                    return
                if not client.load_credentials():
                    print("❌ Error recargando credenciales")
                    return
            else:
                print("❌ Opción inválida")
                return

        while True:
            print("\n🔧 OPCIONES:")
            print("1. 👀 Ver preview de emails")
            print("2. 💾 Hacer backup completo")
            print("3. 📊 Hacer backup limitado")
            print("4. ⚙️  Reconfigurar credenciales")
            print("5. 📁 Crear plantilla .env")
            print("6. 🗑️  Eliminar credenciales guardadas")
            print("7. 🔍 Diagnosticar conexión")
            print("8. 📋 Mostrar configuración actual")
            print("9. 🚪 Salir")
            choice = input("\n➤ Selecciona una opción (1-9): ").strip()

            if choice == '1':
                count = input("¿Cuántos emails mostrar? (default: 10): ").strip()
                try:
                    count = int(count) if count else 10
                    client.list_emails_preview(count)
                except ValueError:
                    print("⚠️  Número inválido, usando 10")
                    client.list_emails_preview(10)
            elif choice == '2':
                confirm = input("⚠️  ¿Confirmas hacer backup de TODOS los emails? (s/N): ").strip().lower()
                if confirm in ['s', 'si', 'sí', 'y', 'yes']:
                    client.backup_all_emails()
                else:
                    print("❌ Backup cancelado")
            elif choice == '3':
                limit = input("¿Cuántos emails hacer backup?: ").strip()
                try:
                    limit = int(limit)
                    if limit > 0:
                        client.backup_all_emails(limit)
                    else:
                        print("❌ Número debe ser mayor que 0")
                except ValueError:
                    print("❌ Número inválido")
            elif choice == '4':
                if not client.load_credentials():
                    print("❌ Error recargando credenciales")
                else:
                    print("❌ Operación cancelada")
            elif choice == '5':
                client.create_env_template()
            elif choice == '6':
                confirm = input("⚠️  ¿Eliminar credenciales guardadas? (s/N): ").strip().lower()
                if confirm in ['s', 'si', 'sí', 'y', 'yes']:
                    client.credential_manager.delete_credentials()
                    print("🚪 Reinicia el programa para configurar nuevas credenciales")
                    break
                else:
                    print("❌ Operación cancelada")
            elif choice == '7':
                client.diagnose_connection()
            elif choice == '8':
                client.show_current_config()
            elif choice == '9':
                print("👋 ¡Hasta luego!")
                break
            else:
                print("❌ Opción inválida. Selecciona una opción del 1 al 9.")
    except KeyboardInterrupt:
        print("\n⚠️  Interrumpido por el usuario")
    except Exception as e:
        print(f"💥 Error crítico: {e}")


if __name__ == "__main__":
    main()
