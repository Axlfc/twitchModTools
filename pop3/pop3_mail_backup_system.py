#!/usr/bin/env python3
"""
POP3 Mail Backup System
A comprehensive solution for backing up emails from POP3 servers
"""

import poplib
import email
import os
import json
import logging
import hashlib
from datetime import datetime
from pathlib import Path
import argparse
import configparser
import time


class POP3BackupSystem:
    def __init__(self, config_file='pop3_backup_config.ini'):
        self.config_file = config_file
        self.config = self.load_config()
        self.setup_logging()

    def load_config(self):
        """Load configuration from INI file"""
        config = configparser.ConfigParser()

        # Create default config if it doesn't exist
        if not os.path.exists(self.config_file):
            self.create_default_config()

        config.read(self.config_file)
        return config

    def create_default_config(self):
        """Create a default configuration file"""
        config = configparser.ConfigParser()

        config['SERVER'] = {
            'host': 'pop.gmail.com',
            'port': '995',
            'use_ssl': 'true',
            'username': 'your_email@gmail.com',
            'password': 'your_app_password'
        }

        config['BACKUP'] = {
            'backup_directory': './mail_backup',
            'max_messages_per_session': '100',
            'delete_after_backup': 'false',
            'backup_format': 'eml',  # eml, mbox, or json
            'create_index': 'true'
        }

        config['LOGGING'] = {
            'log_level': 'INFO',
            'log_file': 'pop3_backup.log'
        }

        with open(self.config_file, 'w') as f:
            config.write(f)

        print(f"Created default config file: {self.config_file}")
        print("Please edit the configuration file with your email settings before running.")

    def setup_logging(self):
        """Setup logging configuration"""
        log_level = getattr(logging, self.config['LOGGING']['log_level'].upper())
        log_file = self.config['LOGGING']['log_file']

        logging.basicConfig(
            level=log_level,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(log_file),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger(__name__)

    def connect_to_server(self):
        """Connect to POP3 server"""
        try:
            host = self.config['SERVER']['host']
            port = int(self.config['SERVER']['port'])
            use_ssl = self.config['SERVER'].getboolean('use_ssl')

            if use_ssl:
                self.pop_conn = poplib.POP3_SSL(host, port)
            else:
                self.pop_conn = poplib.POP3(host, port)

            # Enable debugging if needed
            # self.pop_conn.set_debuglevel(1)

            username = self.config['SERVER']['username']
            password = self.config['SERVER']['password']

            self.pop_conn.user(username)
            self.pop_conn.pass_(password)

            self.logger.info(f"Successfully connected to {host}:{port}")
            return True

        except Exception as e:
            self.logger.error(f"Failed to connect to server: {e}")
            return False

    def get_message_list(self):
        """Get list of messages from server"""
        try:
            response = self.pop_conn.list()
            message_count = len(response[1])
            self.logger.info(f"Found {message_count} messages on server")
            return response[1]
        except Exception as e:
            self.logger.error(f"Failed to get message list: {e}")
            return []

    def create_backup_directory(self):
        """Create backup directory structure"""
        backup_dir = Path(self.config['BACKUP']['backup_directory'])

        # Create main backup directory
        backup_dir.mkdir(parents=True, exist_ok=True)

        # Create subdirectories by date
        today = datetime.now().strftime('%Y-%m-%d')
        self.session_dir = backup_dir / today
        self.session_dir.mkdir(exist_ok=True)

        # Create format-specific directories
        format_type = self.config['BACKUP']['backup_format']
        self.messages_dir = self.session_dir / format_type
        self.messages_dir.mkdir(exist_ok=True)

        self.logger.info(f"Backup directory: {self.messages_dir}")

    def generate_message_id(self, raw_message):
        """Generate unique ID for message based on content"""
        return hashlib.md5(raw_message).hexdigest()

    def backup_message(self, msg_num, raw_message, format_type):
        """Backup a single message in specified format"""
        try:
            message_id = self.generate_message_id(raw_message)

            if format_type == 'eml':
                return self.save_as_eml(msg_num, raw_message, message_id)
            elif format_type == 'mbox':
                return self.save_as_mbox(msg_num, raw_message, message_id)
            elif format_type == 'json':
                return self.save_as_json(msg_num, raw_message, message_id)
            else:
                self.logger.error(f"Unsupported format: {format_type}")
                return None

        except Exception as e:
            self.logger.error(f"Failed to backup message {msg_num}: {e}")
            return None

    def save_as_eml(self, msg_num, raw_message, message_id):
        """Save message as EML file"""
        filename = f"msg_{msg_num:06d}_{message_id}.eml"
        filepath = self.messages_dir / filename

        with open(filepath, 'wb') as f:
            f.write(b'\r\n'.join(raw_message))

        return {
            'msg_num': msg_num,
            'filename': filename,
            'message_id': message_id,
            'size': filepath.stat().st_size,
            'timestamp': datetime.now().isoformat()
        }

    def save_as_mbox(self, msg_num, raw_message, message_id):
        """Save message to MBOX format"""
        mbox_file = self.messages_dir / 'backup.mbox'

        # Parse message to get sender and date
        msg = email.message_from_bytes(b'\r\n'.join(raw_message))
        sender = msg.get('From', 'unknown@unknown.com')
        msg_date = msg.get('Date', datetime.now().strftime('%a %b %d %H:%M:%S %Y'))

        with open(mbox_file, 'ab') as f:
            # MBOX format starts with "From " line
            from_line = f"From {sender} {msg_date}\n".encode()
            f.write(from_line)
            f.write(b'\r\n'.join(raw_message))
            f.write(b'\n\n')

        return {
            'msg_num': msg_num,
            'filename': 'backup.mbox',
            'message_id': message_id,
            'sender': sender,
            'timestamp': datetime.now().isoformat()
        }

    def save_as_json(self, msg_num, raw_message, message_id):
        """Save message as JSON with metadata"""
        msg = email.message_from_bytes(b'\r\n'.join(raw_message))

        # Extract message parts
        message_data = {
            'message_id': message_id,
            'msg_num': msg_num,
            'headers': dict(msg.items()),
            'timestamp': datetime.now().isoformat(),
            'body': self.extract_message_body(msg),
            'attachments': self.extract_attachments(msg)
        }

        filename = f"msg_{msg_num:06d}_{message_id}.json"
        filepath = self.messages_dir / filename

        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(message_data, f, indent=2, ensure_ascii=False)

        return {
            'msg_num': msg_num,
            'filename': filename,
            'message_id': message_id,
            'size': filepath.stat().st_size,
            'timestamp': datetime.now().isoformat()
        }

    def extract_message_body(self, msg):
        """Extract text body from email message"""
        body_parts = []

        if msg.is_multipart():
            for part in msg.walk():
                content_type = part.get_content_type()
                if content_type in ['text/plain', 'text/html']:
                    charset = part.get_content_charset() or 'utf-8'
                    try:
                        body = part.get_payload(decode=True).decode(charset)
                        body_parts.append({
                            'content_type': content_type,
                            'body': body
                        })
                    except:
                        body_parts.append({
                            'content_type': content_type,
                            'body': '[Unable to decode]'
                        })
        else:
            content_type = msg.get_content_type()
            charset = msg.get_content_charset() or 'utf-8'
            try:
                body = msg.get_payload(decode=True).decode(charset)
                body_parts.append({
                    'content_type': content_type,
                    'body': body
                })
            except:
                body_parts.append({
                    'content_type': content_type,
                    'body': '[Unable to decode]'
                })

        return body_parts

    def extract_attachments(self, msg):
        """Extract attachment information"""
        attachments = []

        if msg.is_multipart():
            for part in msg.walk():
                if part.get_content_disposition() == 'attachment':
                    filename = part.get_filename()
                    if filename:
                        attachments.append({
                            'filename': filename,
                            'content_type': part.get_content_type(),
                            'size': len(part.get_payload(decode=True) or b'')
                        })

        return attachments

    def create_backup_index(self, backed_up_messages):
        """Create an index file for the backup session"""
        if not self.config['BACKUP'].getboolean('create_index'):
            return

        index_data = {
            'backup_session': {
                'date': datetime.now().isoformat(),
                'server': self.config['SERVER']['host'],
                'username': self.config['SERVER']['username'],
                'total_messages': len(backed_up_messages),
                'format': self.config['BACKUP']['backup_format']
            },
            'messages': backed_up_messages
        }

        index_file = self.session_dir / 'backup_index.json'
        with open(index_file, 'w') as f:
            json.dump(index_data, f, indent=2)

        self.logger.info(f"Created backup index: {index_file}")

    def run_backup(self):
        """Main backup process"""
        self.logger.info("Starting POP3 backup process")

        # Create backup directory
        self.create_backup_directory()

        # Connect to server
        if not self.connect_to_server():
            return False

        try:
            # Get message list
            message_list = self.get_message_list()
            if not message_list:
                self.logger.info("No messages to backup")
                return True

            # Limit messages per session
            max_messages = int(self.config['BACKUP']['max_messages_per_session'])
            if max_messages > 0:
                message_list = message_list[:max_messages]

            backed_up_messages = []
            format_type = self.config['BACKUP']['backup_format']
            delete_after = self.config['BACKUP'].getboolean('delete_after_backup')

            # Backup each message
            for i, msg_info in enumerate(message_list, 1):
                msg_num = int(msg_info.decode().split()[0])

                try:
                    self.logger.info(f"Backing up message {i}/{len(message_list)} (#{msg_num})")

                    # Retrieve message
                    response = self.pop_conn.retr(msg_num)
                    raw_message = response[1]

                    # Backup message
                    backup_info = self.backup_message(msg_num, raw_message, format_type)
                    if backup_info:
                        backed_up_messages.append(backup_info)

                        # Delete from server if configured
                        if delete_after:
                            self.pop_conn.dele(msg_num)
                            self.logger.info(f"Deleted message #{msg_num} from server")

                    # Small delay to be nice to the server
                    time.sleep(0.1)

                except Exception as e:
                    self.logger.error(f"Failed to backup message #{msg_num}: {e}")
                    continue

            # Create backup index
            self.create_backup_index(backed_up_messages)

            self.logger.info(f"Backup completed: {len(backed_up_messages)} messages backed up")
            return True

        finally:
            # Always close connection
            try:
                self.pop_conn.quit()
                self.logger.info("Disconnected from server")
            except:
                pass

    def list_backups(self):
        """List available backup sessions"""
        backup_dir = Path(self.config['BACKUP']['backup_directory'])

        if not backup_dir.exists():
            print("No backup directory found")
            return

        print("\nAvailable backup sessions:")
        print("-" * 50)

        for session_dir in sorted(backup_dir.iterdir()):
            if session_dir.is_dir():
                index_file = session_dir / 'backup_index.json'
                if index_file.exists():
                    try:
                        with open(index_file) as f:
                            index_data = json.load(f)

                        session_info = index_data['backup_session']
                        print(f"Date: {session_info['date'][:10]}")
                        print(f"Messages: {session_info['total_messages']}")
                        print(f"Format: {session_info['format']}")
                        print(f"Directory: {session_dir}")
                        print("-" * 30)
                    except:
                        print(f"Directory: {session_dir} (no index)")
                        print("-" * 30)


def main():
    parser = argparse.ArgumentParser(description='POP3 Mail Backup System')
    parser.add_argument('--config', default='pop3_backup_config.ini',
                        help='Configuration file path')
    parser.add_argument('--backup', action='store_true',
                        help='Run backup process')
    parser.add_argument('--list', action='store_true',
                        help='List backup sessions')
    parser.add_argument('--setup', action='store_true',
                        help='Create default configuration file')

    args = parser.parse_args()

    backup_system = POP3BackupSystem(args.config)

    if args.setup:
        backup_system.create_default_config()
    elif args.list:
        backup_system.list_backups()
    elif args.backup:
        success = backup_system.run_backup()
        exit(0 if success else 1)
    else:
        parser.print_help()


if __name__ == '__main__':
    main()