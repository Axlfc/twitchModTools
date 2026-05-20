import unittest
import os
import json
import shutil
from pathlib import Path
from pop3_client import EmailClient, SecureCredentialManager
from spam_detector import SpamDetector

class TestEmailClientCore(unittest.TestCase):
    def setUp(self):
        self.test_dir = Path("test_backup")
        self.test_dir.mkdir(exist_ok=True)
        self.config_file = ".test_secure_config"
        if os.path.exists(self.config_file):
            os.remove(self.config_file)

        self.client = EmailClient()
        self.client.backup_dir = self.test_dir
        self.client.credential_manager = SecureCredentialManager(self.config_file)

    def tearDown(self):
        if self.test_dir.exists():
            shutil.rmtree(self.test_dir)
        if os.path.exists(self.config_file):
            os.remove(self.config_file)

    def test_credential_encryption(self):
        master_key = "secret_master_key"
        user = "test@example.com"
        password = "password123"
        display_name = "Test User"

        # Save
        success = self.client.credential_manager.save_credentials(user, password, display_name, master_key)
        self.assertTrue(success)

        # Load with correct key
        u, p, d = self.client.credential_manager.load_credentials(master_key)
        self.assertEqual(u, user)
        self.assertEqual(p, password)
        self.assertEqual(d, display_name)

        # Load with wrong key
        u, p, d = self.client.credential_manager.load_credentials("wrong_key")
        self.assertIsNone(u)

    def test_provider_autodetection(self):
        self.assertTrue(self.client.autodetect_settings("user@gmail.com"))
        self.assertEqual(self.client.pop_server, "pop.gmail.com")

        self.assertTrue(self.client.autodetect_settings("user@tinet.cat"))
        self.assertEqual(self.client.pop_server, "pop3.tinet.cat")

        self.assertFalse(self.client.autodetect_settings("user@unknown.com"))

    def test_spam_detection(self):
        # Spammy email
        spam_email = {
            'subject': 'GANASTE UN PREMIO BITCOIN GRATIS',
            'body_text': 'Haz clic aquí para reclamar tu herencia de bitcoin. Oferta urgente.',
            'from': 'spammer123456789@example.com',
            'headers': {'Authentication-Results': 'spf=fail'}
        }
        analysis = SpamDetector.analyze(spam_email)
        self.assertEqual(analysis['level'], 'spam')
        self.assertTrue(analysis['score'] > 0.7)
        self.assertIn("Fallo en validación SPF", analysis['reasons'])

        # Safe email
        safe_email = {
            'subject': 'Reunión de equipo',
            'body_text': 'Hola, nos vemos a las 10 para discutir el proyecto.',
            'from': 'jefe@empresa.com',
            'headers': {'Authentication-Results': 'spf=pass'}
        }
        analysis = SpamDetector.analyze(safe_email)
        self.assertEqual(analysis['level'], 'safe')
        self.assertEqual(analysis['score'], 0.0)

    def test_html_stripping(self):
        html = "<html><body><h1>Hola</h1><p>Esto es un <b>test</b>.<br>Nueva línea.</p></body></html>"
        text = SpamDetector.strip_tags(html)
        self.assertIn("Hola", text)
        self.assertIn("Esto es un test.", text)
        self.assertNotIn("<html>", text)
        self.assertNotIn("<b>", text)

if __name__ == "__main__":
    unittest.main()
