from pop3_client import EmailClient, SpamDetector

def test_autodetect():
    client = EmailClient()
    assert client.autodetect_settings("test@gmail.com") == True
    assert client.pop_server == "pop.gmail.com"
    assert client.autodetect_settings("user@tinet.cat") == True
    assert client.pop_server == "pop3.tinet.cat"
    print("✅ Autodetect test passed")

def test_spam_detector():
    # Clean email
    email1 = {'subject': 'Hola amigo', 'body_text': '¿Cómo estás?', 'from': 'amigo@example.com'}
    assert SpamDetector.analyze(email1) == 0

    # Spam email
    email2 = {'subject': 'Has ganado un PREMIO de BITCOIN', 'body_text': 'Haz click aquí para cobrar tu herencia de criptomonedas gratis', 'from': 'spammer123456789@spam.com'}
    assert SpamDetector.analyze(email2) == 2

    print("✅ Spam detector test passed")

if __name__ == "__main__":
    test_autodetect()
    test_spam_detector()
