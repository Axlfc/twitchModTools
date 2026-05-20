import re

class SpamDetector:
    """Detector de SPAM simple basado en heurísticas"""

    SPAM_KEYWORDS = [
        'oferta', 'ganado', 'premio', 'bitcoin', 'crypto', 'viagra', 'urgente',
        'cuenta bloqueada', 'herencia', 'loteria', 'beneficio', 'gratis',
        'invertir', 'casino', 'payout', 'jackpot', 'sex', 'dating',
        'promoción', 'descuento', 'clic aquí', 'gana dinero', 'trabaja desde casa'
    ]

    @staticmethod
    def analyze(email_data):
        """
        Analiza un email y devuelve un diccionario con:
        - score (0.0–1.0)
        - level ("safe" / "suspicious" / "spam")
        - reasons (lista de strings)
        """
        score = 0
        reasons = []
        subject = email_data.get('subject', '').lower()
        body = email_data.get('body_text', '').lower()
        sender = email_data.get('from', '').lower()
        headers = email_data.get('headers', {})

        # 1. Búsqueda de palabras clave
        hits = []
        for word in SpamDetector.SPAM_KEYWORDS:
            if word in subject:
                hits.append(f"Palabra sospechosa en asunto: {word}")
                score += 0.4
            if word in body:
                hits.append(f"Palabra sospechosa en cuerpo: {word}")
                score += 0.2

        reasons.extend(hits)

        # 2. Análisis de remitente
        if any(char.isdigit() for char in sender.split('@')[0]) and len(sender.split('@')[0]) > 10:
            reasons.append("Remitente con patrón alfanumérico sospechoso")
            score += 0.3

        # 3. Ratio HTML/Texto
        html_len = len(email_data.get('body_html', ''))
        text_len = len(email_data.get('body_text', ''))
        if html_len > 5000 and text_len < 100:
            reasons.append("Ratio HTML/Texto excesivamente alto")
            score += 0.3

        # 4. Cabeceras SPF/DKIM (vía Authentication-Results o Received-SPF)
        auth_results = headers.get('Authentication-Results', '').lower()
        received_spf = headers.get('Received-SPF', '').lower()

        if 'spf=fail' in auth_results or 'spf=fail' in received_spf:
            reasons.append("Fallo en validación SPF")
            score += 0.5
        elif 'spf=softfail' in auth_results:
            reasons.append("Softfail en validación SPF")
            score += 0.2

        if 'dkim=fail' in auth_results:
            reasons.append("Fallo en validación DKIM")
            score += 0.5

        # Normalizar score a 1.0 máx
        final_score = min(score, 1.0)

        if final_score >= 0.7:
            level = "spam"
        elif final_score >= 0.3:
            level = "suspicious"
        else:
            level = "safe"

        return {
            'score': round(final_score, 2),
            'level': level,
            'reasons': list(set(reasons))
        }

    @staticmethod
    def strip_tags(html):
        """Limpia tags HTML para mostrar texto legible"""
        if not html:
            return ""
        # Eliminar scripts y estilos
        html = re.sub(r'<(script|style)[^>]*>.*?</\1>', '', html, flags=re.DOTALL | re.IGNORECASE)
        # Reemplazar <br> y </p> con saltos de línea
        html = re.sub(r'<br\s*/?>', '\n', html, flags=re.IGNORECASE)
        html = re.sub(r'</p>', '\n\n', html, flags=re.IGNORECASE)
        # Eliminar el resto de tags
        text = re.sub(r'<[^>]+>', '', html)
        # Decodificar entidades básicas
        text = text.replace('&nbsp;', ' ').replace('&lt;', '<').replace('&gt;', '>').replace('&amp;', '&')
        # Normalizar saltos de línea
        text = re.sub(r'\n\s*\n', '\n\n', text)
        return text.strip()
