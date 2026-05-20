import re
import json
import os
import urllib.request
import urllib.parse
from pathlib import Path
import math

class SpamDetector:
    """Detector de SPAM avanzado con capas heurística, bayesiana y externa"""

    SPAM_KEYWORDS = [
        'oferta', 'ganado', 'premio', 'bitcoin', 'crypto', 'viagra', 'urgente',
        'cuenta bloqueada', 'herencia', 'loteria', 'beneficio', 'gratis',
        'invertir', 'casino', 'payout', 'jackpot', 'sex', 'dating',
        'promoción', 'descuento', 'clic aquí', 'gana dinero', 'trabaja desde casa'
    ]

    def __init__(self, config_path='pop3/config.json', feedback_path='email_backup/spam_feedback.json', model_path='pop3/bayes_model.json'):
        self.config_path = Path(config_path)
        self.feedback_path = Path(feedback_path)
        self.model_path = Path(model_path)

        self.config = self.load_config()
        self.feedback = self.load_feedback()
        self.model = self.load_model()

        self.layers_status = {
            'heuristic': True,
            'bayesian': False,
            'qdrant': False,
            'ollama': False
        }

        self.ollama_model = "llama3"
        self._check_external_services()
        self._check_bayesian_status()
        self._init_qdrant()

    def load_config(self):
        if self.config_path.exists():
            with open(self.config_path, 'r', encoding='utf-8') as f:
                return json.load(f)
        return {"white_list": [], "black_list": [], "bayesian_threshold": 200, "quarantine_enabled": True}

    def save_config(self):
        with open(self.config_path, 'w', encoding='utf-8') as f:
            json.dump(self.config, f, indent=2)

    def load_feedback(self):
        if self.feedback_path.exists():
            with open(self.feedback_path, 'r', encoding='utf-8') as f:
                return json.load(f)
        return {}

    def save_feedback(self):
        with open(self.feedback_path, 'w', encoding='utf-8') as f:
            json.dump(self.feedback, f, indent=2)

    def load_model(self):
        if self.model_path.exists():
            with open(self.model_path, 'r', encoding='utf-8') as f:
                return json.load(f)
        return {"spam_count": 0, "not_spam_count": 0, "spam_words": {}, "not_spam_words": {}, "total_emails": 0}

    def save_model(self):
        with open(self.model_path, 'w', encoding='utf-8') as f:
            json.dump(self.model, f, indent=2)

    def _check_external_services(self):
        # Check Ollama
        try:
            with urllib.request.urlopen("http://localhost:11434/api/tags", timeout=1) as response:
                if response.status == 200:
                    data = json.loads(response.read().decode())
                    models = [m['name'] for m in data.get('models', [])]
                    if "llama3:latest" in models or "llama3" in models:
                        self.ollama_model = "llama3"
                        self.layers_status['ollama'] = True
                    elif "mistral:latest" in models or "mistral" in models:
                        self.ollama_model = "mistral"
                        self.layers_status['ollama'] = True
                    elif models:
                        self.ollama_model = models[0].split(':')[0]
                        self.layers_status['ollama'] = True
        except:
            self.layers_status['ollama'] = False

        # Check Qdrant
        try:
            import qdrant_client
            with urllib.request.urlopen("http://localhost:6333", timeout=1) as response:
                if response.status == 200:
                    self.layers_status['qdrant'] = True
        except:
            self.layers_status['qdrant'] = False

    def _init_qdrant(self):
        if self.layers_status['qdrant']:
            try:
                from qdrant_client import QdrantClient
                from qdrant_client.models import VectorParams, Distance
                client = QdrantClient("localhost", port=6333)
                collections = [c.name for c in client.get_collections().collections]
                if "emails" not in collections:
                    client.create_collection(
                        collection_name="emails",
                        vectors_config=VectorParams(size=4096, distance=Distance.COSINE) # llama3 default
                    )
            except:
                self.layers_status['qdrant'] = False

    def _check_bayesian_status(self):
        threshold = self.config.get('bayesian_threshold', 200)
        if self.model['spam_count'] >= threshold and self.model['not_spam_count'] >= threshold:
            self.layers_status['bayesian'] = True
        else:
            self.layers_status['bayesian'] = False

    def _tokenize(self, text):
        if not text: return []
        return re.findall(r'\w+', text.lower())

    def record_feedback(self, uid, action, email_data):
        import datetime
        self.feedback[uid] = {
            "action": action,
            "timestamp": datetime.datetime.now().isoformat(),
            "from": email_data.get('from', ''),
            "subject": email_data.get('subject', '')
        }
        self.save_feedback()

        # Update Bayesian model
        is_spam = (action in ['confirmed_spam', 'spam'])
        is_not_spam = (action in ['not_spam', 'false_positive'])

        if is_spam or is_not_spam:
            text = f"{email_data.get('subject', '')} {email_data.get('body_text', '')}"
            tokens = self._tokenize(text)

            if is_spam:
                self.model['spam_count'] += 1
                word_dict = self.model['spam_words']
            else:
                self.model['not_spam_count'] += 1
                word_dict = self.model['not_spam_words']

            for token in set(tokens): # Count word presence in email
                word_dict[token] = word_dict.get(token, 0) + 1

            self.model['total_emails'] += 1
            self.save_model()
            self._check_bayesian_status()

            # Update Qdrant if available
            if self.layers_status['qdrant']:
                try:
                    embedding = self._get_ollama_embedding(text[:500])
                    if embedding:
                        from qdrant_client import QdrantClient
                        from qdrant_client.models import PointStruct
                        client = QdrantClient("localhost", port=6333)
                        client.upsert(
                            collection_name="emails",
                            points=[PointStruct(
                                id=hashlib.md5(uid.encode()).hexdigest(),
                                vector=embedding,
                                payload={"uid": uid, "label": "spam" if is_spam else "safe"}
                            )]
                        )
                except:
                    pass

    def _analyze_heuristic(self, email_data):
        score = 0
        reasons = []
        subject = email_data.get('subject', '').lower()
        body = email_data.get('body_text', '').lower()
        sender = email_data.get('from', '').lower()
        headers = email_data.get('headers', {})

        hits = []
        for word in self.SPAM_KEYWORDS:
            if word in subject:
                hits.append(f"Palabra sospechosa en asunto: {word}")
                score += 0.4
            if word in body:
                hits.append(f"Palabra sospechosa en cuerpo: {word}")
                score += 0.2
        reasons.extend(hits)

        if any(char.isdigit() for char in sender.split('@')[0]) and len(sender.split('@')[0]) > 10:
            reasons.append("Remitente con patrón alfanumérico sospechoso")
            score += 0.3

        html_len = len(email_data.get('body_html', ''))
        text_len = len(email_data.get('body_text', ''))
        if html_len > 5000 and text_len < 100:
            reasons.append("Ratio HTML/Texto excesivamente alto")
            score += 0.3

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

        return min(score, 1.0), list(set(reasons))

    def _analyze_bayesian(self, email_data):
        if not self.layers_status['bayesian']:
            return 0.5

        text = f"{email_data.get('subject', '')} {email_data.get('body_text', '')}"
        tokens = self._tokenize(text)

        p_spam = self.model['spam_count'] / self.model['total_emails']
        p_not_spam = self.model['not_spam_count'] / self.model['total_emails']

        log_prob_spam = 0
        log_prob_not_spam = 0

        for token in set(tokens):
            if token in self.model['spam_words'] or token in self.model['not_spam_words']:
                count_spam = self.model['spam_words'].get(token, 0)
                count_not_spam = self.model['not_spam_words'].get(token, 0)
                prob_token_spam = (count_spam + 1) / (self.model['spam_count'] + 2)
                prob_token_not_spam = (count_not_spam + 1) / (self.model['not_spam_count'] + 2)
                log_prob_spam += math.log(prob_token_spam)
                log_prob_not_spam += math.log(prob_token_not_spam)

        final_log_spam = log_prob_spam + math.log(p_spam)
        final_log_not_spam = log_prob_not_spam + math.log(p_not_spam)

        try:
            diff = final_log_not_spam - final_log_spam
            if diff > 20: return 0.0
            if diff < -20: return 1.0
            return 1 / (1 + math.exp(diff))
        except:
            return 0.5

    def _analyze_qdrant(self, email_data):
        if not self.layers_status['qdrant']:
            return 0.5

        text = f"{email_data.get('subject', '')}\n{email_data.get('body_text', '')}"
        embedding = self._get_ollama_embedding(text[:500])
        if not embedding:
            return 0.5

        try:
            from qdrant_client import QdrantClient
            client = QdrantClient("localhost", port=6333)
            results = client.search(
                collection_name="emails",
                query_vector=embedding,
                limit=5
            )
            if not results:
                return 0.5

            spam_votes = 0
            for r in results:
                if r.payload.get('label') == 'spam':
                    spam_votes += r.score
                else:
                    spam_votes -= r.score

            # Normalize votes to 0-1 range
            # max votes is approx 5 (if all are spam with score 1.0)
            # min votes is approx -5
            normalized = (spam_votes + 5) / 10
            return min(max(normalized, 0.0), 1.0)
        except:
            return 0.5

    def _get_ollama_embedding(self, text):
        try:
            url = "http://localhost:11434/api/embeddings"
            payload = json.dumps({"model": self.ollama_model, "prompt": text}).encode()
            headers = {'Content-Type': 'application/json'}
            req = urllib.request.Request(url, data=payload, headers=headers)
            with urllib.request.urlopen(req, timeout=2) as response:
                return json.loads(response.read().decode()).get('embedding')
        except:
            return None

    def _analyze_ollama(self, email_data):
        if not self.layers_status['ollama']:
            return 0.5

        try:
            prompt = f"Analyze if this email is SPAM. Return ONLY a number between 0.0 (Safe) and 1.0 (Spam).\nSubject: {email_data.get('subject')}\nBody: {email_data.get('body_text', '')[:300]}"
            url = "http://localhost:11434/api/generate"
            payload = json.dumps({
                "model": self.ollama_model,
                "prompt": prompt,
                "stream": False,
                "options": {"temperature": 0}
            }).encode()
            headers = {'Content-Type': 'application/json'}
            req = urllib.request.Request(url, data=payload, headers=headers)
            with urllib.request.urlopen(req, timeout=2) as response:
                result = json.loads(response.read().decode()).get('response', '')
                match = re.search(r"(\d\.\d+)", result)
                if match:
                    return float(match.group(1))
        except:
            pass
        return 0.5

    def analyze(self, email_data):
        uid = email_data.get('uid')
        sender = email_data.get('from', '').lower()

        # 0. Listas Blanca/Negra (Prioridad absoluta)
        for ok in self.config.get('white_list', []):
            if ok.lower() in sender:
                return {'score': 0.0, 'level': 'safe', 'reasons': ['Remitente en lista blanca'], 'layers': {'white_list': True}}

        for bad in self.config.get('black_list', []):
            if bad.lower() in sender:
                return {'score': 1.0, 'level': 'spam', 'reasons': ['Remitente en lista negra'], 'layers': {'black_list': True}}

        # 0. Feedback previo
        if uid in self.feedback:
            action = self.feedback[uid].get('action')
            if action in ['not_spam', 'false_positive']:
                return {'score': 0.0, 'level': 'safe', 'reasons': ['Marcado manualmente como no spam'], 'layers': {'feedback': True}}
            elif action == 'confirmed_spam':
                return {'score': 1.0, 'level': 'spam', 'reasons': ['Confirmado como spam por el usuario'], 'layers': {'feedback': True}}

        # 1. Capas
        h_score, h_reasons = self._analyze_heuristic(email_data)

        scores = {'heuristic': h_score}
        weights = {'heuristic': 0.4, 'bayesian': 0.25, 'qdrant': 0.20, 'ollama': 0.15}

        if self.layers_status['bayesian']:
            scores['bayesian'] = self._analyze_bayesian(email_data)

        # Ollama/Qdrant activation in gray zone (0.3-0.7)
        if 0.3 <= h_score <= 0.7:
             if self.layers_status['ollama']:
                 scores['ollama'] = self._analyze_ollama(email_data)
             if self.layers_status['qdrant']:
                 scores['qdrant'] = self._analyze_qdrant(email_data)

        # Proportional weight redistribution
        active_layers = [l for l in weights if l in scores]
        total_active_weight = sum(weights[l] for l in active_layers)

        final_score = 0
        for l in active_layers:
            adjusted_weight = weights[l] / total_active_weight
            final_score += scores[l] * adjusted_weight

        final_score = round(min(max(final_score, 0.0), 1.0), 2)

        if final_score >= 0.7:
            level = "spam"
        elif final_score >= 0.3:
            level = "suspicious"
        else:
            level = "safe"

        return {
            'score': final_score,
            'level': level,
            'reasons': h_reasons,
            'layer_scores': scores,
            'active_layers': active_layers
        }

    @staticmethod
    def strip_tags(html):
        if not html: return ""
        html = re.sub(r'<(script|style)[^>]*>.*?</\1>', '', html, flags=re.DOTALL | re.IGNORECASE)
        html = re.sub(r'<br\s*/?>', '\n', html, flags=re.IGNORECASE)
        html = re.sub(r'</p>', '\n\n', html, flags=re.IGNORECASE)
        text = re.sub(r'<[^>]+>', '', html)
        text = text.replace('&nbsp;', ' ').replace('&lt;', '<').replace('&gt;', '>').replace('&amp;', '&')
        text = re.sub(r'\n\s*\n', '\n\n', text)
        return text.strip()

import hashlib
