import requests
import json
import re
from typing import Dict, List, Optional
import time
from datetime import datetime

from config import Config
from utils import generate_message_id


class OllamaAnalyzer:
    def __init__(self, config: Config):
        self.config = config
        self.session = requests.Session()
        self.session.timeout = 30
        self._test_connection()

        # Enhanced JSON parsing patterns
        self.json_patterns = [
            r'\{.*?\}',  # Basic JSON object
            r'```json\s*(\{.*?\})\s*```',  # JSON in code blocks
            r'```\s*(\{.*?\})\s*```',  # JSON in generic code blocks
        ]

    def _test_connection(self):
        """Verifica que Ollama esté funcionando"""
        try:
            response = self.session.get(f"{self.config.OLLAMA_URL}/api/tags")
            if response.status_code == 200:
                models = response.json().get('models', [])
                available_models = [m['name'] for m in models]
                print(f"✅ Ollama conectado. Modelos disponibles: {available_models}")

                # Verify required models exist
                if self.config.OLLAMA_MODEL not in available_models:
                    print(f"⚠️ Modelo {self.config.OLLAMA_MODEL} no encontrado")
                if hasattr(self.config, 'EMBEDDING_MODEL') and self.config.EMBEDDING_MODEL not in available_models:
                    print(f"⚠️ Modelo de embedding {self.config.EMBEDDING_MODEL} no encontrado")
            else:
                print(f"⚠️ Ollama responde pero con error: {response.status_code}")
        except Exception as e:
            print(f"❌ Error conectando a Ollama: {e}")

    def analyze_message(self, message: Dict) -> Dict:
        """Analiza un mensaje individual con manejo mejorado de errores"""
        text = message.get('text', '')
        username = message.get('username', 'unknown')

        # Skip empty messages
        if not text.strip():
            return self._default_analysis(message, "Mensaje vacío")

        # Enhanced prompt with stricter JSON format requirements
        prompt = self._create_analysis_prompt(username, text)

        try:
            # Make request to Ollama
            response = self._make_ollama_request(prompt)

            if response:
                analysis = self._parse_analysis_result(response, message)

                # Special handling for streamer messages
                if username.lower() == "niaghtmares":
                    analysis = self._enhance_streamer_analysis(analysis)

                return analysis
            else:
                return self._default_analysis(message, "Error en respuesta de Ollama")

        except Exception as e:
            print(f"❌ Error analizando mensaje de {username}: {e}")
            return self._default_analysis(message, f"Excepción: {str(e)}")

    def _create_analysis_prompt(self, username: str, text: str) -> str:
        """Crea un prompt optimizado para obtener JSON válido"""
        return f"""Analiza este mensaje de chat de Twitch como moderador experto.

Usuario: {username}
Mensaje: "{text}"

Responde ÚNICAMENTE con un objeto JSON válido (sin texto adicional):

{{
    "toxicity_score": 0.0,
    "spam_probability": 0.0,
    "sentiment": "neutral",
    "categories": [],
    "requires_action": false,
    "action_type": "ignore",
    "reasoning": "Análisis del mensaje",
    "keywords_detected": []
}}

Criterios:
- toxicity_score: 0.0-1.0 (0=inocuo, 1=muy tóxico)
- spam_probability: 0.0-1.0 (0=normal, 1=spam definitivo)
- sentiment: "positive", "neutral", o "negative"
- categories: array de ["spam", "hate", "harassment", "caps", "repetitive", "off_topic"]
- requires_action: true si necesita moderación
- action_type: "ignore", "warn", "timeout", o "ban"
- reasoning: explicación breve en español
- keywords_detected: palabras problemáticas encontradas

IMPORTANTE: Responde solo el JSON, sin texto adicional."""

    def _make_ollama_request(self, prompt: str) -> Optional[str]:
        """Hace petición a Ollama con reintentos"""
        max_retries = 3

        for attempt in range(max_retries):
            try:
                response = self.session.post(
                    f"{self.config.OLLAMA_URL}/api/generate",
                    json={
                        "model": self.config.OLLAMA_MODEL,
                        "prompt": prompt,
                        "stream": False,
                        "options": {
                            "temperature": 0.1,
                            "top_p": 0.9,
                            "num_predict": 300,
                            "stop": ["\n\n", "```"]  # Stop at double newline or code block end
                        }
                    },
                    timeout=30
                )

                if response.status_code == 200:
                    result = response.json().get("response", "")
                    return result.strip()
                else:
                    print(f"❌ Ollama error {response.status_code} (intento {attempt + 1})")

            except requests.exceptions.Timeout:
                print(f"⏱️ Timeout en Ollama (intento {attempt + 1})")
            except Exception as e:
                print(f"❌ Error en petición Ollama: {e} (intento {attempt + 1})")

            if attempt < max_retries - 1:
                time.sleep(1)  # Wait before retry

        return None

    def _parse_analysis_result(self, result: str, original_message: Dict) -> Dict:
        """Parsea el resultado JSON de Ollama con múltiples estrategias"""

        # Strategy 1: Direct JSON parsing
        cleaned_result = self._clean_json_response(result)
        analysis = self._try_json_parse(cleaned_result)

        if analysis:
            return self._finalize_analysis(analysis, original_message)

        # Strategy 2: Extract JSON using regex patterns
        for pattern in self.json_patterns:
            matches = re.findall(pattern, result, re.DOTALL | re.MULTILINE)
            for match in matches:
                if isinstance(match, tuple):
                    match = match[0] if match else ""

                analysis = self._try_json_parse(match)
                if analysis:
                    return self._finalize_analysis(analysis, original_message)

        # Strategy 3: Try to reconstruct JSON from partial response
        analysis = self._reconstruct_json_from_text(result)
        if analysis:
            return self._finalize_analysis(analysis, original_message)

        # Fallback to default analysis
        print(f"⚠️ No se pudo parsear respuesta de Ollama: {result[:150]}...")
        return self._default_analysis(original_message, "Error de parsing JSON")

    def _clean_json_response(self, response: str) -> str:
        """Limpia la respuesta para extraer JSON"""
        # Remove common prefixes/suffixes
        response = response.strip()

        # Remove markdown code blocks
        if response.startswith('```json'):
            response = response.replace('```json', '').replace('```', '')
        elif response.startswith('```'):
            response = response.replace('```', '')

        # Remove any leading/trailing text that's not part of JSON
        json_start = response.find('{')
        json_end = response.rfind('}')

        if json_start != -1 and json_end != -1 and json_end > json_start:
            response = response[json_start:json_end + 1]

        return response.strip()

    def _try_json_parse(self, json_str: str) -> Optional[Dict]:
        """Intenta parsear JSON con manejo de errores"""
        if not json_str or not json_str.strip():
            return None

        try:
            return json.loads(json_str)
        except json.JSONDecodeError as e:
            # Try to fix common JSON issues
            fixed_json = self._fix_common_json_issues(json_str)
            if fixed_json != json_str:
                try:
                    return json.loads(fixed_json)
                except json.JSONDecodeError:
                    pass
            return None

    def _fix_common_json_issues(self, json_str: str) -> str:
        """Intenta corregir problemas comunes en JSON"""
        # Fix trailing commas
        json_str = re.sub(r',(\s*[}\]])', r'\1', json_str)

        # Fix unquoted keys (simple cases)
        json_str = re.sub(r'(\w+):', r'"\1":', json_str)

        # Fix single quotes to double quotes
        json_str = json_str.replace("'", '"')

        # Fix boolean values
        json_str = json_str.replace('True', 'true').replace('False', 'false')

        return json_str

    def _reconstruct_json_from_text(self, text: str) -> Optional[Dict]:
        """Intenta reconstruir JSON desde texto plano"""
        try:
            # Initialize default values
            analysis = {
                'toxicity_score': 0.0,
                'spam_probability': 0.0,
                'sentiment': 'neutral',
                'categories': [],
                'requires_action': False,
                'action_type': 'ignore',
                'reasoning': 'Análisis reconstruido desde texto',
                'keywords_detected': []
            }

            # Extract numerical values
            toxicity_match = re.search(r'toxicity[_\s]*score["\s]*:\s*([0-9.]+)', text, re.IGNORECASE)
            if toxicity_match:
                analysis['toxicity_score'] = float(toxicity_match.group(1))

            spam_match = re.search(r'spam[_\s]*probability["\s]*:\s*([0-9.]+)', text, re.IGNORECASE)
            if spam_match:
                analysis['spam_probability'] = float(spam_match.group(1))

            # Extract sentiment
            sentiment_match = re.search(r'sentiment["\s]*:\s*["\']?(\w+)["\']?', text, re.IGNORECASE)
            if sentiment_match:
                sentiment = sentiment_match.group(1).lower()
                if sentiment in ['positive', 'negative', 'neutral']:
                    analysis['sentiment'] = sentiment

            # Extract boolean values
            action_match = re.search(r'requires[_\s]*action["\s]*:\s*(true|false)', text, re.IGNORECASE)
            if action_match:
                analysis['requires_action'] = action_match.group(1).lower() == 'true'

            return analysis

        except Exception:
            return None

    def _finalize_analysis(self, analysis: Dict, original_message: Dict) -> Dict:
        """Finaliza y valida el análisis"""
        # Ensure all required fields exist with proper types
        analysis.setdefault('toxicity_score', 0.0)
        analysis.setdefault('spam_probability', 0.0)
        analysis.setdefault('sentiment', 'neutral')
        analysis.setdefault('categories', [])
        analysis.setdefault('requires_action', False)
        analysis.setdefault('action_type', 'ignore')
        analysis.setdefault('reasoning', 'Sin análisis específico')
        analysis.setdefault('keywords_detected', [])

        # Validate and clamp numerical values
        analysis['toxicity_score'] = max(0.0, min(1.0, float(analysis['toxicity_score'])))
        analysis['spam_probability'] = max(0.0, min(1.0, float(analysis['spam_probability'])))

        # Validate sentiment
        if analysis['sentiment'] not in ['positive', 'neutral', 'negative']:
            analysis['sentiment'] = 'neutral'

        # Validate action type
        valid_actions = ['ignore', 'warn', 'timeout', 'ban']
        if analysis['action_type'] not in valid_actions:
            analysis['action_type'] = 'ignore'

        # Ensure categories is a list
        if not isinstance(analysis['categories'], list):
            analysis['categories'] = []

        # Add metadata
        analysis['message_id'] = generate_message_id(original_message)
        analysis['analyzed_at'] = datetime.now().isoformat()
        analysis['model_used'] = self.config.OLLAMA_MODEL

        return analysis

    def _enhance_streamer_analysis(self, analysis: Dict) -> Dict:
        """Mejora el análisis para mensajes del streamer"""
        analysis["requires_action"] = True
        analysis["action_type"] = "protect"
        analysis["reasoning"] += " | Mensaje del streamer: priorizar visibilidad y registro completo."
        return analysis

    def _default_analysis(self, message: Dict, reason: str = "Error desconocido") -> Dict:
        """Análisis por defecto cuando falla Ollama"""
        return {
            'toxicity_score': 0.0,
            'spam_probability': 0.0,
            'sentiment': 'neutral',
            'categories': [],
            'requires_action': False,
            'action_type': 'ignore',
            'reasoning': f'Análisis automático falló: {reason}',
            'keywords_detected': [],
            'message_id': generate_message_id(message),
            'analyzed_at': datetime.now().isoformat(),
            'model_used': 'fallback'
        }

    def get_embedding(self, text: str) -> Optional[List[float]]:
        """Obtiene embedding de un texto usando Ollama con reintentos"""
        if not hasattr(self.config, 'EMBEDDING_MODEL'):
            print("⚠️ Modelo de embedding no configurado")
            return None

        max_retries = 2

        for attempt in range(max_retries):
            try:
                response = self.session.post(
                    f"{self.config.OLLAMA_URL}/api/embeddings",
                    json={
                        "model": self.config.EMBEDDING_MODEL,
                        "prompt": text[:1000]  # Limit text length
                    },
                    timeout=20
                )

                if response.status_code == 200:
                    embedding = response.json().get("embedding")
                    if embedding and isinstance(embedding, list):
                        return embedding
                    else:
                        print(f"❌ Embedding response inválido (intento {attempt + 1})")
                else:
                    print(f"❌ Error obteniendo embedding: {response.status_code} (intento {attempt + 1})")

            except requests.exceptions.Timeout:
                print(f"⏱️ Timeout obteniendo embedding (intento {attempt + 1})")
            except Exception as e:
                print(f"❌ Error en embedding: {e} (intento {attempt + 1})")

            if attempt < max_retries - 1:
                time.sleep(0.5)

        return None

    def generate_soothing_message(self, context_summary: str, tension_level: int) -> str:
        """Genera un mensaje tranquilizador adaptado al contexto del chat"""
        prompt = f"""Eres NIA_BOT, un moderador virtual de Twitch.

Contexto del stream: {context_summary}
Nivel de tensión: {tension_level}/5

Genera un mensaje calmado y natural que:
- Invite a la paz y buen rollo
- Sea informal pero respetuoso
- No parezca generado por bot
- Máximo 2 frases

Responde solo el mensaje sin comillas ni explicaciones."""

        try:
            response = self._make_ollama_request(prompt)
            if response:
                # Clean up the response
                message = response.strip().strip('"').strip("'")
                return message
            else:
                return "¡Mantengamos el buen rollo en el chat! 😊"

        except Exception as e:
            print(f"❌ Error generando mensaje tranquilizador: {e}")
            return "¡Vamos a mantener la buena vibra! ✨"

    def get_health_status(self) -> Dict:
        """Obtiene el estado de salud del analizador"""
        try:
            start_time = time.time()
            response = self.session.get(f"{self.config.OLLAMA_URL}/api/tags", timeout=5)
            response_time = time.time() - start_time

            if response.status_code == 200:
                models = response.json().get('models', [])
                return {
                    'status': 'healthy',
                    'response_time_ms': int(response_time * 1000),
                    'available_models': len(models),
                    'target_model_available': self.config.OLLAMA_MODEL in [m['name'] for m in models]
                }
            else:
                return {
                    'status': 'error',
                    'error': f'HTTP {response.status_code}',
                    'response_time_ms': int(response_time * 1000)
                }

        except Exception as e:
            return {
                'status': 'error',
                'error': str(e),
                'response_time_ms': -1
            }