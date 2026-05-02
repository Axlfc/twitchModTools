import json
from typing import List, Dict, Optional
from qdrant_client import QdrantClient
from analyzer_ollama import OllamaAnalyzer
from config import Config

class FallacyAnalyzer:
    def __init__(self, config: Config, ollama_analyzer: OllamaAnalyzer):
        self.config = config
        self.ollama = ollama_analyzer
        self.qdrant = QdrantClient(url=config.QDRANT_URL)
        self.collection_name = "fallacies_inventory"

    def retrieve_candidates(self, text: str, limit: int = 5) -> List[Dict]:
        """Retrieves the most relevant fallacies from the vector store."""
        embedding = self.ollama.get_embedding(text)
        if not embedding:
            return []

        try:
            results = self.qdrant.search(
                collection_name=self.collection_name,
                query_vector=embedding,
                limit=limit
            )
            return [hit.payload for hit in results]
        except Exception as e:
            # Silence expected errors if services are down in certain environments
            return []

    def analyze_fallacies(self, text: str) -> Dict:
        """
        Main entry point for fallacy detection.
        Ensures high precision by performing a two-pass LLM check.
        """
        candidates = self.retrieve_candidates(text)

        # Pass 1: Identification
        initial_result = self._llm_detection(text, candidates)

        if not initial_result.get("has_fallacy") or not initial_result.get("fallacies"):
            return initial_result

        # Pass 2: Strict Verification (for 99% precision)
        verified_fallacies = self.verify_fallacies(text, initial_result["fallacies"])

        # Update result based on verification
        initial_result["fallacies"] = verified_fallacies
        initial_result["has_fallacy"] = len(verified_fallacies) > 0

        return initial_result

    def _llm_detection(self, text: str, candidates: List[Dict]) -> Dict:
        """Reasoning step using Ollama."""

        candidates_str = ""
        for c in candidates:
            candidates_str += f"- {c['id']} ({c['name']}): {c['description']}. Ejemplo: {c['example']}\n"

        prompt = f"""Analiza si el siguiente mensaje contiene alguna falacia lógica basándote en el inventario.

Texto: "{text}"

Candidatos RAG:
{candidates_str if candidates_str else "Analiza según tu conocimiento general del inventario de 229 falacias."}

Responde ÚNICAMENTE con un JSON:
{{
    "has_fallacy": boolean,
    "fallacies": [
        {{
            "id": "FXXX",
            "name": "Nombre",
            "reasoning": "Por qué aplica",
            "severity": 1-3
        }}
    ]
}}"""

        try:
            response = self.ollama._make_ollama_request(prompt)
            if response:
                return self.ollama._parse_analysis_result(response, {"text": text})
            return {"has_fallacy": False, "fallacies": []}
        except Exception as e:
            return {"has_fallacy": False, "fallacies": [], "error": str(e)}

    def verify_fallacies(self, text: str, detected_fallacies: List[Dict]) -> List[Dict]:
        """Strict verification pass to discard false positives."""
        verified = []
        for fallacy in detected_fallacies:
            prompt = f"""EVALUADOR DE PRECISIÓN: Determina con un 99% de seguridad si esta falacia realmente ocurre.

TEXTO: "{text}"
FALACIA: {fallacy['name']} ({fallacy['id']})
RAZÓN DETECTADA: {fallacy['reasoning']}

Si la falacia es INDUDABLE, responde 'SÍ'.
Si hay mínima duda o es un argumento válido, responde 'NO'.
Responde solo 'SÍ' o 'NO'."""

            try:
                response = self.ollama._make_ollama_request(prompt)
                if response and "SÍ" in response.upper() and "NO" not in response.upper():
                    verified.append(fallacy)
            except:
                pass # Discard on error for safety

        return verified

    def update_analysis_dict(self, text: str, analysis: Dict) -> Dict:
        """Helper to update a message analysis dictionary with fallacy data."""
        fallacy_analysis = self.analyze_fallacies(text)
        analysis["fallacies"] = fallacy_analysis.get("fallacies", [])

        if analysis["fallacies"]:
            analysis["requires_action"] = True
            fallacy_names = ', '.join([f['name'] for f in analysis['fallacies']])
            if "reasoning" in analysis:
                analysis["reasoning"] += f" | Falacias: {fallacy_names}"
            else:
                analysis["reasoning"] = f"Falacias: {fallacy_names}"

        return analysis
