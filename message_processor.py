# ==============================================================================
# 1. message_processor.py - Core message processing logic
# ==============================================================================

from typing import List, Dict, Set
from collections import defaultdict
import time
from concurrent.futures import ThreadPoolExecutor

from utils import enrich_message


class MessageProcessor:
    """Handles core message processing logic"""

    def __init__(self, config, db, vector_store, analyzer, session):
        self.config = config
        self.db = db
        self.vector_store = vector_store
        self.analyzer = analyzer
        self.session = session

        self.streamer_username = "niaghtmares"


    def process_messages_batch(self, messages: List[Dict]) -> List[Dict]:
        """Process messages in batches and return alerts"""
        alerts = []
        alert_ids = set()
        processed_count = 0

        print(f"\n🔄 Procesando en lotes de {self.config.BATCH_SIZE} mensajes...")

        for i in range(0, len(messages), self.config.BATCH_SIZE):
            batch = messages[i:i + self.config.BATCH_SIZE]
            batch_num = i // self.config.BATCH_SIZE + 1
            total_batches = (len(messages) - 1) // self.config.BATCH_SIZE + 1

            print(f"📦 Procesando lote {batch_num}/{total_batches} ({len(batch)} mensajes)")

            batch_start = time.time()
            batch_alerts = self._process_single_batch(batch, alert_ids)
            batch_time = time.time() - batch_start

            alerts.extend(batch_alerts)
            processed_count += len(batch)

            # Progress reporting
            if batch_num % 5 == 0 or batch_num == total_batches:
                rate = len(batch) / batch_time if batch_time > 0 else 0
                print(f"   ⚡ Lote {batch_num} completado en {batch_time:.2f}s ({rate:.1f} msg/s)")
                print(
                    f"   📊 Progreso total: {processed_count}/{len(messages)} ({processed_count / len(messages) * 100:.1f}%)")

        return alerts

    def _process_single_batch(self, batch: List[Dict], alert_ids: Set[str]) -> List[Dict]:
        """Process a single batch of messages"""
        batch_alerts = []

        for message in batch:
            try:
                message = enrich_message(message)
                message_id = message['message_id']

                if not message.get("username"):
                    print(f"⚠️ Mensaje sin username detectado: {message}")
                    continue

                # Analyze message
                analysis = self.analyzer.analyze_message(message)
                analysis["message_id"] = message_id

                # Get embedding
                embedding = self.analyzer.get_embedding(message['text'])

                if embedding:
                    # Save to both stores
                    point_id = self._save_with_verification(message, analysis, embedding)

                    # Check for alerts
                    if (analysis['requires_action'] and
                            analysis['message_id'] not in alert_ids and
                            message['username'].lower() != self.streamer_username):
                        alert = self._create_alert(message, analysis)
                        batch_alerts.append(alert)
                        alert_ids.add(analysis['message_id'])

            except Exception as e:
                print(f"❌ Error procesando mensaje de {message.get('username', 'unknown')}: {e}")
                continue

        return batch_alerts

    def _save_with_verification(self, message: Dict, analysis: Dict, embedding: List[float]) -> str:
        """Save to both databases with consistency verification"""
        try:
            point_id = self.vector_store.add_message(message, analysis, embedding)

            if point_id:
                postgres_success = self.db.save_analysis(message, analysis, point_id)
                if not postgres_success:
                    print(f"⚠️ Inconsistencia: Guardado en Qdrant pero falló PostgreSQL")
                return point_id

            return ""
        except Exception as e:
            print(f"❌ Error en guardado verificado: {e}")
            return ""

    def _create_alert(self, message: Dict, analysis: Dict) -> Dict:
        """Create a standardized alert object"""
        return {
            'message_id': analysis['message_id'],
            'username': message['username'],
            'text': message['text'],
            'timestamp': message['timestamp_str'],
            'toxicity': analysis['toxicity_score'],
            'spam_probability': analysis['spam_probability'],
            'action': analysis['action_type'],
            'reason': analysis['reasoning'],
            'categories': analysis.get('categories', []),
            'severity': self._calculate_severity(analysis)
        }

    def _calculate_severity(self, analysis: Dict) -> str:
        """Calculate alert severity based on multiple factors"""
        toxicity = analysis.get('toxicity_score', 0)
        spam = analysis.get('spam_probability', 0)

        if toxicity >= 0.8 or spam >= 0.9:
            return 'CRITICAL'
        elif toxicity >= 0.6 or spam >= 0.7:
            return 'HIGH'
        elif toxicity >= 0.4 or spam >= 0.5:
            return 'MEDIUM'
        else:
            return 'LOW'