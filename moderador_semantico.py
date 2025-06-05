# moderador_semantico.py - REBUILT: Modular orchestration

from pathlib import Path
from datetime import datetime
import time

from config import config
from database_manager import DatabaseManager
from deep_analysis_generator import DeepAnalysisGenerator
from vector_store_qdrant import QdrantVectorStore
from chatterino_parser import ChatterinoParser
from analyzer_ollama import OllamaAnalyzer

from message_processor import MessageProcessor
from deduplication_manager import DeduplicationManager
from alert_manager import AlertManager
from analytics_engine import AnalyticsEngine
from session_tracker import SessionTracker

from deep_analysis_generator import integrate_deep_analysis_to_moderador


class ModeradorSemantico:

    def __init__(self, file_path=None):
        self.config = config
        self.db = DatabaseManager(config)
        self.vector_store = QdrantVectorStore(config, file_path=file_path)
        self.parser = ChatterinoParser()
        self.analyzer = OllamaAnalyzer(config)

        self.session = SessionTracker()
        self.processor = MessageProcessor(config, self.db, self.vector_store, self.analyzer, self.session)
        self.deduplicator = DeduplicationManager(self.db, self.vector_store)
        self.alerts = AlertManager(config)
        self.analytics = AnalyticsEngine(self.db, self.analyzer)

        if file_path:
            self.vector_store.set_collection_for_file(file_path)


    def process_log_file(self, filepath: str):
        self.session.start_session()

        print(f"\n🟢 Iniciando escaneo: {filepath}")
        start_time = datetime.now()

        messages = self.parser.parse_file(filepath)
        if not messages:
            print("❌ No se encontraron mensajes")
            return

        print(f"📊 Total de mensajes parseados: {len(messages)}")

        new_messages = self.deduplicator.filter_new_messages(messages)
        self.session.update_stats(duplicate_count=len(messages) - len(new_messages))

        if not new_messages:
            print("✅ Todos los mensajes ya estaban procesados")
            self.session.finish(start_time)
            return

        print(f"🔄 Procesando {len(new_messages)} nuevos mensajes...")

        alerts = self.processor.process_messages_batch(new_messages)
        if alerts:
            self.alerts.save_alerts(alerts, filepath)

        self.analytics.run_advanced_analysis(messages)

        # ✅ INTEGRACIÓN DE ANÁLISIS SEMÁNTICO CON RAG + IA
        integrate_deep_analysis_to_moderador(
            self,
            messages,
            self.vector_store,
            self.analyzer,
            self.config.OUTPUT_PATH,
            Path(filepath).name
        )

        elapsed_time = (datetime.now() - start_time).total_seconds()
        self.session.print_final_summary(elapsed_time, db=self.db, vector_store=self.vector_store)

        # 🔁 Si la base vectorial está vacía pero hay mensajes en PostgreSQL, restaurarla
        if self.vector_store.count_points() == 0:
            print("⚠️ Qdrant está vacía. Intentando restaurar desde PostgreSQL...")
            self.restore_vector_store(filepath)

    def restore_vector_store(self, filepath: str):
        file_source = Path(filepath).name
        all_msgs = self.db.get_messages_by_file_source(file_source)

        if not all_msgs:
            print("❌ No se encontraron mensajes en PostgreSQL para restaurar")
            return

        restored = 0
        for row in all_msgs:
            message_id = row["message_id"]
            text = row["message_text"]

            if not text:
                continue

            point_id = self.vector_store.upsert_message({
                "id": message_id,
                "text": text,
                "username": row.get("username", "unknown"),
                "timestamp": row.get("timestamp"),
                "file_source": file_source
            })

            # Opcionalmente actualizar el ID de punto en PostgreSQL si quieres sincronía total
            # self.db.update_qdrant_point_id(message_id, point_id)

            restored += 1

        print(f"✅ Restaurados {restored} mensajes a Qdrant desde PostgreSQL")

    def analyze_single_message(self, text: str, username: str = "test_user"):
        raw_message = {
            'text': text,
            'username': username,
            'timestamp': datetime.now(),
            'file_source': 'manual'
        }

        return self.processor.analyze_single_message(raw_message)
