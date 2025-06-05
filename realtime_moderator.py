# ==============================================================================
# realtime_moderator.py - Sistema de Moderación en Tiempo Real Optimizado
# ==============================================================================

import argparse
import time
import threading
from pathlib import Path
from datetime import datetime, timedelta
from typing import List, Dict, Set, Optional
from collections import defaultdict, deque
import json
import hashlib
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

from analyzer_ollama import OllamaAnalyzer
from chatterino_parser import ChatterinoParser
from database_manager import DatabaseManager
from vector_store_qdrant import QdrantVectorStore
from config import config


class LogFileWatcher(FileSystemEventHandler):
    """Watcher para detectar cambios en archivos de log en tiempo real"""

    def __init__(self, moderator):
        self.moderator = moderator
        self.last_positions = {}  # Trackeo de posiciones de archivos

    def on_modified(self, event):
        if event.is_directory or not event.src_path.endswith('.log'):
            return

        filepath = event.src_path
        print(f"📝 Detectado cambio en: {Path(filepath).name}")
        self.moderator.process_new_lines(filepath)


class OptimizedModeradorSemantico:
    def __init__(self):
        self.config = config
        self.parser = ChatterinoParser()
        self.analyzer = OllamaAnalyzer(self.config)
        self.vector_store = QdrantVectorStore(self.config)
        self.db = DatabaseManager(self.config)
        self.streamer_username = "niaghtmares"

        # Cache para optimización
        self.processed_message_cache = set()  # IDs ya procesados
        self.file_positions = {}  # Posiciones de lectura por archivo
        self.message_hashes = set()  # Hashes de mensajes para dedup rápida

        # Sistema de alertas en tiempo real
        self.active_alerts = deque(maxlen=100)
        self.alert_callbacks = []

        # Métricas de rendimiento
        self.stats = {
            'messages_processed': 0,
            'messages_skipped': 0,
            'alerts_generated': 0,
            'processing_time': 0
        }

        self._load_existing_message_cache()

    def _load_existing_message_cache(self):
        """Carga cache de mensajes ya procesados para optimizar arranque"""
        print("🔄 Cargando cache de mensajes existentes...")
        try:
            # Cargar últimos N message_ids de la BD para evitar reprocesamiento
            with self.db.get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute("""
                        SELECT message_id FROM moderation_logs 
                        WHERE analyzed_at > %s
                        ORDER BY analyzed_at DESC
                    """, (datetime.now() - timedelta(hours=24),))

                    self.processed_message_cache = set([row[0] for row in cur.fetchall()])

            print(f"✅ Cache cargado: {len(self.processed_message_cache)} mensajes recientes")
        except Exception as e:
            print(f"⚠️ Error cargando cache: {e}")

    def _quick_message_check(self, message: Dict) -> bool:
        """Check rápido si un mensaje ya fue procesado sin ir a BD"""
        message_id = f"{message['username']}_{message['timestamp'].isoformat()}"

        # Check 1: Cache en memoria
        if message_id in self.processed_message_cache:
            return True

        # Check 2: Hash del contenido para detectar duplicados exactos
        content_hash = hashlib.md5(
            f"{message['username']}_{message['text']}_{message['timestamp_str']}".encode()
        ).hexdigest()

        if content_hash in self.message_hashes:
            return True

        # Si es nuevo, agregarlo al cache
        self.message_hashes.add(content_hash)
        return False

    def process_new_lines(self, filepath: str):
        """Procesa solo las líneas nuevas de un archivo (tiempo real)"""
        start_time = time.time()

        if not Path(filepath).exists():
            return

        # Obtener posición actual del archivo
        current_pos = self.file_positions.get(filepath, 0)

        try:
            with open(filepath, 'r', encoding='utf-8', errors='ignore') as f:
                f.seek(current_pos)
                new_lines = f.readlines()
                self.file_positions[filepath] = f.tell()

            if not new_lines:
                return

            print(f"📊 Procesando {len(new_lines)} líneas nuevas de {Path(filepath).name}")

            # Parsear solo las líneas nuevas
            new_messages = []
            for line in new_lines:
                line = line.strip()
                if line:
                    parsed = self.parser._parse_line(line)
                    if parsed:
                        parsed['file_source'] = Path(filepath).name
                        new_messages.append(parsed)

            if new_messages:
                self._process_message_batch_optimized(new_messages)

        except Exception as e:
            print(f"❌ Error procesando líneas nuevas: {e}")

        elapsed = time.time() - start_time
        self.stats['processing_time'] += elapsed

    def _process_message_batch_optimized(self, messages: List[Dict]):
        """Procesa lote de mensajes con optimizaciones inteligentes"""
        if not messages:
            return

        # Pre-filtrado rápido: eliminar mensajes ya procesados
        unprocessed_messages = []
        for msg in messages:
            if not self._quick_message_check(msg):
                unprocessed_messages.append(msg)
            else:
                self.stats['messages_skipped'] += 1

        if not unprocessed_messages:
            print(f"⚡ Todos los mensajes ya estaban procesados - omitidos {len(messages)}")
            return

        print(f"🚀 Procesando {len(unprocessed_messages)}/{len(messages)} mensajes nuevos")

        # Verificación final contra BD solo para mensajes no filtrados
        existing_ids = self.db.get_existing_message_ids(unprocessed_messages)

        final_messages = []
        for msg in unprocessed_messages:
            message_id = f"{msg['username']}_{msg['timestamp'].isoformat()}"
            if message_id not in existing_ids:
                final_messages.append(msg)

        if not final_messages:
            print(f"⚡ Verificación final: todos ya estaban en BD")
            return

        # Procesar solo mensajes realmente nuevos
        alerts = []
        for message in final_messages:
            try:
                message_id = f"{message['username']}_{message['timestamp'].isoformat()}"

                # Análisis con IA
                analysis = self.analyzer.analyze_message(message)
                embedding = self.analyzer.get_embedding(message['text'])

                if embedding:
                    point_id = self.vector_store.add_message(message, analysis, embedding)
                    self.db.save_analysis(message, analysis, point_id)

                    # Agregar a cache para futuras verificaciones
                    self.processed_message_cache.add(message_id)

                    # Sistema de alertas en tiempo real
                    if analysis['requires_action'] and message['username'].lower() != self.streamer_username:
                        alert = self._create_alert(message, analysis)
                        alerts.append(alert)
                        self.active_alerts.append(alert)
                        self._trigger_alert_callbacks(alert)

                    self.stats['messages_processed'] += 1

            except Exception as e:
                print(f"❌ Error procesando {message.get('username', 'unknown')}: {e}")

        if alerts:
            self.stats['alerts_generated'] += len(alerts)
            self._handle_realtime_alerts(alerts)

    def _create_alert(self, message: Dict, analysis: Dict) -> Dict:
        """Crea alerta estructurada"""
        return {
            'id': hashlib.md5(f"{message['username']}_{message['timestamp']}".encode()).hexdigest()[:8],
            'timestamp': datetime.now(),
            'username': message['username'],
            'text': message['text'],
            'toxicity': analysis['toxicity_score'],
            'spam_prob': analysis['spam_probability'],
            'sentiment': analysis['sentiment'],
            'action': analysis['action_type'],
            'reason': analysis['reasoning'],
            'categories': analysis['categories'],
            'priority': self._calculate_priority(analysis)
        }

    def _calculate_priority(self, analysis: Dict) -> str:
        """Calcula prioridad de la alerta"""
        toxicity = analysis['toxicity_score']
        spam = analysis['spam_probability']

        if toxicity > 0.8 or spam > 0.9:
            return "CRITICAL"
        elif toxicity > 0.6 or spam > 0.7:
            return "HIGH"
        elif toxicity > 0.4 or spam > 0.5:
            return "MEDIUM"
        else:
            return "LOW"

    def _handle_realtime_alerts(self, alerts: List[Dict]):
        """Maneja alertas en tiempo real"""
        critical_alerts = [a for a in alerts if a['priority'] == 'CRITICAL']

        if critical_alerts:
            print(f"\n🚨 ALERTA CRÍTICA - {len(critical_alerts)} mensajes requieren atención inmediata:")
            for alert in critical_alerts:
                print(f"  ⚠️ @{alert['username']}: {alert['text'][:60]}...")
                print(f"     💀 Toxicidad: {alert['toxicity']:.2f} | Acción: {alert['action']}")

        # Aquí puedes agregar webhooks, notificaciones push, etc.
        self._send_webhook_alert(critical_alerts)

    def _send_webhook_alert(self, alerts: List[Dict]):
        """Envía alertas via webhook (implementar según necesidades)"""
        if not alerts or not self.config.WEBHOOK_URL:
            return

        try:
            import requests
            payload = {
                'type': 'moderation_alert',
                'timestamp': datetime.now().isoformat(),
                'alerts': alerts,
                'stream': self.streamer_username
            }

            response = requests.post(self.config.WEBHOOK_URL, json=payload, timeout=5)
            if response.status_code == 200:
                print(f"📡 Webhook enviado exitosamente")
            else:
                print(f"⚠️ Error enviando webhook: {response.status_code}")

        except Exception as e:
            print(f"❌ Error con webhook: {e}")

    def _trigger_alert_callbacks(self, alert: Dict):
        """Ejecuta callbacks registrados para alertas"""
        for callback in self.alert_callbacks:
            try:
                callback(alert)
            except Exception as e:
                print(f"❌ Error en callback de alerta: {e}")

    def register_alert_callback(self, callback):
        """Registra callback para alertas en tiempo real"""
        self.alert_callbacks.append(callback)

    def start_realtime_monitoring(self, logs_directory: str):
        """Inicia monitoreo en tiempo real"""
        logs_path = Path(logs_directory)
        if not logs_path.exists():
            print(f"❌ Directorio no existe: {logs_path}")
            return

        print(f"🔍 Iniciando monitoreo en tiempo real: {logs_path}")

        # Procesar archivos existentes primero (modo catch-up inteligente)
        self._initial_catchup(logs_path)

        # Configurar watcher
        event_handler = LogFileWatcher(self)
        observer = Observer()
        observer.schedule(event_handler, str(logs_path), recursive=False)

        observer.start()
        print(f"✅ Monitoreo iniciado. Presiona Ctrl+C para detener.")

        try:
            while True:
                time.sleep(1)
                self._print_realtime_stats()
        except KeyboardInterrupt:
            print(f"\n⏹️ Deteniendo monitoreo...")
            observer.stop()

        observer.join()

    def _initial_catchup(self, logs_path: Path):
        """Catch-up inicial inteligente para archivos existentes"""
        print(f"🔄 Realizando catch-up inicial...")

        log_files = list(logs_path.glob("*.log"))
        for log_file in log_files:
            try:
                # Procesar solo las últimas N líneas para catch-up rápido
                with open(log_file, 'r', encoding='utf-8', errors='ignore') as f:
                    lines = f.readlines()

                # Solo procesar últimas 100 líneas por archivo en catch-up
                recent_lines = lines[-100:] if len(lines) > 100 else lines
                self.file_positions[str(log_file)] = len(''.join(lines).encode('utf-8'))

                messages = []
                for line in recent_lines:
                    parsed = self.parser._parse_line(line.strip())
                    if parsed:
                        parsed['file_source'] = log_file.name
                        messages.append(parsed)

                if messages:
                    self._process_message_batch_optimized(messages)

            except Exception as e:
                print(f"❌ Error en catch-up de {log_file.name}: {e}")

    def _print_realtime_stats(self):
        """Imprime estadísticas en tiempo real cada 30 segundos"""
        if hasattr(self, '_last_stats_print'):
            if time.time() - self._last_stats_print < 30:
                return

        self._last_stats_print = time.time()

        print(f"\n📊 ESTADÍSTICAS EN TIEMPO REAL [{datetime.now().strftime('%H:%M:%S')}]")
        print(f"   ✅ Procesados: {self.stats['messages_processed']}")
        print(f"   ⚡ Omitidos: {self.stats['messages_skipped']}")
        print(f"   🚨 Alertas: {self.stats['alerts_generated']}")
        print(f"   🕐 Tiempo total: {self.stats['processing_time']:.2f}s")
        print(f"   📝 Cache: {len(self.processed_message_cache)} mensajes")

        # Mostrar alertas activas recientes
        recent_alerts = [a for a in self.active_alerts if
                         (datetime.now() - a['timestamp']).seconds < 600]  # últimos 10 min

        if recent_alerts:
            print(f"   🔥 Alertas activas (10min): {len(recent_alerts)}")

    def get_realtime_dashboard_data(self) -> Dict:
        """Retorna datos para dashboard en tiempo real"""
        recent_alerts = [a for a in self.active_alerts if
                         (datetime.now() - a['timestamp']).seconds < 3600]  # última hora

        return {
            'stats': self.stats.copy(),
            'cache_size': len(self.processed_message_cache),
            'active_alerts': len(recent_alerts),
            'recent_alerts': list(recent_alerts)[-10:],  # últimas 10
            'alert_distribution': {
                'critical': len([a for a in recent_alerts if a['priority'] == 'CRITICAL']),
                'high': len([a for a in recent_alerts if a['priority'] == 'HIGH']),
                'medium': len([a for a in recent_alerts if a['priority'] == 'MEDIUM']),
                'low': len([a for a in recent_alerts if a['priority'] == 'LOW'])
            }
        }


def main():
    parser = argparse.ArgumentParser(description='Moderador Semántico Optimizado - Tiempo Real')
    parser.add_argument('--realtime', '-rt', action='store_true',
                        help='Iniciar monitoreo en tiempo real')
    parser.add_argument('--logs-dir', '-d', default='./data/logs',
                        help='Directorio de logs a monitorear')
    parser.add_argument('--file', '-f', help='Procesar archivo específico (modo batch)')
    parser.add_argument('--message', '-m', help='Analizar mensaje individual')
    parser.add_argument('--user', '-u', default='test_user', help='Usuario para mensaje individual')

    args = parser.parse_args()

    moderator = OptimizedModeradorSemantico()

    # Ejemplo: registrar callback para alertas críticas
    def critical_alert_handler(alert):
        if alert['priority'] == 'CRITICAL':
            print(f"🚨 CALLBACK: Alerta crítica de @{alert['username']}")

    moderator.register_alert_callback(critical_alert_handler)

    if args.realtime:
        moderator.start_realtime_monitoring(args.logs_dir)
    elif args.file:
        # Modo batch optimizado para archivo específico
        moderator.process_new_lines(args.file)
    elif args.message:
        # Análisis individual
        message = {
            'text': args.message,
            'username': args.user,
            'timestamp': datetime.now(),
            'timestamp_str': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            'file_source': 'manual_test'
        }
        analysis = moderator.analyzer.analyze_message(message)
        print(f"\n{'=' * 50}")
        print(f"🔍 ANÁLISIS DE MENSAJE")
        print(f"{'=' * 50}")
        print(f"👤 Usuario: {args.user}")
        print(f"💬 Mensaje: {args.message}")
        print(f"💀 Toxicidad: {analysis['toxicity_score']:.2f}")
        print(f"📧 Spam: {analysis['spam_probability']:.2f}")
        print(f"😊 Sentimiento: {analysis['sentiment']}")
        print(f"🏷️ Categorías: {', '.join(analysis['categories'])}")
        print(f"⚠️ Requiere acción: {analysis['requires_action']}")
        print(f"🎯 Acción sugerida: {analysis['action_type']}")
        print(f"📝 Razonamiento: {analysis['reasoning']}")
    else:
        print("❌ Especifica --realtime, --file o --message")
        parser.print_help()


if __name__ == "__main__":
    main()

