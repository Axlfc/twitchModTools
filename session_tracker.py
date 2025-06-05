# ==============================================================================
# 5. session_tracker.py - Track processing session statistics
# ==============================================================================

from datetime import datetime
import time
from typing import Dict


class SessionTracker:
    """Tracks processing session statistics and performance"""

    def __init__(self):
        self.reset_session()

    def reset_session(self):
        """Reset session statistics"""
        self._session_stats = {
            'start_time': None,
            'processed_count': 0,
            'duplicate_count': 0,
            'alert_count': 0,
            'error_count': 0
        }

    def start_session(self):
        """Start a new processing session"""
        self._session_stats['start_time'] = datetime.now()

    def update_stats(self, **kwargs):
        """Update session statistics"""
        for key, value in kwargs.items():
            if key in self._session_stats:
                self._session_stats[key] = value

    def increment_stat(self, stat_name: str, increment: int = 1):
        """Increment a specific statistic"""
        if stat_name in self._session_stats:
            self._session_stats[stat_name] += increment

    def print_final_summary(self, elapsed_time: float, db=None, vector_store=None):
        """Print comprehensive final summary"""
        print(f"\n{'=' * 60}")
        print(f"📊 RESUMEN FINAL DEL PROCESAMIENTO")
        print(f"{'=' * 60}")

        stats = self._session_stats
        print(f"⏱️  Tiempo total: {elapsed_time:.2f} segundos")
        print(f"📝 Mensajes procesados: {stats['processed_count']}")
        print(f"⚡ Duplicados filtrados: {stats['duplicate_count']}")
        print(f"🚨 Alertas generadas: {stats['alert_count']}")
        print(f"❌ Errores encontrados: {stats['error_count']}")

        if stats['processed_count'] > 0:
            rate = stats['processed_count'] / elapsed_time
            print(f"🚀 Velocidad promedio: {rate:.1f} mensajes/segundo")

        # Database statistics
        if db and vector_store:
            try:
                db_stats = db.get_user_risk_summary()
                qdrant_info = vector_store.get_collection_info()

                if db_stats:
                    print(f"\n⚠️ USUARIOS DE ALTO RIESGO:")
                    for user in db_stats[:3]:
                        print(f"  👤 {user['username']}: {user['total_messages']} msgs, "
                              f"Toxicidad: {user['avg_toxicity']:.2f}")

                if qdrant_info:
                    print(f"📚 Total en base vectorial: {qdrant_info['point_count']} puntos")

            except Exception as e:
                print(f"⚠️ Error obteniendo estadísticas finales: {e}")

    def get_stats(self) -> Dict:
        """Get current session statistics"""
        return self._session_stats.copy()

    def finish(self, start_time, db=None, vector_store=None):
        elapsed_time = (datetime.now() - start_time).total_seconds()
        self.print_final_summary(elapsed_time, db=db, vector_store=vector_store)
