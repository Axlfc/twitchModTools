# ==============================================================================
# database_manager.py - CORREGIDO: Deduplicación mejorada y manejo preciso
# ==============================================================================

import json
from typing import Dict, List, Optional

import psycopg2
from psycopg2.extras import RealDictCursor
import psycopg2.pool
from contextlib import contextmanager

from config import Config
from utils import generate_message_id


class DatabaseManager:
    def __init__(self, config: Config):
        self.config = config
        self.pool = self._create_pool()
        self._init_tables()

    def _create_pool(self):
        """Crea pool de conexiones"""
        try:
            return psycopg2.pool.SimpleConnectionPool(
                1, 10,
                host=self.config.POSTGRES_HOST,
                port=self.config.POSTGRES_PORT,
                database=self.config.POSTGRES_DB,
                user=self.config.POSTGRES_USER,
                password=self.config.POSTGRES_PASSWORD
            )
        except Exception as e:
            print(f"❌ Error conectando a PostgreSQL: {e}")
            return None

    @contextmanager
    def get_connection(self):
        """Context manager para conexiones"""
        conn = None
        try:
            conn = self.pool.getconn()
            yield conn
        finally:
            if conn:
                self.pool.putconn(conn)

    def _init_tables(self):
        """Inicializa las tablas necesarias"""
        create_tables_sql = """
        CREATE TABLE IF NOT EXISTS moderation_logs (
            id SERIAL PRIMARY KEY,
            message_id VARCHAR(255) UNIQUE,
            username VARCHAR(100),
            message_text TEXT,
            timestamp TIMESTAMP,
            file_source VARCHAR(255),
            toxicity_score FLOAT,
            spam_probability FLOAT,
            sentiment VARCHAR(20),
            categories TEXT[],
            requires_action BOOLEAN,
            action_type VARCHAR(50),
            reasoning TEXT,
            keywords_detected TEXT[],
            analyzed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            model_used VARCHAR(100),
            qdrant_point_id VARCHAR(100)
        );

        CREATE TABLE IF NOT EXISTS user_stats (
            id SERIAL PRIMARY KEY,
            username VARCHAR(100) UNIQUE,
            total_messages INTEGER DEFAULT 0,
            toxic_messages INTEGER DEFAULT 0,
            spam_messages INTEGER DEFAULT 0,
            avg_toxicity FLOAT DEFAULT 0.0,
            avg_spam_prob FLOAT DEFAULT 0.0,
            last_seen TIMESTAMP,
            risk_level VARCHAR(20) DEFAULT 'low',
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        CREATE INDEX IF NOT EXISTS idx_moderation_username ON moderation_logs(username);
        CREATE INDEX IF NOT EXISTS idx_moderation_timestamp ON moderation_logs(timestamp);
        CREATE INDEX IF NOT EXISTS idx_moderation_toxicity ON moderation_logs(toxicity_score);
        CREATE INDEX IF NOT EXISTS idx_user_stats_risk ON user_stats(risk_level);
        CREATE INDEX IF NOT EXISTS idx_moderation_message_id ON moderation_logs(message_id);
        """

        try:
            with self.get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(create_tables_sql)
                conn.commit()
            print("✅ Tablas de PostgreSQL inicializadas")
        except Exception as e:
            print(f"❌ Error inicializando tablas: {e}")

    def get_existing_message_ids(self, messages: List[dict]) -> set:
        """🔧 CORREGIDO: Verificación más eficiente de duplicados"""
        if not messages:
            return set()

        # Generar IDs usando la misma lógica que en el procesamiento
        message_ids = []
        for msg in messages:
            message_id = generate_message_id(msg)
            message_ids.append(message_id)

        if not message_ids:
            return set()

        try:
            with self.get_connection() as conn:
                with conn.cursor() as cur:
                    # Usar batch query más eficiente
                    placeholders = ','.join(['%s'] * len(message_ids))
                    cur.execute(f"""
                        SELECT message_id FROM moderation_logs
                        WHERE message_id IN ({placeholders})
                    """, message_ids)
                    existing_ids = set(row[0] for row in cur.fetchall())
                    print(f"🔍 PostgreSQL: {len(existing_ids)} duplicados encontrados de {len(message_ids)} mensajes")
                    return existing_ids
        except Exception as e:
            print(f"❌ Error consultando IDs existentes en PostgreSQL: {e}")
            return set()

    def get_messages_by_file_source(self, file_source: str) -> List[Dict]:
        try:
            with self.get_connection() as conn:
                with conn.cursor(cursor_factory=RealDictCursor) as cur:
                    cur.execute("""
                        SELECT * FROM moderation_logs
                        WHERE file_source = %s
                    """, (file_source,))
                    return [dict(row) for row in cur.fetchall()]
        except Exception as e:
            print(f"❌ Error recuperando mensajes por archivo: {e}")
            return []

    def save_analysis(self, message: dict, analysis: dict, point_id: str):
        """🔧 CORREGIDO: Guardado más robusto con validación estricta"""
        try:
            message_id = analysis.get("message_id")
            if not message_id:
                print("❌ Error: message_id faltante en analysis")
                return False

            with self.get_connection() as conn:
                with conn.cursor() as cur:
                    # Verificar si ya existe ANTES de insertar
                    cur.execute("SELECT COUNT(*) FROM moderation_logs WHERE message_id = %s", (message_id,))
                    exists = cur.fetchone()[0] > 0

                    if exists:
                        print(f"⚠️ Mensaje duplicado saltado en PostgreSQL (ID: {message_id})")
                        return False

                    # Insertar nuevo mensaje
                    cur.execute("""
                        INSERT INTO moderation_logs (
                            message_id, username, message_text, timestamp, file_source,
                            toxicity_score, spam_probability, sentiment,
                            categories, requires_action, action_type, reasoning,
                            keywords_detected, model_used, qdrant_point_id
                        )
                        VALUES (
                            %(message_id)s, %(username)s, %(text)s, %(timestamp)s, %(file_source)s,
                            %(toxicity_score)s, %(spam_probability)s, %(sentiment)s,
                            %(categories)s, %(requires_action)s, %(action_type)s, %(reasoning)s,
                            %(keywords)s, %(model_used)s, %(point_id)s
                        )
                    """, {
                        "message_id": message_id,
                        "username": message.get("username", "unknown"),
                        "text": message.get("text", ""),
                        "timestamp": message.get("timestamp"),
                        "file_source": message.get("file_source", "unknown"),
                        "toxicity_score": analysis.get("toxicity_score", 0.0),
                        "spam_probability": analysis.get("spam_probability", 0.0),
                        "sentiment": analysis.get("sentiment", "neutral"),
                        "categories": analysis.get("categories", []),
                        "requires_action": analysis.get("requires_action", False),
                        "action_type": analysis.get("action_type", "none"),
                        "reasoning": analysis.get("reasoning", ""),
                        "keywords": analysis.get("keywords_detected", []),
                        "model_used": analysis.get("model_used", "ollama"),
                        "point_id": point_id
                    })

                    # Actualizar estadísticas de usuario solo si se insertó
                    if cur.rowcount > 0:
                        username = message.get("username", "").strip().lower()
                        if not username or username in ["unknown", "none", "null"]:
                            username = "bot_or_unknown"

                        self._update_user_stats(cur, username, analysis)
                        print(f"📝 Nuevo mensaje guardado en PostgreSQL (ID: {message_id})")
                        conn.commit()
                        return True
                    else:
                        print(f"⚠️ No se pudo insertar mensaje (ID: {message_id})")
                        return False

        except psycopg2.IntegrityError as e:
            if "duplicate key" in str(e):
                print(f"⚠️ Mensaje duplicado detectado por constraint (ID: {message_id})")
                return False
            else:
                print(f"❌ Error de integridad guardando en PostgreSQL: {e}")
                return False
        except Exception as e:
            print(f"❌ Error guardando en PostgreSQL: {e}")
            return False

    def get_analysis_by_message_id(self, message_id: str) -> Optional[Dict]:
        """🆕 Obtiene el análisis de un mensaje por su ID"""
        try:
            with self.get_connection() as conn:
                with conn.cursor(cursor_factory=RealDictCursor) as cur:
                    cur.execute("""
                        SELECT * FROM moderation_logs
                        WHERE message_id = %s
                    """, (message_id,))
                    result = cur.fetchone()
                    return dict(result) if result else None
        except Exception as e:
            print(f"❌ Error obteniendo análisis por message_id {message_id}: {e}")
            return None

    def get_recent_messages_by_user(self, username: str, limit: int = 10) -> List[Dict]:
        """🆕 Obtiene los mensajes más recientes de un usuario"""
        try:
            with self.get_connection() as conn:
                with conn.cursor(cursor_factory=RealDictCursor) as cur:
                    cur.execute("""
                        SELECT * FROM moderation_logs
                        WHERE username = %s
                        ORDER BY timestamp DESC
                        LIMIT %s
                    """, (username, limit))
                    return [dict(row) for row in cur.fetchall()]
        except Exception as e:
            print(f"❌ Error obteniendo mensajes recientes de {username}: {e}")
            return []

    def get_messages_in_timeframe(self, start_time, end_time) -> List[Dict]:
        """🆕 Obtiene mensajes en un rango de tiempo específico"""
        try:
            with self.get_connection() as conn:
                with conn.cursor(cursor_factory=RealDictCursor) as cur:
                    cur.execute("""
                        SELECT * FROM moderation_logs
                        WHERE timestamp BETWEEN %s AND %s
                        ORDER BY timestamp ASC
                    """, (start_time, end_time))
                    return [dict(row) for row in cur.fetchall()]
        except Exception as e:
            print(f"❌ Error obteniendo mensajes en rango temporal: {e}")
            return []

    def _update_user_stats(self, cursor, username: str, analysis: Dict):
        """Actualiza estadísticas del usuario"""
        cursor.execute("""
            INSERT INTO user_stats (username, total_messages, toxic_messages, spam_messages, 
                                  avg_toxicity, avg_spam_prob, last_seen)
            VALUES (%s, 1, %s, %s, %s, %s, CURRENT_TIMESTAMP)
            ON CONFLICT (username) DO UPDATE SET
                total_messages = user_stats.total_messages + 1,
                toxic_messages = user_stats.toxic_messages + %s,
                spam_messages = user_stats.spam_messages + %s,
                avg_toxicity = (user_stats.avg_toxicity * user_stats.total_messages + %s) / (user_stats.total_messages + 1),
                avg_spam_prob = (user_stats.avg_spam_prob * user_stats.total_messages + %s) / (user_stats.total_messages + 1),
                last_seen = CURRENT_TIMESTAMP,
                updated_at = CURRENT_TIMESTAMP,
                risk_level = CASE 
                    WHEN (user_stats.avg_toxicity * user_stats.total_messages + %s) / (user_stats.total_messages + 1) > 0.7 THEN 'high'
                    WHEN (user_stats.avg_toxicity * user_stats.total_messages + %s) / (user_stats.total_messages + 1) > 0.4 THEN 'medium'
                    ELSE 'low'
                END
        """, (
            username,
            1 if analysis.get('toxicity_score', 0) > 0.5 else 0,
            1 if analysis.get('spam_probability', 0) > 0.5 else 0,
            analysis.get('toxicity_score', 0),
            analysis.get('spam_probability', 0),
            1 if analysis.get('toxicity_score', 0) > 0.5 else 0,
            1 if analysis.get('spam_probability', 0) > 0.5 else 0,
            analysis.get('toxicity_score', 0),
            analysis.get('spam_probability', 0),
            analysis.get('toxicity_score', 0),
            analysis.get('toxicity_score', 0)
        ))

    def get_user_risk_summary(self):
        """Obtiene resumen de usuarios de riesgo"""
        try:
            with self.get_connection() as conn:
                with conn.cursor(cursor_factory=RealDictCursor) as cur:
                    cur.execute("""
                        SELECT username, total_messages, avg_toxicity, avg_spam_prob, 
                               risk_level, last_seen
                        FROM user_stats 
                        WHERE risk_level IN ('medium', 'high')
                        ORDER BY avg_toxicity DESC, total_messages DESC
                        LIMIT 20
                    """)
                    return cur.fetchall()
        except Exception as e:
            print(f"❌ Error obteniendo resumen: {e}")
            return []

    def get_user_message_history(self, username: str, days_back: int = 7) -> List[Dict]:
        """🆕 Obtiene el historial completo de mensajes de un usuario"""
        try:
            with self.get_connection() as conn:
                with conn.cursor(cursor_factory=RealDictCursor) as cur:
                    cur.execute("""
                        SELECT * FROM moderation_logs
                        WHERE username = %s 
                        AND timestamp >= NOW() - INTERVAL '%s days'
                        ORDER BY timestamp DESC
                    """, (username, days_back))
                    return [dict(row) for row in cur.fetchall()]
        except Exception as e:
            print(f"❌ Error obteniendo historial de {username}: {e}")
            return []