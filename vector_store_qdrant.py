# ==============================================================================
# vector_store_qdrant.py - MEJORADO: Colecciones dinámicas por archivo/canal
# ==============================================================================

from typing import Dict, List
from qdrant_client import QdrantClient
from qdrant_client.models import Distance, VectorParams, PointStruct
from qdrant_client.models import Filter, FieldCondition, MatchValue, MatchAny
import uuid
import re
import os
import hashlib
from pathlib import Path
from config import Config
from utils import generate_message_id


class QdrantVectorStore:
    def __init__(self, config: Config, file_path: str = None):
        self.config = config
        self.client = QdrantClient(url=config.QDRANT_URL)
        self.base_collection_name = config.QDRANT_COLLECTION

        # Generar nombre de colección específico para el archivo
        if file_path:
            self.collection_name = self._generate_collection_name(file_path)
        else:
            self.collection_name = None  # Se asignará más tarde al procesar cada archivo
            self.is_fresh_collection = False  # Por defecto, asumimos que existe

        self._ensure_collection()

    def set_collection_for_file(self, file_path: str):
        """Genera nombre de colección a partir del archivo y crea si no existe"""
        self.collection_name = self._generate_collection_name(file_path)
        print(f"🏷️ Colección generada: {self.collection_name} para archivo: {os.path.basename(file_path)}")

        try:
            collections = self.client.get_collections().collections

            # Manejo robusto de formatos diferentes
            if isinstance(collections[0], tuple):
                existing_names = [c[0] for c in collections]
            else:
                existing_names = [c.name for c in collections]

            if self.collection_name not in existing_names:
                print(f"🔧 Creando nueva colección: {self.collection_name}")
                self.client.create_collection(
                    collection_name=self.collection_name,
                    vectors_config=VectorParams(size=768, distance=Distance.COSINE)
                )
                self.is_fresh_collection = True
            else:
                print(f"✅ Usando colección existente: {self.collection_name}")
                self.is_fresh_collection = False

        except Exception as e:
            print(f"❌ Error creando/verificando colección: {e}")
            self.is_fresh_collection = False

    def count_points(self) -> int:
        """Devuelve el número de puntos en la colección actual"""
        try:
            response = self.client.count(
                collection_name=self.collection_name,
                exact=True
            )
            return response.count if hasattr(response, "count") else 0
        except Exception as e:
            print(f"❌ Error contando puntos en Qdrant: {e}")
            return 0

    def _create_collection(self):
        """Create a new Qdrant collection with the appropriate vector parameters"""
        from qdrant_client.models import VectorParams, Distance

        print(f"📦 Creando colección Qdrant: {self.collection_name}")

        try:
            self.client.create_collection(
                collection_name=self.collection_name,
                vectors_config=VectorParams(
                    size=self.config.EMBEDDING_DIM,  # e.g., 768
                    distance=Distance.COSINE
                )
            )
            print(f"✅ Colección '{self.collection_name}' creada exitosamente.")
        except Exception as e:
            print(f"❌ Error creando colección '{self.collection_name}': {e}")

    def _generate_collection_name(self, file_path: str) -> str:
        """Genera un nombre de colección único basado en el archivo"""
        path_obj = Path(file_path)

        # Extraer información del path
        # Ejemplo: /Twitch/Channels/niaghtmares/niaghtmares-321818859132.log
        parts = path_obj.parts

        # Buscar el patrón de Twitch/Channels/[canal]
        collection_parts = []

        if 'Channels' in parts:
            channel_idx = parts.index('Channels')
            if channel_idx + 1 < len(parts):
                channel_name = parts[channel_idx + 1]
                collection_parts.append(f"twitch_{channel_name}")

        # Si no encontramos el patrón, usar el nombre del archivo
        if not collection_parts:
            filename = path_obj.stem  # sin extensión
            # Limpiar caracteres especiales
            clean_name = re.sub(r'[^a-zA-Z0-9_-]', '_', filename)
            collection_parts.append(clean_name)

        # Agregar hash corto para evitar colisiones
        file_hash = hashlib.md5(file_path.encode()).hexdigest()[:8]
        collection_parts.append(file_hash)

        collection_name = "_".join(collection_parts).lower()

        # Asegurar que empiece con letra (requisito de Qdrant)
        if collection_name[0].isdigit():
            collection_name = f"logs_{collection_name}"

        print(f"🏷️ Colección generada: {collection_name} para archivo: {Path(file_path).name}")
        return collection_name

    def _ensure_collection(self):
        """Asegura que la colección existe"""
        try:
            collections = self.client.get_collections().collections
            collection_names = [c.name for c in collections]

            if self.collection_name not in collection_names:
                print(f"🔧 Creando nueva colección: {self.collection_name}")
                self.client.create_collection(
                    collection_name=self.collection_name,
                    vectors_config=VectorParams(size=768, distance=Distance.COSINE)
                )
                self.is_fresh_collection = True
            else:
                print(f"✅ Usando colección existente: {self.collection_name}")
                self.is_fresh_collection = False
        except Exception as e:
            print(f"❌ Error configurando Qdrant: {e}")

    def get_existing_message_ids(self, messages: List[Dict]) -> set:
        """Verificación de duplicados en la colección específica"""
        if not messages:
            return set()

        message_ids = []
        for msg in messages:
            message_id = generate_message_id(msg)
            message_ids.append(message_id)

        if not message_ids:
            return set()

        try:
            batch_size = 50
            existing_ids = set()

            for i in range(0, len(message_ids), batch_size):
                batch_ids = message_ids[i:i + batch_size]

                try:
                    results, _ = self.client.scroll(
                        collection_name=self.collection_name,
                        scroll_filter=Filter(
                            must=[
                                FieldCondition(
                                    key="message_id",
                                    match=MatchAny(any=batch_ids)
                                )
                            ]
                        ),
                        limit=len(batch_ids),
                        with_payload=True
                    )

                    for point in results:
                        if point.payload and 'message_id' in point.payload:
                            existing_ids.add(point.payload['message_id'])

                except Exception as batch_error:
                    print(f"⚠️ Error en lote {i // batch_size + 1}: {batch_error}")
                    continue

            print(f"🔍 {self.collection_name}: {len(existing_ids)} duplicados de {len(message_ids)} mensajes")
            return existing_ids

        except Exception as e:
            print(f"❌ Error verificando duplicados en {self.collection_name}: {e}")
            return set()

    def add_message(self, message: Dict, analysis: Dict, embedding: List[float]) -> str:
        """Inserción en la colección específica"""
        message_id = analysis.get("message_id")
        if not message_id:
            print("❌ Error: message_id faltante en analysis para Qdrant")
            return ""

        try:
            # Verificar duplicados en esta colección específica
            existing_check, _ = self.client.scroll(
                collection_name=self.collection_name,
                scroll_filter=Filter(
                    must=[
                        FieldCondition(
                            key="message_id",
                            match=MatchValue(value=message_id)
                        )
                    ]
                ),
                limit=1,
                with_payload=True
            )

            if existing_check:
                print(f"⚠️ Mensaje duplicado saltado en {self.collection_name} (ID: {message_id})")
                return ""

            point_id = str(uuid.uuid4())

            self.client.upsert(
                collection_name=self.collection_name,
                points=[
                    PointStruct(
                        id=point_id,
                        vector=embedding,
                        payload={
                            "message_id": message_id,
                            "username": message.get("username", "unknown"),
                            "text": message.get("text", ""),
                            "timestamp": message.get("timestamp_str", str(message.get("timestamp", ""))),
                            "file_source": message.get("file_source", "unknown"),
                            "toxicity": analysis.get("toxicity_score", 0.0),
                            "spam_probability": analysis.get("spam_probability", 0.0),
                            "sentiment": analysis.get("sentiment", "neutral"),
                            "requires_action": analysis.get("requires_action", False),
                            "action_type": analysis.get("action_type", "none"),
                            "categories": analysis.get("categories", []),
                            "keywords": analysis.get("keywords_detected", [])
                        }
                    )
                ]
            )

            print(f"✅ Mensaje insertado en {self.collection_name} (msg_id: {message_id[:20]}...)")
            return point_id

        except Exception as e:
            print(f"❌ Error insertando mensaje en {self.collection_name}: {e}")
            return ""

    def search_similar(self, query_embedding: List[float], limit: int = 5, score_threshold: float = 0.7):
        """Busca mensajes similares en la colección específica"""
        try:
            results = self.client.search(
                collection_name=self.collection_name,
                query_vector=query_embedding,
                limit=limit,
                score_threshold=score_threshold
            )
            return results
        except Exception as e:
            print(f"❌ Error buscando en {self.collection_name}: {e}")
            return []

    def get_collection_info(self):
        """Información de la colección específica"""
        try:
            info = self.client.get_collection(self.collection_name)
            count_result = self.client.count(self.collection_name)
            print(f"📊 {self.collection_name}: {count_result.count} puntos")
            return {
                "collection_name": self.collection_name,
                "collection_info": info,
                "point_count": count_result.count
            }
        except Exception as e:
            print(f"❌ Error obteniendo info de {self.collection_name}: {e}")
            return None

    def list_all_collections(self):
        """Lista todas las colecciones disponibles"""
        try:
            collections = self.client.get_collections().collections
            collection_data = []

            for collection in collections:
                try:
                    count = self.client.count(collection.name)
                    collection_data.append({
                        "name": collection.name,
                        "points": count.count
                    })
                except:
                    collection_data.append({
                        "name": collection.name,
                        "points": "Error"
                    })

            return collection_data
        except Exception as e:
            print(f"❌ Error listando colecciones: {e}")
            return []

    def delete_collection(self, collection_name: str = None):
        """Elimina una colección específica"""
        target_collection = collection_name or self.collection_name
        try:
            self.client.delete_collection(target_collection)
            print(f"🗑️ Colección {target_collection} eliminada")
            return True
        except Exception as e:
            print(f"❌ Error eliminando colección {target_collection}: {e}")
            return False

    def get_collection_stats(self):
        """Estadísticas detalladas de la colección actual"""
        try:
            # Obtener todos los puntos para análisis
            all_points, _ = self.client.scroll(
                collection_name=self.collection_name,
                limit=10000,  # Ajustar según necesidad
                with_payload=True
            )

            if not all_points:
                return {"error": "No hay datos en la colección"}

            # Análisis de usuarios
            users = {}
            total_toxicity = 0
            total_spam = 0

            for point in all_points:
                payload = point.payload
                username = payload.get('username', 'unknown')
                toxicity = payload.get('toxicity', 0)
                spam = payload.get('spam_probability', 0)

                if username not in users:
                    users[username] = {
                        'messages': 0,
                        'total_toxicity': 0,
                        'total_spam': 0
                    }

                users[username]['messages'] += 1
                users[username]['total_toxicity'] += toxicity
                users[username]['total_spam'] += spam

                total_toxicity += toxicity
                total_spam += spam

            # Calcular estadísticas
            total_messages = len(all_points)
            avg_toxicity = total_toxicity / total_messages if total_messages > 0 else 0
            avg_spam = total_spam / total_messages if total_messages > 0 else 0

            # Top usuarios por actividad
            top_users = sorted(
                [(user, data['messages']) for user, data in users.items()],
                key=lambda x: x[1],
                reverse=True
            )[:10]

            return {
                "collection_name": self.collection_name,
                "total_messages": total_messages,
                "unique_users": len(users),
                "avg_toxicity": avg_toxicity,
                "avg_spam": avg_spam,
                "top_users": top_users
            }

        except Exception as e:
            print(f"❌ Error obteniendo estadísticas: {e}")
            return {"error": str(e)}