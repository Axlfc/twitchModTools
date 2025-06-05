# ==============================================================================
# 2. deduplication_manager.py - Handle message deduplication
# ==============================================================================

from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from typing import List, Dict

from utils import enrich_message


class DeduplicationManager:
    """Handles message deduplication logic"""

    def __init__(self, db, vector_store):
        self.db = db
        self.vector_store = vector_store

    def filter_new_messages(self, messages: List[Dict]) -> List[Dict]:
        """Enhanced deduplication with internal and external checks"""
        print(f"🔍 Verificando duplicados para {len(messages)} mensajes...")

        try:
            # Enrich all messages first
            enriched_messages = [enrich_message(msg) for msg in messages]

            # If collection is fresh, insert everything without deduplication
            if getattr(self.vector_store, 'is_fresh_collection', False):
                print(f"🚨 Colección nueva detectada. Insertando todos los mensajes sin deduplicación externa.")
                return enriched_messages

            # Level 1: Internal deduplication
            unique_messages = self._remove_internal_duplicates(enriched_messages)

            # Level 2: External deduplication against databases
            new_messages = self._remove_external_duplicates(unique_messages)

            return new_messages

        except Exception as e:
            print(f"❌ Error en filtrado avanzado: {e}")
            return self._simple_duplicate_filter(messages)

    def _remove_internal_duplicates(self, messages: List[Dict]) -> List[Dict]:
        """Remove duplicates within the current batch"""
        seen_ids = set()
        unique_messages = []

        for msg in messages:
            msg_id = msg['message_id']
            if msg_id not in seen_ids:
                seen_ids.add(msg_id)
                unique_messages.append(msg)

        internal_duplicates = len(messages) - len(unique_messages)
        print(f"⚠️ Duplicados internos eliminados: {internal_duplicates}")

        return unique_messages

    def _remove_external_duplicates(self, messages: List[Dict]) -> List[Dict]:
        """Remove duplicates that exist in external databases"""
        with ThreadPoolExecutor(max_workers=2) as executor:
            postgres_future = executor.submit(self.db.get_existing_message_ids, messages)
            qdrant_future = executor.submit(self.vector_store.get_existing_message_ids, messages)

            existing_ids_postgres = postgres_future.result()
            existing_ids_qdrant = qdrant_future.result()

        all_existing_ids = existing_ids_postgres.union(existing_ids_qdrant)

        # Debug inconsistencies
        self._debug_inconsistencies(existing_ids_postgres, existing_ids_qdrant)

        # Filter out existing messages
        new_messages = []
        duplicate_details = defaultdict(int)

        for message in messages:
            message_id = message['message_id']
            if message_id not in all_existing_ids:
                new_messages.append(message)
            else:
                if message_id in existing_ids_postgres:
                    duplicate_details['postgres'] += 1
                if message_id in existing_ids_qdrant:
                    duplicate_details['qdrant'] += 1

        print(f"📊 Detalles de duplicados externos: {dict(duplicate_details)}")
        return new_messages

    def _debug_inconsistencies(self, postgres_ids: set, qdrant_ids: set):
        """Debug inconsistencies between databases"""
        postgres_only = postgres_ids - qdrant_ids
        qdrant_only = qdrant_ids - postgres_ids

        if postgres_only or qdrant_only:
            print(f"⚠️ Inconsistencias detectadas entre backends:")
            print(f"   - Solo en PostgreSQL: {len(postgres_only)}")
            print(f"   - Solo en Qdrant: {len(qdrant_only)}")

    def _simple_duplicate_filter(self, messages: List[Dict]) -> List[Dict]:
        """Fallback simple duplicate filtering"""
        print("🔄 Usando filtrado simple como respaldo...")
        seen_ids = set()
        new_messages = []

        for message in messages:
            message = enrich_message(message)
            message_id = message['message_id']
            if message_id not in seen_ids:
                seen_ids.add(message_id)
                new_messages.append(message)

        return new_messages
