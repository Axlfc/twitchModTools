import re
from datetime import datetime
from typing import List, Dict, Optional
from pathlib import Path
import json


class ChatterinoParser:
    def __init__(self):
        # Patrones expandidos para diferentes formatos de logs de Chatterino
        self.patterns = [
            # Formato estándar: [2025-05-30 19:31:23] <username> mensaje
            r'\[([0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2})\] <([^>]+)> (.+)',

            # Formato con timestamp directo: 2025-05-30 19:31:23 [username]: mensaje
            r'([0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}) \[([^\]]+)\]: (.+)',

            # Formato simple: timestamp username: mensaje
            r'([0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}) ([^:]+): (.+)',

            # Nuevo: Formato IRC-style: [timestamp] username: mensaje
            r'\[([0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2})\] ([^:]+): (.+)',

            # Nuevo: Formato con UTC: [2025-05-30T19:31:23Z] username: mensaje
            r'\[([0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}Z?)\] ([^:]+): (.+)',

            # Nuevo: Solo timestamp entre corchetes: [19:31:23] username: mensaje
            r'\[([0-9]{2}:[0-9]{2}:[0-9]{2})\] ([^:]+): (.+)',

            # Nuevo: Timestamp con milisegundos: [2025-05-30 19:31:23.123] username: mensaje
            r'\[([0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}\.[0-9]+)\] ([^:]+): (.+)',

            # Nuevo: Sin corchetes, directo: 2025-05-30 19:31:23 username mensaje
            r'([0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}) ([^\s]+) (.+)',
        ]

        # Formatos de timestamp para parsing
        self.timestamp_formats = [
            "%Y-%m-%d %H:%M:%S",
            "%Y-%m-%dT%H:%M:%S",
            "%Y-%m-%dT%H:%M:%SZ",
            "%H:%M:%S",
            "%Y-%m-%d %H:%M:%S.%f"
        ]

        # Debug counters
        self.debug_stats = {
            'total_lines': 0,
            'parsed_lines': 0,
            'unknown_format_lines': [],
            'pattern_matches': {i: 0 for i in range(len(self.patterns))}
        }

    def parse_file(self, filepath: str, debug_mode: bool = False) -> List[Dict]:
        """Parsea un archivo de logs de Chatterino con debugging opcional"""
        messages = []

        if not Path(filepath).exists():
            print(f"❌ Archivo no encontrado: {filepath}")
            return messages

        # Reset debug stats
        self.debug_stats = {
            'total_lines': 0,
            'parsed_lines': 0,
            'unknown_format_lines': [],
            'pattern_matches': {i: 0 for i in range(len(self.patterns))}
        }

        with open(filepath, "r", encoding="utf-8", errors="ignore") as f:
            for line_num, line in enumerate(f, 1):
                line = line.strip()
                if not line:
                    continue

                self.debug_stats['total_lines'] += 1

                parsed = self._parse_line(line, line_num, debug_mode)
                if parsed:
                    parsed['line_number'] = line_num
                    parsed['file_source'] = Path(filepath).name
                    messages.append(parsed)
                    self.debug_stats['parsed_lines'] += 1
                elif debug_mode:
                    # Guardar líneas que no se pudieron parsear para debug
                    if len(self.debug_stats['unknown_format_lines']) < 10:  # Limitar ejemplos
                        self.debug_stats['unknown_format_lines'].append({
                            'line_num': line_num,
                            'content': line[:100] + ('...' if len(line) > 100 else '')
                        })

        # Mostrar estadísticas
        success_rate = (self.debug_stats['parsed_lines'] / self.debug_stats['total_lines'] * 100) if self.debug_stats[
                                                                                                         'total_lines'] > 0 else 0
        print(f"✅ Parseados {len(messages)} mensajes de {filepath}")
        print(
            f"📊 Tasa de éxito: {success_rate:.1f}% ({self.debug_stats['parsed_lines']}/{self.debug_stats['total_lines']} líneas)")

        if debug_mode:
            self._print_debug_info()

        return messages

    def _parse_line(self, line: str, line_num: int = 0, debug_mode: bool = False) -> Optional[Dict]:
        """Intenta parsear una línea con diferentes patrones"""

        for i, pattern in enumerate(self.patterns):
            match = re.match(pattern, line)
            if match:
                groups = match.groups()
                if len(groups) >= 3:
                    timestamp_str, username, text = groups[0], groups[1], groups[2]

                    # Intentar parsear timestamp con diferentes formatos
                    timestamp = self._parse_timestamp(timestamp_str)
                    if timestamp:
                        self.debug_stats['pattern_matches'][i] += 1
                        return {
                            'timestamp': timestamp,
                            'timestamp_str': timestamp_str,
                            'username': username.strip(),
                            'text': text.strip(),
                            'raw_line': line,
                            'pattern_used': i
                        }

        # Si debug está activado, no crear mensajes "unknown"
        if debug_mode:
            return None

        # Si no matchea ningún patrón, intentar extraer al menos el texto
        if ':' in line:
            parts = line.split(':', 1)
            if len(parts) == 2:
                return {
                    'timestamp': datetime.now(),
                    'timestamp_str': 'unknown',
                    'username': 'bot_or_unknown',
                    'text': parts[1].strip(),
                    'raw_line': line,
                    'pattern_used': -1
                }

        return None

    def _parse_timestamp(self, timestamp_str: str) -> Optional[datetime]:
        """Intenta parsear el timestamp con diferentes formatos"""
        for fmt in self.timestamp_formats:
            try:
                if fmt == "%H:%M:%S":
                    # Para timestamps solo con hora, usar fecha actual
                    time_part = datetime.strptime(timestamp_str, fmt).time()
                    return datetime.combine(datetime.now().date(), time_part)
                else:
                    return datetime.strptime(timestamp_str, fmt)
            except ValueError:
                continue
        return None

    def _print_debug_info(self):
        """Imprime información de debug detallada"""
        print(f"\n🔍 INFORMACIÓN DE DEBUG:")
        print(f"{'=' * 50}")

        # Estadísticas por patrón
        print("📋 Coincidencias por patrón:")
        pattern_descriptions = [
            "Formato estándar: [timestamp] <username> mensaje",
            "Timestamp directo: timestamp [username]: mensaje",
            "Formato simple: timestamp username: mensaje",
            "IRC-style: [timestamp] username: mensaje",
            "UTC format: [timestampT...Z] username: mensaje",
            "Solo hora: [HH:MM:SS] username: mensaje",
            "Con milisegundos: [timestamp.ms] username: mensaje",
            "Sin corchetes: timestamp username mensaje"
        ]

        for i, (count, desc) in enumerate(zip(self.debug_stats['pattern_matches'].values(), pattern_descriptions)):
            if count > 0:
                print(f"  ✅ Patrón {i}: {count} coincidencias - {desc}")

        # Líneas no parseadas
        if self.debug_stats['unknown_format_lines']:
            print(f"\n❌ Ejemplos de líneas NO parseadas:")
            for example in self.debug_stats['unknown_format_lines']:
                print(f"  Línea {example['line_num']}: {example['content']}")

        print(f"{'=' * 50}")

    def analyze_log_format(self, filepath: str, max_lines: int = 100):
        """Analiza el formato del log para identificar patrones"""
        print(f"🔍 Analizando formato de: {filepath}")

        if not Path(filepath).exists():
            print(f"❌ Archivo no encontrado")
            return

        sample_lines = []
        with open(filepath, "r", encoding="utf-8", errors="ignore") as f:
            for i, line in enumerate(f):
                if i >= max_lines:
                    break
                line = line.strip()
                if line:
                    sample_lines.append(line)

        print(f"📝 Analizando {len(sample_lines)} líneas de muestra:")
        print("=" * 60)

        # Mostrar primeras líneas como ejemplo
        for i, line in enumerate(sample_lines[:10]):
            print(f"Línea {i + 1}: {line}")

        print("=" * 60)

        # Buscar patrones comunes
        common_patterns = []
        for line in sample_lines[:20]:
            # Buscar timestamps
            timestamp_matches = re.findall(r'\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}:\d{2}', line)
            bracket_matches = re.findall(r'\[[^\]]+\]', line)
            username_patterns = re.findall(r'([a-zA-Z0-9_]+):', line)

            if timestamp_matches or bracket_matches:
                common_patterns.append({
                    'line': line[:80] + ('...' if len(line) > 80 else ''),
                    'timestamps': timestamp_matches,
                    'brackets': bracket_matches,
                    'usernames': username_patterns
                })

        print("🔍 Patrones detectados:")
        for i, pattern in enumerate(common_patterns[:5]):
            print(f"\nEjemplo {i + 1}:")
            print(f"  Línea: {pattern['line']}")
            print(f"  Timestamps: {pattern['timestamps']}")
            print(f"  Brackets: {pattern['brackets']}")
            print(f"  Usernames: {pattern['usernames']}")