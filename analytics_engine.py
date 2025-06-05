from collections import defaultdict, Counter
from datetime import timedelta, datetime
import time
import statistics
import re
from typing import List, Dict, Tuple, Optional
import numpy as np


class AnalyticsEngine:
    """Handles advanced analytics and pattern detection"""

    def __init__(self, db, analyzer, streamer_username="niaghtmares"):
        self.db = db
        self.analyzer = analyzer
        self.streamer_username = streamer_username

    def perform_comprehensive_analysis(self, messages: List[Dict]):
        """Run all analytics on the message set"""
        if len(messages) < 5:
            print("⚠️ Muy pocos mensajes para análisis completo")
            return

        print(f"\n🧠 Realizando análisis avanzado de {len(messages)} mensajes...")

        analyses = [
            ("🎭 Deriva emocional", self.analyze_emotional_drift),
            ("👥 Comportamiento de usuarios", self.analyze_user_behavior_patterns),
            ("🌐 Contexto global", self.analyze_global_context),
            ("📊 Tendencias temporales", self.analyze_temporal_trends),
            ("🚨 Detección de brigading", self.detect_potential_brigading),
            ("🎯 Análisis de engagement", self.analyze_engagement_patterns),
            ("🔍 Patrones de spam", self.analyze_spam_patterns),
            ("📈 Métricas de moderación", self.generate_moderation_metrics)
        ]

        for name, analysis_func in analyses:
            try:
                print(f"\n{name}...")
                analysis_func(messages)
            except Exception as e:
                print(f"❌ Error en {name}: {e}")

    def analyze_user_behavior_patterns(self, messages: List[Dict]):
        """Enhanced user behavior analysis with detailed statistics"""
        print("📈 Análisis detallado de comportamiento de usuarios...")

        user_profiles = defaultdict(lambda: {
            'message_count': 0,
            'total_toxicity': 0.0,
            'total_spam': 0.0,
            'categories': defaultdict(int),
            'requires_action_count': 0,
            'messages': [],
            'timestamps': [],
            'message_lengths': [],
            'caps_usage': 0,
            'emote_usage': 0,
            'mention_count': 0,
            'question_count': 0,
            'exclamation_count': 0,
            'url_count': 0,
            'repeated_chars': 0,
            'activity_periods': [],
            'response_times': [],
            'sentiment_scores': [],
            'first_seen': None,
            'last_seen': None,
            'unique_words': set(),
            'conversation_starters': 0,
            'replies_to_others': 0
        })

        # Enhanced message analysis
        previous_messages = {}

        for i, msg in enumerate(messages):
            user = msg['username']
            text = msg.get('text', '')
            timestamp = msg['timestamp']

            if user.lower() == self.streamer_username.lower():
                continue

            profile = user_profiles[user]

            # Basic stats
            profile['message_count'] += 1
            profile['timestamps'].append(timestamp)
            profile['message_lengths'].append(len(text))
            profile['messages'].append(text)

            # Set first/last seen
            if not profile['first_seen']:
                profile['first_seen'] = timestamp
            profile['last_seen'] = timestamp

            # Text pattern analysis
            profile['caps_usage'] += self._count_caps_ratio(text)
            profile['emote_usage'] += len(re.findall(r':\w+:', text))
            profile['mention_count'] += len(re.findall(r'@\w+', text))
            profile['question_count'] += text.count('?')
            profile['exclamation_count'] += text.count('!')
            profile['url_count'] += len(
                re.findall(r'http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\\(\\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+', text))
            profile['repeated_chars'] += self._count_repeated_chars(text)

            # Unique vocabulary
            words = set(re.findall(r'\w+', text.lower()))
            profile['unique_words'].update(words)

            # Conversation analysis
            if i > 0 and self._is_conversation_starter(text, previous_messages.get(user, [])):
                profile['conversation_starters'] += 1
            if self._is_reply_to_others(text, messages[max(0, i - 5):i]):
                profile['replies_to_others'] += 1

            # Response time analysis
            if user in previous_messages and previous_messages[user]:
                last_msg_time = previous_messages[user][-1]['timestamp']
                response_time = (timestamp - last_msg_time).total_seconds()
                if response_time < 300:  # Within 5 minutes
                    profile['response_times'].append(response_time)

            previous_messages.setdefault(user, []).append({'timestamp': timestamp, 'text': text})

            # ML Analysis
            try:
                analysis = self.analyzer.analyze_message(msg)
                profile['total_toxicity'] += analysis.get('toxicity_score', 0.0)
                profile['total_spam'] += analysis.get('spam_probability', 0.0)
                profile['requires_action_count'] += int(analysis.get('requires_action', False))

                sentiment_score = self._sentiment_to_score(analysis.get('sentiment', 'neutral'))
                profile['sentiment_scores'].append(sentiment_score)

                for cat in analysis.get('categories', []):
                    profile['categories'][cat] += 1

            except Exception as e:
                continue

        # Generate comprehensive user reports
        self._generate_detailed_user_reports(user_profiles)
        self._identify_user_archetypes(user_profiles)
        self._analyze_user_interactions(user_profiles, messages)

    def _generate_detailed_user_reports(self, user_profiles: Dict):
        """Generate detailed individual user reports"""
        print("\n📊 Reportes detallados de usuarios:")

        # Sort users by risk score
        risk_scored_users = []
        for user, profile in user_profiles.items():
            if profile['message_count'] == 0:
                continue

            risk_score = self._calculate_user_risk_score(profile)
            risk_scored_users.append((user, profile, risk_score))

        risk_scored_users.sort(key=lambda x: x[2], reverse=True)

        # Top 10 users by activity
        print("\n🏆 Top 10 usuarios más activos:")
        active_users = sorted(user_profiles.items(), key=lambda x: x[1]['message_count'], reverse=True)[:10]

        for i, (user, profile) in enumerate(active_users, 1):
            avg_tox = profile['total_toxicity'] / profile['message_count'] if profile['message_count'] > 0 else 0
            avg_sentiment = statistics.mean(profile['sentiment_scores']) if profile['sentiment_scores'] else 0
            vocabulary_richness = len(profile['unique_words']) / profile['message_count'] if profile[
                                                                                                 'message_count'] > 0 else 0

            print(f"{i:2d}. 👤 {user}")
            print(f"    📨 Mensajes: {profile['message_count']}")
            print(f"    ☣️ Toxicidad promedio: {avg_tox:.3f}")
            print(f"    😊 Sentimiento promedio: {avg_sentiment:+.2f}")
            print(f"    📚 Riqueza vocabulario: {vocabulary_richness:.2f}")
            print(f"    🎯 Ratio engagement: {self._calculate_engagement_ratio(profile):.2f}")
            print()

        # High-risk users detailed analysis
        print("\n🚨 Usuarios de alto riesgo (análisis detallado):")
        high_risk_users = [u for u in risk_scored_users if u[2] > 0.6][:5]

        if not high_risk_users:
            print("✅ No se detectaron usuarios de alto riesgo")
        else:
            for user, profile, risk_score in high_risk_users:
                self._print_detailed_user_profile(user, profile, risk_score)

    def _print_detailed_user_profile(self, user: str, profile: Dict, risk_score: float):
        """Print comprehensive user profile"""
        msg_count = profile['message_count']

        # Calculate averages and metrics
        avg_tox = profile['total_toxicity'] / msg_count if msg_count > 0 else 0
        avg_spam = profile['total_spam'] / msg_count if msg_count > 0 else 0
        avg_length = statistics.mean(profile['message_lengths']) if profile['message_lengths'] else 0
        avg_sentiment = statistics.mean(profile['sentiment_scores']) if profile['sentiment_scores'] else 0
        avg_response_time = statistics.mean(profile['response_times']) if profile['response_times'] else 0

        caps_ratio = profile['caps_usage'] / msg_count if msg_count > 0 else 0
        vocabulary_richness = len(profile['unique_words']) / msg_count if msg_count > 0 else 0

        # Activity span
        activity_span = (profile['last_seen'] - profile['first_seen']).total_seconds() / 3600 if profile[
                                                                                                     'first_seen'] and \
                                                                                                 profile[
                                                                                                     'last_seen'] else 0
        messages_per_hour = msg_count / activity_span if activity_span > 0 else 0

        # Top categories
        top_categories = sorted(profile['categories'].items(), key=lambda x: x[1], reverse=True)[:3]
        category_summary = ', '.join(
            [f"{cat}({count})" for cat, count in top_categories]) if top_categories else "ninguna"

        print(f"🔴 {user} (Riesgo: {risk_score:.2f})")
        print(f"   📊 Actividad: {msg_count} msgs en {activity_span:.1f}h ({messages_per_hour:.1f} msgs/h)")
        print(f"   ⚠️ Moderación: {profile['requires_action_count']} acciones sugeridas")
        print(f"   ☣️ Toxicidad: {avg_tox:.3f} | 📢 Spam: {avg_spam:.3f}")
        print(f"   😊 Sentimiento: {avg_sentiment:+.2f} | 📝 Long. promedio: {avg_length:.1f}")
        print(f"   🔤 Mayúsculas: {caps_ratio:.2f} | 📚 Vocabulario: {vocabulary_richness:.2f}")
        print(f"   ⚡ Tiempo respuesta: {avg_response_time:.1f}s")
        print(f"   🎭 Emotes: {profile['emote_usage']} | 💬 Menciones: {profile['mention_count']}")
        print(f"   🏷️ Categorías: {category_summary}")

        # Behavioral flags
        flags = []
        if caps_ratio > 0.3:
            flags.append("EXCESO_MAYUSCULAS")
        if avg_response_time < 2:
            flags.append("RESPUESTAS_RAPIDAS")
        if profile['repeated_chars'] / msg_count > 2:
            flags.append("SPAM_CARACTERES")
        if vocabulary_richness < 0.5:
            flags.append("VOCABULARIO_POBRE")
        if messages_per_hour > 30:
            flags.append("HIPERACTIVIDAD")

        if flags:
            print(f"   🚩 Banderas: {', '.join(flags)}")
        print()

    def _calculate_user_risk_score(self, profile: Dict) -> float:
        """Calculate comprehensive user risk score"""
        if profile['message_count'] == 0:
            return 0.0

        msg_count = profile['message_count']

        # Toxicity and spam scores (0-1)
        toxicity_score = min(profile['total_toxicity'] / msg_count, 1.0)
        spam_score = min(profile['total_spam'] / msg_count, 1.0)

        # Behavioral risk factors
        caps_ratio = min(profile['caps_usage'] / msg_count, 1.0)
        repeated_chars_ratio = min(profile['repeated_chars'] / (msg_count * 10), 1.0)

        # Action requirement ratio
        action_ratio = profile['requires_action_count'] / msg_count

        # Vocabulary diversity (lower = higher risk)
        vocab_diversity = len(profile['unique_words']) / msg_count if msg_count > 0 else 0
        vocab_risk = max(0, 1 - vocab_diversity) if vocab_diversity < 2 else 0

        # Sentiment volatility
        sentiment_volatility = 0
        if len(profile['sentiment_scores']) > 2:
            sentiment_volatility = statistics.stdev(profile['sentiment_scores']) / 2  # Normalize

        # Combined risk score
        risk_score = (
                toxicity_score * 0.25 +
                spam_score * 0.20 +
                action_ratio * 0.20 +
                caps_ratio * 0.10 +
                repeated_chars_ratio * 0.10 +
                vocab_risk * 0.10 +
                sentiment_volatility * 0.05
        )

        return min(risk_score, 1.0)

    def _calculate_engagement_ratio(self, profile: Dict) -> float:
        """Calculate user engagement ratio"""
        if profile['message_count'] == 0:
            return 0.0

        engagement_score = (
                                   profile['conversation_starters'] * 2 +
                                   profile['replies_to_others'] +
                                   profile['question_count'] +
                                   profile['mention_count']
                           ) / profile['message_count']

        return min(engagement_score, 5.0)  # Cap at 5.0

    def analyze_engagement_patterns(self, messages: List[Dict]):
        """Analyze chat engagement and interaction patterns"""
        print("🎯 Análisis de patrones de engagement...")

        engagement_windows = defaultdict(lambda: {
            'message_count': 0,
            'unique_users': set(),
            'questions': 0,
            'mentions': 0,
            'emotes': 0,
            'avg_length': 0,
            'total_length': 0
        })

        # Analyze in 5-minute windows
        for msg in messages:
            if msg['username'].lower() == self.streamer_username.lower():
                continue

            text = msg.get('text', '')
            timestamp = msg['timestamp']
            window_key = timestamp.replace(minute=(timestamp.minute // 5) * 5, second=0, microsecond=0)

            window = engagement_windows[window_key]
            window['message_count'] += 1
            window['unique_users'].add(msg['username'])
            window['questions'] += text.count('?')
            window['mentions'] += len(re.findall(r'@\w+', text))
            window['emotes'] += len(re.findall(r':\w+:', text))
            window['total_length'] += len(text)

        # Calculate metrics for each window
        peak_engagement = 0
        peak_time = None
        total_engagement = 0

        print("📊 Ventanas de engagement (5 min):")
        for window_time in sorted(engagement_windows.keys()):
            window = engagement_windows[window_time]
            users_count = len(window['unique_users'])
            msg_count = window['message_count']

            if msg_count > 0:
                avg_length = window['total_length'] / msg_count
                engagement_score = (
                        users_count * 2 +
                        window['questions'] +
                        window['mentions'] +
                        min(msg_count / 10, 5)  # Message density bonus
                )

                total_engagement += engagement_score

                if engagement_score > peak_engagement:
                    peak_engagement = engagement_score
                    peak_time = window_time

                if engagement_score > 10:  # High engagement threshold
                    print(f"  🔥 {window_time.strftime('%H:%M')}: "
                          f"{users_count} usuarios, {msg_count} msgs, "
                          f"Score: {engagement_score:.1f}")

        if peak_time:
            print(f"\n🏆 Pico de engagement: {peak_time.strftime('%H:%M')} (Score: {peak_engagement:.1f})")

        avg_engagement = total_engagement / len(engagement_windows) if engagement_windows else 0
        print(f"📈 Engagement promedio: {avg_engagement:.1f}")

    def analyze_spam_patterns(self, messages: List[Dict]):
        """Detect and analyze spam patterns"""
        print("🔍 Análisis de patrones de spam...")

        spam_indicators = {
            'repeated_messages': defaultdict(list),
            'rapid_posting': defaultdict(list),
            'excessive_caps': [],
            'url_spam': [],
            'repeated_characters': [],
            'copy_paste': defaultdict(list)
        }

        user_message_times = defaultdict(list)

        for msg in messages:
            user = msg['username']
            text = msg.get('text', '')
            timestamp = msg['timestamp']

            if user.lower() == self.streamer_username.lower():
                continue

            user_message_times[user].append(timestamp)

            # Repeated messages detection
            text_normalized = re.sub(r'\s+', ' ', text.lower().strip())
            spam_indicators['repeated_messages'][text_normalized].append((user, timestamp))

            # Excessive caps
            if len(text) > 10:
                caps_ratio = sum(1 for c in text if c.isupper()) / len(text)
                if caps_ratio > 0.7:
                    spam_indicators['excessive_caps'].append((user, text, caps_ratio))

            # URL spam
            urls = re.findall(r'http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\\(\\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+', text)
            if urls:
                spam_indicators['url_spam'].append((user, text, len(urls)))

            # Repeated characters
            repeated_pattern = re.findall(r'(.)\1{4,}', text)
            if repeated_pattern:
                spam_indicators['repeated_characters'].append((user, text, len(repeated_pattern)))

        # Rapid posting detection
        for user, timestamps in user_message_times.items():
            if len(timestamps) >= 5:
                timestamps.sort()
                for i in range(len(timestamps) - 4):
                    window = timestamps[i + 4] - timestamps[i]
                    if window.total_seconds() < 30:  # 5 messages in 30 seconds
                        spam_indicators['rapid_posting'][user].append({
                            'start_time': timestamps[i],
                            'message_count': 5,
                            'duration': window.total_seconds()
                        })

        # Report findings
        self._report_spam_findings(spam_indicators)

    def _report_spam_findings(self, spam_indicators: Dict):
        """Report spam detection findings"""

        # Repeated messages
        repeated_msgs = {k: v for k, v in spam_indicators['repeated_messages'].items() if len(v) > 2}
        if repeated_msgs:
            print("🔁 Mensajes repetidos detectados:")
            for text, occurrences in list(repeated_msgs.items())[:5]:
                users = set(user for user, _ in occurrences)
                print(f"  \"{text[:50]}...\" - {len(occurrences)} veces por {len(users)} usuarios")

        # Rapid posting
        if spam_indicators['rapid_posting']:
            print("⚡ Posting rápido detectado:")
            for user, incidents in spam_indicators['rapid_posting'].items():
                for incident in incidents[:2]:  # Show max 2 per user
                    print(f"  👤 {user}: 5 msgs en {incident['duration']:.1f}s "
                          f"({incident['start_time'].strftime('%H:%M:%S')})")

        # Excessive caps
        if spam_indicators['excessive_caps']:
            print("📢 Abuso de mayúsculas:")
            for user, text, ratio in spam_indicators['excessive_caps'][:5]:
                print(f"  👤 {user}: {ratio:.1%} mayúsculas - \"{text[:30]}...\"")

        # URL spam
        if spam_indicators['url_spam']:
            print("🔗 Posible spam de URLs:")
            for user, text, url_count in spam_indicators['url_spam'][:5]:
                print(f"  👤 {user}: {url_count} URLs en mensaje")

    def generate_moderation_metrics(self, messages: List[Dict]):
        """Generate comprehensive moderation metrics and recommendations"""
        print("📈 Métricas de moderación y recomendaciones...")

        total_messages = len([m for m in messages if m['username'].lower() != self.streamer_username.lower()])
        if total_messages == 0:
            return

        # Analyze all messages for moderation metrics
        moderation_stats = {
            'total_analyzed': 0,
            'high_toxicity': 0,
            'high_spam': 0,
            'requires_action': 0,
            'categories': defaultdict(int),
            'sentiment_distribution': defaultdict(int),
            'hourly_violations': defaultdict(int)
        }

        for msg in messages:
            if msg['username'].lower() == self.streamer_username.lower():
                continue

            try:
                analysis = self.analyzer.analyze_message(msg)
                moderation_stats['total_analyzed'] += 1

                if analysis.get('toxicity_score', 0) > 0.7:
                    moderation_stats['high_toxicity'] += 1
                if analysis.get('spam_probability', 0) > 0.7:
                    moderation_stats['high_spam'] += 1
                if analysis.get('requires_action', False):
                    moderation_stats['requires_action'] += 1
                    moderation_stats['hourly_violations'][msg['timestamp'].hour] += 1

                for category in analysis.get('categories', []):
                    moderation_stats['categories'][category] += 1

                sentiment = analysis.get('sentiment', 'neutral')
                moderation_stats['sentiment_distribution'][sentiment] += 1

            except Exception:
                continue

        # Generate metrics report
        print(f"📊 Resumen de moderación:")
        print(f"  📝 Mensajes analizados: {moderation_stats['total_analyzed']:,}")
        print(f"  ⚠️ Requieren acción: {moderation_stats['requires_action']} "
              f"({moderation_stats['requires_action'] / moderation_stats['total_analyzed'] * 100:.1f}%)")
        print(f"  ☣️ Alta toxicidad: {moderation_stats['high_toxicity']} "
              f"({moderation_stats['high_toxicity'] / moderation_stats['total_analyzed'] * 100:.1f}%)")
        print(f"  📢 Alto spam: {moderation_stats['high_spam']} "
              f"({moderation_stats['high_spam'] / moderation_stats['total_analyzed'] * 100:.1f}%)")

        # Sentiment distribution
        print(f"\n😊 Distribución de sentimiento:")
        for sentiment, count in moderation_stats['sentiment_distribution'].items():
            percentage = count / moderation_stats['total_analyzed'] * 100
            print(f"  {sentiment.capitalize()}: {count} ({percentage:.1f}%)")

        # Problem categories
        if moderation_stats['categories']:
            print(f"\n🏷️ Categorías problemáticas:")
            top_categories = sorted(moderation_stats['categories'].items(),
                                    key=lambda x: x[1], reverse=True)[:5]
            for category, count in top_categories:
                percentage = count / moderation_stats['total_analyzed'] * 100
                print(f"  {category}: {count} ({percentage:.1f}%)")

        # Hourly violation patterns
        if moderation_stats['hourly_violations']:
            print(f"\n⏰ Patrones horarios de violaciones:")
            for hour in sorted(moderation_stats['hourly_violations'].keys()):
                count = moderation_stats['hourly_violations'][hour]
                print(f"  {hour:02d}:00 - {count} violaciones")

        # Generate recommendations
        self._generate_moderation_recommendations(moderation_stats, total_messages)

    def _generate_moderation_recommendations(self, stats: Dict, total_messages: int):
        """Generate actionable moderation recommendations"""
        print(f"\n💡 Recomendaciones de moderación:")

        violation_rate = stats['requires_action'] / stats['total_analyzed'] if stats['total_analyzed'] > 0 else 0

        if violation_rate > 0.15:
            print("  🚨 ALTA PRIORIDAD: Tasa de violaciones muy alta (>15%)")
            print("     - Considera activar modo slow")
            print("     - Refuerza las reglas del chat")
            print("     - Aumenta la presencia de moderadores")
        elif violation_rate > 0.08:
            print("  ⚠️ MEDIA PRIORIDAD: Tasa de violaciones moderada (>8%)")
            print("     - Monitorea más frecuentemente")
            print("     - Considera recordatorios de reglas")
        else:
            print("  ✅ BAJA PRIORIDAD: Tasa de violaciones aceptable (<8%)")

        # Specific recommendations based on categories
        top_categories = sorted(stats['categories'].items(), key=lambda x: x[1], reverse=True)
        if top_categories:
            main_issue = top_categories[0][0]
            if main_issue in ['harassment', 'hate_speech']:
                print("     - Priorizar moderación de acoso/odio")
                print("     - Considerar timeout más largos")
            elif main_issue == 'spam':
                print("     - Activar filtros anti-spam")
                print("     - Modo followers-only temporal")
            elif main_issue == 'off_topic':
                print("     - Recordar el tema del stream")
                print("     - Redirigir conversaciones off-topic")

        # Time-based recommendations
        if stats['hourly_violations']:
            peak_hour = max(stats['hourly_violations'], key=stats['hourly_violations'].get)
            peak_violations = stats['hourly_violations'][peak_hour]
            if peak_violations > stats['requires_action'] * 0.3:
                print(f"     - Reforzar moderación a las {peak_hour:02d}:00 (pico de violaciones)")

    # Helper methods
    def _sentiment_to_score(self, sentiment: str) -> float:
        """Convert sentiment to numerical score"""
        mapping = {"positive": 1.0, "neutral": 0.0, "negative": -1.0}
        return mapping.get(sentiment.lower(), 0.0)

    def _count_caps_ratio(self, text: str) -> float:
        """Calculate ratio of capital letters"""
        if not text:
            return 0.0
        letters = [c for c in text if c.isalpha()]
        if not letters:
            return 0.0
        return sum(1 for c in letters if c.isupper()) / len(letters)

    def _count_repeated_chars(self, text: str) -> int:
        """Count instances of repeated characters"""
        return len(re.findall(r'(.)\1{2,}', text))

    def _is_conversation_starter(self, text: str, prev_messages: List) -> bool:
        """Determine if message is a conversation starter"""
        if not prev_messages:
            return True

        # Check if there's a significant gap
        if len(prev_messages) > 0:
            last_time = prev_messages[-1]['timestamp']
            current_time = datetime.now()  # This should be passed as parameter
            if (current_time - last_time).total_seconds() > 300:  # 5 minutes
                return True

        # Check for question or greeting patterns
        starters = ['hello', 'hi', 'hey', 'what', 'how', 'why', 'when', 'where']
        return any(starter in text.lower() for starter in starters)

    def _is_reply_to_others(self, text: str, recent_messages: List) -> bool:
        """Determine if message is a reply to others"""
        # Check for mentions or response patterns
        if '@' in text:
            return True

        # Check for common reply patterns
        reply_patterns = ['yes', 'no', 'agree', 'disagree', 'exactly', 'true', 'false']
        return any(pattern in text.lower() for pattern in reply_patterns)

    def analyze_emotional_drift(self, messages: List[Dict]):
        """Analyze emotional patterns and drift over time with advanced metrics"""
        print("🎭 Analizando deriva emocional del chat...")

        sentiment_timeline = []
        user_trajectories = defaultdict(list)
        toxicity_timeline = []

        # Collect sentiment and toxicity data
        for msg in messages:
            if msg['username'].lower() == self.streamer_username.lower():
                continue

            try:
                analysis = self.analyzer.analyze_message(msg)
                sentiment_score = self._sentiment_to_score(analysis.get('sentiment', 'neutral'))
                toxicity = analysis.get('toxicity_score', 0.0)

                data_point = {
                    'timestamp': msg['timestamp'],
                    'sentiment': sentiment_score,
                    'toxicity': toxicity,
                    'user': msg['username'],
                    'text_length': len(msg.get('text', '')),
                    'requires_action': analysis.get('requires_action', False)
                }

                sentiment_timeline.append(data_point)
                toxicity_timeline.append(data_point)
                user_trajectories[msg['username']].append(data_point)

            except Exception as e:
                continue

        if len(sentiment_timeline) < 5:
            print("⚠️ Datos insuficientes para análisis emocional")
            return

        # Comprehensive emotional analysis
        self._analyze_global_emotional_trend(sentiment_timeline)
        self._analyze_emotional_volatility(sentiment_timeline)
        self._analyze_user_emotional_trajectories(user_trajectories)
        self._detect_emotional_contagion(sentiment_timeline)
        self._analyze_toxicity_patterns(toxicity_timeline)
        self._detect_emotional_tipping_points(sentiment_timeline)

    def analyze_user_behavior_patterns(self, messages: List[Dict]):
        """Analyze individual user behavior patterns"""
        print("📈 Análisis avanzado de comportamiento de usuarios...")

        user_stats = defaultdict(lambda: {
            'message_count': 0,
            'total_toxicity': 0.0,
            'total_spam': 0.0,
            'categories': defaultdict(int),
            'requires_action_count': 0,
            'messages': []
        })

        # Analyze each message
        for msg in messages:
            user = msg['username']

            try:
                analysis = self.analyzer.analyze_message(msg)
            except Exception as e:
                continue

            user_stats[user]['message_count'] += 1
            user_stats[user]['total_toxicity'] += analysis.get('toxicity_score', 0.0)
            user_stats[user]['total_spam'] += analysis.get('spam_probability', 0.0)
            user_stats[user]['requires_action_count'] += int(analysis.get('requires_action', False))

            for cat in analysis.get('categories', []):
                user_stats[user]['categories'][cat] += 1

        # Rank by risk
        flagged_users = sorted(user_stats.items(), key=lambda x: (
            x[1]['requires_action_count'],
            x[1]['total_toxicity'] / max(x[1]['message_count'], 1)
        ), reverse=True)

        self._print_user_risk_report(flagged_users[:5])

    def run_advanced_analysis(self, messages: List[Dict]) -> None:
        """Realiza análisis avanzados de todos los mensajes"""
        print("\n🧠 Realizando análisis avanzado de", len(messages), "mensajes...")
        self.analyze_user_behavior(messages)
        self.analyze_global_context(messages)
        self.analyze_temporal_patterns(messages)
        self.detect_brigading(messages)

    def analyze_user_behavior(self, messages: List[Dict]) -> None:
        """Analiza el comportamiento individual de los usuarios"""
        print("\n📈 Análisis avanzado de comportamiento de usuarios...")
        user_stats = {}

        for msg in messages:
            username = msg.get("username", "desconocido")
            if username not in user_stats:
                user_stats[username] = {
                    "count": 0,
                    "toxicity_sum": 0.0,
                    "spam_sum": 0.0,
                    "categories": set()
                }
            user_stats[username]["count"] += 1
            user_stats[username]["toxicity_sum"] += msg.get("toxicity", 0.0)
            user_stats[username]["spam_sum"] += msg.get("spam", 0.0)
            user_stats[username]["categories"].update(msg.get("categories", []))

        flagged_users = []

        for username, data in user_stats.items():
            count = data["count"]
            avg_tox = data["toxicity_sum"] / count if count else 0
            avg_spam = data["spam_sum"] / count if count else 0

            if avg_tox > 0.7 or avg_spam > 0.7 or count > 100:
                flagged_users.append((username, count, avg_tox, avg_spam))

        if flagged_users:
            print("🚨 Usuarios con posible conducta disruptiva:")
            for user in flagged_users:
                print(f"👤 {user[0]}\n   📨 Mensajes: {user[1]} | ⚠️ Acciones sugeridas: 0\n"
                      f"   ☣️ Toxicidad media: {user[2]:.2f} | 📢 Spam medio: {user[3]:.2f}")

    def analyze_global_context(self, messages: List[Dict]):
        """Analyze global chat context and statistics"""
        print("🌐 Analizando contexto global del chat...")

        total_users = len(set(msg['username'] for msg in messages))
        total_messages = len(messages)

        if messages:
            timestamps = [msg['timestamp'] for msg in messages]
            time_span = max(timestamps) - min(timestamps)
            messages_per_minute = total_messages / max(time_span.total_seconds() / 60, 1)

            print(f"📊 Estadísticas globales:")
            print(f"  👥 Usuarios únicos: {total_users}")
            print(f"  💬 Total mensajes: {total_messages}")
            print(f"  ⏱️ Duración: {time_span}")
            print(f"  📈 Mensajes/minuto: {messages_per_minute:.1f}")

        # User activity distribution
        user_message_counts = Counter(msg['username'] for msg in messages)
        most_active = user_message_counts.most_common(5)

        print(f"🏆 Usuarios más activos:")
        for user, count in most_active:
            percentage = (count / total_messages) * 100
            print(f"  👤 {user}: {count} mensajes ({percentage:.1f}%)")

    def analyze_temporal_trends(self, messages: List[Dict]):
        """Analyze temporal patterns in chat behavior"""
        print("📅 Analizando patrones temporales...")

        hourly_stats = defaultdict(lambda: {'count': 0, 'toxicity': 0, 'users': set()})

        for msg in messages:
            hour = msg['timestamp'].hour

            try:
                analysis = self.analyzer.analyze_message(msg)
                hourly_stats[hour]['count'] += 1
                hourly_stats[hour]['toxicity'] += analysis.get('toxicity_score', 0)
                hourly_stats[hour]['users'].add(msg['username'])
            except Exception:
                continue

        if hourly_stats:
            print("📊 Actividad por hora:")
            for hour in sorted(hourly_stats.keys()):
                stats = hourly_stats[hour]
                avg_toxicity = stats['toxicity'] / stats['count'] if stats['count'] > 0 else 0
                print(f"  {hour:02d}:00 - Mensajes: {stats['count']}, "
                      f"Usuarios únicos: {len(stats['users'])}, "
                      f"Toxicidad promedio: {avg_toxicity:.2f}")

    def detect_brigading(self, messages: List[Dict]) -> None:
        """Detecta posibles ataques de brigading por llegada repentina de múltiples usuarios"""

        print("\n🚨 Buscando indicios de brigading...")

        from collections import defaultdict
        import datetime

        user_first_message: Dict[str, str] = {}
        for msg in messages:
            user = msg.get("username")
            ts = msg.get("timestamp")
            if user and user not in user_first_message:
                user_first_message[user] = ts

        timestamps = list(user_first_message.values())

        # ✅ Protección robusta contra errores de tipo
        parsed_timestamps = []
        for t in timestamps:
            if isinstance(t, str):
                try:
                    parsed_timestamps.append(datetime.datetime.fromisoformat(t))
                except ValueError:
                    print(f"⚠️ Timestamp inválido: {t}")
            elif isinstance(t, datetime.datetime):
                parsed_timestamps.append(t)
            else:
                print(f"⚠️ Tipo de timestamp no soportado: {t}")

        timestamps = sorted(parsed_timestamps)

        if len(timestamps) < 5:
            print("⚠️ Muy pocos datos para detectar brigading.")
            return

        # Análisis temporal
        deltas = [(t2 - t1).total_seconds() for t1, t2 in zip(timestamps[:-1], timestamps[1:])]
        bursts = [d for d in deltas if d < 10]

        if len(bursts) > 3:
            print(f"🚨 Brigading sospechado: {len(bursts)} usuarios nuevos en ráfagas de <10s")
        else:
            print("✅ Sin indicios claros de brigading.")

    def detect_potential_brigading(self, messages: List[Dict]):
        """Detect potential coordinated harassment or brigading"""
        print("🕵️ Detectando posible brigading...")

        user_first_message = {}
        for msg in messages:
            user = msg['username']
            if user not in user_first_message:
                user_first_message[user] = msg['timestamp']

        time_sorted = sorted(user_first_message.items(), key=lambda x: x[1])

        if len(time_sorted) >= 5:
            window_minutes = 10
            suspicious_windows = []

            for i, (user, timestamp) in enumerate(time_sorted):
                window_start = timestamp - timedelta(minutes=window_minutes)
                users_in_window = sum(1 for _, ts in time_sorted[max(0, i - 20):i + 20]
                                      if window_start <= ts <= timestamp + timedelta(minutes=window_minutes))

                if users_in_window >= 4:
                    suspicious_windows.append((timestamp, users_in_window))

            if suspicious_windows:
                print("🚨 Posible brigading detectado:")
                for timestamp, count in suspicious_windows[:3]:
                    print(f"  ⏰ {timestamp.strftime('%H:%M')}: {count} usuarios nuevos en ventana de 10min")
            else:
                print("✅ No se detectaron patrones de brigading")

    def analyze_temporal_patterns(self, messages: List[Dict]) -> None:
        """Analiza patrones temporales en el flujo de mensajes"""
        print("\n⏱️ Analizando patrones temporales...")

        timestamps = []
        for m in messages:
            ts = m.get('timestamp')
            if isinstance(ts, str):
                try:
                    timestamps.append(datetime.fromisoformat(ts))
                except ValueError:
                    print(f"⚠️ Formato inválido de timestamp: {ts}")
            elif isinstance(ts, datetime):
                timestamps.append(ts)
            else:
                print(f"⚠️ Timestamp no reconocido o ausente: {ts}")

        if not timestamps:
            print("⚠️ No se encontraron marcas temporales válidas.")
            return

        # Agrupar por minuto
        minute_buckets = [ts.replace(second=0, microsecond=0) for ts in timestamps]
        counts = Counter(minute_buckets)

        most_active_minute, max_count = counts.most_common(1)[0]
        print(f"📈 Pico de actividad: {most_active_minute.strftime('%H:%M')} con {max_count} mensajes")

        if max_count > 10:
            print("⚠️ Posible raid o burst de actividad detectado.")

        # Detectar minutos sin actividad
        sorted_times = sorted(counts)
        inactivity_periods = 0
        for t1, t2 in zip(sorted_times[:-1], sorted_times[1:]):
            if (t2 - t1).total_seconds() > 120:
                inactivity_periods += 1

        if inactivity_periods > 0:
            print(f"🛑 Periodos de inactividad detectados: {inactivity_periods}")

    def _analyze_emotional_volatility(self, sentiment_timeline: List[float]) -> None:
        """Analiza la volatilidad emocional (cambios bruscos en sentimiento)"""
        if not sentiment_timeline or len(sentiment_timeline) < 2:
            print("📉 Volatilidad emocional: datos insuficientes.")
            return

        # Cálculo de variaciones sucesivas
        diffs = [abs(b - a) for a, b in zip(sentiment_timeline[:-1], sentiment_timeline[1:])]
        max_jump = max(diffs)
        avg_jump = sum(diffs) / len(diffs)

        print(f"📊 Volatilidad emocional:")
        print(f"   🔼 Mayor salto entre mensajes: {max_jump:.2f}")
        print(f"   📉 Variación media entre mensajes: {avg_jump:.2f}")

        if max_jump > 0.8:
            print("⚠️ Posible alteración emocional abrupta detectada.")
        elif avg_jump > 0.4:
            print("⚠️ Alta inestabilidad emocional sostenida en el chat.")

    def _sentiment_to_score(self, sentiment: str) -> float:
        """Convert sentiment to numerical score"""
        mapping = {"positive": 1.0, "neutral": 0.0, "negative": -1.0}
        return mapping.get(sentiment, 0.0)

    def _analyze_global_emotional_trend(self, timeline: List[Dict]):
        """Analyze overall emotional trend"""
        if len(timeline) < 10:
            return

        window_size = 10
        moving_avg = []

        for i in range(len(timeline) - window_size + 1):
            window = timeline[i:i + window_size]
            avg_sentiment = sum(p['sentiment'] for p in window) / len(window)
            moving_avg.append(avg_sentiment)

        if len(moving_avg) >= 3:
            recent_sentiment = sum(moving_avg[-3:]) / 3
            early_sentiment = sum(moving_avg[:3]) / 3
            trend = recent_sentiment - early_sentiment

            print(f"📈 Tendencia emocional global: {trend:+.2f}")
            if trend < -0.3:
                print("🚨 ALERTA: Deterioro emocional significativo detectado")
            elif trend > 0.3:
                print("✨ Mejora emocional positiva detectada")

    def _analyze_user_emotional_trajectories(self, trajectories: Dict):
        """Analyze individual user emotional patterns"""
        concerning_users = []

        for user, trajectory in trajectories.items():
            if len(trajectory) < 3:
                continue

            sentiments = [p['sentiment'] for p in trajectory]
            volatility = sum(abs(sentiments[i] - sentiments[i - 1])
                             for i in range(1, len(sentiments))) / (len(sentiments) - 1)

            if len(sentiments) >= 3:
                recent_avg = sum(sentiments[-3:]) / 3
                early_avg = sum(sentiments[:3]) / 3
                trend = recent_avg - early_avg

                if volatility > 1.0 or trend < -0.5:
                    concerning_users.append({
                        'user': user,
                        'volatility': volatility,
                        'trend': trend,
                        'message_count': len(trajectory)
                    })

        if concerning_users:
            print(f"⚠️ Usuarios con patrones emocionales preocupantes:")
            for user_data in sorted(concerning_users, key=lambda x: x['volatility'], reverse=True)[:3]:
                print(f"  👤 @{user_data['user']}: Volatilidad={user_data['volatility']:.2f}, "
                      f"Tendencia={user_data['trend']:+.2f}")

    def _detect_emotional_contagion(self, timeline: List[Dict]):
        """Detect potential emotional contagion patterns"""
        window_minutes = 2
        time_windows = defaultdict(list)

        for entry in timeline:
            window_key = entry['timestamp'].replace(second=0, microsecond=0)
            window_key = window_key.replace(minute=(window_key.minute // window_minutes) * window_minutes)
            time_windows[window_key].append(entry)

        contagion_events = 0
        for window_time, entries in time_windows.items():
            if len(entries) < 4:
                continue

            negative_burst = sum(1 for e in entries if e['sentiment'] < -0.5)
            if negative_burst >= 3:
                contagion_events += 1
                unique_users = len(set(e['user'] for e in entries))
                print(f"🦠 Posible contagio emocional a las {window_time.strftime('%H:%M')}: "
                      f"{negative_burst} mensajes negativos de {unique_users} usuarios")

        if contagion_events >= 2:
            print(f"🚨 PATRÓN DE CONTAGIO DETECTADO: {contagion_events} eventos de propagación emocional")

    def _print_user_risk_report(self, flagged_users: List):
        """Print user risk analysis report"""
        print("🚨 Usuarios con posible conducta disruptiva:")
        for user, stats in flagged_users:
            if stats['message_count'] == 0:
                continue

            avg_tox = stats['total_toxicity'] / stats['message_count']
            avg_spam = stats['total_spam'] / stats['message_count']
            top_cats = sorted(stats['categories'].items(), key=lambda x: x[1], reverse=True)
            category_summary = ', '.join([f"{c} ({n})" for c, n in top_cats[:3]]) or "ninguna"

            print(f"👤 {user}")
            print(f"   📨 Mensajes: {stats['message_count']} | ⚠️ Acciones sugeridas: {stats['requires_action_count']}")
            print(f"   ☣️ Toxicidad media: {avg_tox:.2f} | 📢 Spam medio: {avg_spam:.2f}")
            print(f"   🏷️ Categorías comunes: {category_summary}")
            print()
