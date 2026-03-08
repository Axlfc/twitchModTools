# twitchModTools

Herramientas para moderación en Twitch — guía rápida para moderadores

Este toolkit nació de la experiencia como moderador/a en canales donde la protección emocional y técnica de la streamer es prioritaria. Está diseñado para que moderadores y equipos de confianza puedan usar, entender y mantener las herramientas con confianza.

Tabla de contenidos
- Introducción
- Inicio rápido (para mods)
- Resumen de herramientas y uso práctico
- Flujo típico de moderación
- Configuración y resolución de problemas
- Contribuir y contacto

**Inicio rápido (para moderadores)**

Requisitos mínimos:
- Python 3.10+ instalado
- Acceso al repositorio y permisos para editar `config.py`

Instalación (desde la raíz del repo):

```
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

Cómo ejecutar (modo básico):

```
python main.py
```

Si solo quieres ejecutar el moderador en tiempo real:

```
python realtime_moderator.py
```

Nota rápida: la mayoría de opciones se ajustan en `config.py`. Cambios en la configuración requieren reiniciar el servicio.

Resumen de las herramientas (qué hacen y cómo puede usarlas un/a mod)

- `main.py`: Punto de entrada. Inicia servicios dependientes y orquesta componentes.
	- Uso para mods: normalmente no hace falta editar; ejecutarlo arranca todo el stack.
- `realtime_moderator.py`: Módulo para moderación en tiempo real.
	- Uso: supervisa mensajes en vivo y aplica reglas inmediatas.
- `moderador_semantico.py`: Moderación basada en semántica (detección de discurso tóxico, acoso, contenido sensible).
	- Uso: útil para marcar mensajes que necesitan revisión humana o bloqueo automático según políticas.
- `twitch_fraud_detector.py`: Detecta patrones de fraude en chat (bots, enlaces maliciosos, patrones de spam).
	- Uso: activa para reducir raids de bots y enlaces sospechosos.
- `message_processor.py`: Canaliza y procesa cada mensaje entrante antes de pasar a detectores.
	- Uso: punto a revisar si mensajes no se procesan correctamente.
- `analyzer_ollama.py`: Integra modelos de lenguaje para análisis semántico (si está configurado).
	- Uso: complementario a `moderador_semantico` para mayor precisión.
- `alert_manager.py`: Envía notificaciones (logs, avisos a mods, alertas externas).
	- Uso: revisa para ver cómo se entregan las alertas (Discord, webhook, correo).
- `deduplication_manager.py`: Evita acciones duplicadas sobre el mismo evento.
	- Uso: útil para evitar múltiples sanciones por el mismo mensaje.
- `session_tracker.py`: Rastrea sesiones y actividad de usuarios.
	- Uso: investigar comportamiento repetido de un usuario.
- `database_manager.py`: Gestión y acceso a almacenamiento local/DB.
	- Uso: ver historial de sanciones y registros.
- `vector_store_qdrant.py`: Maneja embeddings y búsquedas semánticas (si está habilitado).
- `utils.py` y `config.py`: Utilidades y configuración central. Edita `config.py` para activar/desactivar módulos.
- `pop3/EmailBackupClient.py` y `pop3/EmailBackupGUI.py`: Herramientas para respaldos de correo (opcional).

Flujo típico de moderación (resumen)

1. El mensaje entra y pasa por `message_processor.py`.
2. Se ejecutan detectores: `twitch_fraud_detector`, `moderador_semantico`, análisis de deduplicación.
3. Si un detector marca el mensaje, se genera una acción (alerta, borrado, timeout, ban) o una etiqueta para revisión humana.
4. `alert_manager` notifica al equipo y `database_manager` guarda el evento.

Consejos prácticos para mods
- Para desactivar temporalmente una regla, edita `config.py` y reinicia el servicio.
- Usa los logs (revisa la salida de `main.py` o archivos de log configurados) para entender por qué una acción fue tomada.
- Si un falso positivo ocurre, añade una excepción o ajusta los umbrales en `config.py`.

Resolución de problemas comunes
- "No aparecen alertas": confirma que `alert_manager` está habilitado y que las credenciales/webhooks estén configurados en `config.py`.
- "Mensajes no moderados": verifica que `realtime_moderator.py` esté en ejecución y que la conexión con la API de Twitch esté activa.
- "Demasiados falsos positivos": reduce sensibilidad en `moderador_semantico` o desactiva temporalmente `analyzer_ollama`.

Contribuir
- Para correcciones rápidas, abre un pull request con cambios claros en `config.py` o documentación.
- Si añades una nueva regla o detector, documenta su propósito y cómo probarlo.

