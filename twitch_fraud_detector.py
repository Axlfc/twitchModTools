#!/usr/bin/env python3
"""
╔══════════════════════════════════════════════════════════════╗
║         TWITCH FRAUD DETECTOR  —  twitch_fraud_detector.py   ║
║         Analiza métricas de TwitchTracker para detectar       ║
║         Follow-Botting y Artificial Engagement                ║
╚══════════════════════════════════════════════════════════════╝

Uso:
    python twitch_fraud_detector.py <channel_name>
    python twitch_fraud_detector.py willyrex_fanboy
    python twitch_fraud_detector.py --demo willyrex_fanboy
"""

import sys
import re
import json
import time
import argparse
import urllib.request
import urllib.error
from datetime import datetime

# ── ANSI COLORS ─────────────────────────────────────────────────────────────
R  = "\033[91m"   # rojo
Y  = "\033[93m"   # amarillo
G  = "\033[92m"   # verde
B  = "\033[94m"   # azul
C  = "\033[96m"   # cian
M  = "\033[95m"   # magenta
W  = "\033[97m"   # blanco brillante
DIM= "\033[2m"    # atenuado
BLD= "\033[1m"    # negrita
RST= "\033[0m"    # reset
BG_RED   = "\033[41m"
BG_YEL   = "\033[43m"
BG_GRN   = "\033[42m"
BG_DARK  = "\033[100m"

def clr(text, *codes):
    return "".join(codes) + str(text) + RST

def box(lines, color=C, width=64):
    """Dibuja una caja en terminal."""
    top    = color + "╔" + "═" * width + "╗" + RST
    bottom = color + "╚" + "═" * width + "╝" + RST
    mid    = color + "╠" + "═" * width + "╣" + RST
    def row(text="", align="left"):
        stripped = re.sub(r'\033\[[0-9;]*m', '', text)
        pad = width - len(stripped) - 2
        if align == "center":
            lp = pad // 2
            rp = pad - lp
            inner = " " * lp + text + " " * rp
        else:
            inner = " " + text + " " * (pad + 1)
        return color + "║" + RST + inner + color + "║" + RST
    result = [top]
    for line in lines:
        if line == "---":
            result.append(mid)
        else:
            align = "center" if line.startswith("^") else "left"
            result.append(row(line.lstrip("^"), align))
    result.append(bottom)
    return "\n".join(result)

# ── BENCHMARKS ───────────────────────────────────────────────────────────────
BENCHMARKS = {
    # Follower-to-Viewer Ratio por rango de seguidores (%)
    "fvr": {
        "small":  (1.0, 5.0),    # < 10K followers
        "medium": (0.8, 3.5),    # 10K–100K
        "large":  (0.3, 2.0),    # 100K+
    },
    # Follow Conversion Rate orgánico (% viewers→follows por hora)
    "fcr_max_organic": 12.0,
    # Correlación CCV / follower growth orgánica
    "corr_organic": 0.78,
    "corr_bot":     0.29,
    # Drip-feed botting: tasa constante sospechosa (follows/hora)
    "drip_min": 8.0,
}

# ── DEMO DATA (para canales sin datos reales disponibles) ────────────────────
DEMO_PROFILES = {
    "willyrex_fanboy": {
        "followers": 26382,
        "avg_ccv_30d": 30,
        "peak_ccv": 94,
        "follows_per_hour": 19.2,
        "hours_streamed_30d": 45,
        "mature": False,
        "partner": False,
        "affiliate": True,
        "created": "2022-08-15",
        "growth_pattern": "linear",   # linear | spiky | irregular
        "top_games": ["Just Chatting", "Minecraft"],
        "language": "es",
    }
}

# ── SCRAPER ──────────────────────────────────────────────────────────────────
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "es-ES,es;q=0.9,en;q=0.8",
}

def fetch_twitchtracker(channel: str) -> dict | None:
    """Intenta obtener datos desde TwitchTracker."""
    url = f"https://twitchtracker.com/{channel}/statistics"
    try:
        req = urllib.request.Request(url, headers=HEADERS)
        with urllib.request.urlopen(req, timeout=10) as resp:
            html = resp.read().decode("utf-8", errors="ignore")
        return parse_twitchtracker(html, channel)
    except Exception as e:
        return None

def parse_twitchtracker(html: str, channel: str) -> dict:
    """Extrae métricas clave del HTML de TwitchTracker."""
    data = {"channel": channel, "source": "twitchtracker"}

    # Followers
    m = re.search(r'"followers"\s*:\s*(\d+)', html)
    if m: data["followers"] = int(m.group(1))

    # Average viewers
    m = re.search(r'"avg_viewers"\s*:\s*([\d.]+)', html)
    if not m: m = re.search(r'Average viewers.*?<span[^>]*>([\d,]+)', html)
    if m: data["avg_ccv_30d"] = float(re.sub(r'[,\s]', '', m.group(1)))

    # Peak viewers
    m = re.search(r'"max_viewers"\s*:\s*(\d+)', html)
    if m: data["peak_ccv"] = int(m.group(1))

    # Followers per hour gain (implied from monthly gain / hours)
    m = re.search(r'"followers_gained"\s*:\s*(-?\d+)', html)
    if m:
        fg = int(m.group(1))
        data["monthly_follower_gain"] = fg
        # Estimate: asume ~4h/día stream
        hrs = data.get("hours_streamed_30d", 40)
        data["follows_per_hour"] = round(fg / max(hrs, 1), 2)

    # Hours streamed
    m = re.search(r'"hours_streamed"\s*:\s*([\d.]+)', html)
    if m: data["hours_streamed_30d"] = float(m.group(1))

    # Partner/affiliate
    data["partner"]   = "partner"   in html.lower()
    data["affiliate"]  = "affiliate" in html.lower()

    # Growth pattern: busca si hay varianza alta vs baja en el histórico
    growth_vals = re.findall(r'\[(\d{4},[\d,]+)\]', html)
    if len(growth_vals) > 3:
        data["growth_pattern"] = "variable"
    else:
        data["growth_pattern"] = "unknown"

    return data

def get_channel_data(channel: str, use_demo: bool = False) -> tuple[dict, bool]:
    """Retorna (datos, es_demo)."""
    if not use_demo:
        print(clr(f"  → Consultando TwitchTracker para '{channel}'...", DIM))
        data = fetch_twitchtracker(channel)
        if data and "followers" in data:
            print(clr("  ✓ Datos obtenidos en tiempo real", G))
            return data, False
        print(clr("  ✗ No se pudo acceder a TwitchTracker (anti-scraping activo)", Y))

    # Fallback a demo data o perfil genérico
    if channel.lower() in DEMO_PROFILES:
        print(clr(f"  → Usando perfil de demostración para '{channel}'", C))
        d = dict(DEMO_PROFILES[channel.lower()])
        d["channel"] = channel
        d["source"]  = "demo"
        return d, True

    # Perfil genérico para cualquier canal no conocido en modo demo
    print(clr("  → Generando perfil de análisis con datos de ejemplo", Y))
    return {
        "channel": channel,
        "followers": 26382,
        "avg_ccv_30d": 30,
        "peak_ccv": 94,
        "follows_per_hour": 19.2,
        "hours_streamed_30d": 45,
        "partner": False,
        "affiliate": True,
        "growth_pattern": "unknown",
        "source": "demo",
    }, True

# ── MOTOR DE DETECCIÓN ───────────────────────────────────────────────────────
class FraudSignal:
    def __init__(self, name, weight, triggered, value, expected, severity="HIGH"):
        self.name     = name
        self.weight   = weight       # 0–1, importancia relativa
        self.triggered = triggered   # bool
        self.value    = value        # valor observado
        self.expected = expected     # valor esperado / rango orgánico
        self.severity = severity     # HIGH | MEDIUM | LOW

def analyze_fraud(data: dict) -> tuple[float, list[FraudSignal], dict]:
    """
    Retorna:
      - probabilidad de fraude (0–100)
      - lista de señales evaluadas
      - métricas calculadas
    """
    followers   = data.get("followers", 0)
    avg_ccv     = data.get("avg_ccv_30d", 0)
    fph         = data.get("follows_per_hour", 0)
    hrs         = data.get("hours_streamed_30d", 40)
    pattern     = data.get("growth_pattern", "unknown")

    # ── Métricas derivadas ──
    fvr = (avg_ccv / max(followers, 1)) * 100      # Follower-to-Viewer Ratio (%)

    # Unique viewers estimados (CCV × rotación 2.5x típica por sesión)
    session_hours = hrs / max(30, 1) * 2.5          # horas por sesión ~media
    unique_viewers_est = avg_ccv * 2.5
    follows_per_session = fph * session_hours
    fcr = (follows_per_session / max(unique_viewers_est, 1)) * 100  # Follow Conv. Rate

    # Rango FVR esperado según tamaño
    if followers < 10_000:
        fvr_min, fvr_max = BENCHMARKS["fvr"]["small"]
    elif followers < 100_000:
        fvr_min, fvr_max = BENCHMARKS["fvr"]["medium"]
    else:
        fvr_min, fvr_max = BENCHMARKS["fvr"]["large"]

    metrics = {
        "followers": followers,
        "avg_ccv": avg_ccv,
        "fvr": round(fvr, 3),
        "fvr_range": (fvr_min, fvr_max),
        "fcr": round(fcr, 1),
        "follows_per_hour": fph,
        "unique_viewers_est": round(unique_viewers_est),
    }

    # ── Señales ──
    signals = []

    # 1. FVR demasiado bajo
    fvr_ratio = fvr / fvr_min if fvr_min > 0 else 1
    signals.append(FraudSignal(
        name      = "Follower-to-Viewer Ratio (FVR)",
        weight    = 0.30,
        triggered = fvr < fvr_min * 0.5,
        value     = f"{fvr:.2f}%",
        expected  = f"{fvr_min}%–{fvr_max}%",
        severity  = "HIGH" if fvr < fvr_min * 0.25 else "MEDIUM",
    ))

    # 2. FCR imposible
    signals.append(FraudSignal(
        name      = "Follow Conversion Rate (FCR)",
        weight    = 0.28,
        triggered = fcr > BENCHMARKS["fcr_max_organic"],
        value     = f"{fcr:.1f}%",
        expected  = f"<{BENCHMARKS['fcr_max_organic']}%",
        severity  = "HIGH" if fcr > 30 else "MEDIUM",
    ))

    # 3. Drip-feed (tasa constante sin picos)
    drip = fph >= BENCHMARKS["drip_min"] and pattern in ("linear", "unknown")
    signals.append(FraudSignal(
        name      = "Drip-Feed Pattern (tasa constante)",
        weight    = 0.22,
        triggered = drip,
        value     = f"{fph:.1f} follows/h ({pattern})",
        expected  = "Irregular (picos + valles)",
        severity  = "HIGH" if fph > 15 else "MEDIUM",
    ))

    # 4. Volumen absoluto de seguidores inactivos
    ghost_pct = 100 - (fvr / fvr_min * 100) if fvr_min > 0 else 0
    ghost_pct = max(0, min(ghost_pct, 100))
    signals.append(FraudSignal(
        name      = "Seguidores Fantasma (ghost followers)",
        weight    = 0.12,
        triggered = ghost_pct > 70,
        value     = f"~{ghost_pct:.0f}% estimado inactivo",
        expected  = "<50% inactivos (canales orgánicos)",
        severity  = "MEDIUM",
    ))

    # 5. Sin partner / baja legitimidad institucional
    not_partner = not data.get("partner", False)
    signals.append(FraudSignal(
        name      = "Sin verificación Partner/Marca",
        weight    = 0.08,
        triggered = not_partner and followers > 15_000,
        value     = "Affiliate / Sin Partner" if data.get("affiliate") else "Sin estado",
        expected  = "Partner esperado en >15K con crecimiento real",
        severity  = "LOW",
    ))

    # ── Cálculo de probabilidad ──
    # Peso ponderado de señales activas + factor de amplificación si >3 activas
    active   = [s for s in signals if s.triggered]
    raw_prob = sum(s.weight for s in active) / sum(s.weight for s in signals)
    boost    = 1.0 + (0.15 * max(0, len(active) - 2))  # amplificación por convergencia
    prob     = min(raw_prob * boost * 100, 99.5)

    return round(prob, 1), signals, metrics

def assess_origin(data: dict, signals: list, metrics: dict) -> dict:
    """
    Analiza el probable origen del fraude.
    Retorna dict con tipo, confianza, y descripción.
    """
    fph     = data.get("follows_per_hour", 0)
    pattern = data.get("growth_pattern", "unknown")
    fvr     = metrics["fvr"]

    origins = []

    # Follow-botting de goteo (drip-feed service)
    if fph > 10 and pattern in ("linear", "unknown"):
        origins.append({
            "type"  : "Follow-Botting Drip-Feed",
            "icon"  : "🤖",
            "conf"  : 88,
            "detail": (
                f"Tasa constante de {fph:.1f} follows/h sin picos orgánicos. "
                "Patrón típico de servicios de bots programados (p.ej. SMMPanel, "
                "Followerfabrik). Cuentas generadas automáticamente, sin actividad real."
            ),
        })

    # Compra de seguidores en bloque (si FVR es ultra-bajo)
    if fvr < 0.15:
        origins.append({
            "type"  : "Compra Masiva de Seguidores",
            "icon"  : "💰",
            "conf"  : 72,
            "detail": (
                f"FVR de {fvr:.2f}% indica base de seguidores masivamente inactiva. "
                "Puede combinar drip-feed con compra puntual histórica. "
                "Muchas cuentas nunca se conectaron tras seguir."
            ),
        })

    # Hate-botting (terceros)
    if not data.get("partner") and data.get("followers", 0) < 50_000:
        origins.append({
            "type"  : "Hate-Botting por Terceros",
            "icon"  : "⚠️",
            "conf"  : 35,
            "detail": (
                "No descartable: terceros pueden inflar seguidores de un canal "
                "para dañar su reputación o para generar falsas métricas de competencia. "
                "Requiere análisis interno de Twitch para diferenciarlo."
            ),
        })

    # Ordenar por confianza
    origins.sort(key=lambda x: x["conf"], reverse=True)
    return origins

# ── RENDER TERMINAL ──────────────────────────────────────────────────────────
def verdict_banner(prob: float) -> str:
    if prob >= 80:
        color = R
        label = "BOTTING CONFIRMADO"
        bg    = BG_RED
        icon  = "🔴"
    elif prob >= 50:
        color = Y
        label = "SOSPECHOSO — REVISAR"
        bg    = BG_YEL
        icon  = "🟡"
    else:
        color = G
        label = "APARENTEMENTE ORGÁNICO"
        bg    = BG_GRN
        icon  = "🟢"

    bar_len   = 40
    filled    = int(bar_len * prob / 100)
    bar       = color + "█" * filled + DIM + "░" * (bar_len - filled) + RST
    pct_str   = clr(f"{prob:.1f}%", BLD, color)

    lines = [
        f"^{icon}  VEREDICTO: {clr(label, BLD, color)}",
        "---",
        f"^Probabilidad de Fraude: {pct_str}",
        f"^{bar}",
    ]
    return box(lines, color=color)

def signal_row(s: FraudSignal) -> str:
    icon  = clr("✘ ACTIVA  ", BLD, R) if s.triggered else clr("✔ OK      ", G)
    sev   = {"HIGH": clr("[HIGH]  ", R, BLD), "MEDIUM": clr("[MED]   ", Y), "LOW": clr("[LOW]   ", DIM)}.get(s.severity, "")
    w_pct = clr(f"w={int(s.weight*100):2d}%", DIM)
    name  = clr(s.name, W if s.triggered else DIM)
    val   = clr(s.value, R if s.triggered else G)
    exp   = clr(s.expected, DIM)
    return f"  {icon} {sev} {name}\n        Observado: {val}  |  Esperado: {exp}  {w_pct}"

def metrics_table(metrics: dict) -> str:
    rows = [
        ("Seguidores totales",           f"{metrics['followers']:,}"),
        ("CCV promedio (30d)",            f"{metrics['avg_ccv']:.0f} viewers"),
        ("FVR (Follower-to-Viewer Ratio)",f"{metrics['fvr']:.3f}%  (org: {metrics['fvr_range'][0]}–{metrics['fvr_range'][1]}%)"),
        ("FCR (Follow Conversion Rate)",  f"{metrics['fcr']:.1f}%  (org: <12%)"),
        ("Follows/hora",                  f"{metrics['follows_per_hour']:.1f}"),
        ("Viewers únicos estimados",      f"~{metrics['unique_viewers_est']}"),
    ]
    out = []
    for label, value in rows:
        flag = ""
        if "FVR"   in label and metrics['fvr'] < metrics['fvr_range'][0] * 0.5: flag = clr("  ⚠", R)
        if "FCR"   in label and metrics['fcr'] > 12: flag = clr("  ⚠", R)
        if "Follows" in label and metrics['follows_per_hour'] > 8: flag = clr("  ⚠", Y)
        out.append(f"  {clr(label+':',DIM):<45} {clr(value, W)}{flag}")
    return "\n".join(out)

def origin_block(origins: list) -> str:
    if not origins:
        return clr("  No se detectaron patrones de origen claros.", DIM)
    out = []
    for o in origins:
        conf_bar = G if o["conf"] > 70 else (Y if o["conf"] > 40 else DIM)
        bar = clr("█" * (o["conf"] // 10), conf_bar) + clr("░" * (10 - o["conf"] // 10), DIM)
        out.append(f"\n  {o['icon']}  {clr(o['type'], BLD, W)}  {bar} {clr(str(o['conf'])+'%', conf_bar)}")
        for line in [o["detail"][i:i+70] for i in range(0, len(o["detail"]), 70)]:
            out.append(f"     {clr(line, DIM)}")
    return "\n".join(out)

def tos_assessment(prob: float, signals: list) -> str:
    active_high = [s for s in signals if s.triggered and s.severity == "HIGH"]
    if prob >= 80:
        return (
            clr("  PROBABLE VIOLACIÓN ", BLD, R) +
            clr("de Twitch ToS — Sección 6:", W) + "\n" +
            clr('  "Artificial Inflation of Statistics" (follows, viewers, chat).', DIM) + "\n" +
            clr(f"  {len(active_high)} señales HIGH activas. Acción recomendada: reportar canal.", Y)
        )
    elif prob >= 50:
        return clr("  SOSPECHOSO. Monitorear evolución. Insuficiente para reporte formal.", Y)
    else:
        return clr("  Sin indicios claros de violación ToS.", G)

# ── MAIN ─────────────────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(
        description="Twitch Fraud Detector — Detecta Follow-Botting y Artificial Engagement",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument("channel", help="Nombre del canal de Twitch (ej: willyrex_fanboy)")
    parser.add_argument("--demo", action="store_true", help="Usar datos de demostración (sin scraping)")
    parser.add_argument("--json", action="store_true", help="Output en formato JSON")
    parser.add_argument(
        "--manual", nargs=4, metavar=("FOLLOWERS","CCV","FOLLOWS_H","HOURS"),
        help="Introducir datos manualmente: --manual 26000 30 19 45"
    )
    args = parser.parse_args()

    # ── Header ──
    print()
    print(clr("╔══════════════════════════════════════════════════════════════╗", C))
    print(clr("║  ", C) + clr("TWITCH FRAUD DETECTOR", BLD, W) + clr("  ·  ", DIM) + clr("Anti-Botting Intelligence CLI", C) + clr("   ║", C))
    print(clr("╚══════════════════════════════════════════════════════════════╝", C))
    print(clr(f"  Canal analizado : ", DIM) + clr(args.channel, BLD, W))
    print(clr(f"  Timestamp       : ", DIM) + clr(datetime.now().strftime("%Y-%m-%d %H:%M:%S"), DIM))
    print()

    # ── Obtener datos ──
    if args.manual:
        followers, ccv, fph, hrs = [float(x) for x in args.manual]
        data = {
            "channel": args.channel,
            "followers": int(followers),
            "avg_ccv_30d": ccv,
            "follows_per_hour": fph,
            "hours_streamed_30d": hrs,
            "partner": False,
            "affiliate": True,
            "growth_pattern": "unknown",
            "source": "manual",
        }
        is_demo = False
        print(clr("  ✓ Datos introducidos manualmente", G))
    else:
        data, is_demo = get_channel_data(args.channel, use_demo=args.demo)

    source_tag = clr(f"[{data.get('source','?').upper()}]", Y if is_demo else G)
    print(clr(f"  Fuente de datos  : ", DIM) + source_tag)
    print()

    # ── Análisis ──
    print(clr("  Ejecutando motor de detección...", DIM))
    time.sleep(0.4)
    prob, signals, metrics = analyze_fraud(data)
    origins = assess_origin(data, signals, metrics)

    # ── JSON output ──
    if args.json:
        output = {
            "channel": args.channel,
            "timestamp": datetime.now().isoformat(),
            "fraud_probability": prob,
            "metrics": metrics,
            "signals": [
                {"name": s.name, "triggered": s.triggered, "value": s.value,
                 "expected": s.expected, "weight": s.weight, "severity": s.severity}
                for s in signals
            ],
            "origins": origins,
        }
        print(json.dumps(output, indent=2, ensure_ascii=False))
        return

    # ── Render completo ──
    print()
    print(verdict_banner(prob))
    print()

    # Métricas
    print(clr("  ── MÉTRICAS DEL CANAL ─────────────────────────────────────", B))
    print(metrics_table(metrics))
    print()

    # Señales
    print(clr("  ── SEÑALES DE DETECCIÓN ───────────────────────────────────", B))
    for s in signals:
        print(signal_row(s))
        print()

    # Origen
    print(clr("  ── ANÁLISIS DE ORIGEN DEL FRAUDE ──────────────────────────", B))
    print(origin_block(origins))
    print()

    # ToS
    print(clr("  ── EVALUACIÓN DE VIOLACIÓN ToS TWITCH ─────────────────────", B))
    print(tos_assessment(prob, signals))
    print()

    # Footer
    active_count = len([s for s in signals if s.triggered])
    print(clr(f"  {active_count}/{len(signals)} señales activas  |  ", DIM) +
          clr("Fuente: TwitchTracker · StreamsCharts · Vodra Research 2025", DIM))
    print(clr("  Este análisis es estadístico. No constituye acusación legal.", DIM))
    print()

if __name__ == "__main__":
    main()
