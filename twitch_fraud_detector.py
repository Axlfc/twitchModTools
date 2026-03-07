#!/usr/bin/env python3
"""
╔══════════════════════════════════════════════════════════════╗
║         TWITCH FRAUD DETECTOR  —  twitch_fraud_detector.py   ║
║         Analiza métricas de TwitchTracker para detectar       ║
║         Follow-Botting y Artificial Engagement                ║
╚══════════════════════════════════════════════════════════════╝

Uso:
    python twitch_fraud_detector.py
    python twitch_fraud_detector.py --json
    python twitch_fraud_detector.py --demo
"""

import sys
import re
import json
import time
import argparse
from datetime import datetime

# ── ANSI COLORS ──────────────────────────────────────────────────────────────
R   = "\033[91m"
Y   = "\033[93m"
G   = "\033[92m"
B   = "\033[94m"
C   = "\033[96m"
M   = "\033[95m"
W   = "\033[97m"
DIM = "\033[2m"
BLD = "\033[1m"
RST = "\033[0m"
BG_RED = "\033[41m"
BG_YEL = "\033[43m"
BG_GRN = "\033[42m"

def clr(text, *codes):
    return "".join(codes) + str(text) + RST

def box(lines, color=C, width=64):
    top    = color + "╔" + "═" * width + "╗" + RST
    bottom = color + "╚" + "═" * width + "╝" + RST
    mid    = color + "╠" + "═" * width + "╣" + RST
    def row(text="", align="left"):
        stripped = re.sub(r'\033\[[0-9;]*m', '', text)
        pad = width - len(stripped) - 2
        if align == "center":
            lp = pad // 2; rp = pad - lp
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

# ── BENCHMARKS ────────────────────────────────────────────────────────────────
BENCHMARKS = {
    "fvr": {
        "small":  (1.0, 5.0),   # < 10K followers
        "medium": (0.8, 3.5),   # 10K–100K
        "large":  (0.3, 2.0),   # 100K+
    },
    "fcr_max_organic": 12.0,
    "corr_organic": 0.78,
    "corr_bot":     0.29,
    "drip_min": 8.0,
}

# ── DEMO DATA ─────────────────────────────────────────────────────────────────
DEMO_DATA = {
    "channel":            "example_fraud_channel",
    "followers":          25000,
    "avg_ccv_30d":        28.0,
    "follows_per_hour":   18.0,
    "hours_streamed_30d": 42.0,
    "partner":            False,
    "affiliate":          True,
    "growth_pattern":     "linear",
    "source":             "demo",
}

# ── INPUT INTERACTIVO ─────────────────────────────────────────────────────────
#
# Campos mapeados exactamente a lo visible en TwitchTracker sin cálculos:
#
#   SECCIÓN "Various Statistics":
#     - Followers to date      → followers
#     - Followers/hour         → follows_per_hour  ← ya calculado por TT
#
#   SECCIÓN "Lifetime Overview":
#     - Highest number viewers → peak_ccv
#
#   TABLA mensual (mes más reciente con actividad):
#     - Avg Viewers            → avg_ccv_30d
#
# "Hours Streamed" se elimina como input: con follows_per_hour directo
# no necesitamos calcularlo nosotros.
#
FIELDS = [
    (
        "channel",
        "Nombre del canal",
        str,
        None,
    ),
    (
        "followers",
        "Seguidores totales  [Various Statistics → 'Followers to date']",
        int,
        "'Various Statistics' → 'Followers to date'   (ej: 26786)",
    ),
    (
        "avg_ccv_30d",
        "Viewers promedio    [Tabla mensual → 'Avg Viewers', mes más reciente]",
        float,
        "Tabla 'Monthly stats' → columna 'Avg Viewers', fila del mes más reciente  (ej: 20)",
    ),
    (
        "follows_per_hour",
        "Followers/hora      [Various Statistics → 'Followers per hour of stream']",
        float,
        "'Various Statistics' → 'Followers per hour of stream'   (ej: 18.9)",
    ),
    (
        "peak_ccv",
        "Peak viewers        [Lifetime Overview → 'Highest number of viewers']",
        int,
        "'Lifetime Overview' → 'Highest number of viewers'   (ej: 4124)",
    ),
]

def _ask_num(label: str, tipo, hint: str | None, url: str) -> object:
    """Pregunta un campo numérico con instrucciones de dónde encontrarlo."""
    print()
    print(clr(f"  ▸ {label}", BLD, W))
    if hint:
        print(clr(f"    {url}", C))
        print(clr(f"    Busca: {hint}", DIM))
    while True:
        try:
            raw = input(clr("    → Valor: ", Y)).strip()
            if not raw:
                print(clr("    ⚠ Campo obligatorio.", R))
                continue
            if tipo == float:
                clean = re.sub(r'[^\d.]', '', raw.replace(',', '.'))
            else:
                clean = re.sub(r'[^\d]', '', raw)
            return tipo(clean)
        except (ValueError, TypeError):
            print(clr("    ⚠ Introduce un número válido (ej: 26785 o 18.9).", R))

def collect_data_interactive() -> dict:
    """Guía al usuario paso a paso para introducir los datos desde TwitchTracker."""
    print()
    print(box([
        "^📋  INTRODUCCIÓN DE DATOS",
        "---",
        "^Se te pedirá cada métrica con instrucciones",
        "^de dónde encontrarla en TwitchTracker.",
        "^Puedes usar coma o punto como decimal.",
    ], color=C))

    data: dict = {"source": "manual"}
    channel = ""

    for key, label, tipo, hint in FIELDS:
        if key == "channel":
            print()
            print(clr(f"  ▸ {label}", BLD, W))
            channel = input(clr("    → Nombre: ", Y)).strip()
            data["channel"] = channel
            print()
            print(clr(
                f"  → Abre ahora en tu navegador:",
                DIM
            ))
            print(clr(
                f"    https://twitchtracker.com/{channel}/statistics",
                BLD, C
            ))
        else:
            url = f"https://twitchtracker.com/{channel}/statistics"
            data[key] = _ask_num(label, tipo, hint, url)

    # Partner/affiliate
    print()
    print(clr("  ▸ Estado del canal", BLD, W))
    print(clr(f"    https://twitchtracker.com/{channel}/statistics", C))
    print(clr("    Busca: 'Partner', 'Affiliate' o ninguno en el perfil del canal", DIM))
    status_raw = input(clr("    → Opciones [partner / affiliate / ninguno]: ", Y)).strip().lower()
    data["partner"]   = "partner"   in status_raw
    data["affiliate"] = "affiliate" in status_raw or "afiliado" in status_raw

    # Patrón de crecimiento
    print()
    print(clr("  ▸ Patrón del gráfico de seguidores (opcional)", BLD, W))
    print(clr(f"    https://twitchtracker.com/{channel}/statistics", C))
    print(clr("    Busca: el gráfico de seguidores. ¿Sube constante o tiene picos y caídas?", DIM))
    pattern_raw = input(clr("    → Opciones [linear / variable / skip]: ", Y)).strip().lower()
    if "lin" in pattern_raw:
        data["growth_pattern"] = "linear"
    elif "var" in pattern_raw:
        data["growth_pattern"] = "variable"
    else:
        data["growth_pattern"] = "unknown"

    return data

# ── MOTOR DE DETECCIÓN ────────────────────────────────────────────────────────
class FraudSignal:
    def __init__(self, name, weight, triggered, value, expected, severity="HIGH"):
        self.name      = name
        self.weight    = weight
        self.triggered = triggered
        self.value     = value
        self.expected  = expected
        self.severity  = severity

def analyze_fraud(data: dict) -> tuple[float, list[FraudSignal], dict]:
    followers = data.get("followers", 0)
    avg_ccv   = data.get("avg_ccv_30d", 0)
    fph       = data.get("follows_per_hour", 0)
    pattern   = data.get("growth_pattern", "unknown")

    fvr = (avg_ccv / max(followers, 1)) * 100

    # FCR: estimamos viewers únicos como CCV × 2.5 (rotación típica por sesión)
    # y follows por sesión como fph × horas medias estimadas (4h/día × días activos ~30d)
    # Como no pedimos horas, usamos fph directamente como señal de drip-feed
    unique_viewers_est  = avg_ccv * 2.5
    # Estimación conservadora: asumimos ~40h de stream al mes si no hay dato
    hrs_est             = data.get("hours_streamed_30d", 40)
    session_hours       = hrs_est / max(30, 1) * 2.5
    follows_per_session = fph * session_hours
    fcr = (follows_per_session / max(unique_viewers_est, 1)) * 100

    if followers < 10_000:
        fvr_min, fvr_max = BENCHMARKS["fvr"]["small"]
    elif followers < 100_000:
        fvr_min, fvr_max = BENCHMARKS["fvr"]["medium"]
    else:
        fvr_min, fvr_max = BENCHMARKS["fvr"]["large"]

    metrics = {
        "followers":          followers,
        "avg_ccv":            avg_ccv,
        "fvr":                round(fvr, 3),
        "fvr_range":          (fvr_min, fvr_max),
        "fcr":                round(fcr, 1),
        "follows_per_hour":   fph,
        "unique_viewers_est": round(unique_viewers_est),
    }

    signals = []

    signals.append(FraudSignal(
        name      = "Follower-to-Viewer Ratio (FVR)",
        weight    = 0.30,
        triggered = fvr < fvr_min * 0.5,
        value     = f"{fvr:.2f}%",
        expected  = f"{fvr_min}%–{fvr_max}%",
        severity  = "HIGH" if fvr < fvr_min * 0.25 else "MEDIUM",
    ))

    signals.append(FraudSignal(
        name      = "Follow Conversion Rate (FCR)",
        weight    = 0.28,
        triggered = fcr > BENCHMARKS["fcr_max_organic"],
        value     = f"{fcr:.1f}%",
        expected  = f"<{BENCHMARKS['fcr_max_organic']}%",
        severity  = "HIGH" if fcr > 30 else "MEDIUM",
    ))

    drip = fph >= BENCHMARKS["drip_min"] and pattern in ("linear", "unknown")
    signals.append(FraudSignal(
        name      = "Drip-Feed Pattern (tasa constante)",
        weight    = 0.22,
        triggered = drip,
        value     = f"{fph:.1f} follows/h ({pattern})",
        expected  = "Irregular (picos + valles)",
        severity  = "HIGH" if fph > 15 else "MEDIUM",
    ))

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

    signals.append(FraudSignal(
        name      = "Sin verificación Partner/Marca",
        weight    = 0.08,
        triggered = not data.get("partner", False) and followers > 15_000,
        value     = "Affiliate / Sin Partner" if data.get("affiliate") else "Sin estado",
        expected  = "Partner esperado en >15K con crecimiento real",
        severity  = "LOW",
    ))

    active   = [s for s in signals if s.triggered]
    raw_prob = sum(s.weight for s in active) / sum(s.weight for s in signals)
    boost    = 1.0 + (0.15 * max(0, len(active) - 2))
    prob     = min(raw_prob * boost * 100, 99.5)

    return round(prob, 1), signals, metrics

def assess_origin(data: dict, signals: list, metrics: dict) -> list:
    fph     = data.get("follows_per_hour", 0)
    pattern = data.get("growth_pattern", "unknown")
    fvr     = metrics["fvr"]
    origins = []

    if fph > 10 and pattern in ("linear", "unknown"):
        origins.append({
            "type": "Follow-Botting Drip-Feed", "icon": "🤖", "conf": 88,
            "detail": (
                f"Tasa constante de {fph:.1f} follows/h sin picos orgánicos. "
                "Patrón típico de servicios de bots (SMMPanel, Followerfabrik). "
                "Cuentas generadas automáticamente, sin actividad real."
            ),
        })

    if fvr < 0.15:
        origins.append({
            "type": "Compra Masiva de Seguidores", "icon": "💰", "conf": 72,
            "detail": (
                f"FVR de {fvr:.2f}% indica base masivamente inactiva. "
                "Combina posiblemente drip-feed con compra histórica en bloque."
            ),
        })

    if not data.get("partner") and data.get("followers", 0) < 50_000:
        origins.append({
            "type": "Hate-Botting por Terceros", "icon": "⚠️", "conf": 35,
            "detail": (
                "No descartable: terceros pueden inflar seguidores de un canal "
                "para dañar su reputación. Requiere análisis interno de Twitch."
            ),
        })

    origins.sort(key=lambda x: x["conf"], reverse=True)
    return origins

# ── RENDER ────────────────────────────────────────────────────────────────────
def verdict_banner(prob: float) -> str:
    if prob >= 80:
        color, label, icon = R, "BOTTING CONFIRMADO", "🔴"
    elif prob >= 50:
        color, label, icon = Y, "SOSPECHOSO — REVISAR", "🟡"
    else:
        color, label, icon = G, "APARENTEMENTE ORGÁNICO", "🟢"
    bar_len = 40
    filled  = int(bar_len * prob / 100)
    bar     = color + "█" * filled + DIM + "░" * (bar_len - filled) + RST
    return box([
        f"^{icon}  VEREDICTO: {clr(label, BLD, color)}",
        "---",
        f"^Probabilidad de Fraude: {clr(f'{prob:.1f}%', BLD, color)}",
        f"^{bar}",
    ], color=color)

def signal_row(s: FraudSignal) -> str:
    icon = clr("✘ ACTIVA  ", BLD, R) if s.triggered else clr("✔ OK      ", G)
    sev  = {"HIGH": clr("[HIGH]  ", R, BLD), "MEDIUM": clr("[MED]   ", Y),
             "LOW":  clr("[LOW]   ", DIM)}.get(s.severity, "")
    w_pct = clr(f"w={int(s.weight*100):2d}%", DIM)
    name  = clr(s.name, W if s.triggered else DIM)
    val   = clr(s.value, R if s.triggered else G)
    exp   = clr(s.expected, DIM)
    return f"  {icon} {sev} {name}\n        Observado: {val}  |  Esperado: {exp}  {w_pct}"

def metrics_table(metrics: dict) -> str:
    rows = [
        ("Seguidores totales",            f"{metrics['followers']:,}"),
        ("CCV promedio (30d)",             f"{metrics['avg_ccv']:.0f} viewers"),
        ("FVR (Follower-to-Viewer Ratio)", f"{metrics['fvr']:.3f}%  (org: {metrics['fvr_range'][0]}–{metrics['fvr_range'][1]}%)"),
        ("FCR (Follow Conversion Rate)",   f"{metrics['fcr']:.1f}%  (org: <12%)"),
        ("Follows/hora",                   f"{metrics['follows_per_hour']:.1f}"),
        ("Viewers únicos estimados",       f"~{metrics['unique_viewers_est']}"),
    ]
    out = []
    for label, value in rows:
        flag = ""
        if "FVR" in label and metrics["fvr"] < metrics["fvr_range"][0] * 0.5:
            flag = clr("  ⚠", R)
        if "FCR" in label and metrics["fcr"] > 12:
            flag = clr("  ⚠", R)
        if "Follows" in label and metrics["follows_per_hour"] > 8:
            flag = clr("  ⚠", Y)
        out.append(f"  {clr(label+':', DIM):<45} {clr(value, W)}{flag}")
    return "\n".join(out)

def origin_block(origins: list) -> str:
    if not origins:
        return clr("  No se detectaron patrones de origen claros.", DIM)
    out = []
    for o in origins:
        conf_bar = G if o["conf"] > 70 else (Y if o["conf"] > 40 else DIM)
        bar = clr("█" * (o["conf"] // 10), conf_bar) + clr("░" * (10 - o["conf"] // 10), DIM)
        out.append(f"\n  {o['icon']}  {clr(o['type'], BLD, W)}  {bar} {clr(str(o['conf'])+'%', conf_bar)}")
        for chunk in [o["detail"][i:i+70] for i in range(0, len(o["detail"]), 70)]:
            out.append(f"     {clr(chunk, DIM)}")
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

# ── MAIN ──────────────────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(
        description="Twitch Fraud Detector — Análisis interactivo de Follow-Botting",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument("--demo", action="store_true",
                        help="Usar datos de demostración sin introducir nada")
    parser.add_argument("--json", action="store_true",
                        help="Output en formato JSON")
    args = parser.parse_args()

    print()
    print(clr("╔══════════════════════════════════════════════════════════════╗", C))
    print(clr("║  ", C) + clr("TWITCH FRAUD DETECTOR", BLD, W) +
          clr("  ·  ", DIM) + clr("Anti-Botting Intelligence CLI", C) +
          clr("   ║", C))
    print(clr("╚══════════════════════════════════════════════════════════════╝", C))
    print(clr(f"  Timestamp : ", DIM) + clr(datetime.now().strftime("%Y-%m-%d %H:%M:%S"), DIM))
    print()

    if args.demo:
        data = dict(DEMO_DATA)
        print(clr("  → Usando datos de demostración", C))
    else:
        data = collect_data_interactive()

    channel = data.get("channel", "desconocido")
    print()
    print(clr(f"  Canal analizado : ", DIM) + clr(str(channel), BLD, W))
    print(clr(f"  Fuente de datos : ", DIM) + clr(f"[{data.get('source','MANUAL').upper()}]", G))
    print()

    print(clr("  Ejecutando motor de detección...", DIM))
    time.sleep(0.4)
    prob, signals, metrics = analyze_fraud(data)
    origins = assess_origin(data, signals, metrics)

    if args.json:
        output = {
            "channel":           channel,
            "timestamp":         datetime.now().isoformat(),
            "fraud_probability": prob,
            "metrics":           metrics,
            "signals": [
                {"name": s.name, "triggered": s.triggered, "value": s.value,
                 "expected": s.expected, "weight": s.weight, "severity": s.severity}
                for s in signals
            ],
            "origins": origins,
        }
        print(json.dumps(output, indent=2, ensure_ascii=False))
        return

    print()
    print(verdict_banner(prob))
    print()

    print(clr("  ── MÉTRICAS DEL CANAL ─────────────────────────────────────", B))
    print(metrics_table(metrics))
    print()

    print(clr("  ── SEÑALES DE DETECCIÓN ───────────────────────────────────", B))
    for s in signals:
        print(signal_row(s))
        print()

    print(clr("  ── ANÁLISIS DE ORIGEN DEL FRAUDE ──────────────────────────", B))
    print(origin_block(origins))
    print()

    print(clr("  ── EVALUACIÓN DE VIOLACIÓN ToS TWITCH ─────────────────────", B))
    print(tos_assessment(prob, signals))
    print()

    active_count = len([s for s in signals if s.triggered])
    print(clr(f"  {active_count}/{len(signals)} señales activas  |  ", DIM) +
          clr("Fuente: TwitchTracker · StreamsCharts · Vodra Research 2025", DIM))
    print(clr("  Este análisis es estadístico. No constituye acusación legal.", DIM))
    print()

if __name__ == "__main__":
    main()