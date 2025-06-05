# ==============================================================================
# 4. alert_manager.py - Handle alert creation and storage
# ==============================================================================

from pathlib import Path
import json
from datetime import datetime
from collections import defaultdict
from typing import Dict, List


class AlertManager:
    """Handles alert creation, storage, and management"""

    def __init__(self, config):
        self.config = config

    def save_alerts(self, alerts: List[Dict], source_file: str):
        """Save alerts with enhanced formatting and categorization"""
        if not alerts:
            print("ℹ️ No hay alertas para guardar")
            return

        output_dir = Path(self.config.OUTPUT_PATH)
        output_dir.mkdir(parents=True, exist_ok=True)

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"alertas_{Path(source_file).stem}_{timestamp}.json"
        filepath = output_dir / filename

        # Group alerts by severity
        severity_groups = defaultdict(list)
        for alert in alerts:
            severity_groups[alert['severity']].append(alert)

        output_data = {
            'metadata': {
                'generated_at': datetime.now().isoformat(),
                'source_file': source_file,
                'total_alerts': len(alerts),
                'severity_breakdown': {k: len(v) for k, v in severity_groups.items()}
            },
            'alerts_by_severity': dict(severity_groups),
            'all_alerts': alerts
        }

        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(output_data, f, indent=2, ensure_ascii=False, default=str)

        print(f"💾 Alertas guardadas en: {filepath.resolve()}")
        self._print_alert_summary(severity_groups)

    def _print_alert_summary(self, severity_groups: Dict):
        """Print summary of alerts by severity"""
        print(f"\n📋 Resumen de alertas por severidad:")
        for severity in ['CRITICAL', 'HIGH', 'MEDIUM', 'LOW']:
            count = len(severity_groups.get(severity, []))
            if count > 0:
                print(f"  🚨 {severity}: {count} alertas")