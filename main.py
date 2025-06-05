# ==============================================================================
# main.py - Punto de entrada principal del Moderador Semántico
# ==============================================================================

import argparse
from pathlib import Path

from moderador_semantico import ModeradorSemantico
from config import config


def main():
    parser = argparse.ArgumentParser(description='Moderador Semántico de Twitch')
    parser.add_argument('--file', '-f', help='Archivo de logs a procesar')
    parser.add_argument('--message', '-m', help='Mensaje individual a analizar')
    parser.add_argument('--user', '-u', default='test_user', help='Usuario para mensaje individual')
    parser.add_argument('--batch', '-b', action='store_true', help='Procesar todos los logs en el directorio')

    args = parser.parse_args()
    moderador = ModeradorSemantico(file_path=args.file)


    if args.message:
        moderador.analyze_single_message(args.message, args.user)
    elif args.file:
        moderador.process_log_file(args.file)
    elif args.batch:
        logs_dir = Path(config.LOGS_PATH)
        if logs_dir.exists():
            log_files = list(logs_dir.glob("*.log"))
            print(f"📁 Encontrados {len(log_files)} archivos de logs")
            for log_file in log_files:
                moderador.process_log_file(str(log_file))
        else:
            print(f"❌ Directorio de logs no encontrado: {logs_dir}")
    else:
        print("❌ Especifica --file, --message o --batch")
        parser.print_help()


if __name__ == "__main__":
    main()