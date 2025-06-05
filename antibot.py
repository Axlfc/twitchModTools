import re
import os
from pathlib import Path


def detectar_directorio_autohotkey():
    """
    Detecta automáticamente el directorio de AutoHotkey del usuario
    """
    posibles_directorios = [
        Path.home() / "Documents" / "AutoHotkey",
        Path.home() / "Documentos" / "AutoHotkey",
        Path.home() / "Desktop" / "AutoHotkey",
        Path.home() / "Escritorio" / "AutoHotkey",
        Path("./AutoHotkey"),  # Directorio local
        Path("./")  # Directorio actual
    ]

    for directorio in posibles_directorios:
        if directorio.exists():
            return directorio

    # Si no existe ninguno, crearlo en Documents
    directorio_default = Path.home() / "Documents" / "AutoHotkey"
    directorio_default.mkdir(parents=True, exist_ok=True)
    return directorio_default


def extraer_nombres_bots(archivo_entrada="botnames.txt"):
    """
    Extrae nombres de bots desde un archivo de texto
    Soporta múltiples formatos de entrada incluyendo logs de Chatterino
    """
    if not os.path.exists(archivo_entrada):
        print(f"❌ No se encontró el archivo '{archivo_entrada}'")
        print("💡 Crea un archivo 'botnames.txt' con los nombres de los bots")
        print("   Formatos soportados:")
        print("   - Logs de Chatterino: '[20:49:03] streamelements: ¡Gracias por el follow nombre !'")
        print("   - Formato simple: '¡Gracias por el follow nombre !'")
        print("   - Formato corto: '20:49 StreamElements: ¡Gracias por el follow nombre !'")
        print("   - Lista simple: un nombre por línea")
        return []

    try:
        with open(archivo_entrada, 'r', encoding='utf-8') as f:
            contenido = f.read()

        nombres = set()

        # Patrón 1: Formato Chatterino completo con timestamp
        # [20:49:03] streamelements: ¡Gracias por el follow nombre !
        patron_chatterino_completo = r'\[\d{2}:\d{2}:\d{2}\]\s+streamelements:\s+¡Gracias por el follow\s+(\w+)\s+!'
        matches1 = re.findall(patron_chatterino_completo, contenido, re.IGNORECASE)
        nombres.update(matches1)
        if matches1:
            print(f"✅ Encontrados {len(matches1)} bots en formato Chatterino completo")

        # Patrón 2: Formato Chatterino corto
        # 20:49 StreamElements: ¡Gracias por el follow nombre !
        patron_chatterino_corto = r'\d{2}:\d{2}\s+StreamElements:\s+¡Gracias por el follow\s+(\w+)\s+!'
        matches2 = re.findall(patron_chatterino_corto, contenido, re.IGNORECASE)
        nombres.update(matches2)
        if matches2:
            print(f"✅ Encontrados {len(matches2)} bots en formato Chatterino corto")

        # Patrón 3: StreamElements formato original (sin timestamp)
        # ¡Gracias por el follow nombre !
        patron_streamelements = r'¡Gracias por el follow\s+(\w+)\s+!'
        matches3 = re.findall(patron_streamelements, contenido, re.IGNORECASE)
        nombres.update(matches3)
        if matches3:
            print(f"✅ Encontrados {len(matches3)} bots en formato StreamElements original")

        # Patrón 4: Variantes con diferentes emojis/símbolos
        # Gracias por el follow nombre :) Bienvenid@ <33
        patron_variante = r'Gracias por el follow\s+(\w+)\s+[!:)]'
        matches4 = re.findall(patron_variante, contenido, re.IGNORECASE)
        nombres.update(matches4)
        if matches4:
            print(f"✅ Encontrados {len(matches4)} bots en formato variante")

        # Patrón 5: Nombres en líneas separadas (uno por línea)
        lineas = contenido.strip().split('\n')
        nombres_lineas = 0
        for linea in lineas:
            linea = linea.strip()
            # Si la línea contiene solo un nombre válido (sin espacios y caracteres alfanuméricos)
            if linea and not ' ' in linea and re.match(r'^[a-zA-Z0-9_]+$', linea):
                # Evitar que las palabras comunes se consideren nombres de bot
                if len(linea) > 2 and linea.lower() not in ['the', 'and', 'for', 'you', 'are', 'bot', 'follow']:
                    nombres.add(linea)
                    nombres_lineas += 1

        if nombres_lineas > 0:
            print(f"✅ Encontrados {nombres_lineas} nombres en formato de lista simple")

        # Patrón 6: Lista de nombres separados por comas
        if ',' in contenido and not any(['¡Gracias' in contenido, 'StreamElements' in contenido]):
            nombres_csv = [nombre.strip() for nombre in contenido.split(',')]
            nombres_csv_validos = [n for n in nombres_csv if n and re.match(r'^[a-zA-Z0-9_]+$', n) and len(n) > 2]
            nombres.update(nombres_csv_validos)
            if nombres_csv_validos:
                print(f"✅ Encontrados {len(nombres_csv_validos)} nombres en formato CSV")

        # Filtrar nombres que parezcan reales vs bots
        nombres_filtrados = set()
        for nombre in nombres:
            # Los nombres de bots suelen tener patrones específicos
            if (len(nombre) >= 6 and  # Mínimo 6 caracteres
                    (re.search(r'\d', nombre) or  # Contiene números
                     re.search(r'[a-z]{3,}\d+', nombre) or  # Letras seguidas de números
                     re.search(r'\d+[a-z]{3,}', nombre) or  # Números seguidos de letras
                     re.search(r'[a-z]{2,}\d[a-z]{2,}', nombre) or  # Letras-número-letras
                     len(set(nombre.lower())) < len(nombre) * 0.7)):  # Muchos caracteres repetidos
                nombres_filtrados.add(nombre)
            elif len(nombre) >= 8:  # Nombres largos sin números también pueden ser bots
                nombres_filtrados.add(nombre)

        # Si no encontramos nombres con el filtro, usar todos los encontrados
        if not nombres_filtrados and nombres:
            print("⚠️  Filtro de bots muy restrictivo, usando todos los nombres encontrados")
            nombres_filtrados = nombres

        return list(nombres_filtrados)

    except Exception as e:
        print(f"❌ Error al leer el archivo: {e}")
        return []


def generar_script_autohotkey(nombres_bots, directorio_ahk=None):
    """
    Genera script de AutoHotkey para ban masivo, con notificación única al final
    """
    if not nombres_bots:
        print("❌ No hay bots para procesar")
        return False

    if directorio_ahk is None:
        directorio_ahk = detectar_directorio_autohotkey()

    archivo_ahk = directorio_ahk / "massive_ban.ahk"

    # Generar contenido del script sin countdown ni notificaciones intermedias
    ahk_content = f'''#NoEnv
SendMode Input
SetWorkingDir %A_ScriptDir%

; Configuración UTF-8
FileEncoding, UTF-8

; Variables globales
totalBots := {len(nombres_bots)}

; F12 para ban masivo automático (se inicia de inmediato)
F12::
    MsgBox, 4, ⚠️ Confirmación de Ban Masivo, ¿Estás seguro de banear %totalBots% bots?`nEsta acción no se puede deshacer.`nAsegúrate de tener el chat enfocado.
    IfMsgBox No
        return

    ; Inicio inmediato del proceso de ban (sin countdown)
'''

    # Agregar comandos de ban individuales sin notificaciones intermedias
    for nombre in nombres_bots:
        ahk_content += f'''    SendRaw, /ban {nombre}
    Send, {{Enter}}
    Sleep, 100  ; Pausa mínima entre comandos
'''

    ahk_content += f'''
    ; Notificación final al terminar
    TrayTip, ✅ Ban Masivo Completado, Se baneó a {len(nombres_bots)} bots., 3, 1
    return

; F11 para timeout masivo (24h) - se inicia de inmediato
F11::
    MsgBox, 4, ⏱️ Confirmación de Timeout Masivo, ¿Timeout de 24 horas para %totalBots% bots?`nMenos severo que un ban permanente.
    IfMsgBox No
        return

    ; Inicio inmediato del proceso de timeout (sin notificaciones intermedias)
'''

    # Agregar comandos de timeout individuales sin notificaciones intermedias
    for nombre in nombres_bots:
        ahk_content += f'''    SendRaw, /timeout {nombre} 86400
    Send, {{Enter}}
    Sleep, 100  ; Pausa mínima entre comandos
'''

    ahk_content += f'''
    ; Notificación final al terminar
    TrayTip, ⏱️ Timeout Masivo Completado, Se aplicó timeout a {len(nombres_bots)} bots., 3, 1
    return

; F9 para mostrar ayuda
F9::
    MsgBox, 0, Instrucciones, F12=Ban masivo | F11=Timeout 24h | ESC=Salir
    return

; ESC para salir
Esc::ExitApp
'''

    try:
        with open(archivo_ahk, 'w', encoding='utf-8') as f:
            f.write(ahk_content)

        print(f"✅ Script AutoHotkey generado: {archivo_ahk}")
        return True

    except Exception as e:
        print(f"❌ Error al generar script: {e}")
        return False


def generar_lista_comandos(nombres_bots, directorio_salida=None):
    """
    Genera archivos de texto con comandos para copiar manualmente
    """
    if directorio_salida is None:
        directorio_salida = Path("./")

    # Archivo con comandos de ban
    archivo_bans = directorio_salida / "comandos_ban.txt"
    with open(archivo_bans, 'w', encoding='utf-8') as f:
        f.write("# Comandos de Ban - Copiar y pegar individualmente\n")
        f.write(f"# Total: {len(nombres_bots)} bots\n")
        f.write("# Generado automáticamente\n\n")

        for nombre in nombres_bots:
            f.write(f"/ban {nombre}\n")

    # Archivo con comandos de timeout
    archivo_timeouts = directorio_salida / "comandos_timeout.txt"
    with open(archivo_timeouts, 'w', encoding='utf-8') as f:
        f.write("# Comandos de Timeout (24h) - Copiar y pegar individualmente\n")
        f.write(f"# Total: {len(nombres_bots)} bots\n")
        f.write("# Generado automáticamente\n\n")

        for nombre in nombres_bots:
            f.write(f"/timeout {nombre} 86400\n")

    print(f"✅ Comandos guardados en:")
    print(f"   📁 Bans: {archivo_bans}")
    print(f"   📁 Timeouts: {archivo_timeouts}")


def mostrar_resumen(nombres_bots):
    """
    Muestra un resumen de los bots encontrados
    """
    print(f"\n📊 RESUMEN:")
    print(f"   🤖 Bots detectados: {len(nombres_bots)}")

    if len(nombres_bots) > 0:
        print(f"\n👁️  VISTA PREVIA (primeros 15):")
        for i, nombre in enumerate(nombres_bots[:15], 1):
            print(f"   {i:2d}. {nombre}")

        if len(nombres_bots) > 15:
            print(f"   ... y {len(nombres_bots) - 15} más")

        # Mostrar algunos ejemplos de patrones detectados
        print(f"\n🔍 ANÁLISIS DE PATRONES:")
        con_numeros = sum(1 for n in nombres_bots if re.search(r'\d', n))
        solo_letras = len(nombres_bots) - con_numeros
        print(f"   📊 Con números: {con_numeros}")
        print(f"   📊 Solo letras: {solo_letras}")


def main():
    """
    Función principal del programa
    """
    print("🤖 GENERADOR DE COMANDOS DE BAN MASIVO")
    print("=" * 50)
    print("📝 Versión mejorada - Compatible con logs de Chatterino")
    print()

    # 1. Extraer nombres de bots
    print("🔍 Buscando bots en 'botnames.txt'...")
    nombres_bots = extraer_nombres_bots("botnames.txt")

    if not nombres_bots:
        print("\n💡 AYUDA - FORMATOS SOPORTADOS:")
        print("   1. Logs de Chatterino completos:")
        print("      [20:49:03] streamelements: ¡Gracias por el follow botname !")
        print("   2. Logs de Chatterino cortos:")
        print("      20:49 StreamElements: ¡Gracias por el follow botname !")
        print("   3. Mensajes StreamElements directos:")
        print("      ¡Gracias por el follow botname !")
        print("   4. Lista simple (un nombre por línea)")
        print("   5. Lista separada por comas")
        print()
        print("📋 INSTRUCCIONES:")
        print("   1. Crea un archivo 'botnames.txt'")
        print("   2. Copia y pega los logs de Chatterino o lista de nombres")
        print("   3. Ejecuta este script de nuevo")
        return

    # 2. Mostrar resumen
    mostrar_resumen(nombres_bots)

    # 3. Generar archivos
    print(f"\n🛠️  GENERANDO ARCHIVOS...")

    # Detectar directorio AutoHotkey
    directorio_ahk = detectar_directorio_autohotkey()
    print(f"📂 Directorio AutoHotkey: {directorio_ahk}")

    # Generar script AutoHotkey
    if generar_script_autohotkey(nombres_bots, directorio_ahk):
        print("✅ Script AutoHotkey creado")

    # Generar listas de comandos
    generar_lista_comandos(nombres_bots)

    # 4. Instrucciones finales
    print(f"\n📋 INSTRUCCIONES DE USO:")
    print(f"")
    print(f"🎮 OPCIÓN 1 - AutoHotkey (Automático):")
    print(f"   1. Ejecuta: {directorio_ahk}/massive_ban.ahk")
    print(f"   2. Abre Chatterino/Twitch")
    print(f"   3. Enfoca el chat")
    print(f"   4. Presiona F12 (ban) o F11 (timeout)")
    print(f"   5. Presiona F9 para ayuda")
    print(f"")
    print(f"📝 OPCIÓN 2 - Manual (Copiar/Pegar):")
    print(f"   1. Abre 'comandos_ban.txt' o 'comandos_timeout.txt'")
    print(f"   2. Copia comandos individuales")
    print(f"   3. Pega en el chat de Twitch")
    print(f"")
    print(f"⚠️  IMPORTANTE:")
    print(f"   • Asegúrate de tener permisos de moderador")
    print(f"   • Los bans (F12) son permanentes")
    print(f"   • Los timeouts (F11) duran 24 horas")
    print(f"   • Revisa la lista antes de ejecutar")
    print(f"")
    print(f"✅ ¡Todo listo para usar!")


if __name__ == "__main__":
    main()