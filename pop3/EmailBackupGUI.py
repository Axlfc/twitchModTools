import tkinter as tk
from tkinter import ttk, messagebox, filedialog
import threading
import os
import json
from email.header import decode_header
import poplib
import email
import datetime
from pathlib import Path


class EmailBackupGUI:
    def __init__(self, master):
        self.master = master
        self.master.title("Cliente POP3 para Backup de Correos")
        self.master.geometry("800x600")

        # Inicializar cliente de backup (similar a tu implementación existente)
        self.backup_client = None
        self.config_file = "email_backup_config.json"
        self.load_configuration()

        self.create_widgets()

    def create_widgets(self):
        # Notebook para diferentes secciones
        self.notebook = ttk.Notebook(self.master)
        self.notebook.pack(fill='both', expand=True, padx=10, pady=10)

        # Pestaña de configuración
        self.config_frame = ttk.Frame(self.notebook)
        self.notebook.add(self.config_frame, text='Configuración')
        self.create_config_tab()

        # Pestaña de visualización de emails
        self.view_frame = ttk.Frame(self.notebook)
        self.notebook.add(self.view_frame, text='Vista Previa')
        self.create_view_tab()

        # Pestaña de backup
        self.backup_frame = ttk.Frame(self.notebook)
        self.notebook.add(self.backup_frame, text='Backup')
        self.create_backup_tab()

        # Pestaña de diagnóstico
        self.diag_frame = ttk.Frame(self.notebook)
        self.notebook.add(self.diag_frame, text='Diagnóstico')
        self.create_diag_tab()

    def create_config_tab(self):
        # Configuración de servidor
        server_frame = ttk.LabelFrame(self.config_frame, text="Configuración del Servidor")
        server_frame.pack(fill='x', padx=5, pady=5)

        ttk.Label(server_frame, text="Servidor POP3:").grid(row=0, column=0, sticky='w', padx=5, pady=2)
        self.pop_server = ttk.Entry(server_frame)
        self.pop_server.grid(row=0, column=1, sticky='ew', padx=5, pady=2)
        self.pop_server.insert(0, self.config.get('server', 'pop3.tinet.cat'))

        ttk.Label(server_frame, text="Puerto:").grid(row=1, column=0, sticky='w', padx=5, pady=2)
        self.pop_port = ttk.Entry(server_frame)
        self.pop_port.grid(row=1, column=1, sticky='ew', padx=5, pady=2)
        self.pop_port.insert(0, str(self.config.get('port', 995)))

        # Credenciales
        cred_frame = ttk.LabelFrame(self.config_frame, text="Credenciales")
        cred_frame.pack(fill='x', padx=5, pady=5)

        ttk.Label(cred_frame, text="Usuario:").grid(row=0, column=0, sticky='w', padx=5, pady=2)
        self.username = ttk.Entry(cred_frame)
        self.username.grid(row=0, column=1, sticky='ew', padx=5, pady=2)
        self.username.insert(0, self.config.get('username', ''))

        ttk.Label(cred_frame, text="Contraseña:").grid(row=1, column=0, sticky='w', padx=5, pady=2)
        self.password = ttk.Entry(cred_frame, show="*")
        self.password.grid(row=1, column=1, sticky='ew', padx=5, pady=2)
        self.password.insert(0, self.config.get('password', ''))

        # Directorio de backup
        dir_frame = ttk.LabelFrame(self.config_frame, text="Directorio de Backup")
        dir_frame.pack(fill='x', padx=5, pady=5)

        self.backup_dir = ttk.Entry(dir_frame)
        self.backup_dir.pack(side='left', fill='x', expand=True, padx=5, pady=2)
        self.backup_dir.insert(0, self.config.get('backup_dir', 'email_backup'))

        ttk.Button(dir_frame, text="Seleccionar", command=self.select_backup_dir).pack(side='right', padx=5)

        # Botones de acción
        btn_frame = ttk.Frame(self.config_frame)
        btn_frame.pack(fill='x', pady=10)

        ttk.Button(btn_frame, text="Guardar Configuración", command=self.save_configuration).pack(side='left', padx=5)
        ttk.Button(btn_frame, text="Conectar", command=self.connect_to_server).pack(side='left', padx=5)
        ttk.Button(btn_frame, text="Desconectar", command=self.disconnect_from_server).pack(side='left', padx=5)

        # Estado de conexión
        self.connection_status = ttk.Label(self.config_frame, text="Estado: Desconectado", foreground="red")
        self.connection_status.pack(pady=5)

    def create_view_tab(self):
        # Controles
        control_frame = ttk.Frame(self.view_frame)
        control_frame.pack(fill='x', pady=5)

        ttk.Label(control_frame, text="Mostrar últimos:").pack(side='left', padx=5)
        self.preview_count = ttk.Spinbox(control_frame, from_=1, to=100, width=5)
        self.preview_count.pack(side='left', padx=5)
        self.preview_count.set(10)

        ttk.Button(control_frame, text="Actualizar Vista", command=self.update_preview).pack(side='left', padx=5)

        # Tabla de emails
        self.email_tree = ttk.Treeview(self.view_frame, columns=("Fecha", "De", "Asunto"), show='headings')
        self.email_tree.heading("Fecha", text="Fecha")
        self.email_tree.heading("De", text="De")
        self.email_tree.heading("Asunto", text="Asunto")
        self.email_tree.pack(fill='both', expand=True, padx=5, pady=5)

        # Scrollbar
        scrollbar = ttk.Scrollbar(self.view_frame, orient=tk.VERTICAL, command=self.email_tree.yview)
        self.email_tree.configure(yscrollcommand=scrollbar.set)
        scrollbar.pack(side=tk.RIGHT, fill=tk.Y)

    def create_backup_tab(self):
        # Opciones de backup
        options_frame = ttk.LabelFrame(self.backup_frame, text="Opciones de Backup")
        options_frame.pack(fill='x', padx=5, pady=5)

        self.backup_all = tk.BooleanVar(value=True)
        ttk.Checkbutton(options_frame, text="Hacer backup de todos los emails",
                        variable=self.backup_all, command=self.toggle_backup_limit).grid(
            row=0, column=0, columnspan=2, sticky='w', padx=5)

        ttk.Label(options_frame, text="Número de emails a guardar:").grid(
            row=1, column=0, sticky='w', padx=15, pady=2)
        self.backup_limit = ttk.Spinbox(options_frame, from_=1, to=1000, width=10)
        self.backup_limit.grid(row=1, column=1, sticky='w', padx=5)
        self.backup_limit.set(10)
        self.backup_limit.config(state='disabled')

        # Progreso
        self.progress = ttk.Progressbar(self.backup_frame, mode='determinate')
        self.progress.pack(fill='x', padx=5, pady=5)

        # Resultados
        self.result_text = tk.Text(self.backup_frame, height=10, state='disabled')
        self.result_text.pack(fill='both', expand=True, padx=5, pady=5)

        # Botón de inicio
        ttk.Button(self.backup_frame, text="Iniciar Backup", command=self.start_backup).pack(pady=10)

    def create_diag_tab(self):
        # Diagnóstico de conexión
        diag_frame = ttk.LabelFrame(self.diag_frame, text="Diagnóstico de Conexión")
        diag_frame.pack(fill='both', expand=True, padx=5, pady=5)

        self.diag_text = tk.Text(diag_frame, height=20, state='disabled')
        self.diag_text.pack(fill='both', expand=True, padx=5, pady=5)

        ttk.Button(diag_frame, text="Ejecutar Diagnóstico", command=self.run_diagnosis).pack(pady=5)

    def load_configuration(self):
        """Cargar la configuración guardada"""
        if os.path.exists(self.config_file):
            try:
                with open(self.config_file, 'r') as f:
                    self.config = json.load(f)
            except Exception as e:
                print(f"Error cargando configuración: {e}")
                self.config = {}
        else:
            self.config = {}

    def save_configuration(self):
        """Guardar la configuración"""
        self.config['server'] = self.pop_server.get().strip()
        self.config['port'] = int(self.pop_port.get().strip())
        self.config['username'] = self.username.get().strip()
        self.config['password'] = self.password.get().strip()
        self.config['backup_dir'] = self.backup_dir.get().strip()

        try:
            with open(self.config_file, 'w') as f:
                json.dump(self.config, f, indent=2)
            messagebox.showinfo("Éxito", "¡Configuración guardada correctamente!")
        except Exception as e:
            messagebox.showerror("Error", f"No se pudo guardar la configuración:\n{e}")

    def select_backup_dir(self):
        """Seleccionar directorio de backup"""
        directory = filedialog.askdirectory(initialdir=self.backup_dir.get())
        if directory:
            self.backup_dir.delete(0, tk.END)
            self.backup_dir.insert(0, directory)

    def connect_to_server(self):
        """Conectar al servidor POP3"""
        if not self.validate_credentials():
            return

        self.append_result("Conectando al servidor...", self.diag_text)

        try:
            # Crear una instancia del cliente de backup
            self.backup_client = poplib.POP3_SSL(self.pop_server.get(), int(self.pop_port.get()))

            # Autenticar
            self.backup_client.user(self.username.get())
            self.backup_client.pass_(self.password.get())

            self.connection_status.config(text="Estado: Conectado", foreground="green")
            messagebox.showinfo("Éxito", "¡Conexión establecida correctamente!")
            self.append_result("Conexión exitosa\n", self.diag_text)

        except Exception as e:
            self.connection_status.config(text="Estado: Error de conexión", foreground="orange")
            messagebox.showerror("Error", f"No se pudo conectar al servidor:\n{str(e)}")
            self.append_result(f"Error: {str(e)}\n", self.diag_text)

    def disconnect_from_server(self):
        """Desconectar del servidor"""
        if self.backup_client:
            try:
                self.backup_client.quit()
                self.backup_client = None
                self.connection_status.config(text="Estado: Desconectado", foreground="red")
                messagebox.showinfo("Éxito", "¡Desconexión realizada correctamente!")
            except Exception as e:
                messagebox.showerror("Error", f"No se pudo desconectar correctamente:\n{str(e)}")

    def validate_credentials(self):
        """Validar que las credenciales estén completas"""
        if not self.pop_server.get().strip() or not self.pop_port.get().strip():
            messagebox.showerror("Error", "Debe especificar el servidor y puerto POP3")
            return False

        if not self.username.get().strip() or not self.password.get().strip():
            messagebox.showerror("Error", "Debe especificar usuario y contraseña")
            return False

        return True

    def update_preview(self):
        """Actualizar la vista previa de emails"""
        if not self.backup_client:
            messagebox.showerror("Error", "Primero debe conectarse al servidor")
            return

        try:
            # Obtener lista de emails
            num_messages = len(self.backup_client.list()[1])
            count = min(int(self.preview_count.get()), num_messages)

            # Limpiar tabla
            for item in self.email_tree.get_children():
                self.email_tree.delete(item)

            # Agregar emails recientes
            for i in range(max(1, num_messages - count + 1), num_messages + 1):
                try:
                    headers = self.backup_client.top(i, 0)[1]
                    email_obj = email.message_from_bytes(b"\n".join(headers))

                    subject = self.decode_mime_words(email_obj.get('Subject', 'Sin asunto'))[:50]
                    sender = self.decode_mime_words(email_obj.get('From', 'Desconocido'))[:30]
                    date = email_obj.get('Date', 'Fecha desconocida')[:20]

                    self.email_tree.insert('', 'end', iid=i, values=(date, sender, subject))
                except Exception as e:
                    self.email_tree.insert('', 'end', iid=i, values=("Error", f"Email {i}", str(e)[:100]))

        except Exception as e:
            messagebox.showerror("Error", f"No se pudo obtener la lista de emails:\n{str(e)}")

    def toggle_backup_limit(self):
        """Habilitar/deshabilitar el límite de backup"""
        state = 'normal' if not self.backup_all.get() else 'disabled'
        self.backup_limit.config(state=state)

    def start_backup(self):
        """Iniciar proceso de backup"""
        if not self.backup_client:
            messagebox.showerror("Error", "Primero debe conectarse al servidor")
            return

        if not self.backup_all.get():
            limit = int(self.backup_limit.get())
        else:
            limit = None

        self.progress['value'] = 0
        self.result_text.config(state='normal')
        self.result_text.delete(1.0, tk.END)
        self.result_text.config(state='disabled')

        # Ejecutar el backup en un hilo separado para no bloquear la UI
        threading.Thread(target=self.perform_backup, args=(limit,), daemon=True).start()

    def perform_backup(self, limit=None):
        """Realizar el backup de emails"""
        try:
            num_messages = len(self.backup_client.list()[1])
            total = min(num_messages, limit) if limit else num_messages

            self.append_result(f"Iniciando backup de {total} emails...\n", self.result_text)

            backup_dir = self.backup_dir.get()
            if not os.path.exists(backup_dir):
                os.makedirs(backup_dir)

            successful_backups = 0
            for i in range(1, total + 1):
                self.append_result(f"Procesando email {i}/{total}... ", self.result_text)

                try:
                    raw_email = b"\n".join(self.backup_client.retr(i)[1])
                    email_obj = email.message_from_bytes(raw_email)

                    if self.save_email(email_obj, backup_dir, i):
                        successful_backups += 1
                        self.append_result("✅\n", self.result_text, 'success')
                    else:
                        self.append_result("❌\n", self.result_text, 'error')
                except Exception as e:
                    self.append_result(f"❌ Error: {str(e)}\n", self.result_text, 'error')

                # Actualizar barra de progreso
                progress = (i / total) * 100
                self.progress['value'] = progress

            summary = {
                'timestamp': datetime.datetime.now().isoformat(),
                'total_emails': total,
                'successful_backups': successful_backups,
                'server': self.pop_server.get(),
                'username': self.username.get()
            }

            with open(os.path.join(backup_dir, 'backup_summary.json'), 'w') as f:
                json.dump(summary, f, indent=2)

            self.append_result(f"\nBackup completado: {successful_backups}/{total} emails guardados", self.result_text)

        except Exception as e:
            self.append_result(f"❌ Error durante el backup: {str(e)}", self.result_text, 'error')

    def save_email(self, email_obj, backup_dir, email_id):
        """Guardar un email individual con sus adjuntos"""
        try:
            # Extraer información básica
            subject = self.decode_mime_words(email_obj.get('Subject', 'Sin asunto'))
            sender = self.decode_mime_words(email_obj.get('From', 'Desconocido'))
            date = email_obj.get('Date', 'Fecha desconocida')

            # Crear estructura de datos del email
            email_data = {
                'id': email_id,
                'subject': subject,
                'from': sender,
                'to': self.decode_mime_words(email_obj.get('To', '')),
                'date': date,
                'headers': dict(email_obj.items()),
                'body_text': '',
                'body_html': '',
                'attachments': []
            }

            # Crear carpeta específica para este email
            safe_subject = "".join(c for c in subject if c.isalnum() or c in (' ', '-', '_')).rstrip()[:50]
            email_folder = os.path.join(backup_dir, f"email_{email_id:04d}_{safe_subject}")
            os.makedirs(email_folder, exist_ok=True)

            # Procesar partes del mensaje
            if email_obj.is_multipart():
                for part in email_obj.walk():
                    content_type = part.get_content_type()
                    content_disposition = str(part.get("Content-Disposition", ""))

                    # Guardar cuerpo de texto plano
                    if content_type == "text/plain" and "attachment" not in content_disposition:
                        body = part.get_payload(decode=True)
                        if body:
                            email_data['body_text'] = body.decode('utf-8', errors='ignore')

                    # Guardar cuerpo HTML
                    elif content_type == "text/html" and "attachment" not in content_disposition:
                        body = part.get_payload(decode=True)
                        if body:
                            email_data['body_html'] = body.decode('utf-8', errors='ignore')

                    # Guardar archivo adjunto
                    elif "attachment" in content_disposition:
                        filename = part.get_filename()
                        if filename:
                            decoded_filename = self.decode_mime_words(filename)
                            payload = part.get_payload(decode=True)
                            if payload:
                                # Guardar el archivo en la carpeta correspondiente
                                filepath = os.path.join(email_folder, decoded_filename)
                                with open(filepath, 'wb') as f:
                                    f.write(payload)

                                # Registrar en JSON
                                email_data['attachments'].append({
                                    'filename': decoded_filename,
                                    'content_type': content_type,
                                    'size': len(payload)
                                })
            else:
                # Email simple (no multipart)
                body = email_obj.get_payload(decode=True)
                if body:
                    email_data['body_text'] = body.decode('utf-8', errors='ignore')

            # Guardar metadata del email en formato JSON
            filename = f"email_{email_id:04d}_{safe_subject}.json"
            filepath = os.path.join(email_folder, filename)
            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(email_data, f, ensure_ascii=False, indent=2)

            return True

        except Exception as e:
            print(f"Error guardando email {email_id}: {e}")
            return False

    def run_diagnosis(self):
        """Ejecutar diagnóstico de conexión"""
        if not self.validate_credentials():
            return

        self.diag_text.config(state='normal')
        self.diag_text.delete(1.0, tk.END)
        self.diag_text.config(state='disabled')

        threading.Thread(target=self.perform_diagnosis, daemon=True).start()

    def perform_diagnosis(self):
        """Realizar diagnóstico detallado de conexión"""
        self.append_result("🔍 === DIAGNÓSTICO DE CONEXIÓN ===\n", self.diag_text)

        # Verificar credenciales
        if not self.username.get() or not self.password.get():
            self.append_result("❌ No hay credenciales configuradas\n", self.diag_text, 'error')
            return

        self.append_result(f"✅ Servidor: {self.pop_server.get()}\n", self.diag_text)
        self.append_result(f"✅ Puerto: {self.pop_port.get()}\n", self.diag_text)
        self.append_result(f"✅ Usuario: {self.username.get()}\n", self.diag_text)
        self.append_result(f"✅ Password: {'Configurado' if self.password.get() else 'No configurado'}\n\n",
                           self.diag_text)

        # Verificar conectividad básica
        self.append_result(f"🌐 Probando conectividad a {self.pop_server.get()}:{self.pop_port.get()}...",
                           self.diag_text)

        try:
            import socket
            sock = socket.create_connection((self.pop_server.get(), self.pop_port.get()), timeout=10)
            sock.close()
            self.append_result(" ✅\n", self.diag_text, 'success')
        except Exception as e:
            self.append_result(f" ❌ ({str(e)})\n", self.diag_text, 'error')
            return

        # Probar conexión POP3
        self.append_result("\n🔐 Probando conexión POP3...\n", self.diag_text)

        try:
            test_server = poplib.POP3_SSL(self.pop_server.get(), int(self.pop_port.get()))
            self.append_result("✅ Conexión SSL establecida\n", self.diag_text, 'success')

            # Probar usuario
            try:
                response = test_server.user(self.username.get())
                self.append_result(f"✅ Usuario aceptado: {response.decode()}\n", self.diag_text, 'success')
            except Exception as e:
                self.append_result(f"❌ Error con usuario: {str(e)}\n", self.diag_text, 'error')
                test_server.quit()
                return

            # Probar contraseña
            try:
                response = test_server.pass_(self.password.get())
                self.append_result(f"✅ Contraseña aceptada: {response.decode()}\n", self.diag_text, 'success')
                self.append_result("🎉 ¡Autenticación exitosa!\n", self.diag_text, 'success')
            except Exception as e:
                self.append_result(f"❌ Error con contraseña: {str(e)}\n", self.diag_text, 'error')
                self.append_result("\n💡 POSIBLES SOLUCIONES:\n", self.diag_text)
                self.append_result("   1. Verifica que la contraseña sea correcta\n", self.diag_text)
                self.append_result("   2. Comprueba si necesitas una 'contraseña de aplicación'\n", self.diag_text)
                self.append_result("   3. Verifica que POP3 esté habilitado en tu cuenta\n", self.diag_text)
                self.append_result("   4. Revisa si hay espacios extra en las credenciales\n", self.diag_text)
                self.append_result("   5. Algunos proveedores requieren autenticación de 2 factores\n", self.diag_text)

            test_server.quit()
        except Exception as e:
            self.append_result(f"❌ Error de conexión POP3: {str(e)}\n", self.diag_text, 'error')
            self.append_result("\n💡 VERIFICA:\n", self.diag_text)
            self.append_result("   - Servidor y puerto correctos\n", self.diag_text)
            self.append_result("   - Firewall o proxy corporativo\n", self.diag_text)
            self.append_result("   - Certificados SSL válidos\n", self.diag_text)

    def append_result(self, text, text_widget, tag=None):
        """Añadir texto al área de resultados"""
        text_widget.config(state='normal')
        text_widget.insert(tk.END, text, (tag,) if tag else ())
        text_widget.config(state='disabled')
        text_widget.see(tk.END)

    def decode_mime_words(self, s):
        """Decodificar headers de email"""
        if not s:
            return ""
        try:
            decoded_parts = decode_header(s)
            decoded_string = ""
            for part, encoding in decoded_parts:
                if isinstance(part, bytes):
                    decoded_string += part.decode(encoding or 'utf-8', errors='ignore')
                else:
                    decoded_string += str(part)
            return decoded_string
        except Exception:
            return str(s)


if __name__ == "__main__":
    root = tk.Tk()
    app = EmailBackupGUI(root)
    root.mainloop()