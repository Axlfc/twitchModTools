import tkinter as tk
from tkinter import ttk, messagebox, filedialog, simpledialog
import threading
import socket
import poplib
from pop3_client import EmailClient
from spam_detector import SpamDetector
from datetime import datetime
try:
    from tkinterweb import HtmlFrame
    HAS_TKINTERWEB = True
except ImportError:
    HAS_TKINTERWEB = False

class Pop3Gui:
    def __init__(self, master):
        self.master = master
        self.master.title("Python Email Client (POP3/SMTP)")
        self.master.geometry("1000x750")

        self.client = EmailClient()
        self.master_key = None
        self.local_emails = [] # Todos los emails cargados
        self.displayed_emails = [] # Emails actualmente en el Treeview (filtrados o no)
        self.email_id_to_data = {} # Mapeo de ID de Treeview -> email_data

        self.setup_styles()
        self.create_widgets()

        # Intentar carga inicial
        self.initial_load()

    def setup_styles(self):
        style = ttk.Style()
        style.configure("SpamSafe.Treeview", foreground="green")
        style.configure("SpamSuspicious.Treeview", foreground="#CCCC00")
        style.configure("SpamSpam.Treeview", foreground="red")

    def create_widgets(self):
        self.notebook = ttk.Notebook(self.master)
        self.notebook.pack(fill='both', expand=True, padx=5, pady=5)

        # 1. Bandeja de Entrada
        self.inbox_frame = ttk.Frame(self.notebook)
        self.notebook.add(self.inbox_frame, text="📥 Bandeja")
        self.create_inbox_tab()

        # 2. Redactar
        self.compose_frame = ttk.Frame(self.notebook)
        self.notebook.add(self.compose_frame, text="📝 Redactar")
        self.create_compose_tab()

        # 3. Backup
        self.backup_frame = ttk.Frame(self.notebook)
        self.notebook.add(self.backup_frame, text="💾 Backup")
        self.create_backup_tab()

        # 4. Diagnóstico
        self.diag_frame = ttk.Frame(self.notebook)
        self.notebook.add(self.diag_frame, text="🔍 Diagnóstico")
        self.create_diag_tab()

        # 5. Configuración
        self.config_frame = ttk.Frame(self.notebook)
        self.notebook.add(self.config_frame, text="⚙️ Ajustes")
        self.create_config_tab()

    def create_inbox_tab(self):
        top_bar = ttk.Frame(self.inbox_frame)
        top_bar.pack(fill='x', padx=5, pady=5)

        ttk.Label(top_bar, text="Buscar:").pack(side='left', padx=2)
        self.search_entry = ttk.Entry(top_bar)
        self.search_entry.pack(side='left', fill='x', expand=True, padx=2)
        ttk.Button(top_bar, text="🔍", command=self.perform_search).pack(side='left', padx=2)
        ttk.Button(top_bar, text="🔄", command=self.refresh_inbox).pack(side='left', padx=2)

        self.paned = ttk.PanedWindow(self.inbox_frame, orient=tk.VERTICAL)
        self.paned.pack(fill='both', expand=True, padx=5, pady=5)

        list_frame = ttk.Frame(self.paned)
        self.paned.add(list_frame, weight=1)

        columns = ("spam", "date", "from", "subject")
        self.tree = ttk.Treeview(list_frame, columns=columns, show='headings', selectmode='browse')
        self.tree.heading("spam", text="🛡️")
        self.tree.heading("date", text="Fecha")
        self.tree.heading("from", text="De")
        self.tree.heading("subject", text="Asunto")
        self.tree.column("spam", width=30, anchor='center')
        self.tree.column("date", width=150)
        self.tree.column("from", width=200)
        self.tree.column("subject", width=400)
        self.tree.pack(side='left', fill='both', expand=True)
        self.tree.bind("<<TreeviewSelect>>", self.on_email_select)

        scroll = ttk.Scrollbar(list_frame, orient=tk.VERTICAL, command=self.tree.yview)
        self.tree.configure(yscrollcommand=scroll.set)
        scroll.pack(side='right', fill='y')

        self.viewer_frame = ttk.Frame(self.paned)
        self.paned.add(self.viewer_frame, weight=2)
        self.header_label = tk.Label(self.viewer_frame, text="Selecciona un correo", font=('Arial', 9, 'bold'), justify='left', anchor='w')
        self.header_label.pack(fill='x', padx=5, pady=5)

        if HAS_TKINTERWEB:
            self.html_viewer = HtmlFrame(self.viewer_frame)
            self.html_viewer.pack(fill='both', expand=True)
        else:
            self.text_viewer = tk.Text(self.viewer_frame, wrap='word')
            self.text_viewer.pack(fill='both', expand=True)

    def create_compose_tab(self):
        form = ttk.Frame(self.compose_frame, padding=10)
        form.pack(fill='both', expand=True)
        ttk.Label(form, text="Para:").grid(row=0, column=0, sticky='w', pady=2)
        self.to_entry = ttk.Entry(form)
        self.to_entry.grid(row=0, column=1, sticky='ew', pady=2)
        ttk.Label(form, text="Asunto:").grid(row=1, column=0, sticky='w', pady=2)
        self.subject_entry = ttk.Entry(form)
        self.subject_entry.grid(row=1, column=1, sticky='ew', pady=2)
        ttk.Label(form, text="Mensaje:").grid(row=2, column=0, sticky='nw', pady=2)
        self.body_text = tk.Text(form, height=12)
        self.body_text.grid(row=2, column=1, sticky='nsew', pady=2)
        btn_frame = ttk.Frame(form)
        btn_frame.grid(row=3, column=1, sticky='e', pady=10)
        ttk.Button(btn_frame, text="📎 Adjuntar", command=self.attach_files).pack(side='left', padx=5)
        ttk.Button(btn_frame, text="🚀 Enviar", command=self.send_email).pack(side='left', padx=5)
        form.columnconfigure(1, weight=1); form.rowconfigure(2, weight=1)
        self.attachments = []

    def create_backup_tab(self):
        frame = ttk.Frame(self.backup_frame, padding=20)
        frame.pack(fill='both', expand=True)
        ttk.Label(frame, text="💾 Respaldo de Correos", font=('Arial', 12, 'bold')).pack(pady=10)
        self.backup_progress = ttk.Progressbar(frame, mode='indeterminate')
        self.backup_progress.pack(fill='x', pady=10)
        ttk.Button(frame, text="Iniciar Descarga POP3", command=self.run_backup).pack(pady=10)
        self.backup_status = ttk.Label(frame, text="Listo")
        self.backup_status.pack()

    def create_diag_tab(self):
        frame = ttk.Frame(self.diag_frame, padding=10)
        frame.pack(fill='both', expand=True)
        self.diag_output = tk.Text(frame, state='disabled', wrap='word', bg='#f0f0f0')
        self.diag_output.pack(fill='both', expand=True, pady=5)
        ttk.Button(frame, text="🔍 Ejecutar Test de Conexión", command=self.run_diagnosis).pack()

    def create_config_tab(self):
        container = ttk.Frame(self.config_frame, padding=10)
        container.pack(fill='both', expand=True)

        # Autodetección
        detect_frame = ttk.LabelFrame(container, text="Detección Automática", padding=5)
        detect_frame.pack(fill='x', pady=5)
        self.email_detect = ttk.Entry(detect_frame); self.email_detect.pack(side='left', fill='x', expand=True, padx=5)
        ttk.Button(detect_frame, text="Configurar", command=self.auto_config).pack(side='left')

        # Servidores
        serv_frame = ttk.LabelFrame(container, text="Servidores", padding=5)
        serv_frame.pack(fill='x', pady=5)
        grid = ttk.Frame(serv_frame); grid.pack(fill='x')
        ttk.Label(grid, text="POP:").grid(row=0, column=0); self.pop_serv_entry = ttk.Entry(grid); self.pop_serv_entry.grid(row=0, column=1, sticky='ew')
        ttk.Label(grid, text="Port:").grid(row=0, column=2); self.pop_port_entry = ttk.Entry(grid, width=8); self.pop_port_entry.grid(row=0, column=3)
        ttk.Label(grid, text="SMTP:").grid(row=1, column=0); self.smtp_serv_entry = ttk.Entry(grid); self.smtp_serv_entry.grid(row=1, column=1, sticky='ew')
        ttk.Label(grid, text="Port:").grid(row=1, column=2); self.smtp_port_entry = ttk.Entry(grid, width=8); self.smtp_port_entry.grid(row=1, column=3)
        grid.columnconfigure(1, weight=1)

        # Credenciales
        cred_frame = ttk.LabelFrame(container, text="Identidad (Segura)", padding=5)
        cred_frame.pack(fill='x', pady=5)
        ttk.Label(cred_frame, text="Nombre:").grid(row=0, column=0); self.name_entry = ttk.Entry(cred_frame); self.name_entry.grid(row=0, column=1, sticky='ew')
        ttk.Label(cred_frame, text="Email:").grid(row=1, column=0); self.user_entry = ttk.Entry(cred_frame); self.user_entry.grid(row=1, column=1, sticky='ew')
        ttk.Label(cred_frame, text="Password:").grid(row=2, column=0); self.pass_entry = ttk.Entry(cred_frame, show="*"); self.pass_entry.grid(row=2, column=1, sticky='ew')
        cred_frame.columnconfigure(1, weight=1)

        ttk.Button(container, text="💾 Guardar con Clave Maestra", command=self.save_config).pack(pady=10)

    # Lógica
    def initial_load(self):
        u, p, d = self.client.credential_manager.load_from_env()
        if u and p:
            self.client.username, self.client.password, self.client.display_name = u, p, d
            self.update_config_ui()
            self.refresh_inbox()
        else:
            # Intentar cargar desde archivo seguro
            if self.client.credential_manager.config_file.exists():
                self.ask_master_key()

    def ask_master_key(self):
        key = simpledialog.askstring("Clave Maestra", "Introduce tu clave maestra para desbloquear credenciales:", show='*')
        if key:
            u, p, d = self.client.credential_manager.load_credentials(key)
            if u and p:
                self.master_key = key
                self.client.username, self.client.password, self.client.display_name = u, p, d
                self.update_config_ui()
                self.refresh_inbox()
            else:
                messagebox.showerror("Error", "Clave maestra incorrecta")

    def update_config_ui(self):
        self.pop_serv_entry.delete(0, tk.END); self.pop_serv_entry.insert(0, self.client.pop_server)
        self.pop_port_entry.delete(0, tk.END); self.pop_port_entry.insert(0, str(self.client.pop_port))
        self.smtp_serv_entry.delete(0, tk.END); self.smtp_serv_entry.insert(0, self.client.smtp_server)
        self.smtp_port_entry.delete(0, tk.END); self.smtp_port_entry.insert(0, str(self.client.smtp_port))
        self.name_entry.delete(0, tk.END); self.name_entry.insert(0, self.client.display_name)
        self.user_entry.delete(0, tk.END); self.user_entry.insert(0, self.client.username or "")

    def refresh_inbox(self):
        self.local_emails = self.client.get_local_emails()
        self.displayed_emails = list(self.local_emails)
        self.update_tree()

    def update_tree(self):
        for item in self.tree.get_children(): self.tree.delete(item)
        self.email_id_to_data = {}
        for em in self.displayed_emails:
            level = em.get('spam_level', "safe")
            icon = "🟢" if level == "safe" else "🟡" if level == "suspicious" else "🔴"
            item_id = self.tree.insert("", "end", values=(icon, em.get('date'), em.get('from'), em.get('subject')), tags=(f"Spam{level.capitalize()}",))
            self.email_id_to_data[item_id] = em

    def perform_search(self):
        q = self.search_entry.get().lower()
        if not q:
            self.displayed_emails = list(self.local_emails)
        else:
            self.displayed_emails = [
                em for em in self.local_emails
                if q in em.get('subject','').lower() or q in em.get('from','').lower() or q in em.get('body_text','').lower()
            ]
        self.update_tree()

    def on_email_select(self, event):
        selected = self.tree.selection()
        if not selected: return
        item_id = selected[0]
        email_data = self.email_id_to_data.get(item_id)
        if email_data:
            spam_reasons = "\n".join(email_data.get('spam_reasons', []))
            spam_info = f"\n[SPAM] Razones:\n{spam_reasons}" if spam_reasons else ""

            self.header_label.config(text=f"De: {email_data.get('from')}\nAsunto: {email_data.get('subject')}{spam_info}")

            body_html = email_data.get('body_html', '')
            body_text = email_data.get('body_text', '')

            if HAS_TKINTERWEB:
                self.html_viewer.load_html(body_html if body_html else f"<html><body><pre>{body_text}</pre></body></html>")
            else:
                self.text_viewer.config(state='normal'); self.text_viewer.delete(1.0, tk.END)
                clean_text = body_text if body_text else SpamDetector.strip_tags(body_html)
                self.text_viewer.insert(tk.END, clean_text); self.text_viewer.config(state='disabled')

    def send_email(self):
        if not self.client.username or not self.client.password:
            messagebox.showwarning("Configuración", "Configura tus credenciales antes de enviar.")
            return
        def task():
            if self.client.send_email(self.to_entry.get(), self.subject_entry.get(), self.body_text.get(1.0, tk.END), self.attachments):
                messagebox.showinfo("Éxito", "Email enviado"); self.body_text.delete(1.0, tk.END)
            else: messagebox.showerror("Error", "Fallo al enviar")
        threading.Thread(target=task, daemon=True).start()

    def run_backup(self):
        if not self.client.username: messagebox.showwarning("Aviso", "Configura tu cuenta primero"); return
        self.backup_progress.start(); self.backup_status.config(text="Descargando...")
        def task():
            success = self.client.backup_all_emails()
            self.master.after(0, lambda: (self.backup_progress.stop(), self.refresh_inbox()))
            msg = "Completado" if success else "Error"
            self.master.after(0, lambda: self.backup_status.config(text=msg))
        threading.Thread(target=task, daemon=True).start()

    def run_diagnosis(self):
        self.diag_output.config(state='normal'); self.diag_output.delete(1.0, tk.END)
        def log(msg): self.master.after(0, lambda: (self.diag_output.insert(tk.END, msg + "\n"), self.diag_output.see(tk.END)))
        def task():
            log(f"--- Diagnóstico {datetime.now().strftime('%H:%M:%S')} ---")

            # POP3
            log(f"Probando POP3: {self.client.pop_server}:{self.client.pop_port}")
            try:
                s = socket.create_connection((self.client.pop_server, self.client.pop_port), 10); s.close(); log("✅ Conectividad TCP POP3 OK")
                p = poplib.POP3_SSL(self.client.pop_server, self.client.pop_port, timeout=10); log("✅ SSL Handshake POP3 OK")
                p.user(self.client.username); p.pass_(self.client.password); log("✅ Autenticación POP3 OK"); p.quit()
            except Exception as e: log(f"❌ Error POP3: {e}")

            # SMTP
            log(f"\nProbando SMTP: {self.client.smtp_server}:{self.client.smtp_port}")
            try:
                s = socket.create_connection((self.client.smtp_server, self.client.smtp_port), 10); s.close(); log("✅ Conectividad TCP SMTP OK")
                success, msg = self.client.test_smtp_auth()
                if success:
                    log("✅ Autenticación SMTP OK")
                else:
                    log(f"❌ Error Autenticación SMTP: {msg}")
            except Exception as e: log(f"❌ Error SMTP: {e}")

            log("\n--- Fin del diagnóstico ---")
            self.master.after(0, lambda: self.diag_output.config(state='disabled'))
        threading.Thread(target=task, daemon=True).start()

    def auto_config(self):
        email = self.email_detect.get()
        if self.client.autodetect_settings(email): self.update_config_ui(); messagebox.showinfo("Detección", "Configuración aplicada.")
        else: messagebox.showwarning("Detección", "No se encontró el dominio.")

    def save_config(self):
        self.client.pop_server = self.pop_serv_entry.get()
        self.client.pop_port = int(self.pop_port_entry.get())
        self.client.smtp_server = self.smtp_serv_entry.get()
        self.client.smtp_port = int(self.smtp_port_entry.get())
        self.client.display_name = self.name_entry.get()
        user = self.user_entry.get(); pwd = self.pass_entry.get()

        m_key = simpledialog.askstring("Clave Maestra", "Crea/Confirma tu clave maestra para cifrar los datos:", show='*')
        if m_key and len(m_key) >= 8:
            if self.client.credential_manager.save_credentials(user, pwd, self.client.display_name, m_key):
                self.client.username = user; self.client.password = pwd; self.master_key = m_key
                messagebox.showinfo("Éxito", "Configuración guardada de forma segura.")
            else: messagebox.showerror("Error", "No se pudo guardar.")
        else: messagebox.showerror("Error", "La clave maestra debe tener al menos 8 caracteres.")

    def attach_files(self):
        f = filedialog.askopenfilenames(); self.attachments.extend(f)
        if f: messagebox.showinfo("Adjuntos", f"{len(f)} archivos añadidos.")

if __name__ == "__main__":
    root = tk.Tk(); app = Pop3Gui(root); root.mainloop()
