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
        self.master.geometry("1100x800")

        self.client = EmailClient()
        self.master_key = None

        # Bandeja
        self.local_emails = []
        self.displayed_emails = []
        self.email_id_to_data = {}

        # Spam
        self.spam_emails = []
        self.displayed_spam_emails = []
        self.spam_id_to_data = {}

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

        # 2. Spam (NUEVA)
        self.spam_frame = ttk.Frame(self.notebook)
        self.notebook.add(self.spam_frame, text="🛡️ Spam")
        self.create_spam_tab()

        # 3. Redactar
        self.compose_frame = ttk.Frame(self.notebook)
        self.notebook.add(self.compose_frame, text="📝 Redactar")
        self.create_compose_tab()

        # 4. Backup
        self.backup_frame = ttk.Frame(self.notebook)
        self.notebook.add(self.backup_frame, text="💾 Backup")
        self.create_backup_tab()

        # 5. Diagnóstico
        self.diag_frame = ttk.Frame(self.notebook)
        self.notebook.add(self.diag_frame, text="🔍 Diagnóstico")
        self.create_diag_tab()

        # 6. Configuración
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

    def create_spam_tab(self):
        top_bar = ttk.Frame(self.spam_frame)
        top_bar.pack(fill='x', padx=5, pady=5)

        ttk.Label(top_bar, text="Buscar:").pack(side='left', padx=2)
        self.spam_search_entry = ttk.Entry(top_bar)
        self.spam_search_entry.pack(side='left', fill='x', expand=True, padx=2)
        ttk.Button(top_bar, text="🔍", command=self.perform_spam_search).pack(side='left', padx=2)

        # Botones de Acción
        actions_bar = ttk.Frame(self.spam_frame)
        actions_bar.pack(fill='x', padx=5)
        ttk.Button(actions_bar, text="✅ Entregar", command=lambda: self.spam_action("deliver")).pack(side='left', padx=2)
        ttk.Button(actions_bar, text="🚫 Es Spam", command=lambda: self.spam_action("confirm")).pack(side='left', padx=2)
        ttk.Button(actions_bar, text="🟢 No es Spam", command=lambda: self.spam_action("not_spam")).pack(side='left', padx=2)
        ttk.Button(actions_bar, text="🗑️ Eliminar", command=lambda: self.spam_action("delete")).pack(side='left', padx=2)

        self.spam_paned = ttk.PanedWindow(self.spam_frame, orient=tk.VERTICAL)
        self.spam_paned.pack(fill='both', expand=True, padx=5, pady=5)

        list_frame = ttk.Frame(self.spam_paned)
        self.spam_paned.add(list_frame, weight=1)

        columns = ("score", "date", "from", "subject")
        self.spam_tree = ttk.Treeview(list_frame, columns=columns, show='headings', selectmode='browse')
        self.spam_tree.heading("score", text="Score")
        self.spam_tree.heading("date", text="Fecha")
        self.spam_tree.heading("from", text="De")
        self.spam_tree.heading("subject", text="Asunto")
        self.spam_tree.column("score", width=50, anchor='center')
        self.spam_tree.column("date", width=150)
        self.spam_tree.column("from", width=200)
        self.spam_tree.column("subject", width=400)
        self.spam_tree.pack(side='left', fill='both', expand=True)
        self.spam_tree.bind("<<TreeviewSelect>>", self.on_spam_select)

        scroll = ttk.Scrollbar(list_frame, orient=tk.VERTICAL, command=self.spam_tree.yview)
        self.spam_tree.configure(yscrollcommand=scroll.set)
        scroll.pack(side='right', fill='y')

        self.spam_viewer_frame = ttk.Frame(self.spam_paned)
        self.spam_paned.add(self.spam_viewer_frame, weight=2)
        self.spam_header_label = tk.Label(self.spam_viewer_frame, text="Selecciona un correo", font=('Arial', 9, 'bold'), justify='left', anchor='w')
        self.spam_header_label.pack(fill='x', padx=5, pady=5)

        if HAS_TKINTERWEB:
            self.spam_html_viewer = HtmlFrame(self.spam_viewer_frame)
            self.spam_html_viewer.pack(fill='both', expand=True)
        else:
            self.spam_text_viewer = tk.Text(self.spam_viewer_frame, wrap='word')
            self.spam_text_viewer.pack(fill='both', expand=True)

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

        # SPAM INDICATORS (NEW)
        spam_indic_frame = ttk.LabelFrame(container, text="Estado Filtros Spam", padding=5)
        spam_indic_frame.pack(fill='x', pady=5)
        self.indic_heur = ttk.Label(spam_indic_frame, text="Heurístico: ⚫")
        self.indic_heur.pack(side='left', padx=10)
        self.indic_bayes = ttk.Label(spam_indic_frame, text="Bayesiano: ⚫")
        self.indic_bayes.pack(side='left', padx=10)
        self.indic_qdrant = ttk.Label(spam_indic_frame, text="Qdrant: ⚫")
        self.indic_qdrant.pack(side='left', padx=10)
        self.indic_ollama = ttk.Label(spam_indic_frame, text="Ollama: ⚫")
        self.indic_ollama.pack(side='left', padx=10)

        self.bayes_status_label = ttk.Label(container, text="Bayesiano: Cargando...", font=('Arial', 8, 'italic'))
        self.bayes_status_label.pack(fill='x', padx=10)

        # QUARANTINE TOGGLE (NEW)
        self.quarantine_var = tk.BooleanVar()
        ttk.Checkbutton(container, text="Activar Cuarentena (mover a pestaña Spam)", variable=self.quarantine_var).pack(pady=5)

        # WHITE/BLACK LISTS (NEW)
        lists_frame = ttk.Frame(container)
        lists_frame.pack(fill='both', expand=True, pady=5)

        w_frame = ttk.LabelFrame(lists_frame, text="Lista Blanca (Direcciones/Dominios)", padding=5)
        w_frame.pack(side='left', fill='both', expand=True, padx=2)
        self.white_list_text = tk.Text(w_frame, height=5, width=20)
        self.white_list_text.pack(fill='both', expand=True)

        b_frame = ttk.LabelFrame(lists_frame, text="Lista Negra (Bloqueados)", padding=5)
        b_frame.pack(side='left', fill='both', expand=True, padx=2)
        self.black_list_text = tk.Text(b_frame, height=5, width=20)
        self.black_list_text.pack(fill='both', expand=True)

        # Credenciales
        cred_frame = ttk.LabelFrame(container, text="Identidad (Segura)", padding=5)
        cred_frame.pack(fill='x', pady=5)
        ttk.Label(cred_frame, text="Nombre:").grid(row=0, column=0); self.name_entry = ttk.Entry(cred_frame); self.name_entry.grid(row=0, column=1, sticky='ew')
        ttk.Label(cred_frame, text="Email:").grid(row=1, column=0); self.user_entry = ttk.Entry(cred_frame); self.user_entry.grid(row=1, column=1, sticky='ew')
        ttk.Label(cred_frame, text="Password:").grid(row=2, column=0); self.pass_entry = ttk.Entry(cred_frame, show="*"); self.pass_entry.grid(row=2, column=1, sticky='ew')
        cred_frame.columnconfigure(1, weight=1)

        ttk.Button(container, text="💾 Guardar Configuración", command=self.save_config).pack(pady=10)

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

        # Spam settings
        config = self.client.spam_detector.config
        self.quarantine_var.set(config.get('quarantine_enabled', True))

        self.white_list_text.delete(1.0, tk.END)
        self.white_list_text.insert(tk.END, "\n".join(config.get('white_list', [])))

        self.black_list_text.delete(1.0, tk.END)
        self.black_list_text.insert(tk.END, "\n".join(config.get('black_list', [])))

        # Indicators
        status = self.client.spam_detector.layers_status
        self.indic_heur.config(text=f"Heurístico: {'🟢' if status['heuristic'] else '⚫'}")
        self.indic_bayes.config(text=f"Bayesiano: {'🟢' if status['bayesian'] else '⚫'}")
        self.indic_qdrant.config(text=f"Qdrant: {'🟢' if status['qdrant'] else '⚫'}")
        self.indic_ollama.config(text=f"Ollama: {'🟢' if status['ollama'] else '⚫'}")

        # Bayesian details
        model = self.client.spam_detector.model
        threshold = config.get('bayesian_threshold', 200)
        msg = f"Clasificador Bayesiano: {model['spam_count']} spam / {model['not_spam_count']} no-spam clasificados."
        if not status['bayesian']:
            msg += f" ⚠️ Poco fiable — necesitas clasificar {threshold} de cada tipo."
        self.bayes_status_label.config(text=msg)

    def refresh_inbox(self):
        def task():
            self.local_emails, self.spam_emails = self.client.get_local_emails()
            self.displayed_emails = list(self.local_emails)
            self.displayed_spam_emails = list(self.spam_emails)
            self.master.after(0, self.update_tree)
            self.master.after(0, self.update_spam_tree)
        threading.Thread(target=task, daemon=True).start()

    def update_tree(self):
        for item in self.tree.get_children(): self.tree.delete(item)
        self.email_id_to_data = {}
        for em in self.displayed_emails:
            level = em.get('spam_level', "safe")
            icon = "🟢" if level == "safe" else "🟡" if level == "suspicious" else "🔴"
            item_id = self.tree.insert("", "end", values=(icon, em.get('date'), em.get('from'), em.get('subject')), tags=(f"Spam{level.capitalize()}",))
            self.email_id_to_data[item_id] = em

    def update_spam_tree(self):
        for item in self.spam_tree.get_children(): self.spam_tree.delete(item)
        self.spam_id_to_data = {}
        for em in self.displayed_spam_emails:
            score = em.get('spam_score', 0.0)
            item_id = self.spam_tree.insert("", "end", values=(score, em.get('date'), em.get('from'), em.get('subject')))
            self.spam_id_to_data[item_id] = em

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

    def perform_spam_search(self):
        q = self.spam_search_entry.get().lower()
        if not q:
            self.displayed_spam_emails = list(self.spam_emails)
        else:
            self.displayed_spam_emails = [
                em for em in self.spam_emails
                if q in em.get('subject','').lower() or q in em.get('from','').lower() or q in em.get('body_text','').lower()
            ]
        self.update_spam_tree()

    def on_email_select(self, event):
        selected = self.tree.selection()
        if not selected: return
        item_id = selected[0]
        email_data = self.email_id_to_data.get(item_id)
        self._show_email(email_data, self.header_label, self.html_viewer if HAS_TKINTERWEB else self.text_viewer)

    def on_spam_select(self, event):
        selected = self.spam_tree.selection()
        if not selected: return
        item_id = selected[0]
        email_data = self.spam_id_to_data.get(item_id)
        self._show_email(email_data, self.spam_header_label, self.spam_html_viewer if HAS_TKINTERWEB else self.spam_text_viewer)

    def _show_email(self, email_data, label, viewer):
        if email_data:
            spam_reasons = "\n".join(email_data.get('spam_reasons', []))
            layer_info = ", ".join([f"{k}: {v}" for k,v in email_data.get('layer_scores', {}).items()])
            spam_info = f"\n[SPAM] Razones: {spam_reasons}\nCapas: {layer_info}" if spam_reasons else ""

            label.config(text=f"De: {email_data.get('from')}\nAsunto: {email_data.get('subject')}{spam_info}")

            body_html = email_data.get('body_html', '')
            body_text = email_data.get('body_text', '')

            if HAS_TKINTERWEB:
                viewer.load_html(body_html if body_html else f"<html><body><pre>{body_text}</pre></body></html>")
            else:
                viewer.config(state='normal'); viewer.delete(1.0, tk.END)
                clean_text = body_text if body_text else SpamDetector.strip_tags(body_html)
                viewer.insert(tk.END, clean_text); viewer.config(state='disabled')

    def spam_action(self, action):
        selected = self.spam_tree.selection()
        if not selected:
            messagebox.showwarning("Selección", "Selecciona un correo de la lista de Spam.")
            return

        item_id = selected[0]
        email_data = self.spam_id_to_data.get(item_id)
        uid = email_data.get('uid')

        def task():
            if action == "delete":
                if messagebox.askyesno("Confirmar", "¿Seguro que quieres eliminar este correo permanentemente?"):
                    if self.client.delete_email(uid):
                        self.master.after(0, self.refresh_inbox)
            elif action == "deliver":
                self.client.spam_detector.record_feedback(uid, "not_spam", email_data)
                # En esta v1, mover a bandeja es simplemente que al refrescar ya no sea clasificado igual o lo manejamos manual
                # Para forzar que aparezca en bandeja, lo movemos en el detector (simulado)
                messagebox.showinfo("Acción", "Correo marcado para entregar. Refrescando...")
                self.master.after(0, self.refresh_inbox)
            elif action == "confirm":
                self.client.spam_detector.record_feedback(uid, "confirmed_spam", email_data)
                messagebox.showinfo("Acción", "Spam confirmado.")
                self.master.after(0, self.refresh_inbox)
            elif action == "not_spam":
                self.client.spam_detector.record_feedback(uid, "false_positive", email_data)
                messagebox.showinfo("Acción", "Marcado como Falso Positivo. Refrescando...")
                self.master.after(0, self.refresh_inbox)

        threading.Thread(target=task, daemon=True).start()

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
            log(f"Probando POP3: {self.client.pop_server}:{self.client.pop_port}")
            try:
                s = socket.create_connection((self.client.pop_server, self.client.pop_port), 10); s.close(); log("✅ Conectividad TCP POP3 OK")
                p = poplib.POP3_SSL(self.client.pop_server, self.client.pop_port, timeout=10); log("✅ SSL Handshake POP3 OK")
                p.user(self.client.username); p.pass_(self.client.password); log("✅ Autenticación POP3 OK"); p.quit()
            except Exception as e: log(f"❌ Error POP3: {e}")
            log(f"\nProbando SMTP: {self.client.smtp_server}:{self.client.smtp_port}")
            try:
                s = socket.create_connection((self.client.smtp_server, self.client.smtp_port), 10); s.close(); log("✅ Conectividad TCP SMTP OK")
                success, msg = self.client.test_smtp_auth()
                if success: log("✅ Autenticación SMTP OK")
                else: log(f"❌ Error Autenticación SMTP: {msg}")
            except Exception as e: log(f"❌ Error SMTP: {e}")
            log("\n--- Fin del diagnóstico ---")
            self.master.after(0, lambda: self.diag_output.config(state='disabled'))
        threading.Thread(target=task, daemon=True).start()

    def auto_config(self):
        email = self.email_detect.get()
        if self.client.autodetect_settings(email): self.update_config_ui(); messagebox.showinfo("Detección", "Configuración aplicada.")
        else: messagebox.showwarning("Detección", "No se encontró el dominio.")

    def save_config(self):
        # Update client settings
        self.client.pop_server = self.pop_serv_entry.get()
        self.client.pop_port = int(self.pop_port_entry.get())
        self.client.smtp_server = self.smtp_serv_entry.get()
        self.client.smtp_port = int(self.smtp_port_entry.get())
        self.client.display_name = self.name_entry.get()
        user = self.user_entry.get(); pwd = self.pass_entry.get()

        # Update Spam Detector settings
        self.client.spam_detector.config['quarantine_enabled'] = self.quarantine_var.get()
        self.client.spam_detector.config['white_list'] = [line.strip() for line in self.white_list_text.get(1.0, tk.END).splitlines() if line.strip()]
        self.client.spam_detector.config['black_list'] = [line.strip() for line in self.black_list_text.get(1.0, tk.END).splitlines() if line.strip()]
        self.client.spam_detector.save_config()

        # Ask master key only if credentials changed OR if we don't have it
        if user != self.client.username or pwd != self.client.password or not self.master_key:
            m_key = simpledialog.askstring("Clave Maestra", "Crea/Confirma tu clave maestra para cifrar los datos:", show='*')
            if m_key and len(m_key) >= 8:
                if self.client.credential_manager.save_credentials(user, pwd, self.client.display_name, m_key):
                    self.client.username = user; self.client.password = pwd; self.master_key = m_key
                    messagebox.showinfo("Éxito", "Configuración guardada.")
                else: messagebox.showerror("Error", "No se pudo guardar.")
            elif m_key: messagebox.showerror("Error", "La clave maestra debe tener al menos 8 caracteres.")
            else: return # Cancelled
        else:
            # Update credentials file with existing key
            self.client.credential_manager.save_credentials(user, pwd, self.client.display_name, self.master_key)
            messagebox.showinfo("Éxito", "Configuración guardada.")

        self.update_config_ui()

    def attach_files(self):
        f = filedialog.askopenfilenames(); self.attachments.extend(f)
        if f: messagebox.showinfo("Adjuntos", f"{len(f)} archivos añadidos.")

if __name__ == "__main__":
    root = tk.Tk(); app = Pop3Gui(root); root.mainloop()
