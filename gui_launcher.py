from __future__ import annotations

import os
import sys
import threading
import tkinter as tk
from tkinter import ttk

# =========================================================
# PALETA DE COLORES Y ESTILOS MODERNOS
# =========================================================
C_BG          = "#F8FAFC"
C_WHITE       = "#FFFFFF"
C_BLUE        = "#2563EB"
C_BLUE_HOVER  = "#1D4ED8"
C_BLUE_LIGHT  = "#EFF6FF"
C_IMP         = "#2563EB"
C_EXP         = "#059669"
C_EXP_LIGHT   = "#ECFDF5"
C_TEXT        = "#0F172A"
C_SUBTEXT     = "#64748B"
C_OK          = "#10B981"
C_ERROR       = "#EF4444"
C_DISABLED    = "#E2E8F0"
C_BORDER      = "#E2E8F0"

C_CARD_HOVER  = "#F1F5F9"

FONT_TITLE = ("Segoe UI", 24, "bold")
FONT_SUB   = ("Segoe UI", 12)
FONT_LABEL = ("Segoe UI", 11)
FONT_BOLD  = ("Segoe UI", 11, "bold")
FONT_BTN   = ("Segoe UI", 12, "bold")
FONT_SMALL = ("Segoe UI", 10)
FONT_MSG   = ("Segoe UI", 11)
FONT_ICON  = ("Segoe UI Emoji", 20)
FONT_ICON_LG = ("Segoe UI Emoji", 32)

# =========================================================
# DATOS DE MODOS
# =========================================================
MODOS = [
    ("ocr",       "📄", "1. Extracción OCR",
     "Lee los PDFs SWIFT y extrae\nlos campos en Swift Completos."),
    ("post_auto", "📋", "2. Traslado Automático",
     "Mueve los registros corregidos\nmanualmente a Swift Completos."),
    ("plantilla", "📊", "3. Generar Plantilla",
     "Crea el archivo Origen_Destino\nlisto para subir a Bancolombia."),
    ("cruces",    "🔗", "4. Cruces de Datos",
     "Asigna Formulario, Llave y\nLlave OD a Swift Completos."),
    ("completo",  "🚀", "Proceso Completo",
     "Ejecuta los 4 pasos en secuencia\nde forma ininterrumpida."),
]

# =========================================================
# TARJETA SELECCIONABLE
# =========================================================
class OptionCard(tk.Frame):
    def __init__(self, parent, icon, title, desc, value, var, accent_var=None, **kw):
        super().__init__(parent, bg=C_WHITE, highlightthickness=1,
                         highlightbackground=C_BORDER, cursor="hand2", **kw)
        self._value      = value
        self._var        = var
        self._accent_var = accent_var
        self._hovering   = False
        
        inner = tk.Frame(self, bg=C_WHITE, padx=20, pady=16)
        inner.pack(fill="both", expand=True)

        self._icon_label = tk.Label(inner, text=icon, font=FONT_ICON, bg=C_WHITE, width=2)
        self._icon_label.grid(row=0, column=0, rowspan=2, padx=(0, 16), sticky="ns")

        self._title_label = tk.Label(inner, text=title, font=FONT_BOLD, bg=C_WHITE, fg=C_TEXT, anchor="w")
        self._title_label.grid(row=0, column=1, sticky="w")

        self._desc_label = tk.Label(inner, text=desc, font=FONT_SMALL, bg=C_WHITE, fg=C_SUBTEXT, justify="left", anchor="w")
        self._desc_label.grid(row=1, column=1, sticky="w")

        self._radio_frame = tk.Frame(inner, bg=C_WHITE, width=24, height=24)
        self._radio_frame.grid(row=0, column=2, rowspan=2, padx=(16, 0))
        self._radio_frame.grid_propagate(False)
        
        self._radio = tk.Canvas(self._radio_frame, width=20, height=20, bg=C_WHITE, highlightthickness=0)
        self._radio.pack()
        self._draw_radio(False)

        inner.columnconfigure(1, weight=1)

        for w in self._all_widgets(self):
            w.bind("<Button-1>", self._click)
            w.bind("<Enter>",    self._hover_on)
            w.bind("<Leave>",    self._hover_off)

        var.trace_add("write", self._refresh)
        self._refresh()

    def _all_widgets(self, w):
        yield w
        for child in w.winfo_children():
            yield from self._all_widgets(child)

    def _click(self, _=None):
        self._var.set(self._value)

    def _hover_on(self, _=None):
        self._hovering = True
        self._refresh()

    def _hover_off(self, _=None):
        self._hovering = False
        self._refresh()

    def _accent_colors(self):
        tipo = self._accent_var.get() if self._accent_var else "imp"
        return (C_EXP_LIGHT, C_EXP) if tipo == "exp" else (C_BLUE_LIGHT, C_BLUE)

    def _draw_radio(self, selected):
        self._radio.delete("all")
        _, fg_circle = self._accent_colors()
        if selected:
            self._radio.create_oval(2, 2, 18, 18, fill=fg_circle, outline=fg_circle)
            self._radio.create_line(6, 10, 9, 13, fill="white", width=2, capstyle=tk.ROUND)
            self._radio.create_line(9, 13, 14, 7, fill="white", width=2, capstyle=tk.ROUND)
        else:
            self._radio.create_oval(2, 2, 18, 18, fill=C_WHITE, outline=C_BORDER, width=2)

    def _refresh(self, *_):
        sel = self._var.get() == self._value
        bg_sel, border_sel = self._accent_colors()

        bg = bg_sel if sel else (C_CARD_HOVER if self._hovering else C_WHITE)
        border = border_sel if sel else (C_SUBTEXT if self._hovering else C_BORDER)

        self.configure(bg=bg, highlightbackground=border, highlightthickness=2 if sel else 1)
        self._draw_radio(sel)

        for w in self._all_widgets(self):
            try:
                if w not in (self, self._radio_frame, self._radio):
                    w.configure(bg=bg)
            except tk.TclError:
                pass


# =========================================================
# VENTANA PRINCIPAL
# =========================================================
class PipelineGUI:
    def __init__(self, root: tk.Tk):
        self.root = root
        self.root.title("Origen Destino DIAN")
        self.root.configure(bg=C_BG)
        
        # Tamaño máximo y configuraciones de ventana
        self.root.maxsize(1760, 990)
        self.root.minsize(850, 650)
        self.root.resizable(True, True)
        self._fullscreen = False
        
        self._tipo    = tk.StringVar(value="imp")
        self._modo    = tk.StringVar(value="ocr")
        self._forzar  = tk.BooleanVar(value=False)
        self._running = False

        self._build()
        self._center(1100, 800)
        
        self.root.bind("<F11>", self._toggle_fullscreen)
        self.root.bind("<Escape>", self._exit_fullscreen)

    def _toggle_fullscreen(self, event=None):
        self._fullscreen = not self._fullscreen
        self.root.attributes("-fullscreen", self._fullscreen)

    def _exit_fullscreen(self, event=None):
        if self._fullscreen:
            self._toggle_fullscreen()

    def _center(self, w, h):
        screen_width = self.root.winfo_screenwidth()
        screen_height = self.root.winfo_screenheight()
        x = (screen_width - w) // 2
        y = (screen_height - h) // 2
        self.root.geometry(f"{w}x{h}+{x}+{y}")

    # ── Construcción con SCROLL ───────────────────────────
    def _build(self):
        # 1. ENCABEZADO FIJO
        hdr = tk.Frame(self.root, bg=C_BLUE, pady=24)
        hdr.pack(fill="x", side="top")
        
        hdr_content = tk.Frame(hdr, bg=C_BLUE)
        hdr_content.pack()
        
        tk.Label(hdr_content, text="🏦", font=("Segoe UI Emoji", 32), bg=C_BLUE).pack(side="left", padx=(0, 16))
        title_frame = tk.Frame(hdr_content, bg=C_BLUE)
        title_frame.pack(side="left")
        
        tk.Label(title_frame, text="Origen Destino DIAN", font=FONT_TITLE, bg=C_BLUE, fg="white").pack(anchor="w")
        tk.Label(title_frame, text="Automatización de procesamiento SWIFT", font=FONT_SUB, bg=C_BLUE, fg=C_BLUE_LIGHT).pack(anchor="w")

        # 2. SISTEMA DE SCROLL (CANVAS + SCROLLBAR)
        self.canvas = tk.Canvas(self.root, bg=C_BG, highlightthickness=0)
        self.scrollbar = ttk.Scrollbar(self.root, orient="vertical", command=self.canvas.yview)
        
        self.scrollable_frame = tk.Frame(self.canvas, bg=C_BG)
        
        self.scrollable_frame.bind(
            "<Configure>",
            lambda e: self.canvas.configure(scrollregion=self.canvas.bbox("all"))
        )
        
        self.canvas.create_window((0, 0), window=self.scrollable_frame, anchor="nw", tags="frame")
        self.canvas.configure(yscrollcommand=self.scrollbar.set)
        
        self.canvas.pack(side="left", fill="both", expand=True)
        self.scrollbar.pack(side="right", fill="y")
        
        self.root.bind_all("<MouseWheel>", self._on_mousewheel)
        self.canvas.bind("<Configure>", self._on_canvas_configure)

        # 3. CONTENIDO (DENTRO DEL SCROLL)
        body = tk.Frame(self.scrollable_frame, bg=C_BG, padx=48, pady=32)
        body.pack(fill="both", expand=True)

        # ── Sección 1: Tipo ──
        self._sec(body, "¿Qué flujo operativo desea procesar?")
        tipo_frame = tk.Frame(body, bg=C_BG)
        tipo_frame.pack(fill="x", pady=(8, 32))

        self._btn_imp = self._tipo_btn(tipo_frame, "📥  Importaciones", "imp")
        self._btn_exp = self._tipo_btn(tipo_frame, "📤  Exportaciones", "exp")
        self._tipo.trace_add("write", self._refresh_tipo)
        self._refresh_tipo()

        # ── Sección 2: Modo ──
        self._sec(body, "Seleccione la acción a ejecutar")
        grid = tk.Frame(body, bg=C_BG)
        grid.pack(fill="x", pady=(12, 24))
        grid.columnconfigure(0, weight=1)
        grid.columnconfigure(1, weight=1)

        for i, (val, icon, title, desc) in enumerate(MODOS[:-1]):
            r, c = divmod(i, 2)
            card = OptionCard(grid, icon, title, desc, val, self._modo, accent_var=self._tipo)
            card.grid(row=r, column=c, sticky="nsew", padx=(0, 12) if c == 0 else (12, 0), pady=8)

        # Proceso Completo (ocupa ambas columnas)
        val, icon, title, desc = MODOS[-1]
        card_last = OptionCard(grid, icon, title, desc, val, self._modo, accent_var=self._tipo)
        card_last.grid(row=2, column=0, columnspan=2, sticky="ew", pady=(8, 0))

        self._tipo.trace_add("write", lambda *_: [
            c._refresh() for c in grid.winfo_children() if isinstance(c, OptionCard)
        ])

        # ── Opciones extra ──
        extra_frame = tk.Frame(body, bg=C_BG)
        extra_frame.pack(fill="x", pady=(0, 32))

        self._chk_forzar = tk.Checkbutton(
            extra_frame,
            text="Forzar lectura de PDFs desde cero (ignorar caché actual)",
            variable=self._forzar, bg=C_BG, fg=C_SUBTEXT, activebackground=C_BG,
            selectcolor=C_WHITE, font=FONT_SMALL, cursor="hand2"
        )
        self._modo.trace_add("write", self._refresh_forzar)
        self._refresh_forzar()

        # ── Botón Ejecutar ──
        btn_frame = tk.Frame(body, bg=C_BG)
        btn_frame.pack(fill="x", pady=(0, 32))
        
        self.btn_run = tk.Button(
            btn_frame, text="Iniciar procesamiento  🚀", font=FONT_BTN,
            bg=C_BLUE, fg="white", activebackground=C_BLUE_HOVER, activeforeground="white",
            relief="flat", padx=32, pady=16, cursor="hand2", command=self._on_run
        )
        self.btn_run.pack(anchor="w")

        # ── Panel de Estado ──
        self._build_status(body)

    def _on_mousewheel(self, event):
        self.canvas.yview_scroll(int(-1*(event.delta/120)), "units")

    def _on_canvas_configure(self, event):
        self.canvas.itemconfig("frame", width=event.width)

    def _sec(self, parent, text):
        tk.Label(parent, text=text, font=FONT_BOLD, bg=C_BG, fg=C_TEXT).pack(anchor="w", pady=(0, 4))

    def _tipo_btn(self, parent, text, value):
        btn = tk.Button(
            parent, text=text, font=FONT_BOLD, relief="flat", padx=24, pady=12,
            cursor="hand2", command=lambda: self._tipo.set(value)
        )
        btn.pack(side="left", padx=(0, 12))
        return btn

    def _refresh_tipo(self, *_):
        sel = self._tipo.get()
        for btn, val, col in [(self._btn_imp, "imp", C_IMP), (self._btn_exp, "exp", C_EXP)]:
            if sel == val:
                btn.configure(bg=col, fg="white", highlightthickness=0)
            else:
                btn.configure(bg=C_WHITE, fg=C_SUBTEXT, highlightthickness=1, highlightbackground=C_BORDER)
        
        # btn_run puede no existir aún si se llama durante _build
        if hasattr(self, "btn_run"):
            if sel == "exp":
                self.btn_run.configure(bg=C_EXP, activebackground="#047857")
            else:
                self.btn_run.configure(bg=C_BLUE, activebackground=C_BLUE_HOVER)

    def _refresh_forzar(self, *_):
        if self._modo.get() in ("ocr", "completo"):
            self._chk_forzar.pack(anchor="w")
        else:
            self._chk_forzar.pack_forget()
            self._forzar.set(False)

    # ── Panel de estado ───────────────────────────────────
    def _build_status(self, parent):
        self._status_outer = tk.Frame(parent, bg=C_WHITE, highlightthickness=1, highlightbackground=C_BORDER)
        self._status_outer.pack(fill="x", pady=(0, 24))

        self._status_inner = tk.Frame(self._status_outer, bg=C_WHITE, padx=24, pady=24)
        self._status_inner.pack(fill="x")

        top = tk.Frame(self._status_inner, bg=C_WHITE)
        top.pack(fill="x")

        self._lbl_icon = tk.Label(top, text="💡", font=FONT_ICON_LG, bg=C_WHITE)
        self._lbl_icon.pack(side="left", padx=(0, 20))

        col = tk.Frame(top, bg=C_WHITE)
        col.pack(side="left", fill="x", expand=True)

        self._lbl_msg = tk.Label(col, text="Panel de Resultados", font=FONT_BOLD, bg=C_WHITE, fg=C_TEXT, anchor="w")
        self._lbl_msg.pack(fill="x")

        self._lbl_sub = tk.Label(col, text="El resumen del proceso aparecerá aquí.", font=FONT_MSG, bg=C_WHITE, fg=C_SUBTEXT, anchor="w")
        self._lbl_sub.pack(fill="x", pady=(4, 0))

        style = ttk.Style()
        style.theme_use("clam")
        style.configure("Modern.Horizontal.TProgressbar", troughcolor=C_BG, background=C_BLUE, thickness=6, borderwidth=0)
        self._progress = ttk.Progressbar(self._status_inner, style="Modern.Horizontal.TProgressbar", mode="indeterminate")

        self._rows_frame = tk.Frame(self._status_inner, bg=C_WHITE)

    def _set_status(self, icon, msg, sub="", color=C_TEXT, progress=False):
        def _do():
            self._lbl_icon.configure(text=icon)
            self._lbl_msg.configure(text=msg, fg=color)
            self._lbl_sub.configure(text=sub)
            for w in self._rows_frame.winfo_children(): w.destroy()
            
            if progress:
                # Cambiar color barra según tipo
                bar_color = C_EXP if self._tipo.get() == "exp" else C_BLUE
                ttk.Style().configure("Modern.Horizontal.TProgressbar", background=bar_color)
                self._progress.pack(fill="x", pady=(20, 0))
                self._progress.start(10)
            else:
                self._progress.stop()
                self._progress.pack_forget()
        self.root.after(0, _do)

    def _set_rows(self, rows: list):
        def _do():
            for w in self._rows_frame.winfo_children(): w.destroy()
            self._rows_frame.pack(fill="x", pady=(20, 0))
            
            cards_frame = tk.Frame(self._rows_frame, bg=C_WHITE)
            cards_frame.pack(fill="x")
            
            for i, (icon, label, value) in enumerate(rows):
                bg_color = C_BG if i % 2 == 0 else C_WHITE
                row = tk.Frame(cards_frame, bg=bg_color, padx=16, pady=12)
                row.pack(fill="x")
                
                tk.Label(row, text=icon, font=("Segoe UI Emoji", 14), bg=bg_color, width=2).pack(side="left", padx=(0, 12))
                tk.Label(row, text=label, font=FONT_SMALL, bg=bg_color, fg=C_SUBTEXT, anchor="w").pack(side="left")
                tk.Label(row, text=value, font=FONT_BOLD, bg=bg_color, fg=C_TEXT, anchor="e").pack(side="right", fill="x", expand=True)
        self.root.after(0, _do)

    # ── Ejecución ─────────────────────────────────────────
    def _on_run(self):
        if self._running: return
        tipo = self._tipo.get()
        modo = self._modo.get()
        label = dict((v, t) for v, _, t, _ in MODOS)[modo]
        
        self._set_status("⏳", "Procesamiento en curso...", f"Acción: {label}", progress=True)
        self._set_running(True)

        threading.Thread(target=self._execute, args=(tipo, modo, self._forzar.get()), daemon=True).start()

    def _execute(self, tipo, modo, forzar):
        try:
            from core.logger import init_logging
            from main import run_pipeline
            init_logging()

            result = run_pipeline(modo=modo, forzar=forzar, confirmar=False, tipo=tipo)
            dur = f"Completado en {result.duracion_segundos:.1f} segundos"

            if result.exitoso:
                self._set_status("✅", "Operación finalizada con éxito", dur, color=C_OK)
                self._set_rows(self._summary_rows(result))
            else:
                err = result.errores[0] if result.errores else "Revisa la consola para más detalles."
                self._set_status("❌", "El proceso se detuvo por un error", _friendly(err), color=C_ERROR)

        except Exception as e:
            import traceback
            traceback.print_exc()
            self._set_status("❌", "Error crítico en la ejecución", _friendly(str(e)), color=C_ERROR)
        finally:
            self._set_running(False)

    def _summary_rows(self, result) -> list:
        rows = []
        total_pdf = result.pdfs_nuevos_v1 + result.pdfs_nuevos_v2
        if total_pdf:
            rows.append(("📄", "Documentos leídos", str(total_pdf)))
            rows.append(("✔", "Registros consolidados", str(result.pdfs_completos)))
            if result.pdfs_incompletos:
                rows.append(("⚠", "Requieren revisión manual", str(result.pdfs_incompletos)))
        if result.manuales_movidos:
            rows.append(("📋", "Trasladados exitosamente", str(result.manuales_movidos)))
        if getattr(result, "plantilla_registros", 0):
            rows.append(("📊", "Registros listos para Bancolombia", str(result.plantilla_registros)))
        if result.formularios_cruzados:
            rows.append(("🔗", "Formularios asignados", str(result.formularios_cruzados)))
        for w in result.advertencias[:2]:
            rows.append(("⚠", "Aviso del sistema", w))
        return rows

    def _set_running(self, running: bool):
        self._running = running
        btn_bg = C_DISABLED if running else (C_EXP if self._tipo.get() == "exp" else C_BLUE)
        
        self.btn_run.configure(
            state="disabled" if running else "normal",
            text="Procesando... ⏳" if running else "Iniciar procesamiento  🚀",
            bg=btn_bg, fg=C_SUBTEXT if running else "white", cursor="watch" if running else "hand2"
        )
        for widget in self.scrollable_frame.winfo_children():
            try: widget.configure(state="disabled" if running else "normal")
            except: pass

def _friendly(raw: str) -> str:
    r = raw.lower()
    if "permission" in r: return "Cierra Excel u otros programas que estén usando los archivos."
    if "filenotfound" in r: return "Falta un archivo esencial en la carpeta de trabajo."
    return "Revisa que los directorios y archivos estén correctamente ubicados."

if __name__ == "__main__":
    root = tk.Tk()
    try: root.iconbitmap("icon.ico")
    except Exception: pass
    PipelineGUI(root)
    root.mainloop()