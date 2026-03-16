from __future__ import annotations

import threading
import customtkinter as ctk
from tkinter import messagebox

# =========================================================
# TEMA Y PALETA — idéntica al repositorio PayPal
# =========================================================
ctk.set_appearance_mode("light")
ctk.set_default_color_theme("blue")

C_BG           = "#FFFFFF"      # Fondo ventana
C_SURFACE      = "#F0F2F5"      # Fondo frames / cards
C_BORDER       = "#E4E6EB"      # Bordes sutiles
C_PRIMARY_IMP  = "#08129B"      # Azul importaciones
C_PRIMARY_EXP  = "#059669"      # Verde exportaciones
C_PRIMARY_GTO  = "#B45309"      # Naranja gastos
C_PRIMARY_HVR  = "#060D6F"      # Hover azul
C_EXP_HVR      = "#047857"      # Hover verde
C_GTO_HVR      = "#92400E"      # Hover naranja
C_TEXT         = "#1C1E21"      # Texto principal
C_SUBTEXT      = "#65676B"      # Texto secundario
C_OK           = "#388E3C"      # Verde éxito
C_ERROR        = "#D32F2F"      # Rojo error
C_WARNING      = "#F57C00"      # Naranja advertencia
C_DISABLED     = "#BCC0C4"      # Gris deshabilitado

FONT_TITLE  = ("Segoe UI", 22, "bold")
FONT_SUB    = ("Segoe UI", 11)
FONT_LABEL  = ("Segoe UI", 11)
FONT_BOLD   = ("Segoe UI", 11, "bold")
FONT_BTN    = ("Segoe UI", 13, "bold")
FONT_SMALL  = ("Segoe UI", 10)
FONT_MONO   = ("Consolas", 11)

# =========================================================
# DATOS DE MODOS
# =========================================================
MODOS = [
    ("ocr",       "📄", "1. Extracción / Lectura",
     "Lee los PDFs SWIFT o correos GTO\ny extrae los campos en Swift Completos."),
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
# TARJETA SELECCIONABLE — CTk
# =========================================================
class OptionCard(ctk.CTkFrame):
    def __init__(self, parent, icon, title, desc, value, var,
                 tipo_var=None, show_forzar=False, forzar_var=None, **kw):
        super().__init__(
            parent,
            fg_color=C_BG,
            border_color=C_BORDER,
            border_width=1,
            corner_radius=12,
            cursor="hand2",
            **kw,
        )
        self._value     = value
        self._var       = var
        self._tipo_var  = tipo_var
        self._hovering  = False

        # — Contenido interior —
        inner = ctk.CTkFrame(self, fg_color="transparent")
        inner.pack(fill="both", expand=True, padx=18, pady=14)
        inner.columnconfigure(1, weight=1)

        # Icono
        self._icon_lbl = ctk.CTkLabel(
            inner, text=icon, font=("Segoe UI Emoji", 22),
            fg_color="transparent", text_color=C_TEXT, width=32
        )
        self._icon_lbl.grid(row=0, column=0, rowspan=2, padx=(0, 14), sticky="ns")

        # Título
        self._title_lbl = ctk.CTkLabel(
            inner, text=title, font=FONT_BOLD,
            fg_color="transparent", text_color=C_TEXT, anchor="w"
        )
        self._title_lbl.grid(row=0, column=1, sticky="w")

        # Descripción
        self._desc_lbl = ctk.CTkLabel(
            inner, text=desc, font=FONT_SMALL,
            fg_color="transparent", text_color=C_SUBTEXT,
            justify="left", anchor="w"
        )
        self._desc_lbl.grid(row=1, column=1, sticky="w")

        # Radio indicator (canvas simple dentro de CTkLabel vacío)
        self._radio_canvas = ctk.CTkCanvas(
            inner, width=22, height=22,
            bg=C_BG, highlightthickness=0
        )
        self._radio_canvas.grid(row=0, column=2, rowspan=2, padx=(14, 0))
        self._draw_radio(False)

        # Checkbox forzar (solo para "completo")
        self._chk_forzar = None
        if show_forzar and forzar_var is not None:
            self._chk_forzar = ctk.CTkCheckBox(
                inner,
                text="Forzar lectura desde cero (ignorar caché)",
                variable=forzar_var,
                font=("Segoe UI", 10),
                text_color=C_SUBTEXT,
                fg_color=C_PRIMARY_IMP,
                hover_color=C_PRIMARY_HVR,
                checkmark_color=C_BG,
                border_color=C_BORDER,
                corner_radius=4,
            )
            self._chk_forzar.grid(row=2, column=1, columnspan=2, sticky="w", pady=(8, 0))

        # Bind en todos los widgets hijos
        for w in self._all_widgets(self):
            w.bind("<Button-1>", self._click)
            w.bind("<Enter>",    self._hover_on)
            w.bind("<Leave>",    self._hover_off)

        # Excluir el checkbox del bind de clic de la card
        if self._chk_forzar:
            for w in self._all_widgets(self._chk_forzar):
                w.unbind("<Button-1>")

        var.trace_add("write", self._refresh)
        if tipo_var:
            tipo_var.trace_add("write", self._refresh)
        self._refresh()

    # ── utilidades ──────────────────────────────────────────
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

    def _accent(self):
        if self._tipo_var and self._tipo_var.get() == "exp":
            return ("#ECFDF5", C_PRIMARY_EXP)
        if self._tipo_var and self._tipo_var.get() == "gto":
            return ("#FEF3C7", C_PRIMARY_GTO)
        return ("#EEF0FB", C_PRIMARY_IMP)

    def _draw_radio(self, selected, color=C_PRIMARY_IMP):
        c = self._radio_canvas
        c.delete("all")
        if selected:
            c.create_oval(2, 2, 20, 20, fill=color, outline=color)
            c.create_line(6, 11, 9, 14, fill="white", width=2, capstyle="round")
            c.create_line(9, 14, 16, 7, fill="white", width=2, capstyle="round")
        else:
            c.create_oval(2, 2, 20, 20, fill=C_BG, outline=C_BORDER, width=2)

    def _refresh(self, *_):
        selected = self._var.get() == self._value
        bg_sel, accent = self._accent()

        if selected:
            bg     = bg_sel
            border = accent
            bw     = 2
        elif self._hovering:
            bg     = C_SURFACE
            border = C_SUBTEXT
            bw     = 1
        else:
            bg     = C_BG
            border = C_BORDER
            bw     = 1

        self.configure(fg_color=bg, border_color=border, border_width=bw)
        self._draw_radio(selected, accent)

        # Actualizar bg del canvas del radio y widgets internos
        self._radio_canvas.configure(bg=bg)
        for w in self._all_widgets(self):
            try:
                if isinstance(w, (ctk.CTkLabel, ctk.CTkFrame, ctk.CTkCanvas)):
                    if w is not self._chk_forzar:
                        w.configure(fg_color=bg)
            except Exception:
                pass

        # Actualizar color del checkbox si corresponde
        if self._chk_forzar:
            tipo_sel = self._tipo_var.get() if self._tipo_var else "imp"
            if tipo_sel == "exp":
                accent_color = C_PRIMARY_EXP
                hover_color  = C_EXP_HVR
            elif tipo_sel == "gto":
                accent_color = C_PRIMARY_GTO
                hover_color  = C_GTO_HVR
            else:
                accent_color = C_PRIMARY_IMP
                hover_color  = C_PRIMARY_HVR
            self._chk_forzar.configure(fg_color=accent_color, hover_color=hover_color)


# =========================================================
# VENTANA PRINCIPAL
# =========================================================
class PipelineGUI(ctk.CTk):
    def __init__(self):
        super().__init__()
        self.title("Origen Destino DIAN")
        self.configure(fg_color=C_BG)
        self.minsize(860, 660)
        self.resizable(True, True)

        self._tipo   = ctk.StringVar(value="imp")
        self._modo   = ctk.StringVar(value="ocr")
        self._forzar = ctk.BooleanVar(value=False)
        self._running = False

        self._build()
        self._center(1100, 820)

    def _center(self, w, h):
        sw = self.winfo_screenwidth()
        sh = self.winfo_screenheight()
        x  = (sw - w) // 2
        y  = (sh - h) // 2
        self.geometry(f"{w}x{h}+{x}+{y}")

    # ── Construcción ─────────────────────────────────────────
    def _build(self):
        # ── HEADER (blanco, igual que PayPal) ──
        header = ctk.CTkFrame(self, fg_color=C_BG, height=72, corner_radius=0)
        header.pack(fill="x", side="top")
        header.pack_propagate(False)

        hdr_inner = ctk.CTkFrame(header, fg_color="transparent")
        hdr_inner.pack(expand=True, side="left", padx=40)

        ctk.CTkLabel(
            hdr_inner, text="🏦",
            font=("Segoe UI Emoji", 28), fg_color="transparent"
        ).pack(side="left", padx=(0, 12), pady=16)

        titles = ctk.CTkFrame(hdr_inner, fg_color="transparent")
        titles.pack(side="left")
        ctk.CTkLabel(
            titles, text="Origen Destino DIAN",
            font=FONT_TITLE, text_color=C_PRIMARY_IMP, fg_color="transparent"
        ).pack(anchor="w")
        ctk.CTkLabel(
            titles, text="Automatización de procesamiento SWIFT",
            font=FONT_SUB, text_color=C_SUBTEXT, fg_color="transparent"
        ).pack(anchor="w")

        # Separador bajo el header
        sep = ctk.CTkFrame(self, fg_color=C_BORDER, height=1, corner_radius=0)
        sep.pack(fill="x")

        # ── ÁREA PRINCIPAL con scroll ──
        self._main = ctk.CTkFrame(self, fg_color=C_BG)
        self._main.pack(fill="both", expand=True, padx=40, pady=24)

        self._scroll = ctk.CTkScrollableFrame(
            self._main, fg_color=C_BG,
            scrollbar_button_color=C_BORDER,
            scrollbar_button_hover_color=C_SUBTEXT,
        )
        self._scroll.pack(fill="both", expand=True)

        body = ctk.CTkFrame(self._scroll, fg_color="transparent")
        body.pack(fill="both", expand=True, padx=4, pady=4)

        # ── Sección 1: Selector de flujo ──
        self._section(body, "¿Qué flujo operativo desea procesar?")
        tipo_row = ctk.CTkFrame(body, fg_color="transparent")
        tipo_row.pack(fill="x", pady=(8, 28))

        self._btn_imp = self._tipo_button(tipo_row, "📥  Importaciones", "imp")
        self._btn_exp = self._tipo_button(tipo_row, "📤  Exportaciones", "exp")
        self._btn_gto = self._tipo_button(tipo_row, "💸  Gastos",        "gto")
        self._tipo.trace_add("write", self._refresh_tipo)
        self._refresh_tipo()

        # ── Sección 2: Modo ──
        self._section(body, "Seleccione la acción a ejecutar")
        grid = ctk.CTkFrame(body, fg_color="transparent")
        grid.pack(fill="x", pady=(10, 24))
        grid.columnconfigure(0, weight=1)
        grid.columnconfigure(1, weight=1)

        self._cards: list[OptionCard] = []

        for i, (val, icon, title, desc) in enumerate(MODOS[:-1]):
            r, c = divmod(i, 2)
            card = OptionCard(
                grid, icon, title, desc, val, self._modo,
                tipo_var=self._tipo,
            )
            card.grid(
                row=r, column=c, sticky="nsew",
                padx=(0, 8) if c == 0 else (8, 0), pady=6
            )
            self._cards.append(card)

        # Card "Proceso Completo" con checkbox integrado
        val, icon, title, desc = MODOS[-1]
        card_full = OptionCard(
            grid, icon, title, desc, val, self._modo,
            tipo_var=self._tipo,
            show_forzar=True,
            forzar_var=self._forzar,
        )
        card_full.grid(row=2, column=0, columnspan=2, sticky="ew", pady=(6, 0))
        self._cards.append(card_full)

        # También mostrar forzar en card OCR cuando esté seleccionada
        # (se maneja via _refresh_forzar_ocr)
        self._modo.trace_add("write", lambda *_: self._on_modo_change())

        # ── Botón Ejecutar ──
        btn_row = ctk.CTkFrame(body, fg_color="transparent")
        btn_row.pack(fill="x", pady=(8, 28))

        self.btn_run = ctk.CTkButton(
            btn_row,
            text="Iniciar procesamiento  🚀",
            font=FONT_BTN,
            fg_color=C_PRIMARY_IMP,
            hover_color=C_PRIMARY_HVR,
            text_color=C_BG,
            height=48,
            corner_radius=10,
            command=self._on_run,
        )
        self.btn_run.pack(anchor="w")
        self._tipo.trace_add("write", self._refresh_btn_color)

        # ── Panel de Estado ──
        self._build_status(body)

    # ── Helpers de construcción ──────────────────────────────
    def _section(self, parent, text):
        ctk.CTkLabel(
            parent, text=text,
            font=FONT_BOLD, text_color=C_TEXT,
            fg_color="transparent", anchor="w"
        ).pack(fill="x", pady=(0, 2))

    def _tipo_button(self, parent, text, value):
        btn = ctk.CTkButton(
            parent, text=text, font=FONT_BOLD,
            height=40, corner_radius=8,
            fg_color=C_SURFACE, text_color=C_SUBTEXT,
            hover_color=C_BORDER, border_width=0,
            command=lambda v=value: self._tipo.set(v),
        )
        btn.pack(side="left", padx=(0, 10))
        return btn

    def _refresh_tipo(self, *_):
        sel = self._tipo.get()
        color_map = {"imp": C_PRIMARY_IMP, "exp": C_PRIMARY_EXP, "gto": C_PRIMARY_GTO}
        for btn, val in [
            (self._btn_imp, "imp"),
            (self._btn_exp, "exp"),
            (self._btn_gto, "gto"),
        ]:
            if sel == val:
                color = color_map[val]
                btn.configure(fg_color=color, text_color=C_BG, hover_color=color)
            else:
                btn.configure(fg_color=C_SURFACE, text_color=C_SUBTEXT, hover_color=C_BORDER)

    def _refresh_btn_color(self, *_):
        if self._running:
            return
        tipo_sel = self._tipo.get()
        if tipo_sel == "exp":
            self.btn_run.configure(fg_color=C_PRIMARY_EXP, hover_color=C_EXP_HVR)
        elif tipo_sel == "gto":
            self.btn_run.configure(fg_color=C_PRIMARY_GTO, hover_color=C_GTO_HVR)
        else:
            self.btn_run.configure(fg_color=C_PRIMARY_IMP, hover_color=C_PRIMARY_HVR)

    def _on_modo_change(self):
        # Cuando se selecciona OCR o no-completo, resetear forzar
        if self._modo.get() not in ("ocr", "completo"):
            self._forzar.set(False)

    # ── Panel de estado ──────────────────────────────────────
    def _build_status(self, parent):
        self._status_card = ctk.CTkFrame(
            parent,
            fg_color=C_SURFACE,
            border_color=C_BORDER,
            border_width=1,
            corner_radius=14,
        )
        self._status_card.pack(fill="x", pady=(0, 16))

        inner = ctk.CTkFrame(self._status_card, fg_color="transparent")
        inner.pack(fill="both", expand=True, padx=24, pady=20)

        # Fila superior: icono + textos
        top = ctk.CTkFrame(inner, fg_color="transparent")
        top.pack(fill="x")

        self._st_icon = ctk.CTkLabel(
            top, text="💡",
            font=("Segoe UI Emoji", 30),
            fg_color="transparent", width=44
        )
        self._st_icon.pack(side="left", padx=(0, 18))

        col = ctk.CTkFrame(top, fg_color="transparent")
        col.pack(side="left", fill="x", expand=True)

        self._st_msg = ctk.CTkLabel(
            col, text="Panel de Resultados",
            font=FONT_BOLD, text_color=C_TEXT,
            fg_color="transparent", anchor="w"
        )
        self._st_msg.pack(fill="x")

        self._st_sub = ctk.CTkLabel(
            col, text="El resumen del proceso aparecerá aquí.",
            font=FONT_LABEL, text_color=C_SUBTEXT,
            fg_color="transparent", anchor="w"
        )
        self._st_sub.pack(fill="x", pady=(3, 0))

        # Barra de progreso (oculta por defecto)
        self._progress = ctk.CTkProgressBar(
            inner,
            height=6,
            corner_radius=3,
            progress_color=C_PRIMARY_IMP,
            fg_color=C_BORDER,
        )

        # Área de métricas
        self._metrics_frame = ctk.CTkFrame(inner, fg_color="transparent")

    # ── Actualización de estado ──────────────────────────────
    def _set_status(self, icon, msg, sub="", color=C_TEXT, progress=False):
        def _do():
            self._st_icon.configure(text=icon)
            self._st_msg.configure(text=msg, text_color=color)
            self._st_sub.configure(text=sub)

            for w in self._metrics_frame.winfo_children():
                w.destroy()
            self._metrics_frame.pack_forget()

            if progress:
                tipo_sel = self._tipo.get()
                if tipo_sel == "exp":
                    bar_color = C_PRIMARY_EXP
                elif tipo_sel == "gto":
                    bar_color = C_PRIMARY_GTO
                else:
                    bar_color = C_PRIMARY_IMP
                self._progress.configure(progress_color=bar_color)
                self._progress.pack(fill="x", pady=(16, 0))
                self._progress.configure(mode="indeterminate")
                self._progress.start()
            else:
                self._progress.stop()
                self._progress.pack_forget()
        self.after(0, _do)

    def _set_rows(self, rows: list):
        def _do():
            for w in self._metrics_frame.winfo_children():
                w.destroy()
            self._metrics_frame.pack(fill="x", pady=(18, 0))

            for i, (icon, label, value) in enumerate(rows):
                row_bg = C_BG if i % 2 == 0 else C_SURFACE
                row = ctk.CTkFrame(
                    self._metrics_frame,
                    fg_color=row_bg,
                    corner_radius=8,
                )
                row.pack(fill="x", pady=2)

                ctk.CTkLabel(
                    row, text=icon,
                    font=("Segoe UI Emoji", 15),
                    fg_color="transparent", width=32
                ).pack(side="left", padx=(12, 8), pady=10)

                ctk.CTkLabel(
                    row, text=label,
                    font=FONT_SMALL, text_color=C_SUBTEXT,
                    fg_color="transparent", anchor="w"
                ).pack(side="left", fill="x", expand=True)

                ctk.CTkLabel(
                    row, text=value,
                    font=FONT_BOLD, text_color=C_TEXT,
                    fg_color="transparent", anchor="e"
                ).pack(side="right", padx=(0, 16), pady=10)

        self.after(0, _do)

    # ── Ejecución ────────────────────────────────────────────
    def _on_run(self):
        if self._running:
            return
        tipo  = self._tipo.get()
        modo  = self._modo.get()
        label = next(t for v, _, t, _ in MODOS if v == modo)

        self._set_status("⏳", "Procesamiento en curso…", f"Acción: {label}", progress=True)
        self._set_running(True)
        threading.Thread(
            target=self._execute,
            args=(tipo, modo, self._forzar.get()),
            daemon=True,
        ).start()

    def _execute(self, tipo, modo, forzar):
        try:
            from core.logger import init_logging
            from main import run_pipeline
            init_logging()

            result = run_pipeline(modo=modo, forzar=forzar, confirmar=False, tipo=tipo)
            dur = f"Completado en {result.duracion_segundos:.1f} s"

            if result.exitoso:
                self._set_status("✅", "Operación finalizada con éxito", dur, color=C_OK)
                self._set_rows(self._summary_rows(result))
            else:
                err = result.errores[0] if result.errores else "Revisa la consola."
                self._set_status("❌", "El proceso se detuvo por un error",
                                 _friendly(err), color=C_ERROR)

        except Exception as e:
            import traceback
            traceback.print_exc()
            self._set_status("❌", "Error crítico en la ejecución",
                             _friendly(str(e)), color=C_ERROR)
        finally:
            self._set_running(False)

    def _summary_rows(self, result) -> list:
        rows = []
        total_pdf = result.pdfs_nuevos_v1 + result.pdfs_nuevos_v2
        if total_pdf:
            rows.append(("📄", "Documentos leídos",           str(total_pdf)))
            rows.append(("✔",  "Registros consolidados",      str(result.pdfs_completos)))
            if result.pdfs_incompletos:
                rows.append(("⚠", "Requieren revisión manual", str(result.pdfs_incompletos)))
        if result.manuales_movidos:
            rows.append(("📋", "Trasladados exitosamente",     str(result.manuales_movidos)))
        if getattr(result, "plantilla_registros", 0):
            rows.append(("📊", "Registros para Bancolombia",   str(result.plantilla_registros)))
        if result.formularios_cruzados:
            rows.append(("🔗", "Formularios asignados",        str(result.formularios_cruzados)))
        if result.llaves_cruzadas:
            rows.append(("🗝", "Llaves asignadas",             str(result.llaves_cruzadas)))
        for w in result.advertencias[:2]:
            rows.append(("⚠", "Aviso del sistema", w))
        return rows

    def _set_running(self, running: bool):
        self._running = running
        if running:
            self.btn_run.configure(
                state="disabled",
                text="Procesando… ⏳",
                fg_color=C_DISABLED,
                text_color=C_SUBTEXT,
            )
        else:
            tipo_sel = self._tipo.get()
            if tipo_sel == "exp":
                color = C_PRIMARY_EXP
                hover = C_EXP_HVR
            elif tipo_sel == "gto":
                color = C_PRIMARY_GTO
                hover = C_GTO_HVR
            else:
                color = C_PRIMARY_IMP
                hover = C_PRIMARY_HVR
            self.btn_run.configure(
                state="normal",
                text="Iniciar procesamiento",
                fg_color=color,
                hover_color=hover,
                text_color=C_BG,
            )


# =========================================================
# UTILIDADES
# =========================================================
def _friendly(raw: str) -> str:
    r = raw.lower()
    if "permission" in r:
        return "Cierra Excel u otros programas que estén usando los archivos."
    if "filenotfound" in r:
        return "Falta un archivo esencial en la carpeta de trabajo."
    return "Revisa que los directorios y archivos estén correctamente ubicados."


# =========================================================
# ENTRY POINT
# =========================================================
if __name__ == "__main__":
    app = PipelineGUI()
    try:
        app.iconbitmap("icon.ico")
    except Exception:
        pass
    app.mainloop()