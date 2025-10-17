#!/usr/bin/env python3
import os
import sys
import time
import math
import threading
import subprocess
import re
import textwrap
from pathlib import Path
from collections import deque

class UploadCancelled(Exception):
    """Raised when a user cancels an active upload."""
    pass

# ---------------- Dependency check (GUI-safe fallback) ----------------
required = ["tkinter", "minio", "urllib3", "tqdm"]
missing = []
for mod in required:
    try:
        __import__(mod)
    except Exception:
        missing.append(mod)

if missing:
    try:
        import tkinter as tk
        from tkinter import messagebox
        _rt = tk.Tk()
        _rt.withdraw()
        pip_missing = " ".join([m for m in missing if m != "tkinter"])
        msg = (
            "The following Python modules are missing:\n\n"
            + "\n".join(missing)
            + ("\n\nInstall with:\n    pip install " + pip_missing if pip_missing else "")
            + "\n\nIf 'tkinter' is missing, reinstall Python with Tcl/Tk support.\n"
              "Windows: reinstall from python.org installer (Tkinter is included)."
        )
        messagebox.showerror("Missing Python Modules", msg)
        _rt.destroy()
    except Exception as e:
        print("ERROR: Missing modules:", ", ".join(missing))
        if any(m for m in missing if m != "tkinter"):
            print("Run: pip install", " ".join([m for m in missing if m != "tkinter"]))
        print("If 'tkinter' is missing, reinstall Python from python.org with Tcl/Tk support.")
        print(f"Extra info: {e}")
    sys.exit(1)

# Safe to import GUI + MinIO
import tkinter as tk
from tkinter import filedialog, messagebox, ttk
from tkinter import font as tkfont
from minio import Minio
from minio.error import S3Error
#from s3 import get_client  # switch to "from s3 import get_client3 as get_client" for AWS-only env client
from s3 import (
    get_client3 as get_client,
    load_settings as load_s3_settings,
    save_settings as save_s3_settings,
    CONFIG_PATH as S3_CONFIG_PATH,
)
# ---------------- Small helpers ----------------
_BUCKET_RE = re.compile(r"^(?!-)[a-z0-9-]{3,63}(?<!-)$")
def is_valid_bucket_name(name):
    if not name or len(name) < 3 or len(name) > 63: return False
    if not _BUCKET_RE.match(name): return False
    if re.match(r"^\d{1,3}(\.\d{1,3}){3}$", name): return False
    return True

def human_eta(seconds_or_none):
    if seconds_or_none is None: return "—"
    try:
        val = float(seconds_or_none)
    except (TypeError, ValueError):
        return "—"
    if val <= 0:
        return "00:00:00"
    s = int(math.ceil(val))
    return f"{s//3600:02d}:{(s%3600)//60:02d}:{s%60:02d}"

def human_size(num_bytes, suffix="B"):
    try:
        num = float(num_bytes)
    except (TypeError, ValueError):
        num = 0.0
    for unit in ("", "K", "M", "G", "T", "P"):
        if abs(num) < 1024.0 or unit == "P":
            return f"{num:5.1f} {unit}{suffix}".strip()
        num /= 1024.0
    return f"{num:.1f} P{suffix}"

def _reset_progress_metrics(*labels, reset_footer=False):
    for lbl in labels:
        if lbl is None:
            continue
        try:
            lbl._inst_ema = None
            lbl._inst_samples = 0
            lbl._avg_speed = 0.0
            lbl._speed_hist = deque()
        except Exception:
            pass
    if reset_footer:
        statusbar._last_upd = 0.0

def _wrap_lines(message, width_chars=80):
    text = "" if message is None else str(message)
    if width_chars <= 0:
        width_chars = 80
    lines = []
    for raw in text.splitlines() or [text]:
        if not raw:
            lines.append("")
            continue
        wrapped = textwrap.wrap(
            raw,
            width=width_chars,
            replace_whitespace=False,
            drop_whitespace=False,
            break_long_words=False,
            break_on_hyphens=False,
        )
        if not wrapped:
            lines.append("")
        else:
            lines.extend(wrapped)
    return "\n".join(lines)

def _estimate_char_width(widget, minimum=40):
    try:
        px = widget.winfo_width()
        if px and px > 1:
            return max(minimum, px // 7)
    except Exception:
        pass
    try:
        w = int(widget.cget("width"))
        if w > 0:
            return max(minimum, w)
    except Exception:
        pass
    return max(minimum, 80)

def _append_wrapped_text(widget, message):
    if widget is None:
        return
    width = _estimate_char_width(widget)
    text = _wrap_lines(message, width)
    if isinstance(widget, tk.Text):
        readonly = str(widget.cget("state")) == "disabled"
        if readonly:
            widget.config(state="normal")
        widget.insert(tk.END, (text + "\n") if text else "\n")
        if readonly:
            widget.config(state="disabled")
        widget.see(tk.END)
    else:
        widget.config(text=text or "")
    _reset_progress_metrics(widget)

def _truncate_middle(text, max_len=72):
    if not text:
        return ""
    if len(text) <= max_len:
        return text
    keep = max_len - 1
    left = keep // 2
    right = keep - left
    return text[:left] + "…" + text[-right:]

def _format_transfer_meta(kind, name, transferred, total, avg_Bps, elapsed_sec, note=None):
    display_name = _truncate_middle(name, 72)
    parts = [f"{kind}: {display_name}" if display_name else kind]

    if total:
        pct = (transferred / total) * 100 if total else 0.0
        parts.append(f"{human_size(transferred)} of {human_size(total)} ({pct:0.1f}%)")
    else:
        parts.append(f"{human_size(transferred)} transferred")

    if avg_Bps and avg_Bps > 1:
        parts.append(f"Avg {human_size(avg_Bps)}/s")
    else:
        parts.append("Avg —")

    parts.append(f"Elapsed {human_eta(elapsed_sec)}")

    if note:
        parts.append(note)

    return "  |  ".join(parts)

def _update_transfer_meta(label, kind, name, transferred, total, avg_Bps, elapsed_sec, note=None):
    if label is None:
        return
    label.config(text=_format_transfer_meta(kind, name, transferred, total, avg_Bps, elapsed_sec, note))

def _set_initial_window_size(win, preferred=(1280, 780), minimum=(960, 600), margin=40):
    """Apply a compact default size while keeping the window resizable."""
    min_w, min_h = minimum
    win.minsize(min_w, min_h)
    try:
        win.update_idletasks()
        screen_w = max(win.winfo_screenwidth(), min_w)
        screen_h = max(win.winfo_screenheight(), min_h)
        avail_w = max(min_w, screen_w - margin * 2)
        avail_h = max(min_h, screen_h - margin * 2)
        width = max(min_w, min(preferred[0], avail_w))
        height = max(min_h, min(preferred[1], avail_h))
        x = max(0, (screen_w - width) // 2)
        y = max(0, (screen_h - height) // 2)
        win.geometry(f"{width}x{height}+{x}+{y}")
    except Exception:
        win.geometry(f"{preferred[0]}x{preferred[1]}")

# ---------------- Theme / Style ----------------
def apply_theme(root):
    style = ttk.Style(root)
    try: style.theme_use("clam")
    except tk.TclError: pass

    dark = os.environ.get("UI_DARK", "1") not in ("0", "false", "False")

    def _blend_hex(color, target, ratio):
        """Return a hex color blended towards target by ratio (0-1)."""
        c = color.lstrip("#"); t = target.lstrip("#")
        cr, cg, cb = int(c[0:2], 16), int(c[2:4], 16), int(c[4:6], 16)
        tr, tg, tb = int(t[0:2], 16), int(t[2:4], 16), int(t[4:6], 16)
        nr = int(cr + (tr - cr) * ratio)
        ng = int(cg + (tg - cg) * ratio)
        nb = int(cb + (tb - cb) * ratio)
        return f"#{nr:02x}{ng:02x}{nb:02x}"

    if dark:
        BG      = "#141518"; SURFACE = "#1b1d22"; RAISED  = "#22252b"
        TEXT    = "#e9ecf1"; SUBTLE  = "#9aa2ad"; ACCENT  = "#4c8df6"
        TEXTAREA_BG = "#101114"; STATUS_BG = "#1b1d22"
    else:
        BG      = "#f7f7fb"; SURFACE = "#ffffff"; RAISED  = "#ffffff"
        TEXT    = "#1c2230"; SUBTLE  = "#5c6678"; ACCENT  = "#2d6ae3"
        TEXTAREA_BG = "#ffffff"; STATUS_BG = "#f4f6fb"

    available_fonts = set(tkfont.families(root))

    def pick_font(preferred, size, weight=None):
        for fam in preferred:
            if fam in available_fonts:
                if weight:
                    return (fam, size, weight)
                return (fam, size)
        fallback = preferred[-1] if preferred else "Arial"
        if weight:
            return (fallback, size, weight)
        return (fallback, size)

    TEXT_STACK = ["SF Pro Text", "Segoe UI", "Helvetica Neue", "Helvetica", "Arial"]
    DISPLAY_STACK = ["SF Pro Display", "Segoe UI Semibold", "Helvetica Neue", "Helvetica", "Arial"]
    FONT_TEXT = pick_font(TEXT_STACK, 12)
    FONT_TEXT_BOLD = pick_font(TEXT_STACK, 12, "bold")
    FONT_SMALL = pick_font(TEXT_STACK, 10)
    FONT_SMALL_BOLD = pick_font(TEXT_STACK, 10, "bold")
    FONT_HEADER = pick_font(DISPLAY_STACK, 16, "bold")
    FONT_PROGRESS = pick_font(TEXT_STACK, 11, "bold")
    FONT_ICON = pick_font(DISPLAY_STACK, 24, "bold")
    ACCENT_GLOW = _blend_hex(ACCENT, "#ffffff", 0.35)
    ACCENT_SHADOW = _blend_hex(ACCENT, "#000000", 0.25)

    root.configure(bg=BG)
    style.configure(".", background=BG, foreground=TEXT, fieldbackground=SURFACE, highlightthickness=0)
    style.configure("TLabel", background=BG, foreground=TEXT, font=FONT_TEXT)
    style.configure("Muted.TLabel", foreground=SUBTLE)
    style.configure("Header.TLabel", font=FONT_HEADER)
    style.configure("Small.TLabel", font=FONT_SMALL, foreground=SUBTLE)
    style.configure("Card.TFrame", background=SURFACE, relief="flat")
    style.configure("Toolbar.TFrame", background=BG)
    style.configure("TEntry", fieldbackground=RAISED, foreground=TEXT, insertcolor=TEXT, bordercolor=RAISED)
    style.map("TEntry", bordercolor=[("focus", ACCENT)])
    style.configure("Accent.TButton", padding=(14,8), font=FONT_TEXT_BOLD,
                    background=ACCENT, foreground="white")
    style.map("Accent.TButton", background=[("active", ACCENT), ("disabled", "#6c7aa3")])
    style.configure("Neutral.TButton", padding=(12,7), font=FONT_TEXT,
                    background=RAISED, foreground=TEXT)
    style.map("Neutral.TButton", background=[("active", "#2a2d34"), ("disabled", "#2a2d34")])
    style.configure("TNotebook", background=BG, borderwidth=0)
    style.configure("TNotebook.Tab", padding=(14,8), font=FONT_TEXT, background=BG)
    style.map("TNotebook.Tab", background=[("selected", SURFACE)], foreground=[("selected", TEXT)])
    style.configure("Horizontal.TProgressbar", troughcolor=RAISED, background=ACCENT, thickness=8)
    style.configure("TSeparator", background=SUBTLE)
    style.configure("ProgressValue.TLabel", background=SURFACE, foreground=ACCENT, font=FONT_PROGRESS)
    style.configure("ProgressFrame.TFrame", background=SURFACE, padding=(2,6))
    style.layout(
        "Modern.Horizontal.TProgressbar",
        [
            ("Horizontal.Progressbar.trough",
             {"children": [("Horizontal.Progressbar.pbar", {"side": "left", "sticky": "nswe"})],
              "sticky": "nswe"})
        ],
    )
    style.configure(
        "Modern.Horizontal.TProgressbar",
        troughcolor=_blend_hex(SURFACE, BG, 0.2),
        bordercolor=_blend_hex(SURFACE, BG, 0.2),
        background=ACCENT,
        lightcolor=ACCENT_GLOW,
        darkcolor=ACCENT_SHADOW,
        borderwidth=0,
        thickness=14,
        relief="flat"
    )
    style.map(
        "Modern.Horizontal.TProgressbar",
        background=[("active", ACCENT_GLOW), ("!disabled", ACCENT)],
        lightcolor=[("active", ACCENT_GLOW)],
        darkcolor=[("active", ACCENT)]
    )
    metric_bg = _blend_hex(SURFACE, ACCENT, 0.18 if not dark else 0.12)
    style.configure("Metrics.TFrame", background=metric_bg, padding=(12,8))
    style.configure("Metrics.TLabel", background=metric_bg, foreground=TEXT, font=FONT_SMALL)

    danger_base = "#ff6b6b" if dark else "#d64545"
    danger_bg = _blend_hex(SURFACE, danger_base, 0.22 if dark else 0.12)
    danger_hover = _blend_hex(danger_base, "#ffffff", 0.18)

    section_bg = _blend_hex(SURFACE, BG, 0.12 if dark else 0.06)
    info_bg = _blend_hex(SURFACE, ACCENT, 0.22 if dark else 0.18)
    info_fg = ACCENT

    style.configure(
        "Danger.TButton",
        padding=(14,8),
        font=FONT_TEXT_BOLD,
        background=danger_base,
        foreground="white",
        borderwidth=0,
    )
    style.map(
        "Danger.TButton",
        background=[("active", danger_hover), ("disabled", _blend_hex(danger_base, BG, 0.65))],
        foreground=[("disabled", _blend_hex("#ffffff", BG, 0.35))],
    )

    style.configure("DangerCallout.TFrame", background=danger_bg, padding=(4,4,4,4))
    style.configure(
        "DangerCalloutTitle.TLabel",
        background=danger_bg,
        foreground=danger_base,
        font=FONT_TEXT_BOLD,
    )
    style.configure(
        "DangerCalloutText.TLabel",
        background=danger_bg,
        foreground=TEXT,
        font=FONT_SMALL,
    )
    style.configure(
        "DangerCalloutIcon.TLabel",
        background=danger_bg,
        foreground=danger_base,
        font=FONT_ICON,
    )

    style.configure("AccentCallout.TFrame", background=info_bg, padding=(4,4,4,4))
    style.configure(
        "AccentCalloutTitle.TLabel",
        background=info_bg,
        foreground=info_fg,
        font=FONT_TEXT_BOLD,
    )
    style.configure(
        "AccentCalloutText.TLabel",
        background=info_bg,
        foreground=TEXT,
        font=FONT_SMALL,
    )
    style.configure(
        "AccentCalloutIcon.TLabel",
        background=info_bg,
        foreground=info_fg,
        font=FONT_ICON,
    )

    style.configure("Section.TFrame", background=section_bg, padding=(4,4,4,4))
    style.configure(
        "SectionBody.TFrame",
        background=section_bg,
    )
    style.configure(
        "SectionHeading.TLabel",
        background=section_bg,
        foreground=TEXT,
        font=FONT_TEXT_BOLD,
    )
    style.configure(
        "SectionHint.TLabel",
        background=section_bg,
        foreground=SUBTLE,
        font=FONT_SMALL,
    )
    style.configure(
        "SectionLabel.TLabel",
        background=section_bg,
        foreground=TEXT,
        font=FONT_TEXT,
    )
    style.configure(
        "Section.TCheckbutton",
        background=section_bg,
        foreground=TEXT,
        font=FONT_TEXT,
    )
    style.map(
        "Section.TCheckbutton",
        background=[("active", section_bg)],
        foreground=[("disabled", SUBTLE)],
    )
    style.configure("SectionToolbar.TFrame", background=section_bg)
    style.configure(
        "Section.TRadiobutton",
        background=section_bg,
        foreground=TEXT,
        font=FONT_TEXT,
    )
    style.map(
        "Section.TRadiobutton",
        background=[("active", section_bg)],
        foreground=[("disabled", SUBTLE)],
    )

    info_bg = _blend_hex(SURFACE, ACCENT, 0.12 if dark else 0.08)
    style.configure("Info.TFrame", background=info_bg, padding=(12,12))
    style.configure("InfoHeading.TLabel", background=info_bg, foreground=TEXT, font=FONT_TEXT_BOLD)
    style.configure("InfoHint.TLabel", background=info_bg, foreground=SUBTLE, font=FONT_SMALL)
    style.configure("StatusInfo.TLabel", background=section_bg, foreground=SUBTLE, font=FONT_SMALL)
    style.configure("Success.TLabel", background=section_bg, foreground="#3ddc84", font=FONT_SMALL)
    style.configure("Error.TLabel", background=section_bg, foreground="#ff6b6b", font=FONT_SMALL)
    style.configure("InfoLink.TLabel", background=info_bg, foreground=ACCENT, font=FONT_SMALL)
    style.map("InfoLink.TLabel", foreground=[("active", ACCENT_GLOW)])

    style.configure(
        "List.Treeview",
        background=SURFACE,
        fieldbackground=SURFACE,
        foreground=TEXT,
        bordercolor=SURFACE,
        rowheight=24,
        font=FONT_TEXT,
    )
    style.map(
        "List.Treeview",
        background=[("selected", ACCENT)],
        foreground=[("selected", "white")],
        bordercolor=[("focus", ACCENT)]
    )
    style.configure(
        "List.Treeview.Heading",
        background=BG,
        foreground=SUBTLE,
        font=FONT_SMALL_BOLD,
        bordercolor=BG
    )
    style.map(
        "List.Treeview.Heading",
        background=[("active", SURFACE)]
    )

    def card(parent):
        outer = ttk.Frame(parent, style="Card.TFrame", padding=(16,16,16,16))
        outer.grid_columnconfigure(0, weight=1)
        return outer
    palette = {
        "dark": dark,
        "BG": BG,
        "SURFACE": SURFACE,
        "TEXT": TEXT,
        "SUBTLE": SUBTLE,
        "ACCENT": ACCENT,
        "TEXTAREA_BG": TEXTAREA_BG,
        "TEXTAREA_FG": TEXT,
        "STATUS_BG": STATUS_BG,
        "ACCENT_GLOW": ACCENT_GLOW,
        "METRIC_BG": metric_bg,
        "DANGER": danger_base,
        "DANGER_BG": danger_bg,
        "SECTION_BG": section_bg,
    }
    return card, palette

# ---------------- GUI root ----------------
root = tk.Tk()
root.title("S3 / MinIO Manager")

card, palette = apply_theme(root)
_set_initial_window_size(root)

# Header
header = ttk.Frame(root, style="Toolbar.TFrame")
header.pack(fill="x", padx=14, pady=(10,4))
ttk.Label(header, text="S3 / MinIO Manager", style="Header.TLabel").pack(side="left")
env_bits = []
if os.environ.get("AWS_REGION"): env_bits.append(f"region: {os.environ.get('AWS_REGION')}")
if os.environ.get("AWS_S3_ENDPOINT"): env_bits.append(os.environ.get("AWS_S3_ENDPOINT"))
if env_bits:
    ttk.Label(header, text=" | ".join(env_bits), style="Small.TLabel").pack(side="right")

notebook = ttk.Notebook(root)
notebook.pack(fill="both", expand=True, padx=12, pady=(0,10))


statusbar = ttk.Label(root, text="Ready", style="Small.TLabel", anchor="w")
statusbar.pack(fill="x", padx=16, pady=(0,12))

cancel_event = threading.Event()
PADX = 10; PADY = 8

_initial_settings = load_s3_settings()

_layout_state = {"compact": False, "settings_compact": False}

def _settings_bool(value, default=True):
    if value is None:
        return default
    return str(value).lower() not in ("0", "false", "no")

def _default_endpoint(region: str) -> str:
    region = (region or "").strip()
    return f"s3.{region}.amazonaws.com" if region else ""


def _display_config_path() -> str:
    try:
        return "~/" + str(S3_CONFIG_PATH.relative_to(Path.home()))
    except Exception:
        return str(S3_CONFIG_PATH)

cfg_region = tk.StringVar(value=_initial_settings.get("AWS_REGION", ""))
cfg_access_key = tk.StringVar(value=_initial_settings.get("AWS_ACCESS_KEY_ID", ""))
cfg_secret_key = tk.StringVar(value=_initial_settings.get("AWS_SECRET_ACCESS_KEY", ""))
initial_endpoint = (_initial_settings.get("AWS_S3_ENDPOINT", "") or "").strip()
use_custom_flag = _initial_settings.get("USE_CUSTOM_ENDPOINT")
use_custom = bool(use_custom_flag) if use_custom_flag is not None else bool(initial_endpoint)
cfg_endpoint = tk.StringVar(value=initial_endpoint)
cfg_custom_endpoint = tk.BooleanVar(value=use_custom)
cfg_secure = tk.BooleanVar(value=_settings_bool(_initial_settings.get("AWS_S3_SECURE"), True))
cfg_status = tk.StringVar(value="")
PROVIDER_AWS = "aws"
PROVIDER_MINIO = "minio"
cfg_provider = tk.StringVar(value=_initial_settings.get("PROVIDER", PROVIDER_AWS))
cfg_test_status = tk.StringVar(value="")
if cfg_provider.get() == PROVIDER_MINIO and use_custom_flag is None:
    cfg_custom_endpoint.set(True)
    if not initial_endpoint:
        cfg_endpoint.set("127.0.0.1:9000")
cfg_path_style = tk.BooleanVar(value=_settings_bool(_initial_settings.get("AWS_S3_PATH_STYLE"), False))
cfg_show_secret = tk.BooleanVar(value=False)

def _provider_display_name(provider: str) -> str:
    return "AWS" if provider == PROVIDER_AWS else "MinIO / Custom"

def _current_provider_state():
    return {
        "AWS_REGION": cfg_region.get().strip(),
        "AWS_S3_ENDPOINT": cfg_endpoint.get().strip(),
        "AWS_ACCESS_KEY_ID": cfg_access_key.get().strip(),
        "AWS_SECRET_ACCESS_KEY": cfg_secret_key.get().strip(),
        "USE_CUSTOM_ENDPOINT": cfg_custom_endpoint.get(),
        "AWS_S3_PATH_STYLE": cfg_path_style.get(),
        "AWS_S3_SECURE": cfg_secure.get(),
    }

def _restore_provider_state(provider: str) -> bool:
    state = _provider_snapshot.get(provider)
    if not state:
        return False
    cfg_region.set(state.get("AWS_REGION", ""))
    cfg_endpoint.set(state.get("AWS_S3_ENDPOINT", ""))
    cfg_access_key.set(state.get("AWS_ACCESS_KEY_ID", ""))
    cfg_secret_key.set(state.get("AWS_SECRET_ACCESS_KEY", ""))
    cfg_custom_endpoint.set(bool(state.get("USE_CUSTOM_ENDPOINT", provider != PROVIDER_AWS)))
    cfg_path_style.set(bool(state.get("AWS_S3_PATH_STYLE", False)))
    cfg_secure.set(bool(state.get("AWS_S3_SECURE", True)))
    return True

_provider_snapshot = {cfg_provider.get(): _current_provider_state()}
_active_provider = {"value": cfg_provider.get()}
_provider_loading = False

def _snapshot_has_credentials(snapshot):
    if not snapshot:
        return False
    access = (snapshot.get("AWS_ACCESS_KEY_ID") or "").strip()
    secret = (snapshot.get("AWS_SECRET_ACCESS_KEY") or "").strip()
    return bool(access and secret)

def _refresh_configuration_status(saved_at=None):
    provider = cfg_provider.get()
    display_name = _provider_display_name(provider)
    s_info_path.config(text=f"🗂 {display_name} settings file: {s_display_path} (click to reveal)")

    if provider == PROVIDER_AWS:
        s_info_hint.config(
            text="Leave the endpoint blank to use the default AWS endpoint for the selected region.",
            style="InfoHint.TLabel",
        )
    else:
        s_info_hint.config(
            text="Enter the MinIO or custom endpoint as host[:port]. Region is optional.",
            style="InfoHint.TLabel",
        )

    snapshot = _provider_snapshot.get(provider)
    message = None
    if _snapshot_has_credentials(snapshot):
        if provider == PROVIDER_AWS:
            region = (snapshot.get("AWS_REGION") or "").strip()
            region_display = region or "unset"
            derived = _default_endpoint(region) if region else "derived automatically"
            message = f"✅ AWS settings saved • region {region_display} → {derived}"
        else:
            endpoint = (snapshot.get("AWS_S3_ENDPOINT") or "").strip() or "unset"
            https = "HTTPS on" if _settings_bool(snapshot.get("AWS_S3_SECURE"), True) else "HTTPS off"
            message = f"✅ MinIO settings saved • endpoint {endpoint} • {https}"

    if not message:
        if provider == PROVIDER_AWS:
            message = "⚠️ Enter AWS credentials and click 'Save Settings' to persist them."
        else:
            message = "⚠️ Enter MinIO endpoint, access key, and secret, then click 'Save Settings'."

    if saved_at:
        message += f" • saved {saved_at}"

    cfg_status.set(message)

# =============== UPLOAD TAB ===============
upload_tab = ttk.Frame(notebook)
notebook.add(upload_tab, text="📤 Upload")
u_card = card(upload_tab)
u_card.pack(fill="both", expand=True)

up_bucket = tk.StringVar()
up_key = tk.StringVar()
up_file = tk.StringVar()
up_create = tk.BooleanVar(value=True)
_upload_key_state = {"manual": False, "auto_value": "", "suspend": False, "last_path": ""}

# Upload guidance callout
u_callout = ttk.Frame(u_card, style="AccentCallout.TFrame", padding=(18,16))
u_callout.grid_columnconfigure(1, weight=1)
u_callout_icon = ttk.Label(u_callout, text="🚀", style="AccentCalloutIcon.TLabel")
u_callout_title = ttk.Label(u_callout, text="Quick Upload", style="AccentCalloutTitle.TLabel")
u_callout_text = ttk.Label(
    u_callout,
    text="Ship a local file to object storage and keep an eye on progress, throughput, and status updates in real time.",
    style="AccentCalloutText.TLabel",
    justify="left"
)
u_callout_icon.grid(row=0, column=0, rowspan=2, sticky="n", padx=(0,12))
u_callout_title.grid(row=0, column=1, sticky="w")
u_callout_text.grid(row=1, column=1, sticky="we")

# Upload form section
u_form_section = ttk.Frame(u_card, style="Section.TFrame", padding=(18,16))
u_form_section.grid_columnconfigure(1, weight=1)
u_form_section.grid_columnconfigure(2, weight=0)
u_form_title = ttk.Label(u_form_section, text="Target Details", style="SectionHeading.TLabel")
u_form_hint = ttk.Label(
    u_form_section,
    text="Set the destination bucket and object key. Leave the key blank to reuse the filename automatically.",
    style="SectionHint.TLabel",
    justify="left"
)
u_form_sep = ttk.Separator(u_form_section, orient="horizontal")

u_lbl_bucket = ttk.Label(u_form_section, text="Bucket", style="SectionLabel.TLabel")
u_ent_bucket = ttk.Entry(u_form_section, textvariable=up_bucket)

u_lbl_key = ttk.Label(u_form_section, text="Object Key", style="SectionLabel.TLabel")
u_ent_key = ttk.Entry(u_form_section, textvariable=up_key)

u_lbl_file = ttk.Label(u_form_section, text="Local File", style="SectionLabel.TLabel")
u_ent_file = ttk.Entry(u_form_section, textvariable=up_file)
def pick_upload_file():
    f = filedialog.askopenfilename()
    if f:
        up_file.set(f)
        up_btn_start.focus_set()
u_btn_browse = ttk.Button(u_form_section, text="Browse…", style="Neutral.TButton", command=pick_upload_file)

u_chk_create = ttk.Checkbutton(
    u_form_section,
    text="Create bucket automatically when missing",
    variable=up_create,
    style="Section.TCheckbutton"
)

u_form_title.grid(row=0, column=0, columnspan=3, sticky="w")
u_form_hint.grid(row=1, column=0, columnspan=3, sticky="we", pady=(4,12))
u_form_sep.grid(row=2, column=0, columnspan=3, sticky="we", pady=(0,16))
u_lbl_bucket.grid(row=3, column=0, sticky="w", pady=(0,4))
u_ent_bucket.grid(row=3, column=1, columnspan=2, sticky="we", pady=(0,4), padx=(12,0))
u_lbl_key.grid(row=4, column=0, sticky="w", pady=(8,4))
u_ent_key.grid(row=4, column=1, columnspan=2, sticky="we", pady=(8,4), padx=(12,0))
u_lbl_file.grid(row=5, column=0, sticky="w", pady=(8,4))
u_ent_file.grid(row=5, column=1, sticky="we", pady=(8,4), padx=(12,0))
u_btn_browse.grid(row=5, column=2, sticky="e", padx=(12,0), pady=(8,4))
u_chk_create.grid(row=6, column=0, columnspan=3, sticky="w", pady=(16,0))

u_status_section = ttk.Frame(u_card, style="Section.TFrame", padding=(18,16))
u_status_section.grid_columnconfigure(0, weight=1)
u_status_section.grid_rowconfigure(2, weight=1)
u_status_title = ttk.Label(u_status_section, text="Transfer Monitor", style="SectionHeading.TLabel")
u_status_hint = ttk.Label(
    u_status_section,
    text="Track metrics, throughput, and any status messages while the upload runs.",
    style="SectionHint.TLabel",
    justify="left"
)
u_status_body = ttk.Frame(u_status_section, style="SectionBody.TFrame")
u_status_body.grid_columnconfigure(0, weight=1)

up_metrics = ttk.Frame(u_status_body, style="Metrics.TFrame")
up_metric_file = ttk.Label(up_metrics, text="📄 File: —", style="Metrics.TLabel", anchor="w", justify="left")
up_metric_dest = ttk.Label(up_metrics, text="🎯 Destination: —", style="Metrics.TLabel", anchor="w", justify="left")
up_metric_meta = ttk.Label(up_metrics, text="🕒 Modified: —", style="Metrics.TLabel", anchor="w", justify="left")
up_metric_file.pack(fill="x", pady=(0, 2))
up_metric_dest.pack(fill="x", pady=(0, 2))
up_metric_meta.pack(fill="x")

up_progress_frame = ttk.Frame(u_status_body, style="ProgressFrame.TFrame")

u_btns = ttk.Frame(u_status_body, style="SectionToolbar.TFrame")
up_btn_start = ttk.Button(u_btns, text="Start Upload", style="Accent.TButton", state="disabled")
up_btn_cancel = ttk.Button(u_btns, text="Cancel", style="Neutral.TButton", state="disabled")
up_btn_cancel.pack(side="right", padx=6)
up_btn_start.pack(side="right", padx=6)

u_status_title.grid(row=0, column=0, sticky="w")
u_status_hint.grid(row=1, column=0, sticky="we", pady=(4,12))
u_status_body.grid(row=2, column=0, sticky="nsew")
up_metrics.pack(fill="x")
up_progress_frame.pack(fill="x", pady=(16,0))
u_btns.pack(fill="x", pady=(16,0))

up_progress_frame.grid_columnconfigure(0, weight=1)
up_meta_label = ttk.Label(
    up_progress_frame,
    text="No upload in progress.",
    style="Small.TLabel",
    justify="left",
    anchor="w"
)
up_meta_label.grid(row=0, column=0, sticky="we", pady=(0,6))
up_progress = ttk.Progressbar(
    up_progress_frame,
    orient="horizontal",
    mode="determinate",
    style="Modern.Horizontal.TProgressbar"
)
up_progress.grid(row=1, column=0, sticky="we")
up_status = ttk.Label(
    up_progress_frame,
    text="Idle",
    style="ProgressValue.TLabel",
    justify="left",
    anchor="w"
)
up_status.grid(row=2, column=0, sticky="we", pady=(6,0))
up_status_text = up_status

# =============== DOWNLOAD TAB ===============
dl_tab = ttk.Frame(notebook)
notebook.add(dl_tab, text="📥 Download")
d_card = card(dl_tab); d_card.pack(fill="both", expand=True)

dl_bucket = tk.StringVar()
dl_key = tk.StringVar()
dl_out = tk.StringVar()

# Download guidance callout
d_callout = ttk.Frame(d_card, style="AccentCallout.TFrame", padding=(18,16))
d_callout.grid_columnconfigure(1, weight=1)
d_callout_icon = ttk.Label(d_callout, text="📥", style="AccentCalloutIcon.TLabel")
d_callout_title = ttk.Label(d_callout, text="Smart Download", style="AccentCalloutTitle.TLabel")
d_callout_text = ttk.Label(
    d_callout,
    text="Pull objects or whole prefixes down to your machine with live status, resume-friendly progress, and quick access to destination info.",
    style="AccentCalloutText.TLabel",
    justify="left"
)
d_callout_icon.grid(row=0, column=0, rowspan=2, sticky="n", padx=(0,12))
d_callout_title.grid(row=0, column=1, sticky="w")
d_callout_text.grid(row=1, column=1, sticky="we")

# Download form section
d_form_section = ttk.Frame(d_card, style="Section.TFrame", padding=(18,16))
d_form_section.grid_columnconfigure(1, weight=1)
d_form_section.grid_columnconfigure(2, weight=0)
d_form_title = ttk.Label(d_form_section, text="Source & Destination", style="SectionHeading.TLabel")
d_form_hint = ttk.Label(
    d_form_section,
    text="Choose the bucket/object combination and point to a folder or file path where the download should land.",
    style="SectionHint.TLabel",
    justify="left"
)
d_form_sep = ttk.Separator(d_form_section, orient="horizontal")

d_lbl_bucket = ttk.Label(d_form_section, text="Bucket", style="SectionLabel.TLabel")
d_ent_bucket = ttk.Entry(d_form_section, textvariable=dl_bucket)

d_lbl_key = ttk.Label(d_form_section, text="Object Key or Prefix", style="SectionLabel.TLabel")
d_ent_key = ttk.Entry(d_form_section, textvariable=dl_key)

d_lbl_out = ttk.Label(d_form_section, text="Local Destination", style="SectionLabel.TLabel")
d_ent_out = ttk.Entry(d_form_section, textvariable=dl_out)
def pick_out_dir():
    d = filedialog.askdirectory()
    if d:
        dl_out.set(d)
d_btn_browse = ttk.Button(d_form_section, text="Browse…", style="Neutral.TButton", command=pick_out_dir)

d_form_title.grid(row=0, column=0, columnspan=3, sticky="w")
d_form_hint.grid(row=1, column=0, columnspan=3, sticky="we", pady=(4,12))
d_form_sep.grid(row=2, column=0, columnspan=3, sticky="we", pady=(0,16))
d_lbl_bucket.grid(row=3, column=0, sticky="w", pady=(0,4))
d_ent_bucket.grid(row=3, column=1, columnspan=2, sticky="we", pady=(0,4), padx=(12,0))
d_lbl_key.grid(row=4, column=0, sticky="w", pady=(8,4))
d_ent_key.grid(row=4, column=1, columnspan=2, sticky="we", pady=(8,4), padx=(12,0))
d_lbl_out.grid(row=5, column=0, sticky="w", pady=(8,4))
d_ent_out.grid(row=5, column=1, sticky="we", pady=(8,4), padx=(12,0))
d_btn_browse.grid(row=5, column=2, sticky="e", padx=(12,0), pady=(8,4))

# Download status section
d_status_section = ttk.Frame(d_card, style="Section.TFrame", padding=(18,16))
d_status_section.grid_columnconfigure(0, weight=1)
d_status_section.grid_rowconfigure(2, weight=1)
d_status_title = ttk.Label(d_status_section, text="Download Monitor", style="SectionHeading.TLabel")
d_status_hint = ttk.Label(
    d_status_section,
    text="Follow progress, rates, and status updates for the current download task.",
    style="SectionHint.TLabel",
    justify="left"
)
d_status_body = ttk.Frame(d_status_section, style="SectionBody.TFrame")
d_status_body.grid_columnconfigure(0, weight=1)

dl_metrics = ttk.Frame(d_status_body, style="Metrics.TFrame")
dl_metric_object = ttk.Label(dl_metrics, text="🗂️ Object: —", style="Metrics.TLabel", anchor="w", justify="left")
dl_metric_dest = ttk.Label(dl_metrics, text="💾 Save to: —", style="Metrics.TLabel", anchor="w", justify="left")
dl_metric_meta = ttk.Label(dl_metrics, text="📶 Status: Ready", style="Metrics.TLabel", anchor="w", justify="left")
dl_metric_object.pack(fill="x", pady=(0, 2))
dl_metric_dest.pack(fill="x", pady=(0, 2))
dl_metric_meta.pack(fill="x")

dl_progress_frame = ttk.Frame(d_status_body, style="ProgressFrame.TFrame")
dl_progress_frame.grid_columnconfigure(0, weight=1)
dl_meta_label = ttk.Label(
    dl_progress_frame,
    text="No download in progress.",
    style="Small.TLabel",
    justify="left",
    anchor="w"
)
dl_meta_label.grid(row=0, column=0, sticky="we", pady=(0,6))
dl_progress = ttk.Progressbar(
    dl_progress_frame,
    orient="horizontal",
    mode="determinate",
    style="Modern.Horizontal.TProgressbar"
)
dl_progress.grid(row=1, column=0, sticky="we")
dl_status = ttk.Label(
    dl_progress_frame,
    text="Idle",
    style="ProgressValue.TLabel",
    justify="left",
    anchor="w"
)
dl_status.grid(row=2, column=0, sticky="we", pady=(6,0))
dl_status_text = dl_status
d_btns = ttk.Frame(d_status_body, style="SectionToolbar.TFrame")
dl_btn_start = ttk.Button(d_btns, text="Start Download", style="Accent.TButton")
dl_btn_cancel = ttk.Button(d_btns, text="Cancel", style="Neutral.TButton", state="disabled")
dl_btn_cancel.pack(side="right", padx=6)
dl_btn_start.pack(side="right", padx=6)

d_status_title.grid(row=0, column=0, sticky="w")
d_status_hint.grid(row=1, column=0, sticky="we", pady=(4,12))
d_status_body.grid(row=2, column=0, sticky="nsew")
dl_metrics.pack(fill="x")
dl_progress_frame.pack(fill="x", pady=(16,0))
d_btns.pack(fill="x", pady=(16,0))
# =============== LIST TAB ===============
ls_tab = ttk.Frame(notebook)
notebook.add(ls_tab, text="📄 List")
l_card = card(ls_tab); l_card.pack(fill="both", expand=True)

ls_bucket = tk.StringVar()
ls_prefix = tk.StringVar()
ls_recursive = tk.BooleanVar(value=True)

l_callout = ttk.Frame(l_card, style="AccentCallout.TFrame", padding=(18,16))
l_callout.grid_columnconfigure(1, weight=1)
l_callout_icon = ttk.Label(l_callout, text="🧭", style="AccentCalloutIcon.TLabel")
l_callout_title = ttk.Label(l_callout, text="Object Explorer", style="AccentCalloutTitle.TLabel")
l_callout_text = ttk.Label(
    l_callout,
    text="Scan buckets quickly and drill into prefixes with live counters and metadata summaries.",
    style="AccentCalloutText.TLabel",
    justify="left"
)
l_callout_icon.grid(row=0, column=0, rowspan=2, sticky="n", padx=(0,12))
l_callout_title.grid(row=0, column=1, sticky="w")
l_callout_text.grid(row=1, column=1, sticky="we")

l_form_section = ttk.Frame(l_card, style="Section.TFrame", padding=(18,16))
l_form_section.grid_columnconfigure(1, weight=1)
l_form_title = ttk.Label(l_form_section, text="Query Filters", style="SectionHeading.TLabel")
l_form_hint = ttk.Label(
    l_form_section,
    text="Provide a bucket and optional prefix. Enable recursive listing to traverse the entire subtree.",
    style="SectionHint.TLabel",
    justify="left"
)
l_form_sep = ttk.Separator(l_form_section, orient="horizontal")

l_lbl_bucket = ttk.Label(l_form_section, text="Bucket", style="SectionLabel.TLabel")
l_ent_bucket = ttk.Entry(l_form_section, textvariable=ls_bucket)

l_lbl_prefix = ttk.Label(l_form_section, text="Prefix (optional)", style="SectionLabel.TLabel")
l_ent_prefix = ttk.Entry(l_form_section, textvariable=ls_prefix)

l_chk_recursive = ttk.Checkbutton(
    l_form_section,
    text="Recursive listing",
    variable=ls_recursive,
    style="Section.TCheckbutton"
)

l_actions = ttk.Frame(l_form_section, style="SectionToolbar.TFrame")
ls_btn = ttk.Button(l_actions, text="List Objects", style="Accent.TButton")
ls_btn.pack(side="right")

l_form_title.grid(row=0, column=0, columnspan=2, sticky="w")
l_form_hint.grid(row=1, column=0, columnspan=2, sticky="we", pady=(4,12))
l_form_sep.grid(row=2, column=0, columnspan=2, sticky="we", pady=(0,16))
l_lbl_bucket.grid(row=3, column=0, sticky="w", pady=(0,4))
l_ent_bucket.grid(row=3, column=1, sticky="we", pady=(0,4), padx=(12,0))
l_lbl_prefix.grid(row=4, column=0, sticky="w", pady=(8,4))
l_ent_prefix.grid(row=4, column=1, sticky="we", pady=(8,4), padx=(12,0))
l_chk_recursive.grid(row=5, column=0, columnspan=2, sticky="w", pady=(16,0))
l_actions.grid(row=6, column=0, columnspan=2, sticky="e", pady=(16,0))

l_results_section = ttk.Frame(l_card, style="Section.TFrame", padding=(18,16))
l_results_section.grid_columnconfigure(0, weight=1)
l_results_section.grid_rowconfigure(2, weight=1)
l_results_title = ttk.Label(l_results_section, text="Results", style="SectionHeading.TLabel")
l_results_hint = ttk.Label(
    l_results_section,
    text="Objects populate below with size metadata. Sorting and tag colors highlight errors and informational entries.",
    style="SectionHint.TLabel",
    justify="left"
)
l_results_body = ttk.Frame(l_results_section, style="SectionBody.TFrame")
l_results_body.grid_columnconfigure(0, weight=1)

ls_metrics = ttk.Frame(l_results_body, style="Metrics.TFrame")
ls_metric_count = ttk.Label(ls_metrics, text="🧾 Objects: —", style="Metrics.TLabel", anchor="w")
ls_metric_size = ttk.Label(ls_metrics, text="📦 Total size: —", style="Metrics.TLabel", anchor="w")
ls_metric_prefix = ttk.Label(ls_metrics, text="🔍 Prefix: (none)", style="Metrics.TLabel", anchor="w")
ls_metric_count.pack(side="left", padx=(0, 12))
ls_metric_size.pack(side="left", padx=(0, 12))
ls_metric_prefix.pack(side="left")

ls_output_frame = ttk.Frame(l_results_body, style="Card.TFrame")
ls_tree = ttk.Treeview(ls_output_frame, columns=("size", "name"), show="headings", style="List.Treeview", height=16)
ls_tree.heading("size", text="Size")
ls_tree.heading("name", text="Object Key")
ls_tree.column("size", width=130, anchor="e", stretch=False)
ls_tree.column("name", width=540, anchor="w")
ls_output_scroll_y = ttk.Scrollbar(ls_output_frame, orient="vertical", command=ls_tree.yview)
ls_output_scroll_x = ttk.Scrollbar(ls_output_frame, orient="horizontal", command=ls_tree.xview)
ls_tree.config(yscrollcommand=ls_output_scroll_y.set, xscrollcommand=ls_output_scroll_x.set)
ls_tree.grid(row=0, column=0, sticky="nsew")
ls_output_scroll_y.grid(row=0, column=1, sticky="ns")
ls_output_scroll_x.grid(row=1, column=0, sticky="we")
ls_output_frame.grid_rowconfigure(0, weight=1)
ls_output_frame.grid_columnconfigure(0, weight=1)
ls_tree.tag_configure("muted", foreground="#9aa2ad")
ls_tree.tag_configure("error", foreground="#ff6b6b", font=("SF Pro Text", 12, "bold"))
ls_tree.tag_configure("info", foreground=palette["ACCENT"])

ls_summary = ttk.Label(l_results_body, text="Ready", style="SectionHint.TLabel", justify="left", anchor="w")

l_results_title.grid(row=0, column=0, sticky="w")
l_results_hint.grid(row=1, column=0, sticky="we", pady=(4,12))
l_results_body.grid(row=2, column=0, sticky="nsew")
ls_metrics.pack(fill="x")
ls_output_frame.pack(fill="both", expand=True, pady=(12,8))
ls_summary.pack(fill="x", pady=(8,0))

# =============== DELETE OBJECT TAB ===============
delobj_tab = ttk.Frame(notebook)
notebook.add(delobj_tab, text="🗑️ Delete Object")
do_card = card(delobj_tab); do_card.pack(fill="both", expand=True)

do_bucket = tk.StringVar()
do_key = tk.StringVar()
do_all_versions = tk.BooleanVar(value=False)

do_callout = ttk.Frame(do_card, style="DangerCallout.TFrame", padding=(18,16))
do_callout.grid_columnconfigure(1, weight=1)
do_callout_icon = ttk.Label(do_callout, text="⚠️", style="DangerCalloutIcon.TLabel")
do_callout_title = ttk.Label(do_callout, text="Delete Object", style="DangerCalloutTitle.TLabel")
do_callout_text = ttk.Label(
    do_callout,
    text="Permanently remove a single object or wipe all of its versions. Confirm the bucket and key before executing this action.",
    style="DangerCalloutText.TLabel",
    justify="left"
)
do_callout_icon.grid(row=0, column=0, rowspan=2, sticky="n", padx=(0,12))
do_callout_title.grid(row=0, column=1, sticky="w")
do_callout_text.grid(row=1, column=1, sticky="we")

do_form_section = ttk.Frame(do_card, style="Section.TFrame", padding=(18,16))
do_form_section.grid_columnconfigure(1, weight=1)
do_form_title = ttk.Label(do_form_section, text="Object Target", style="SectionHeading.TLabel")
do_form_hint = ttk.Label(
    do_form_section,
    text="Specify the bucket and object key you want to remove. Enable version cleanup to delete every revision.",
    style="SectionHint.TLabel",
    justify="left"
)
do_form_sep = ttk.Separator(do_form_section, orient="horizontal")

do_lbl_bucket = ttk.Label(do_form_section, text="Bucket", style="SectionLabel.TLabel")
do_ent_bucket = ttk.Entry(do_form_section, textvariable=do_bucket)

do_lbl_key = ttk.Label(do_form_section, text="Object Key", style="SectionLabel.TLabel")
do_ent_key = ttk.Entry(do_form_section, textvariable=do_key)

do_chk_allv = ttk.Checkbutton(
    do_form_section,
    text="Delete all versions when available",
    variable=do_all_versions,
    style="Section.TCheckbutton"
)
do_actions = ttk.Frame(do_form_section, style="SectionToolbar.TFrame")
do_btn = ttk.Button(do_actions, text="Delete Object", style="Danger.TButton")
do_btn.pack(side="right")

do_form_title.grid(row=0, column=0, columnspan=2, sticky="w")
do_form_hint.grid(row=1, column=0, columnspan=2, sticky="we", pady=(4,12))
do_form_sep.grid(row=2, column=0, columnspan=2, sticky="we", pady=(0,16))
do_lbl_bucket.grid(row=3, column=0, sticky="w", pady=(0,4))
do_ent_bucket.grid(row=3, column=1, sticky="we", pady=(0,4), padx=(12,0))
do_lbl_key.grid(row=4, column=0, sticky="w", pady=(8,4))
do_ent_key.grid(row=4, column=1, sticky="we", pady=(8,4), padx=(12,0))
do_chk_allv.grid(row=5, column=0, columnspan=2, sticky="w", pady=(0,12))
do_actions.grid(row=6, column=0, columnspan=2, sticky="e")

do_status_section = ttk.Frame(do_card, style="Section.TFrame", padding=(18,16))
do_status_section.grid_columnconfigure(0, weight=1)
do_status_section.grid_rowconfigure(2, weight=1)
do_status_title = ttk.Label(do_status_section, text="Activity Log", style="SectionHeading.TLabel")
do_status_hint = ttk.Label(
    do_status_section,
    text="Progress messages and errors appear here as the delete tasks run.",
    style="SectionHint.TLabel",
    justify="left"
)
do_status_body = ttk.Frame(do_status_section, style="SectionBody.TFrame")
do_status_body.grid_columnconfigure(0, weight=1)
do_status_body.grid_rowconfigure(0, weight=1)
do_status_text = tk.Text(
    do_status_body,
    height=8,
    wrap="word",
    relief="flat",
    font=("SF Pro Text", 11)
)
do_status_text.config(state="disabled")

do_status_title.grid(row=0, column=0, sticky="w")
do_status_hint.grid(row=1, column=0, sticky="we", pady=(4,12))
do_status_body.grid(row=2, column=0, sticky="nsew")
do_status_text.grid(row=0, column=0, sticky="nsew")

# =============== DELETE BUCKET TAB ===============
delbucket_tab = ttk.Frame(notebook)
notebook.add(delbucket_tab, text="🗑️ Delete Bucket")
db_card = card(delbucket_tab); db_card.pack(fill="both", expand=True)

db_bucket = tk.StringVar()
db_force = tk.BooleanVar(value=True)
db_include_versions = tk.BooleanVar(value=True)

db_callout = ttk.Frame(db_card, style="DangerCallout.TFrame", padding=(18,16))
db_callout.grid_columnconfigure(1, weight=1)
db_callout_icon = ttk.Label(db_callout, text="🗑️", style="DangerCalloutIcon.TLabel")
db_callout_title = ttk.Label(db_callout, text="Delete Bucket", style="DangerCalloutTitle.TLabel")
db_callout_text = ttk.Label(
    db_callout,
    text="Empty a bucket and remove it from your cluster. Forced deletion will purge objects and optional version history first.",
    style="DangerCalloutText.TLabel",
    justify="left"
)
db_callout_icon.grid(row=0, column=0, rowspan=2, sticky="n", padx=(0,12))
db_callout_title.grid(row=0, column=1, sticky="w")
db_callout_text.grid(row=1, column=1, sticky="we")

db_form_section = ttk.Frame(db_card, style="Section.TFrame", padding=(18,16))
db_form_section.grid_columnconfigure(1, weight=1)
db_form_title = ttk.Label(db_form_section, text="Bucket Cleanup", style="SectionHeading.TLabel")
db_form_hint = ttk.Label(
    db_form_section,
    text="For force deletions the contents are emptied first. Include versions to clean up historical revisions as well.",
    style="SectionHint.TLabel",
    justify="left"
)
db_form_sep = ttk.Separator(db_form_section, orient="horizontal")
db_lbl_bucket = ttk.Label(db_form_section, text="Bucket", style="SectionLabel.TLabel")
db_ent_bucket = ttk.Entry(db_form_section, textvariable=db_bucket)

db_chk_force = ttk.Checkbutton(
    db_form_section,
    text="Force delete - remove objects before deleting the bucket",
    variable=db_force,
    style="Section.TCheckbutton"
)
db_chk_versions = ttk.Checkbutton(
    db_form_section,
    text="Include versioned objects during cleanup",
    variable=db_include_versions,
    style="Section.TCheckbutton"
)
db_actions = ttk.Frame(db_form_section, style="SectionToolbar.TFrame")
db_btn = ttk.Button(db_actions, text="Delete Bucket", style="Danger.TButton")
db_btn.pack(side="right")

db_form_title.grid(row=0, column=0, columnspan=2, sticky="w")
db_form_hint.grid(row=1, column=0, columnspan=2, sticky="we", pady=(4,12))
db_form_sep.grid(row=2, column=0, columnspan=2, sticky="we", pady=(0,16))
db_lbl_bucket.grid(row=3, column=0, sticky="w", pady=(0,4))
db_ent_bucket.grid(row=3, column=1, sticky="we", pady=(0,12), padx=(12,0))
db_chk_force.grid(row=4, column=0, columnspan=2, sticky="w", pady=(0,8))
db_chk_versions.grid(row=5, column=0, columnspan=2, sticky="w", pady=(0,12))
db_actions.grid(row=6, column=0, columnspan=2, sticky="e")

db_status_section = ttk.Frame(db_card, style="Section.TFrame", padding=(18,16))
db_status_section.grid_columnconfigure(0, weight=1)
db_status_section.grid_rowconfigure(2, weight=1)
db_status_title = ttk.Label(db_status_section, text="Bucket Activity", style="SectionHeading.TLabel")
db_status_hint = ttk.Label(
    db_status_section,
    text="Watch the cleanup go step-by-step, including forced deletions and version removals.",
    style="SectionHint.TLabel",
    justify="left"
)
db_status_frame = ttk.Frame(db_status_section, style="SectionBody.TFrame")
db_status_frame.grid_columnconfigure(0, weight=1)
db_status_frame.grid_rowconfigure(0, weight=1)
db_status_text = tk.Text(
    db_status_frame,
    wrap="word",
    height=12,
    relief="flat",
    font=("SF Pro Text", 11)
)
db_status_text.config(state="disabled")

db_status_title.grid(row=0, column=0, sticky="w")
db_status_hint.grid(row=1, column=0, sticky="we", pady=(4,12))
db_status_frame.grid(row=2, column=0, sticky="nsew")
db_status_text.grid(row=0, column=0, sticky="nsew")

# =============== SETTINGS TAB ===============
settings_tab = ttk.Frame(notebook)
notebook.add(settings_tab, text="⚙️ Settings")
s_card = card(settings_tab); s_card.pack(fill="both", expand=True)

s_callout = ttk.Frame(s_card, style="AccentCallout.TFrame", padding=(18,16))
s_callout.grid_columnconfigure(1, weight=1)
s_callout_icon = ttk.Label(s_callout, text="🛠️", style="AccentCalloutIcon.TLabel")
s_callout_title = ttk.Label(s_callout, text="Connection Settings", style="AccentCalloutTitle.TLabel")
s_callout_text = ttk.Label(
    s_callout,
    text="Store your S3 / MinIO credentials locally so uploads and downloads work without environment variables.",
    style="AccentCalloutText.TLabel",
    justify="left"
)
s_callout_icon.grid(row=0, column=0, rowspan=2, sticky="n", padx=(0,12))
s_callout_title.grid(row=0, column=1, sticky="w")
s_callout_text.grid(row=1, column=1, sticky="we")
s_callout.pack(fill="x", pady=(PADY, PADY))

s_form_section = ttk.Frame(s_card, style="Section.TFrame", padding=(18,16))
s_form_section.grid_columnconfigure(0, weight=0)
s_form_section.grid_columnconfigure(1, weight=2)
s_form_section.grid_columnconfigure(2, weight=1)
s_form_title = ttk.Label(s_form_section, text="Credentials", style="SectionHeading.TLabel")
s_form_hint = ttk.Label(
    s_form_section,
    text="Values are saved to your user profile. They are used when establishing new connections for uploads, downloads, and deletes.",
    style="SectionHint.TLabel",
    justify="left"
)
s_form_sep = ttk.Separator(s_form_section, orient="horizontal")

s_lbl_provider = ttk.Label(s_form_section, text="Provider", style="SectionLabel.TLabel")
s_provider_opts = ttk.Frame(s_form_section, style="SectionBody.TFrame")
s_provider_aws = ttk.Radiobutton(s_provider_opts, text="AWS", variable=cfg_provider, value=PROVIDER_AWS, style="Section.TRadiobutton", command=lambda: _on_provider_change())
s_provider_minio = ttk.Radiobutton(s_provider_opts, text="MinIO / Custom", variable=cfg_provider, value=PROVIDER_MINIO, style="Section.TRadiobutton", command=lambda: _on_provider_change())
s_provider_aws.pack(side="left", padx=(0,12))
s_provider_minio.pack(side="left")

s_lbl_region = ttk.Label(s_form_section, text="Region", style="SectionLabel.TLabel")
s_ent_region = ttk.Entry(s_form_section, textvariable=cfg_region, width=26)
s_region_hint = ttk.Label(s_form_section, text="Example: us-east-1", style="SectionHint.TLabel", justify="left")

s_chk_custom_endpoint = ttk.Checkbutton(
    s_form_section,
    text="Specify a custom S3 endpoint (advanced)",
    variable=cfg_custom_endpoint,
    style="Section.TCheckbutton",
    command=lambda: _update_endpoint_field()
)

s_lbl_endpoint = ttk.Label(s_form_section, text="S3 Endpoint", style="SectionLabel.TLabel")
s_ent_endpoint = ttk.Entry(s_form_section, textvariable=cfg_endpoint, width=26)
s_endpoint_hint = ttk.Label(s_form_section, text="Example: 127.0.0.1:9000", style="SectionHint.TLabel", justify="left")

s_lbl_access = ttk.Label(s_form_section, text="Access Key ID", style="SectionLabel.TLabel")
s_ent_access = ttk.Entry(s_form_section, textvariable=cfg_access_key, width=26)
s_access_hint = ttk.Label(s_form_section, text="Example: AKIAIOSFODNN7EXAMPLE", style="SectionHint.TLabel", justify="left")

s_lbl_secret = ttk.Label(s_form_section, text="Secret Access Key", style="SectionLabel.TLabel")
s_secret_frame = ttk.Frame(s_form_section, style="SectionBody.TFrame")
s_secret_frame.grid_columnconfigure(0, weight=1)
s_ent_secret = ttk.Entry(s_secret_frame, textvariable=cfg_secret_key, show="•", width=28)
def _toggle_secret():
    if cfg_show_secret.get():
        cfg_show_secret.set(False)
        s_ent_secret.config(show="•")
        s_btn_secret_toggle.config(text="Show")
    else:
        cfg_show_secret.set(True)
        s_ent_secret.config(show="")
        s_btn_secret_toggle.config(text="Hide")
s_btn_secret_toggle = ttk.Button(s_secret_frame, text="Show", width=6, command=_toggle_secret)
s_ent_secret.pack(side="left", fill="x", expand=True)
s_btn_secret_toggle.pack(side="left", padx=(8,0))
s_secret_hint = ttk.Label(s_form_section, text="Paste the secret key exactly as provided.", style="SectionHint.TLabel", justify="left")
path_style_row = ttk.Frame(s_form_section, style="SectionBody.TFrame")
path_style_row.grid_columnconfigure(0, weight=1)
path_style_hint = ttk.Label(
    path_style_row,
    text="Enable path-style addressing when your S3-compatible service requires it.",
    style="SectionHint.TLabel",
    justify="left"
)
path_style_switch = ttk.Checkbutton(
    path_style_row,
    text="Use path-style addressing",
    variable=cfg_path_style,
    style="Section.TCheckbutton"
)
path_style_switch.pack(side="left", padx=(0, 8))
path_style_hint.pack(side="left", fill="x", expand=True)
path_style_row.grid_remove()

https_row = ttk.Frame(s_form_section, style="SectionBody.TFrame")
https_row.grid_columnconfigure(0, weight=1)
https_switch = ttk.Checkbutton(
    https_row,
    text="Use HTTPS",
    variable=cfg_secure,
    style="Section.TCheckbutton"
)
https_hint = ttk.Label(
    https_row,
    text="Disable only if your server is running without TLS (not recommended).",
    style="SectionHint.TLabel",
    justify="left"
)
https_switch.pack(side="left", padx=(0, 8))
https_hint.pack(side="left", fill="x", expand=True)
https_row.grid_remove()

s_actions = ttk.Frame(s_form_section, style="SectionToolbar.TFrame")
s_test_status_label = ttk.Label(s_actions, textvariable=cfg_test_status, style="StatusInfo.TLabel")
s_btn_save = ttk.Button(s_actions, text="Save Settings", style="Accent.TButton")
s_btn_test = ttk.Button(s_actions, text="Test Connection", style="Neutral.TButton")
s_test_status_label.pack(side="left")
s_btn_test.pack(side="right", padx=(8,0), pady=(0,2))
s_btn_save.pack(side="right", padx=(0,8), pady=(0,2))

s_form_section.pack(fill="x", pady=(0, PADY))

s_divider = ttk.Separator(s_card, orient="horizontal")
s_divider.pack(fill="x", padx=18, pady=(0,12))

s_info_section = ttk.Frame(s_card, style="Info.TFrame", padding=(18,16))
s_info_section.grid_columnconfigure(0, weight=1)
s_info_title = ttk.Label(s_info_section, text="Configuration Status", style="InfoHeading.TLabel")
s_display_path = _display_config_path()
s_info_path = ttk.Label(
    s_info_section,
    text=f"🗂 Settings file: {s_display_path} (click to reveal)",
    style="InfoLink.TLabel",
    justify="left",
    cursor="hand2"
)
s_info_path.configure(takefocus=True)
s_info_hint = ttk.Label(
    s_info_section,
    text="Leave the endpoint blank to use the default AWS endpoint for the selected region.",
    style="InfoHint.TLabel",
    justify="left"
)
s_info_message = ttk.Label(s_info_section, textvariable=cfg_status, style="InfoHint.TLabel", justify="left")
s_info_title.grid(row=0, column=0, sticky="w")
s_info_path.grid(row=1, column=0, sticky="w", pady=(4,4))
s_info_hint.grid(row=2, column=0, sticky="we", pady=(0,8))
s_info_message.grid(row=3, column=0, sticky="we")
s_info_section.pack(fill="x", pady=(0, PADY))
# Bottom spacer prevents the last line from being obscured on some window managers
s_bottom_spacer = ttk.Frame(s_card, style="Section.TFrame", height=14)
s_bottom_spacer.pack(fill="x", pady=(0, 6))

for widget, background in (
    (do_status_text, palette["STATUS_BG"]),
    (db_status_text, palette["STATUS_BG"]),
):
    widget.configure(
        bg=background,
        fg=palette["TEXTAREA_FG"],
        insertbackground=palette["TEXTAREA_FG"],
        highlightthickness=0,
        borderwidth=0,
    )

# ---------------- Responsive layout engine ----------------
def layout_settings_form(compact=False):
    _layout_state["settings_compact"] = compact
    provider_now = cfg_provider.get()

    to_reset = [
        s_form_title,
        s_form_hint,
        s_form_sep,
        s_lbl_provider,
        s_provider_opts,
        s_lbl_region,
        s_ent_region,
        s_region_hint,
        s_chk_custom_endpoint,
        s_lbl_endpoint,
        s_ent_endpoint,
        s_endpoint_hint,
        s_lbl_access,
        s_ent_access,
        s_access_hint,
        s_lbl_secret,
        s_secret_frame,
        s_secret_hint,
        path_style_row,
        https_row,
        s_actions,
    ]
    for widget in to_reset:
        try:
            widget.grid_forget()
        except Exception:
            pass

    if compact:
        s_form_section.grid_columnconfigure(0, weight=1)
        s_form_section.grid_columnconfigure(1, weight=0)
        s_form_section.grid_columnconfigure(2, weight=0)
    else:
        s_form_section.grid_columnconfigure(0, weight=0)
        s_form_section.grid_columnconfigure(1, weight=2)
        s_form_section.grid_columnconfigure(2, weight=1)

    row = 0
    s_form_title.grid(row=row, column=0, columnspan=3, sticky="w")
    row += 1
    s_form_hint.grid(row=row, column=0, columnspan=3, sticky="we", pady=(4, 12))
    row += 1
    s_form_sep.grid(row=row, column=0, columnspan=3, sticky="we", pady=(0, 16))
    row += 1

    def add_row(label=None, control=None, hint=None, *, full=False, pady=(0, 4), control_sticky="we"):
        nonlocal row
        if compact:
            if label is not None:
                label.grid(row=row, column=0, columnspan=3, sticky="w", pady=pady)
                row += 1
            if control is not None:
                control.grid(row=row, column=0, columnspan=3, sticky=control_sticky, pady=(0, 4))
                row += 1
            if hint is not None:
                hint.grid(row=row, column=0, columnspan=3, sticky="we", pady=(4, 0))
                row += 1
            return

        if full or label is None:
            if label is not None:
                label.grid(row=row, column=0, columnspan=3, sticky="w", pady=pady)
                row += 1
            if control is not None:
                control.grid(row=row, column=0, columnspan=3, sticky=control_sticky, pady=pady)
                row += 1
            if hint is not None:
                hint.grid(row=row, column=0, columnspan=3, sticky="we", pady=(4, 0))
                row += 1
            return

        label.grid(row=row, column=0, sticky="w", pady=pady)
        if control is not None:
            span = 2 if hint is None else 1
        control.grid(row=row, column=1, columnspan=span, sticky=control_sticky, pady=pady, padx=(16,0))
        if hint is not None:
            hint.grid(row=row, column=2, sticky="w", pady=pady, padx=(12, 0))
        row += 1

    add_row(s_lbl_provider, s_provider_opts, control_sticky="w")
    add_row(s_lbl_region, s_ent_region, s_region_hint, pady=(8, 4))
    # Provider-specific rows
    if provider_now == PROVIDER_MINIO:
        # Always show endpoint for MinIO; hide the custom-endpoint checkbox
        try:
            s_chk_custom_endpoint.grid_remove()
        except Exception:
            pass
        add_row(s_lbl_endpoint, s_ent_endpoint, s_endpoint_hint, pady=(8, 4))
    else:
        # AWS: hide endpoint inputs and the custom-endpoint checkbox
        try:
            s_chk_custom_endpoint.grid_remove()
        except Exception:
            pass
        try:
            s_lbl_endpoint.grid_remove()
            s_ent_endpoint.grid_remove()
            s_endpoint_hint.grid_remove()
        except Exception:
            pass
    add_row(s_lbl_access, s_ent_access, s_access_hint, pady=(8, 4))
    add_row(s_lbl_secret, s_secret_frame, s_secret_hint, pady=(8, 4))
    if provider_now == PROVIDER_MINIO:
        path_style_row.grid_remove()
        https_row.grid(row=row, column=0, columnspan=3, sticky="ew", pady=(12,0))
        row += 1
    else:
        path_style_row.grid_remove()
        https_row.grid_remove()
    s_actions.grid(row=row, column=0, columnspan=3, sticky="ew", pady=(16, 0))

def grid_config(frame, cols):
    try:
        existing_cols = frame.grid_size()[0]
    except Exception:
        existing_cols = 0
    total = max(existing_cols, cols, 4)
    for c in range(total):
        frame.columnconfigure(c, weight=0)
    for c in range(cols):
        frame.columnconfigure(c, weight=1)

def layout_upload(compact=False):
    for w in (u_callout, u_form_section, u_status_section):
        w.grid_forget()
    for r in range(4):
        u_card.rowconfigure(r, weight=0)
    if compact:
        row = 0
        u_callout.grid(row=row, column=0, sticky="we", padx=PADX, pady=(PADY, PADY)); row += 1
        u_form_section.grid(row=row, column=0, sticky="nsew", padx=PADX, pady=(0, PADY)); row += 1
        u_status_section.grid(row=row, column=0, sticky="nsew", padx=PADX, pady=(0, PADY))
        u_card.rowconfigure(row, weight=1)
        grid_config(u_card, 1)
    else:
        row = 0
        u_callout.grid(row=row, column=0, columnspan=2, sticky="we", padx=PADX, pady=(PADY, PADY)); row += 1
        u_form_section.grid(row=row, column=0, sticky="nsew", padx=(PADX, PADX//2), pady=(0, PADY))
        u_status_section.grid(row=row, column=1, sticky="nsew", padx=(PADX//2, PADX), pady=(0, PADY))
        u_card.rowconfigure(row, weight=1)
        grid_config(u_card, 2)
        u_card.columnconfigure(0, weight=1)
        u_card.columnconfigure(1, weight=3)

def layout_download(compact=False):
    for w in (d_callout, d_form_section, d_status_section):
        w.grid_forget()
    for r in range(4):
        d_card.rowconfigure(r, weight=0)
    if compact:
        row = 0
        d_callout.grid(row=row, column=0, sticky="we", padx=PADX, pady=(PADY, PADY)); row += 1
        d_form_section.grid(row=row, column=0, sticky="nsew", padx=PADX, pady=(0, PADY)); row += 1
        d_status_section.grid(row=row, column=0, sticky="nsew", padx=PADX, pady=(0, PADY))
        d_card.rowconfigure(row, weight=1)
        grid_config(d_card, 1)
    else:
        row = 0
        d_callout.grid(row=row, column=0, columnspan=2, sticky="we", padx=PADX, pady=(PADY, PADY)); row += 1
        d_form_section.grid(row=row, column=0, sticky="nsew", padx=(PADX, PADX//2), pady=(0, PADY))
        d_status_section.grid(row=row, column=1, sticky="nsew", padx=(PADX//2, PADX), pady=(0, PADY))
        d_card.rowconfigure(row, weight=1)
        grid_config(d_card, 2)
        d_card.columnconfigure(0, weight=1)
        d_card.columnconfigure(1, weight=3)

def layout_list(compact=False):
    for w in (l_callout, l_form_section, l_results_section):
        w.grid_forget()
    for r in range(4):
        l_card.rowconfigure(r, weight=0)
    if compact:
        row = 0
        l_callout.grid(row=row, column=0, sticky="we", padx=PADX, pady=(PADY, PADY)); row += 1
        l_form_section.grid(row=row, column=0, sticky="nsew", padx=PADX, pady=(0, PADY)); row += 1
        l_results_section.grid(row=row, column=0, sticky="nsew", padx=PADX, pady=(0, PADY))
        l_card.rowconfigure(row, weight=1)
        grid_config(l_card, 1)
    else:
        row = 0
        l_callout.grid(row=row, column=0, columnspan=3, sticky="we", padx=PADX, pady=(PADY, PADY)); row += 1
        l_form_section.grid(row=row, column=0, sticky="nsew", padx=(PADX, PADX//2), pady=(0, PADY))
        l_results_section.grid(row=row, column=1, columnspan=2, sticky="nsew", padx=(PADX//2, PADX), pady=(0, PADY))
        l_card.rowconfigure(row, weight=1)
        grid_config(l_card, 3)

def layout_delete_object(compact=False):
    for w in (do_callout, do_form_section, do_status_section):
        w.grid_forget()
    for r in range(4):
        do_card.rowconfigure(r, weight=0)
    if compact:
        row = 0
        do_callout.grid(row=row, column=0, sticky="we", padx=PADX, pady=(PADY, PADY)); row+=1
        do_form_section.grid(row=row, column=0, sticky="nsew", padx=PADX, pady=(0,PADY)); row+=1
        do_status_section.grid(row=row, column=0, sticky="nsew", padx=PADX, pady=(0,PADY))
        do_card.rowconfigure(row, weight=1)
        grid_config(do_card, 1)
    else:
        row = 0
        do_callout.grid(row=row, column=0, columnspan=2, sticky="we", padx=PADX, pady=(PADY, PADY)); row+=1
        do_form_section.grid(row=row, column=0, sticky="nsew", padx=(PADX, PADX//2), pady=(0,PADY))
        do_status_section.grid(row=row, column=1, sticky="nsew", padx=(PADX//2, PADX), pady=(0,PADY))
        do_card.rowconfigure(row, weight=1)
        grid_config(do_card, 2)
        do_card.columnconfigure(0, weight=1, uniform="delete_object_sections")
        do_card.columnconfigure(1, weight=1, uniform="delete_object_sections")

def layout_delete_bucket(compact=False):
    for w in (db_callout, db_form_section, db_status_section):
        w.grid_forget()
    if compact:
        row = 0
        db_callout.grid(row=row, column=0, sticky="we", padx=PADX, pady=(PADY, PADY)); row+=1
        db_form_section.grid(row=row, column=0, sticky="nsew", padx=PADX, pady=(0,PADY)); row+=1
        db_status_section.grid(row=row, column=0, sticky="nsew", padx=PADX, pady=(0,PADY))
        db_card.rowconfigure(row, weight=1)
        grid_config(db_card, 1)
    else:
        row = 0
        db_callout.grid(row=row, column=0, columnspan=2, sticky="we", padx=PADX, pady=(PADY, PADY)); row+=1
        db_form_section.grid(row=row, column=0, sticky="nsew", padx=(PADX, PADX//2), pady=(0,PADY))
        db_status_section.grid(row=row, column=1, sticky="nsew", padx=(PADX//2, PADX), pady=(0,PADY))
        db_card.rowconfigure(row, weight=1)
        grid_config(db_card, 2)
        db_card.columnconfigure(0, weight=1, uniform="delete_bucket_sections")
        db_card.columnconfigure(1, weight=1, uniform="delete_bucket_sections")

# Initial layout (wide by default)
layout_upload(compact=False)
layout_download(compact=False)
layout_list(compact=False)
layout_delete_object(compact=False)
layout_delete_bucket(compact=False)
layout_settings_form(compact=False)



def _update_endpoint_field(*_):
    provider = cfg_provider.get()
    custom = cfg_custom_endpoint.get()

    if provider == PROVIDER_AWS and not custom:
        # AWS uses derived endpoints; disable manual entry.
        s_ent_endpoint.config(state="disabled")
        derived = _default_endpoint(cfg_region.get())
        if derived:
            s_endpoint_hint.config(
                text=f"AWS endpoint derived from region: {derived}",
                style="StatusInfo.TLabel",
            )
        else:
            s_endpoint_hint.config(
                text="AWS endpoint will be derived from the selected region.",
                style="StatusInfo.TLabel",
            )
    else:
        s_ent_endpoint.config(state="normal")
        if provider == PROVIDER_MINIO and not cfg_endpoint.get().strip():
            cfg_endpoint.set("127.0.0.1:9000")
        hint_value = cfg_endpoint.get().strip()
        if not hint_value:
            hint_value = "s3.<region>.amazonaws.com" if provider == PROVIDER_AWS else "play.min.io:9000"
        s_endpoint_hint.config(text=f"Example: {hint_value}", style="SectionHint.TLabel")

    layout_settings_form(_layout_state.get("settings_compact", False))
    _on_endpoint_change()


def _on_endpoint_change(*_):
    provider = cfg_provider.get()
    endpoint = cfg_endpoint.get().strip()
    ep_lower = endpoint.lower()
    if provider == PROVIDER_MINIO:
        if ep_lower.endswith(":9000") and cfg_secure.get():
            cfg_secure.set(False)
    elif provider == PROVIDER_AWS:
        if not cfg_secure.get():
            cfg_secure.set(True)
    _validate_fields()


def _on_provider_change(*_):
    provider = cfg_provider.get()
    global _provider_loading
    if _provider_loading:
        return

    prev = _active_provider.get("value")
    if prev:
        _provider_snapshot[prev] = _current_provider_state()

    _provider_loading = True
    restored = _restore_provider_state(provider)

    if provider == PROVIDER_AWS:
        s_chk_custom_endpoint.state(["disabled"])
        cfg_custom_endpoint.set(False)
        if not cfg_region.get().strip():
            cfg_region.set("us-east-1")
        s_lbl_region.config(text="Region")
        s_region_hint.config(text="Example: us-east-1")
        cfg_secure.set(True)
    else:
        s_chk_custom_endpoint.state(["!disabled"])
        s_lbl_region.config(text="Region (optional)")
        s_region_hint.config(text="Example: us-east-1 (leave blank to use server default)")
        if not restored:
            cfg_region.set("")
            cfg_access_key.set("")
            cfg_secret_key.set("")
            cfg_endpoint.set("")
            cfg_custom_endpoint.set(True)
            cfg_secure.set(True)
        cfg_path_style.set(False)
        if not cfg_custom_endpoint.get():
            cfg_custom_endpoint.set(True)

    _active_provider["value"] = provider
    _provider_loading = False
    _set_test_status("")
    _update_endpoint_field()
    _provider_snapshot[provider] = _current_provider_state()
    _refresh_configuration_status()


def _set_test_status(message, style="StatusInfo.TLabel"):
    cfg_test_status.set(message)
    s_test_status_label.config(style=style)
    if message:
        s_test_status_label.pack_configure(side="left", padx=(0,8))
    else:
        s_test_status_label.pack_configure(side="left", padx=(0,0))


REGION_RE = re.compile(r"^[a-z]{2}-[a-z0-9-]+-\d+$")
ENDPOINT_RE = re.compile(r"^[A-Za-z0-9.-]+(:\d+)?$")


def _validate_fields(*_):
    provider = cfg_provider.get()
    region = cfg_region.get().strip()
    access = cfg_access_key.get().strip()
    secret = cfg_secret_key.get().strip()
    endpoint = cfg_endpoint.get().strip()

    if provider == PROVIDER_AWS:
        valid_region = bool(REGION_RE.match(region))
        if valid_region:
            s_region_hint.config(text="✅ Region looks good (e.g., us-east-1)", style="Success.TLabel")
        else:
            s_region_hint.config(text="🔴 Region required (e.g., us-east-1)", style="Error.TLabel")
    else:
        if region and not REGION_RE.match(region):
            valid_region = False
            s_region_hint.config(text="🔴 Region format should look like us-east-1", style="Error.TLabel")
        else:
            valid_region = True
            s_region_hint.config(text="✅ Region optional (leave blank to use server default)", style="Success.TLabel")

    # Endpoint validation
    require_custom = (provider == PROVIDER_MINIO) or cfg_custom_endpoint.get()
    if require_custom:
        valid_endpoint = bool(ENDPOINT_RE.match(endpoint))
        if valid_endpoint:
            s_endpoint_hint.config(text=f"✅ Endpoint format looks good ({endpoint or ''})", style="Success.TLabel")
        else:
            s_endpoint_hint.config(text="🔴 Endpoint must be host[:port] (e.g., play.min.io:9000)", style="Error.TLabel")
    else:
        # AWS with derived endpoint from region
        if provider == PROVIDER_AWS:
            if region:
                derived = _default_endpoint(region)
                if derived:
                    s_endpoint_hint.config(
                        text=f"AWS endpoint derived from region: {derived}",
                        style="StatusInfo.TLabel",
                    )
                else:
                    s_endpoint_hint.config(
                        text="Region set, endpoint will be derived automatically when needed.",
                        style="StatusInfo.TLabel",
                    )
                valid_endpoint = True
            else:
                s_endpoint_hint.config(
                    text="Set a region to derive the AWS endpoint automatically.",
                    style="Error.TLabel",
                )
                valid_endpoint = False
        else:
            s_endpoint_hint.config(
                text="Custom endpoint required for this provider.",
                style="Error.TLabel",
            )
            valid_endpoint = False

    if provider == PROVIDER_AWS:
        valid_access = 16 <= len(access) <= 128
        if valid_access:
            s_access_hint.config(text="✅ Access key length looks good", style="Success.TLabel")
        else:
            s_access_hint.config(text="🔴 Access key must be 16–128 characters", style="Error.TLabel")

        valid_secret = len(secret) >= 16
        if valid_secret:
            s_secret_hint.config(text="✅ Secret key captured", style="Success.TLabel")
        else:
            s_secret_hint.config(text="🔴 Secret key must be at least 16 characters", style="Error.TLabel")
    else:
        valid_access = len(access) >= 3
        if valid_access:
            s_access_hint.config(text="✅ Access key captured", style="Success.TLabel")
        else:
            s_access_hint.config(text="🔴 Access key required for MinIO / custom", style="Error.TLabel")

        valid_secret = len(secret) >= 8
        if valid_secret:
            s_secret_hint.config(text="✅ Secret key captured", style="Success.TLabel")
        else:
            s_secret_hint.config(text="🔴 Secret key must be at least 8 characters", style="Error.TLabel")

    can_test = all([valid_region, valid_endpoint, valid_access, valid_secret])
    if not cfg_test_status.get().startswith("⏳"):
        if can_test:
            s_btn_test.state(["!disabled"])
        else:
            s_btn_test.state(["disabled"])
    return can_test

def _collect_settings():
    region = cfg_region.get().strip()
    provider = cfg_provider.get()
    use_custom = (provider == PROVIDER_MINIO) or cfg_custom_endpoint.get()
    endpoint_val = cfg_endpoint.get().strip() if use_custom else ""
    return {
        "AWS_REGION": region,
        "AWS_ACCESS_KEY_ID": cfg_access_key.get().strip(),
        "AWS_SECRET_ACCESS_KEY": cfg_secret_key.get().strip(),
        "AWS_S3_ENDPOINT": endpoint_val,
        "AWS_S3_SECURE": "true" if cfg_secure.get() else "false",
        "PROVIDER": provider,
        "USE_CUSTOM_ENDPOINT": use_custom,
        "AWS_S3_PATH_STYLE": "true" if cfg_path_style.get() else "false",
    }


def _effective_endpoint(settings):
    provider = settings.get("PROVIDER", PROVIDER_AWS)
    region = (settings.get("AWS_REGION") or "").strip()
    endpoint = (settings.get("AWS_S3_ENDPOINT") or "").strip()
    use_custom = bool(settings.get("USE_CUSTOM_ENDPOINT"))
    if provider == PROVIDER_MINIO:
        return endpoint
    if endpoint:
        return endpoint
    if not use_custom and provider == PROVIDER_AWS and region:
        return _default_endpoint(region)
    return ""


def _apply_env_from_settings(settings):
    global _provider_loading
    mapping = {
        "AWS_REGION": "AWS_REGION",
        "AWS_ACCESS_KEY_ID": "AWS_ACCESS_KEY_ID",
        "AWS_SECRET_ACCESS_KEY": "AWS_SECRET_ACCESS_KEY",
        "AWS_S3_ENDPOINT": "AWS_S3_ENDPOINT",
        "AWS_S3_PATH_STYLE": "AWS_S3_PATH_STYLE",
    }
    for key, env_key in mapping.items():
        value = settings.get(key, "")
        if value:
            os.environ[env_key] = value
        else:
            os.environ.pop(env_key, None)
    os.environ["AWS_S3_SECURE"] = settings.get("AWS_S3_SECURE", "true")
    provider = settings.get("PROVIDER", cfg_provider.get())

    _provider_loading = True
    cfg_provider.set(provider)
    cfg_region.set(settings.get("AWS_REGION", ""))
    cfg_endpoint.set(settings.get("AWS_S3_ENDPOINT", ""))
    cfg_access_key.set(settings.get("AWS_ACCESS_KEY_ID", ""))
    cfg_secret_key.set(settings.get("AWS_SECRET_ACCESS_KEY", ""))

    custom = settings.get("USE_CUSTOM_ENDPOINT")
    if provider == PROVIDER_MINIO:
        cfg_custom_endpoint.set(True)
    else:
        cfg_custom_endpoint.set(bool(custom))

    cfg_secure.set(_settings_bool(settings.get("AWS_S3_SECURE"), True))
    cfg_path_style.set(_settings_bool(settings.get("AWS_S3_PATH_STYLE"), False))

    _active_provider["value"] = provider
    _provider_snapshot[provider] = _current_provider_state()
    _provider_loading = False

    _on_provider_change()
    _validate_fields()
    _refresh_configuration_status()


def _persist_settings():
    data = _collect_settings()
    save_s3_settings(data)
    _apply_env_from_settings(data)
    return data


def _on_settings_save():
    data = _collect_settings()
    provider = data.get("PROVIDER", cfg_provider.get())
    required_fields = [
        ("Access Key ID", "AWS_ACCESS_KEY_ID"),
        ("Secret Access Key", "AWS_SECRET_ACCESS_KEY"),
    ]
    if provider == PROVIDER_AWS:
        required_fields.insert(0, ("AWS Region", "AWS_REGION"))
    if data.get("USE_CUSTOM_ENDPOINT"):
        required_fields.append(("Endpoint", "AWS_S3_ENDPOINT"))
    missing = [label for label, key in required_fields if not data.get(key)]
    if missing:
        statusbar.config(text="Cannot save: missing " + ", ".join(missing))
        _set_test_status("")
        _validate_fields()
        return None

    data = _persist_settings()
    timestamp = time.strftime("%a %H:%M")
    statusbar.config(text="Connection settings saved.")
    _set_test_status("")
    _validate_fields()
    _refresh_configuration_status(saved_at=timestamp)
    return data


def _on_settings_test():
    data = _collect_settings()
    provider = data.get("PROVIDER", cfg_provider.get())
    required_fields = [
        ("Access Key ID", "AWS_ACCESS_KEY_ID"),
        ("Secret Access Key", "AWS_SECRET_ACCESS_KEY"),
    ]
    if provider == PROVIDER_AWS:
        required_fields.insert(0, ("AWS Region", "AWS_REGION"))

    if data.get("USE_CUSTOM_ENDPOINT"):
        required_fields.append(("Endpoint", "AWS_S3_ENDPOINT"))
    missing = [label for label, key in required_fields if not data.get(key)]
    if missing:
        _set_test_status("🟠 Missing values: " + ", ".join(missing), "Error.TLabel")
        statusbar.config(text="Connection test failed.")
        _validate_fields()
        return

    endpoint_for_test = _effective_endpoint(data)
    if not endpoint_for_test:
        _set_test_status("🟠 Unable to determine an endpoint. Specify a custom endpoint or set a valid region.", "Error.TLabel")
        statusbar.config(text="Connection test failed.")
        return

    def run_test():
        start = time.perf_counter()
        try:
            use_path = _settings_bool(data.get("AWS_S3_PATH_STYLE"), False)
            minio_kwargs = dict(
                secure=_settings_bool(data.get("AWS_S3_SECURE"), True),
                region=data["AWS_REGION"] or None,
            )
            http_client = None
            try:
                from urllib3 import PoolManager, util
            except Exception:
                http_client = None
            else:
                try:
                    http_client = PoolManager(
                        timeout=util.Timeout(connect=3.0, read=6.0),
                        retries=0,
                    )
                except Exception:
                    http_client = None
            if http_client is not None:
                minio_kwargs["http_client"] = http_client
            try:
                # Newer MinIO SDKs (v7.2.5+) support `bucket_lookup`
                client = Minio(
                    endpoint_for_test,
                    access_key=data["AWS_ACCESS_KEY_ID"],
                    secret_key=data["AWS_SECRET_ACCESS_KEY"],
                    bucket_lookup=("path" if use_path else "auto"),
                    **minio_kwargs,
                )
            except TypeError:
                # Older SDKs do not accept `bucket_lookup`; fall back to defaults
                client = Minio(
                    endpoint_for_test,
                    access_key=data["AWS_ACCESS_KEY_ID"],
                    secret_key=data["AWS_SECRET_ACCESS_KEY"],
                    **minio_kwargs,
                )
            buckets = client.list_buckets()
            elapsed_ms = (time.perf_counter() - start) * 1000.0
            bucket_count = len(buckets)
        except Exception as exc:
            _set_test_status(f"🔴 Connection failed: {_truncate_middle(str(exc), 120)}", "Error.TLabel")
            statusbar.config(text="Connection test failed.")
        else:
            count_text = "no buckets" if bucket_count == 0 else f"{bucket_count} bucket{'s' if bucket_count != 1 else ''}"
            _set_test_status(f"✅ Connected in {elapsed_ms:.0f} ms • {count_text}", "Success.TLabel")
            statusbar.config(text="Connection test succeeded.")
        finally:
            s_btn_test.state(["!disabled"])
            _validate_fields()

    _set_test_status("⏳ Testing connection…", "StatusInfo.TLabel")
    s_btn_test.state(["disabled"])
    root.after(100, run_test)


_apply_env_from_settings(_initial_settings or {})
_refresh_configuration_status()
s_btn_save.config(command=_on_settings_save)
s_btn_test.config(command=_on_settings_test)
cfg_custom_endpoint.trace_add("write", lambda *_: _update_endpoint_field())
cfg_region.trace_add("write", lambda *_: _update_endpoint_field())
cfg_provider.trace_add("write", lambda *_: _on_provider_change())
cfg_endpoint.trace_add("write", lambda *_: _on_endpoint_change())
cfg_access_key.trace_add("write", lambda *_: _validate_fields())
cfg_secret_key.trace_add("write", lambda *_: _validate_fields())
cfg_path_style.trace_add("write", lambda *_: _validate_fields())
cfg_secure.trace_add("write", lambda *_: _validate_fields())
_on_provider_change()
_set_test_status("")
_validate_fields()

def _reveal_settings_file(event=None):
    path = S3_CONFIG_PATH
    target = path if path.exists() else path.parent
    try:
        if sys.platform.startswith("darwin"):
            if path.exists():
                subprocess.run(["open", "-R", str(path)], check=False)
            else:
                subprocess.run(["open", str(target)], check=False)
        elif os.name == "nt":
            if path.exists():
                subprocess.run(["explorer", f"/select,{str(path)}"], check=False)
            else:
                subprocess.run(["explorer", str(target)], check=False)
        else:
            subprocess.run(["xdg-open", str(target)], check=False)
    except Exception:
        messagebox.showinfo("Settings Location", f"Settings file path:\n{path}")

s_info_path.bind("<Button-1>", _reveal_settings_file)
s_info_path.bind("<Return>", _reveal_settings_file)

# Ensure long status messages wrap instead of stretching the window
def _update_progress_wrap():
    try:
        wrap = max(320, root.winfo_width() - 360)
    except Exception:
        wrap = 320
    for lbl in (up_status, dl_status, up_meta_label, dl_meta_label, ls_summary,
                u_callout_text, u_form_hint, u_status_hint,
                d_callout_text, d_form_hint, d_status_hint,
                l_callout_text, l_form_hint, l_results_hint,
                up_metric_file, up_metric_dest, up_metric_meta,
                dl_metric_object, dl_metric_dest, dl_metric_meta,
                ls_metric_count, ls_metric_size, ls_metric_prefix,
                do_callout_text, do_form_hint, do_status_hint,
                db_callout_text, db_form_hint, db_status_hint,
                s_callout_text, s_form_hint, s_region_hint, s_endpoint_hint,
                s_access_hint, s_secret_hint,
                s_info_hint, s_info_message, s_info_path):
        try:
            parent_width = 0
            try:
                parent = lbl.master
                if parent is not None:
                    parent_width = max(parent.winfo_width(), parent.winfo_reqwidth())
            except Exception:
                parent_width = 0
            effective = parent_width if parent_width and parent_width > 0 else wrap
            if effective > 160:
                candidate = effective - 40
            else:
                candidate = effective
            lbl.configure(wraplength=max(100, min(wrap, candidate)))
        except Exception:
            pass

_update_progress_wrap()

# Switch between compact and wide based on window width
def on_resize(event):
    try:
        w = root.winfo_width()
        compact = (w < 820)  # breakpoint
        if _layout_state["compact"] != compact:
            _layout_state["compact"] = compact
            layout_upload(compact)
            layout_download(compact)
            layout_list(compact)
            layout_delete_object(compact)
            layout_delete_bucket(compact)
        settings_compact = (w < 960)
        if _layout_state["settings_compact"] != settings_compact:
            layout_settings_form(settings_compact)
        _update_progress_wrap()
    except Exception:
        pass

root.bind("<Configure>", on_resize)
root.after(0, _update_progress_wrap)

# Ensure persisted credentials exist before performing S3 actions.
def _require_saved_credentials(action_text: str) -> bool:
    settings = load_s3_settings()
    provider = settings.get("PROVIDER", cfg_provider.get() or PROVIDER_AWS)
    if provider not in (PROVIDER_AWS, PROVIDER_MINIO):
        provider = PROVIDER_AWS
    required = [
        ("Access Key ID", "AWS_ACCESS_KEY_ID"),
        ("Secret Access Key", "AWS_SECRET_ACCESS_KEY"),
    ]
    if provider == PROVIDER_AWS:
        required.insert(0, ("AWS Region", "AWS_REGION"))
    else:
        required.append(("Endpoint", "AWS_S3_ENDPOINT"))

    missing = [label for label, key in required if not str(settings.get(key, "")).strip()]
    if missing:
        provider_name = _provider_display_name(provider)
        missing_text = ", ".join(missing)
        message = (
            f"Cannot {action_text} because {provider_name} connection settings are incomplete or not saved.\n\n"
            "Open the Settings tab, provide the required values, and click 'Save Settings'.\n\n"
            f"Missing: {missing_text}"
        )
        messagebox.showwarning("Connection Settings Required", message)
        statusbar.config(text=f"Missing connection settings: {missing_text}")
        try:
            notebook.select(settings_tab)
        except Exception:
            pass
        return False
    return True

# ---------------- Logic (same as before, but no extra_headers) ----------------
def _maybe_autofill_upload_key_from_path(path):
    path = (path or "").strip()
    state = _upload_key_state
    if path == state.get("last_path"):
        return
    prev_auto = state.get("auto_value", "")
    base = os.path.basename(path) if path else ""
    state["last_path"] = path
    if not base:
        state["auto_value"] = ""
        return
    state["auto_value"] = base
    current = up_key.get().strip()
    if (not state["manual"]) or (not current) or (current == prev_auto):
        state["suspend"] = True
        up_key.set(base)
        state["suspend"] = False
        state["manual"] = False


def _maybe_enable_upload(*_):
    ok = bool(up_bucket.get().strip() and up_file.get().strip())
    up_btn_start.config(state=("normal" if ok else "disabled"))


def _update_upload_summary(*_):
    path = up_file.get().strip()
    bucket = up_bucket.get().strip()
    key = up_key.get().strip()

    if path and os.path.isfile(path):
        size = os.path.getsize(path)
        base = os.path.basename(path) or path
        mtime = time.strftime("%Y-%m-%d %H:%M", time.localtime(os.path.getmtime(path)))
        up_metric_file.config(text=f"📄 File: {human_size(size)} • {_truncate_middle(base, 40)}")
        up_metric_meta.config(text=f"🕒 Modified: {mtime}")
    elif path:
        base = os.path.basename(path) or path
        up_metric_file.config(text=f"📄 File: {_truncate_middle(base, 40)}")
        up_metric_meta.config(text="🕒 Modified: —")
    else:
        up_metric_file.config(text="📄 File: —")
        up_metric_meta.config(text="🕒 Modified: —")

    if bucket:
        if key:
            dest_key = key
        elif path and os.path.basename(path):
            dest_key = os.path.basename(path)
        else:
            dest_key = "(set object key)"
        dest_display = f"{bucket}/{dest_key}"
        if up_create.get():
            dest_display += " (auto-create)"
        up_metric_dest.config(text=f"🎯 Destination: {_truncate_middle(dest_display, 60)}")
    else:
        up_metric_dest.config(text="🎯 Destination: —")

    if str(up_btn_cancel.cget("state")).lower() == "disabled":
        if path and os.path.isfile(path):
            size = os.path.getsize(path)
            note = f"🚀 Ready • {human_size(size)} file"
            if bucket:
                note += f" to {bucket}"
            else:
                note += " (set bucket)"
        else:
            note = "No upload in progress."
        up_meta_label.config(text=note)

def _on_upload_field_change(*_):
    _maybe_autofill_upload_key_from_path(up_file.get())
    _maybe_enable_upload()
    _update_upload_summary()


def _on_upload_key_var_change(*_):
    state = _upload_key_state
    if not state.get("suspend"):
        current = up_key.get().strip()
        auto_val = state.get("auto_value", "")
        state["manual"] = bool(current and current != auto_val)
    else:
        current = up_key.get().strip()
        auto_val = state.get("auto_value", "")
        if not current or current == auto_val:
            state["manual"] = False
    _on_upload_field_change()

up_bucket.trace_add("write", _on_upload_field_change)
up_file.trace_add("write", _on_upload_field_change)
up_key.trace_add("write", _on_upload_key_var_change)
up_create.trace_add("write", _on_upload_field_change)
_on_upload_field_change()

def _update_download_summary(*_):
    bucket = dl_bucket.get().strip()
    key = dl_key.get().strip()
    dest = dl_out.get().strip()

    if key:
        if bucket:
            obj_text = f"{bucket}/{key}"
        else:
            obj_text = key
    else:
        obj_text = "(set object key)"
    dl_metric_object.config(text=f"🗂️ Object: {_truncate_middle(obj_text, 60)}")

    if dest:
        expanded = os.path.expanduser(dest)
        dl_metric_dest.config(text=f"💾 Save to: {_truncate_middle(expanded, 60)}")
    else:
        dl_metric_dest.config(text="💾 Save to: —")

    if str(dl_btn_cancel.cget("state")).lower() == "disabled":
        parts = ["Ready"]
        if not bucket:
            parts.append("set bucket")
        if not key:
            parts.append("set key")
        if not dest:
            parts.append("set destination")
        dl_metric_meta.config(text="📶 Status: " + " • ".join(parts))

dl_bucket.trace_add("write", _update_download_summary)
dl_key.trace_add("write", _update_download_summary)
dl_out.trace_add("write", _update_download_summary)
_update_download_summary()


def upload_start():
    bucket = up_bucket.get().lower().strip()
    key = (up_key.get().strip() or os.path.basename(up_file.get()))
    path = up_file.get().strip()

    if not is_valid_bucket_name(bucket):
        messagebox.showerror("Invalid bucket name",
                             "Bucket names must be 3–63 chars, lowercase letters/numbers/hyphens only,\n"
                             "and cannot look like an IP address.")
        return
    if not os.path.isfile(path):
        messagebox.showerror("File not found", path)
        return

    if not _require_saved_credentials("start an upload"):
        return

    cancel_event.clear()
    up_btn_start.config(state="disabled")
    up_btn_cancel.config(state="normal")
    _update_textbox(up_status_text, "Starting upload…")
    _reset_progress_metrics(up_status, reset_footer=True)
    statusbar.config(text=f"Uploading {os.path.basename(path)}…")

    try:
        total = os.stat(path).st_size
    except Exception as e:
        messagebox.showerror("Error", str(e))
        up_btn_start.config(state="normal")
        up_btn_cancel.config(state="disabled")
        return

    up_progress["value"] = 0
    up_progress["maximum"] = total
    source_name = os.path.basename(path) or os.path.basename(key) or path
    display_name = f"{source_name} → {bucket}/{key}"
    context = {"display": display_name, "start": time.time()}
    _update_transfer_meta(up_meta_label, "Upload", context["display"], 0, total, 0.0, 0.0, note="Preparing…")

    def worker():
        context["start"] = time.time()
        result_note = "Completed"
        try:
            client = get_client()
        except Exception as e:
            result_note = f"Client error: {e}"
            elapsed_fail = max(time.time() - context["start"], 1e-3)
            note_text = _truncate_middle(result_note, 64)
            root.after(0, lambda elapsed=elapsed_fail, note=note_text:
                       _update_transfer_meta(up_meta_label, "Upload", context["display"], 0, total, 0.0, elapsed, note))
            return root.after(0, lambda e=e: (
                _update_textbox(up_status_text, f"Client error: {e}"),
                _rearm(up_btn_start, up_btn_cancel)
            ))

        # Optionally create bucket
        if up_create.get():
            try:
                root.after(0, lambda: _update_textbox(up_status_text, "Checking or creating bucket…"))
                if not client.bucket_exists(bucket):
                    region = os.environ.get("AWS_REGION")
                    client.make_bucket(bucket, location=(region if region != "us-east-1" else None))
            except Exception as e:
                result_note = f"Bucket error: {e}"
                elapsed_fail = max(time.time() - context["start"], 1e-3)
                note_text = _truncate_middle(result_note, 64)
                root.after(0, lambda elapsed=elapsed_fail, note=note_text:
                           _update_transfer_meta(up_meta_label, "Upload", context["display"], 0, total, 0.0, elapsed, note))
                return root.after(0, lambda e=e: (
                    _update_textbox(up_status_text, f"Bucket error: {e}"),
                    _rearm(up_btn_start, up_btn_cancel)
                ))
        seen = 0
        t0 = context["start"]
        last_time = t0

        def push_update(transferred, avg_speed, elapsed_total):
            _update_transfer_meta(
                up_meta_label,
                "Upload",
                context["display"],
                transferred,
                total,
                avg_speed,
                elapsed_total,
                note=None,
            )
            _update_bar(up_progress, up_status_text, total, transferred)

        class ProgressFile:
            def __init__(self, p):
                self.f = open(p, "rb")
                self.cancelled = False
            def read(self, n):
                nonlocal seen
                nonlocal last_time
                if cancel_event.is_set():
                    self.cancelled = True
                    raise UploadCancelled("Upload cancelled by user")
                requested = 512 * 1024 if n is None else min(max(n, 1), 512 * 1024)
                chunk = self.f.read(requested)
                if chunk:
                    seen += len(chunk)
                    now = time.time()
                    dt = max(now - last_time, 1e-3)
                    last_time = now
                    spd = len(chunk) / dt
                    elapsed_total = max(now - t0, 1e-3)
                    avg_speed = seen / elapsed_total if elapsed_total > 0 else 0.0
                    root.after(0, lambda s=seen, avg=avg_speed, elapsed=elapsed_total:
                               push_update(s, avg, elapsed))
                return chunk
            def __getattr__(self, n): return getattr(self.f, n)
            def close(self): self.f.close()

        fp = None
        try:
            fp = ProgressFile(path)
            root.after(0, lambda: _update_textbox(up_status_text, "Uploading…"))
            client.put_object(
                bucket_name=bucket,
                object_name=key,
                data=fp,
                length=total,
                part_size=8 * 1024 * 1024,
            )
            if cancel_event.is_set() or getattr(fp, "cancelled", False):
                result_note = "Cancelled"
                root.after(0, lambda: _update_textbox(up_status_text, "⚠️ Upload cancelled"))
            else:
                root.after(0, lambda: _update_textbox(up_status_text, f"✅ Upload complete: {key}"))
        except UploadCancelled:
            result_note = "Cancelled"
            root.after(0, lambda: _update_textbox(up_status_text, "⚠️ Upload cancelled"))
        except S3Error as e:
            result_note = f"S3 error: {e}"
            root.after(0, lambda e=e: _update_textbox(up_status_text, f"S3 error: {e}"))
        except Exception as e:
            lowered = str(e).lower()
            if cancel_event.is_set() or "not enough data" in lowered:
                result_note = "Cancelled"
                root.after(0, lambda: _update_textbox(up_status_text, "⚠️ Upload cancelled"))
            else:
                result_note = f"Unexpected error: {e}"
                root.after(0, lambda e=e: _update_textbox(up_status_text, f"Unexpected error: {e}"))
        finally:
            try:
                if fp is not None:
                    fp.close()
            except Exception:
                pass
            elapsed_final = max(time.time() - context["start"], 1e-3)
            avg_final = (seen / elapsed_final) if elapsed_final > 0 else 0.0
            note_text = _truncate_middle(result_note, 64) if result_note else None
            root.after(0, lambda s=seen, avg=avg_final, elapsed=elapsed_final, note=note_text:
                       _update_transfer_meta(up_meta_label, "Upload", context["display"], s, total, avg, elapsed, note))
            root.after(0, lambda: _rearm(up_btn_start, up_btn_cancel))

    threading.Thread(target=worker, daemon=True).start()

def upload_cancel():
    cancel_event.set()
    _reset_progress_metrics(up_status)
    up_status.config(text="Cancelling…")
    statusbar.config(text="Cancelling…")
    up_meta_label.config(text="Cancelling upload…")

up_btn_start.config(command=upload_start)
up_btn_cancel.config(command=upload_cancel)

def download_start():
    bucket = dl_bucket.get().lower().strip()
    key = dl_key.get().strip()
    outp = dl_out.get().strip()

    if not is_valid_bucket_name(bucket):
        messagebox.showerror("Invalid bucket name", "Please enter a valid bucket name.")
        return
    if not key:
        messagebox.showerror("Missing key", "Please enter an object key.")
        return
    if not outp:
        messagebox.showerror("Missing path", "Please choose a destination folder or file path.")
        return

    if not _require_saved_credentials("start a download"):
        return

    cancel_event.clear()
    dl_btn_start.config(state="disabled")
    dl_btn_cancel.config(state="normal")
    _update_textbox(dl_status_text, "Starting download…")
    _reset_progress_metrics(dl_status, reset_footer=True)
    statusbar.config(text=f"Downloading {key}…")
    _update_download_summary()
    dl_metric_meta.config(text="📶 Status: Starting…")
    context = {"display": f"{bucket}/{key}", "start": time.time(), "total": None}
    _update_transfer_meta(dl_meta_label, "Download", context["display"], 0, 0, 0.0, 0.0, note="Preparing…")

    def worker():
        context["start"] = time.time()
        result_note = "Completed"
        try:
            client = get_client()
        except Exception as e:
            result_note = f"Client error: {e}"
            elapsed_fail = max(time.time() - context["start"], 1e-3)
            note_text = _truncate_middle(result_note, 64)
            root.after(0, lambda elapsed=elapsed_fail, note=note_text:
                       _update_transfer_meta(dl_meta_label, "Download", context["display"], 0, context.get("total"), 0.0, elapsed, note))
            return root.after(0, lambda e=e: (
                _update_textbox(dl_status_text, f"Client error: {e}"),
                _rearm(dl_btn_start, dl_btn_cancel)
            ))

        try:
            stat = client.stat_object(bucket, key)
            total = getattr(stat, "size", None)
        except Exception:
            total = None
        context["total"] = total

        from pathlib import Path
        p = Path(outp).expanduser()
        if p.exists() and p.is_dir():
            out_file = str(p / os.path.basename(key))
        else:
            if not p.exists() and not p.suffix:
                p.mkdir(parents=True, exist_ok=True)
                out_file = str(p / os.path.basename(key))
            else:
                p.parent.mkdir(parents=True, exist_ok=True)
                out_file = str(p)

        context["display"] = f"{bucket}/{key} → {out_file}"
        root.after(0, lambda: _update_transfer_meta(
            dl_meta_label,
            "Download",
            context["display"],
            0,
            context.get("total") or 0,
            0.0,
            0.0,
            note="Preparing…",
        ))

        if total:
            dl_progress["maximum"] = total
        else:
            dl_progress["mode"] = "indeterminate"
            root.after(0, dl_progress.start)

        seen = 0
        last_time = context["start"]

        def push_update(transferred, avg_speed, elapsed_total):
            _update_transfer_meta(
                dl_meta_label,
                "Download",
                context["display"],
                transferred,
                context.get("total"),
                avg_speed,
                elapsed_total,
                note=None,
            )
            _update_bar(dl_progress, dl_status_text, context.get("total"), transferred)

        try:
            resp = client.get_object(bucket, key)
            with open(out_file, "wb") as f:
                while True:
                    if cancel_event.is_set():
                        break
                    chunk = resp.read(32 * 1024)
                    if not chunk:
                        break
                    f.write(chunk)
                    seen += len(chunk)
                    now = time.time()
                    dt_chunk = max(now - last_time, 1e-3)
                    last_time = now
                    elapsed_total = max(now - context["start"], 1e-3)
                    avg_speed = seen / elapsed_total if elapsed_total > 0 else 0.0
                    root.after(0, lambda s=seen, avg=avg_speed, elapsed=elapsed_total:
                               push_update(s, avg, elapsed))
            resp.close(); resp.release_conn()
            if cancel_event.is_set():
                result_note = "Cancelled"
                root.after(0, lambda: _update_textbox(dl_status_text, "⚠️ Download cancelled"))
            else:
                root.after(0, lambda: _update_textbox(dl_status_text, f"✅ Downloaded to: {out_file}"))
        except S3Error as e:
            result_note = f"S3 error: {e}"
            root.after(0, lambda e=e: _update_textbox(dl_status_text, f"S3 error: {e}"))
        except Exception as e:
            if cancel_event.is_set():
                result_note = "Cancelled"
                root.after(0, lambda: _update_textbox(dl_status_text, "⚠️ Download cancelled"))
            else:
                result_note = f"Unexpected error: {e}"
                root.after(0, lambda e=e: _update_textbox(dl_status_text, f"Unexpected error: {e}"))
        finally:
            root.after(0, lambda: _rearm(dl_btn_start, dl_btn_cancel))
            if not total:
                root.after(0, dl_progress.stop)
                dl_progress["mode"] = "determinate"
                dl_progress["value"] = 0
            elapsed_final = max(time.time() - context["start"], 1e-3)
            avg_final = (seen / elapsed_final) if elapsed_final > 0 else 0.0
            note_text = _truncate_middle(result_note, 64) if result_note else None
            root.after(0, lambda s=seen, avg=avg_final, elapsed=elapsed_final, note=note_text:
                       _update_transfer_meta(dl_meta_label, "Download", context["display"], s, context.get("total"), avg, elapsed, note))

    threading.Thread(target=worker, daemon=True).start()

dl_btn_start.config(command=download_start)
def download_cancel():
    cancel_event.set()
    _reset_progress_metrics(dl_status)
    dl_status.config(text="Cancelling…")
    statusbar.config(text="Cancelling…")
    dl_metric_meta.config(text="📶 Status: Cancelling…")
    _update_download_summary()

dl_btn_cancel.config(command=download_cancel)

def do_list():
    bucket = ls_bucket.get().lower().strip()
    prefix = ls_prefix.get().strip() or None
    if not is_valid_bucket_name(bucket):
        messagebox.showerror("Invalid bucket", "Please enter a valid bucket name."); return
    if not _require_saved_credentials("list objects"):
        ls_summary.config(text="Cannot list objects without saved connection settings.")
        statusbar.config(text="Missing connection settings.")
        return
    ls_btn.config(state="disabled")
    for item in ls_tree.get_children():
        ls_tree.delete(item)
    statusbar.config(text="Listing objects…")
    ls_summary.config(text="Listing objects…")
    ls_metric_count.config(text="🧾 Objects: —")
    ls_metric_size.config(text="📦 Total size: —")
    ls_metric_prefix.config(text=f"🔍 Prefix: {prefix or '(none)'}")
    def worker():
        try: client = get_client()
        except Exception as e:
            return root.after(0, lambda e=e: (
                ls_tree.insert("", "end", values=("!", f"Client error: {e}"), tags=("error",)),
                ls_btn.config(state="normal"),
                statusbar.config(text="Ready"),
                ls_summary.config(text=f"Client error: {e}"),
            ))
        try:
            if not client.bucket_exists(bucket):
                return root.after(0, lambda: (
                    ls_tree.insert("", "end", values=("—", "Bucket does not exist."), tags=("error",)),
                    ls_btn.config(state="normal"),
                    statusbar.config(text="Ready"),
                    ls_summary.config(text="Bucket does not exist."),
                ))
            try:
                iterator = client.list_objects(
                    bucket,
                    prefix=prefix,
                    recursive=ls_recursive.get(),
                    use_api="S3v2",
                )
            except TypeError:
                iterator = client.list_objects(
                    bucket,
                    prefix=prefix,
                    recursive=ls_recursive.get(),
                )

            count = 0
            total_bytes = 0
            chunk = []

            def emit_chunk(rows):
                def inner():
                    for size_txt, name_txt in rows:
                        ls_tree.insert("", "end", values=(size_txt, name_txt))
                root.after(0, inner)

            for obj in iterator:
                name = getattr(obj, "object_name", None) or getattr(obj, "key", "")
                size = getattr(obj, "size", 0) or 0
                try:
                    numeric_size = int(size)
                except (TypeError, ValueError):
                    numeric_size = 0
                chunk.append((human_size(numeric_size), name))
                count += 1
                total_bytes += numeric_size
                if len(chunk) >= 200:
                    emit_chunk(chunk[:])
                    chunk.clear()

            if chunk:
                emit_chunk(chunk[:])

            if count == 0:
                summary = "No objects found."
                if prefix:
                    summary += f" (prefix '{prefix}')"
                root.after(0, lambda summary=summary: (
                    ls_tree.insert("", "end", values=("—", summary), tags=("muted",)),
                    statusbar.config(text=summary),
                    ls_summary.config(text=summary),
                    ls_metric_count.config(text="🧾 Objects: 0"),
                    ls_metric_size.config(text="📦 Total size: 0 B"),
                ))
            else:
                summary = f"Listed {count} object{'s' if count != 1 else ''}"
                if prefix:
                    summary += f" under '{prefix}'"
                summary += f" • {human_size(total_bytes)} total"
                root.after(0, lambda summary=summary: (
                    statusbar.config(text=summary),
                    ls_summary.config(text=summary),
                    ls_metric_count.config(text=f"🧾 Objects: {count}"),
                    ls_metric_size.config(text=f"📦 Total size: {human_size(total_bytes)}"),
                ))
        except S3Error as e:
            root.after(0, lambda e=e: (
                ls_tree.insert("", "end", values=("!", f"S3 error: {e}"), tags=("error",)),
                statusbar.config(text="Error"),
                ls_summary.config(text=f"S3 error: {e}"),
            ))
        except Exception as e:
            root.after(0, lambda e=e: (
                ls_tree.insert("", "end", values=("!", f"Error: {e}"), tags=("error",)),
                statusbar.config(text="Error"),
                ls_summary.config(text=f"Error: {e}"),
            ))
        finally:
            root.after(0, lambda: ls_btn.config(state="normal"))
    threading.Thread(target=worker, daemon=True).start()

ls_btn.config(command=do_list)

def do_delete_object():
    bucket = do_bucket.get().lower().strip()
    key = do_key.get().strip()

    if not is_valid_bucket_name(bucket):
        messagebox.showerror("Invalid bucket", "Please enter a valid bucket name.")
        return
    if not key:
        messagebox.showerror("Missing key", "Enter an object key.")
        return

    if not _require_saved_credentials("delete an object"):
        return

    do_btn.config(state="disabled")
    _update_textbox(do_status_text, "Deleting…")


    def worker():
        try:
            client = get_client()
        except Exception as e:
            return root.after(0, lambda e=e: (
                _update_textbox(do_status_text, f"Client error: {e}"),
                do_btn.config(state="normal")
            ))

        try:
            client.remove_object(bucket, key)
            root.after(0, lambda: _update_textbox(do_status_text, f"✅ Deleted object: {key}"))
        except S3Error as e:
            root.after(0, lambda e=e: (
                _update_textbox(do_status_text, f"S3 error: {e}"),
            ))
        except Exception as e:
            root.after(0, lambda e=e: _update_textbox(do_status_text, f"Unexpected error: {e}"))
        finally:
            root.after(0, lambda: do_btn.config(state="normal"))

    threading.Thread(target=worker, daemon=True).start()

do_btn.config(command=do_delete_object)

def do_delete_bucket():
    bucket = db_bucket.get().lower().strip()

    # Validate input
    if not is_valid_bucket_name(bucket):
        messagebox.showerror(
            "Invalid bucket name",
            "Bucket names must be 3–63 chars, lowercase letters/numbers/hyphens only."
        )
        return

    if not _require_saved_credentials("delete a bucket"):
        return

    if not messagebox.askyesno("Confirm Delete", f"Delete bucket '{bucket}'?\n\n⚠️ This action cannot be undone."):
        return

    db_btn.config(state="disabled")
    _update_textbox(db_status_text, "Deleting bucket…")


    def worker():
        try:
            client = get_client()
        except Exception as e:
            return root.after(
                0,
                lambda e=e: (
                    _update_textbox(db_status_text, f"Client error: {e}"),
                    db_btn.config(state="normal")
                )
            )

        try:
            # Force emptying if enabled
            if db_force.get():
                removed, errors = 0, 0
                root.after(0, lambda: _update_textbox(db_status_text, "Emptying bucket before deletion…"))

                try:
                    iterator = client.list_objects(
                        bucket,
                        prefix=None,
                        recursive=True,
                        include_version=db_include_versions.get(),
                    )
                except TypeError:
                    # Some clients (MinIO older versions) don't accept include_version
                    iterator = client.list_objects(bucket, prefix=None, recursive=True)

                for obj in iterator:
                    if cancel_event.is_set():
                        break
                    vid = getattr(obj, "version_id", None) if db_include_versions.get() else None
                    try:
                        client.remove_object(bucket, obj.object_name, version_id=vid)
                        removed += 1
                        if removed % 25 == 0:  # occasional UI feedback
                            root.after(0, lambda r=removed: _update_textbox(
                                db_status_text, f"🧹 Removed {r} objects so far…"))
                    except S3Error as e:
                        errors += 1
                        root.after(0, lambda e=e: _update_textbox(
                            db_status_text, f"⚠️ Error deleting object: {e}"))

                msg = f"Emptied {removed} objects"
                if errors:
                    msg += f" with {errors} error(s)"
                root.after(0, lambda msg=msg: _update_textbox(db_status_text, msg))

            # Proceed with actual deletion
            root.after(0, lambda: _update_textbox(db_status_text, "Removing bucket…"))
            client.remove_bucket(bucket)
            root.after(0, lambda: _update_textbox(db_status_text, f"✅ Bucket '{bucket}' deleted successfully."))

        except S3Error as e:
            root.after(0, lambda e=e: _update_textbox(db_status_text, f"S3 error: {e}"))

        except Exception as e:
            root.after(0, lambda e=e: _update_textbox(db_status_text, f"Unexpected error: {e}"))

        finally:
            root.after(0, lambda: db_btn.config(state="normal"))

    threading.Thread(target=worker, daemon=True).start()

db_btn.config(command=do_delete_bucket)


# ---------------- shared UI helpers ----------------
def _update_bar(bar, status_label, total, seen, _inst_unused=None, _avg_unused=None):
    """Minimal, stable progress line:
       XX.X% | transferred / total | speed/s | ETA hh:mm:ss
    """
    # Progressbar value
    if total:
        if "maximum" not in bar.keys() or bar["maximum"] != total:
            bar["maximum"] = total
        bar["value"] = min(seen, total)
    else:
        bar["value"] = seen

    # --- Sliding window speed (~1.5–5s of data) ---
    now = time.time()
    hist = getattr(status_label, "_speed_hist", None)
    if hist is None:
        hist = deque()
        status_label._speed_hist = hist

    hist.append((now, int(seen)))
    # keep up to 5 seconds, but we’ll require >=1.5s for ETA
    while len(hist) > 1 and (now - hist[0][0]) > 5.0:
        hist.popleft()

    # window speed
    window_Bps = 0.0
    if len(hist) >= 2:
        t0, s0 = hist[0]
        t1, s1 = hist[-1]
        dt = max(t1 - t0, 1e-6)
        window_Bps = max((s1 - s0) / dt, 0.0)

    # early average (since transfer started)
    first_t, first_s = hist[0]
    elapsed = max(now - first_t, 1e-6)
    early_avg_Bps = max((seen - first_s) / elapsed, 0.0)

    have_window = (now - first_t) >= 1.5 and window_Bps > 1.0  # need at least 1.5s of samples

    if have_window and early_avg_Bps > 0:
        # Blend window and overall averages to avoid jitter or unrealistic spikes
        ratio = window_Bps / early_avg_Bps if early_avg_Bps else 1.0
        if ratio > 2.5:
            effective_Bps = 0.35 * window_Bps + 0.65 * early_avg_Bps
        elif ratio < 0.4:
            effective_Bps = 0.65 * window_Bps + 0.35 * early_avg_Bps
        else:
            effective_Bps = 0.5 * window_Bps + 0.5 * early_avg_Bps
    elif have_window:
        effective_Bps = window_Bps
    else:
        effective_Bps = early_avg_Bps

    eta_seconds = None
    line_text = ""
    if total:
        pct = 100.0 * (seen / max(total, 1))
        if effective_Bps > 1.0:
            eta_seconds = max(total - seen, 0) / effective_Bps
        eta_txt = human_eta(eta_seconds)
        line_text = (
            f"{pct:5.1f}%  |  {human_size(seen)} / {human_size(total)}"
            f"  |  {human_size(effective_Bps)}/s  |  ETA {eta_txt}"
        )
    else:
        line_text = f"{human_size(seen)} transferred  |  {human_size(effective_Bps)}/s"

    status_label.config(text=line_text)

    # Optional: keep the bottom statusbar short (not a duplicate wall of text)
    try:
        last = getattr(statusbar, "_last_upd", 0.0)
        if now - last > 0.5:
            if total:
                pct = 100.0 * (seen / max(total, 1))
                eta_txt = human_eta(eta_seconds)
                statusbar.config(text=f"{pct:0.1f}%  •  {human_size(effective_Bps)}/s  •  ETA {eta_txt}")
            else:
                statusbar.config(text=f"{human_size(seen)}  •  {human_size(effective_Bps)}/s")
            statusbar._last_upd = now
    except Exception:
        pass

def _finish_err(status_label, msg):
    status_label.config(text="❌ Error")
    try: messagebox.showerror("Operation failed", msg)
    except Exception: pass

def _rearm(start_btn, cancel_btn):
    start_btn.config(state="normal")
    cancel_btn.config(state="disabled")
    statusbar.config(text="Ready")
    try:
        _update_upload_summary()
    except Exception:
        pass
    try:
        _update_download_summary()
    except Exception:
        pass

def _update_textbox(widget, msg):
    """Safely update read-only Text boxes for wrapped multiline output."""
    if isinstance(widget, tk.Text):
        widget.config(state="normal")
        width = _estimate_char_width(widget)
        widget.delete("1.0", tk.END)
        widget.insert(tk.END, _wrap_lines(msg, width))
        widget.config(state="disabled")
        widget.see("end")
    else:
        widget.config(text=msg)
    _reset_progress_metrics(widget)

# ---------------- Shortcuts ----------------
root.bind("<Command-o>", lambda e: pick_upload_file())

# ---------------- Run ----------------
if __name__ == "__main__":
    root.mainloop()
