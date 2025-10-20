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
from tkinter import filedialog, messagebox, simpledialog, ttk
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
from auth_store import (
    verify_user as auth_verify_user,
    create_user as auth_create_user,
    user_count as auth_user_count,
    list_usernames as auth_list_usernames,
    get_user as auth_get_user,
)
import secrets
import datetime

_initial_settings = load_s3_settings()
THEME_PALETTE = {}
INPUT_MAX_WIDTH = 200
HEADER_INFO_LABEL = None
HEADER_TOPBAR = None  # reference to the gradient topbar (created after login)
CURRENT_USER = {"id": None, "username": None}
_UP_KEY_TOUCHED = False
_UP_KEY_SET_PROGRAMMATICALLY = False
_upload_busy = False

def _refresh_upload_button(reschedule=True):
    """Enable Start Upload when bucket, file, and key exist and no upload is active."""
    try:
        ok = True
        for varname in ("up_bucket", "up_file", "up_key"):
            v = globals().get(varname)
            if v is None or not str(v.get()).strip():
                ok = False
                break
        if globals().get("_upload_busy", False):
            ok = False
        btn = globals().get("up_btn_start")
        if btn is not None:
            btn.config(state=("normal" if ok else "disabled"))
    except Exception:
        pass
    finally:
        if reschedule:
            try:
                r = globals().get("root")
                if r is not None:
                    r.after(300, _refresh_upload_button)
            except Exception:
                pass

def _mark_up_key_touched(*_):
    global _UP_KEY_TOUCHED, _UP_KEY_SET_PROGRAMMATICALLY
    if _UP_KEY_SET_PROGRAMMATICALLY:
        return
    _UP_KEY_TOUCHED = True

def _hex_to_rgb_tuple(value):
    v = value.lstrip("#")
    if len(v) == 3:
        v = "".join(ch * 2 for ch in v)
    return tuple(int(v[i:i+2], 16) for i in (0, 2, 4))


def _blend(c1, c2, t):
    r1, g1, b1 = _hex_to_rgb_tuple(c1)
    r2, g2, b2 = _hex_to_rgb_tuple(c2)
    r = int(r1 + (r2 - r1) * float(t))
    g = int(g1 + (g2 - g1) * float(t))
    b = int(b1 + (b2 - b1) * float(t))
    return f"#{r:02x}{g:02x}{b:02x}"


class RoundedField(tk.Frame):
    """Rounded input wrapper with a canvas border and optional trailing widget.
    Set always_glow=True to keep the blue ring on at all times.
    """

    def __init__(self, master, textvariable=None, show=None, height=44, radius=10, padding=12, always_glow=False, max_width=INPUT_MAX_WIDTH, **kw):
        bg = THEME_PALETTE.get("SURFACE", "#1b1d22")
        super().__init__(master, bg=bg, highlightthickness=0, bd=0, **kw)
        self.radius = int(radius)
        self.pad = int(padding)
        self.height = int(height)
        self.border = 2
        self.focused = False
        self.always_glow = bool(always_glow)
        self.canvas = tk.Canvas(self, height=self.height, highlightthickness=0, bd=0, bg=bg)
        self.canvas.grid(row=0, column=0, sticky="nsew")
        self.columnconfigure(0, weight=1)
        self.rowconfigure(0, weight=1)

        self.overlay = tk.Frame(self, bg=bg, highlightthickness=0, bd=0)
        self.overlay.place(x=self.pad, y=self.pad, width=10, height=self.height - self.pad * 2)
        self.overlay.grid_propagate(False)
        self.overlay.columnconfigure(0, weight=1)

        # Use a plain tk.Entry so we can force exact colors; this avoids cases
        # where ttk theme overrides make text invisible on some mac builds.
        self.entry = tk.Entry(
            self.overlay,
            textvariable=textvariable,
            show=show if show else "",
            relief="flat",
            highlightthickness=0,
            highlightbackground=THEME_PALETTE.get("SURFACE", "#1b1d22"),
            highlightcolor=THEME_PALETTE.get("SURFACE", "#1b1d22"),
            bd=0,
            bg=THEME_PALETTE.get("SURFACE", "#1b1d22"),
            # Force a high‑contrast foreground to avoid theme overrides on macOS
            fg="#e6ebf3",
            insertbackground="#e6ebf3",
            selectbackground=_blend(THEME_PALETTE.get("ACCENT", "#4c8df6"), "#ffffff", 0.15),
            selectforeground="#ffffff",
            disabledbackground=THEME_PALETTE.get("SURFACE", "#1b1d22"),
            disabledforeground=_blend(THEME_PALETTE.get("TEXT", "#e9ecf1"), THEME_PALETTE.get("BG", "#141518"), 0.5),
            font=("SF Pro Text", 14)
        )
        # --- Ensure enough vertical room for descenders (g, y, p, q, j) ---
        try:
            fnt = tkfont.Font(font=self.entry.cget("font"))
            ascent = int(fnt.metrics("ascent") or 0)
            descent = int(fnt.metrics("descent") or 0)
            linespace = int(fnt.metrics("linespace") or (ascent + descent))
            # Desired inner content height: glyph box + a little breathing room
            desired_inner = ascent + descent + 4  # +4px guard band
            # Ensure outer widget height is large enough for border + padding
            min_outer = desired_inner + 2 * (self.pad + self.border)
            if self.height < min_outer:
                self.height = min_outer
            # Compute ipady so Entry's text box >= desired_inner
            extra = max(0, (desired_inner - linespace) // 2)
        except Exception:
            extra = 6  # fallback
        # Slightly larger internal padding keeps glyphs clear of the border on macOS
        self.entry.grid(row=0, column=0, sticky="nsew", ipady=extra, pady=(0,0))

        self.trailing_holder = tk.Frame(self.overlay, bg=bg, highlightthickness=0, bd=0)
        self.trailing_holder.grid(row=0, column=1, sticky="ns", padx=(10, 0))
        self.trailing_holder.configure(width=64)
        self.overlay.columnconfigure(1, weight=0, minsize=64)
        self.overlay.rowconfigure(0, weight=1)
        self.trailing_holder.grid_propagate(False)

        self.canvas.bind("<Configure>", self._redraw)
        self.entry.bind("<FocusIn>", lambda e: self._set_focus(True))
        self.entry.bind("<FocusOut>", lambda e: self._set_focus(False))
        self._redraw()

    def _set_focus(self, val):
        self.focused = bool(val)
        self._redraw()



    def _redraw(self, event=None):
        w = max(self.winfo_width(), 1)
        h = self.height
        self.canvas.config(height=h)
        # Nudge up by 1px and give +2px height to avoid clipping descenders
        self.overlay.place(x=self.pad + self.border,
                           y=self.pad + self.border - 1,
                           width=max(w - (self.pad + self.border) * 2, 10),
                           height=h - (self.pad + self.border) * 2 + 2)
        self.canvas.delete("all")

        # Background: match surface exactly (no shadow look)
        surface = THEME_PALETTE.get("SURFACE", "#1b1d22")
        r = self.radius
        self._round_rect(1, 1, w - 1, h - 1, r, fill=surface, outline=surface, width=1)

        # Edge shine: draw a split blue border (top/left lighter, bottom/right richer)
        if self.focused or self.always_glow:
            accent = THEME_PALETTE.get("ACCENT", "#4c8df6")
            light = _blend(accent, "#ffffff", 0.45)
            mid   = _blend(accent, "#ffffff", 0.25)
            dark  = accent
            # Outer heavy stroke gives the main glow
            self._split_gradient_border(1, 1, w-1, h-1, r, light, dark, width=2)
            # Inner fine stroke softens the transition
            self._split_gradient_border(2, 2, w-2, h-2, max(0, r-1), mid, accent, width=1)
        else:
            subtle = _blend(surface, THEME_PALETTE.get("BG", "#141518"), 0.28)
            self._split_gradient_border(1, 1, w-1, h-1, r, subtle, subtle, width=1)

    def _round_rect(self, x1, y1, x2, y2, r, **kwargs):
        r = max(0, min(r, int((x2 - x1) / 2), int((y2 - y1) / 2)))
        if r <= 0:
            return self.canvas.create_rectangle(x1, y1, x2, y2, **kwargs)
        # four arcs
        self.canvas.create_arc(x1, y1, x1 + 2 * r, y1 + 2 * r, start=90, extent=90, style="pieslice", **kwargs)
        self.canvas.create_arc(x2 - 2 * r, y1, x2, y1 + 2 * r, start=0, extent=90, style="pieslice", **kwargs)
        self.canvas.create_arc(x1, y2 - 2 * r, x1 + 2 * r, y2, start=180, extent=90, style="pieslice", **kwargs)
        self.canvas.create_arc(x2 - 2 * r, y2 - 2 * r, x2, y2, start=270, extent=90, style="pieslice", **kwargs)
        # center rectangles to connect arcs
        self.canvas.create_rectangle(x1 + r, y1, x2 - r, y2, **kwargs)
        self.canvas.create_rectangle(x1, y1 + r, x2, y2 - r, **kwargs)

    def _split_gradient_border(self, x1, y1, x2, y2, r, col_top_left, col_bottom_right, width=2):
        """Draw a rounded rectangle border with two tones: lighter on top/left,
        richer on bottom/right to approximate a gradient ring.
        """
        r = max(0, min(r, int((x2 - x1) / 2), int((y2 - y1) / 2)))
        # arcs as stroked arcs (style='arc')
        # top-left corner + top/left edges: light
        self.canvas.create_arc(x1, y1, x1 + 2*r, y1 + 2*r, start=90, extent=90, style='arc', outline=col_top_left, width=width)
        # top edge
        self.canvas.create_line(x1 + r, y1, x2 - r, y1, fill=col_top_left, width=width)
        # left edge
        self.canvas.create_line(x1, y1 + r, x1, y2 - r, fill=col_top_left, width=width)
        # bottom-right corner + bottom/right edges: dark
        self.canvas.create_arc(x2 - 2*r, y2 - 2*r, x2, y2, start=270, extent=90, style='arc', outline=col_bottom_right, width=width)
        # bottom edge
        self.canvas.create_line(x1 + r, y2, x2 - r, y2, fill=col_bottom_right, width=width)
        # right edge
        self.canvas.create_line(x2, y1 + r, x2, y2 - r, fill=col_bottom_right, width=width)
        # remaining two arcs to close the ring with appropriate tones
        self.canvas.create_arc(x2 - 2*r, y1, x2, y1 + 2*r, start=0, extent=90, style='arc', outline=col_top_left, width=width)
        self.canvas.create_arc(x1, y2 - 2*r, x1 + 2*r, y2, start=180, extent=90, style='arc', outline=col_bottom_right, width=width)

    def place_right(self, widget):
        holder = self.trailing_holder
        # Remove any existing children that are not the same widget
        for child in list(holder.winfo_children()):
            if child is widget:
                continue
            try:
                child.destroy()
            except Exception:
                pass
        try:
            # For ttk.Label trailing controls (e.g., "Show"), center vertically
            if str(widget.master) != str(holder):
                # Cannot reparent in Tk – create a visually identical clone
                try:
                    text = widget.cget("text")
                except Exception:
                    text = ""
                clone = ttk.Label(holder, text=text, style="LoginLink.TLabel", anchor="center", padding=(0,0,0,0))
                clone.pack(side="right", fill="both", expand=True, padx=(8,0))
                return clone
            # Reuse existing widget, ensure it fills the holder to stay centered
            try:
                widget.configure(anchor="center")
            except Exception:
                pass
            widget.pack_forget()
            widget.pack(side="right", fill="both", expand=True, padx=(8,0))
        except Exception:
            # Fallback clone if anything above fails
            try:
                text = widget.cget("text")
            except Exception:
                text = ""
            ttk.Label(holder, text=text, style="LoginLink.TLabel", anchor="center", padding=(0,0,0,0)).pack(side="right", fill="both", expand=True, padx=(8,0))


class StrengthMeter(tk.Canvas):
    def __init__(self, master, height=12, **kw):
        super().__init__(master, height=height, highlightthickness=0, bd=0, **kw)
        self.score = 0
        self.height = height
        self.bind("<Configure>", lambda e: self._redraw())

    def update_score(self, score):
        self.score = max(0, min(100, int(score)))
        self._redraw()

    def _redraw(self):
        self.delete("all")
        w = max(self.winfo_width(), 1)
        h = self.height
        bg = _blend(THEME_PALETTE.get("SURFACE", "#1b1d22"), THEME_PALETTE.get("BG", "#141518"), 0.2)
        # trough
        self.create_rectangle(0, h // 2 - 3, w, h // 2 + 3, fill=bg, outline=bg)
        # segments
        colors = ["#ff7a59", "#f39c12", "#f7d154", "#3ddc84"]
        segs = len(colors)
        seg_w = w / segs
        filled = w * (self.score / 100.0)
        x = 0
        for i, col in enumerate(colors):
            x2 = (i + 1) * seg_w
            draw_to = min(x2, filled)
            if draw_to > x:
                self.create_rectangle(x + 1, h // 2 - 4, draw_to - 1, h // 2 + 4, fill=col, outline=col)
            x = x2


class RoundedButton(tk.Canvas):
    def __init__(self, master, text, command=None, primary=True, radius=10, pad_x=18, pad_y=10):
        super().__init__(master, height=40, highlightthickness=0, bd=0, background=THEME_PALETTE.get("SURFACE", "#1b1d22"))
        self.text = text
        self.command = command
        self.radius = radius
        self.pad_x = pad_x
        self.pad_y = pad_y
        self.primary = primary
        self.state = "normal"
        self.bind("<Configure>", self._redraw)
        self.bind("<Button-1>", self._on_click)
        self.bind("<Enter>", lambda e: self._set_hover(True))
        self.bind("<Leave>", lambda e: self._set_hover(False))
        self._hover = False

    def configure(self, **kw):
        if "text" in kw:
            self.text = kw.pop("text")
        if "state" in kw:
            self.state = kw.pop("state")
        if kw:
            super().configure(**kw)
        self._redraw()

    def _colors(self):
        if self.primary:
            base = THEME_PALETTE.get("ACCENT", "#4c8df6")
            hover = _blend(base, "#ffffff", 0.12)
            text = "#ffffff"
            border = base
        else:
            base = _blend(THEME_PALETTE.get("SURFACE", "#1b1d22"), THEME_PALETTE.get("BG", "#141518"), 0.22)
            hover = _blend(base, THEME_PALETTE.get("ACCENT", "#4c8df6"), 0.18)
            text = THEME_PALETTE.get("TEXT", "#e9ecf1")
            border = _blend(base, THEME_PALETTE.get("BG", "#141518"), 0.35)
        if self._hover and self.state == "normal":
            base = hover
        if self.state == "disabled":
            base = _blend(base, THEME_PALETTE.get("BG", "#141518"), 0.35)
            text = _blend(text, THEME_PALETTE.get("BG", "#141518"), 0.35)
        return base, border, text

    def _round_rect(self, x1, y1, x2, y2, r, fill, outline, width):
        r = max(0, min(r, int((x2 - x1)/2), int((y2 - y1)/2)))
        self.create_arc(x1, y1, x1+2*r, y1+2*r, start=90, extent=90, style="pieslice", fill=fill, outline=outline, width=width)
        self.create_arc(x2-2*r, y1, x2, y1+2*r, start=0, extent=90, style="pieslice", fill=fill, outline=outline, width=width)
        self.create_arc(x1, y2-2*r, x1+2*r, y2, start=180, extent=90, style="pieslice", fill=fill, outline=outline, width=width)
        self.create_arc(x2-2*r, y2-2*r, x2, y2, start=270, extent=90, style="pieslice", fill=fill, outline=outline, width=width)
        self.create_rectangle(x1+r, y1, x2-r, y2, fill=fill, outline=outline, width=width)
        self.create_rectangle(x1, y1+r, x2, y2-r, fill=fill, outline=outline, width=width)

    def _redraw(self, event=None):
        self.delete("all")
        w = max(self.winfo_width(), 60)
        h = max(self.winfo_height(), 36)
        base, border, textcol = self._colors()
        self._round_rect(1, 1, w-1, h-1, self.radius, fill=base, outline=border, width=1)
        self.create_text(w//2, h//2, text=self.text, fill=textcol, font=("SF Pro Text", 12, "bold"))

    def _set_hover(self, val):
        self._hover = val
        self._redraw()

    def _on_click(self, event):
        if self.state == "disabled":
            return
        if callable(self.command):
            self.command()




class RoundedPill(tk.Canvas):
    def __init__(self, master, text="", radius=12, pad_x=12, pad_y=8, command=None):
        super().__init__(master, height=36, highlightthickness=0, bd=0, background=THEME_PALETTE.get("SURFACE", "#1b1d22"))
        self.text = text
        self.radius = radius
        self.pad_x = pad_x
        self.pad_y = pad_y
        self.command = command
        self.bind("<Configure>", self._redraw)
        self.bind("<Button-1>", lambda e: command() if callable(command) else None)
        self._hover = False
        self.bind("<Enter>", lambda e: self._set_hover(True))
        self.bind("<Leave>", lambda e: self._set_hover(False))

    def _redraw(self, event=None):
        self.delete("all")
        w = max(self.winfo_width(), 70)
        h = max(self.winfo_height(), 22)
        base = _blend(
            THEME_PALETTE.get("SURFACE", "#1b1d22"),
            THEME_PALETTE.get("ACCENT", "#4c8df6"),
            0.08 if THEME_PALETTE.get("dark", True) else 0.16,
        )
        if self._hover:
            base = _blend(base, THEME_PALETTE.get("ACCENT", "#4c8df6"), 0.18)
        # Draw only a simple rectangle (no rounded corners)
        self.create_rectangle(0, 0, w, h, fill=base, outline="", width=0)
        self.create_text(
            w // 2,
            h // 2,
            text=self.text,
            fill=THEME_PALETTE.get("TEXT", "#e9ecf1"),
            font=("SF Pro Text", 8, "bold"),
        )

    def _set_hover(self, v):
        self._hover = v
        self._redraw()


# ---- Auto-upgrade entries in non-login tabs to RoundedField -----------------

def _is_inside_login(widget):
    try:
        from tkinter import Misc
        w = widget
        # Walk up the parents until root
        while isinstance(w, tk.Misc) and w is not None:
            if isinstance(w, LoginFrame):
                return True
            w = w.master
    except Exception:
        pass
    return False


def _stringvar_from_entry(entry):
    """Fetch existing textvariable from an Entry (if any) or mirror its content into a new StringVar."""
    try:
        name = str(entry.cget("textvariable") or "")
        if name:
            # Bind to the same underlying Tcl variable name
            return tk.StringVar(master=entry, name=name)
    except Exception:
        pass
    # Fall back to a new variable seeded with current text
    try:
        return tk.StringVar(value=entry.get())
    except Exception:
        return tk.StringVar()


def _replace_entry_with_rounded(entry):
    """Replace a tk/ttk Entry with a RoundedField in the same grid cell.
    Skips anything inside the login/register frame.
    """
    try:
        if _is_inside_login(entry):
            return
        if getattr(entry, "_rounded_applied", False):
            return
        info = entry.grid_info()
        if not info:
            # Only support grid-managed widgets for now
            return
        parent = entry.master
        var = _stringvar_from_entry(entry)
        show = ""
        try:
            show = entry.cget("show") or ""
        except Exception:
            show = ""
        # Create the rounded field with persistent glow
        rf = RoundedField(parent, textvariable=var, show=(show or None), height=44, always_glow=True, max_width=INPUT_MAX_WIDTH)
        # Match the original grid placement
        rf.grid(row=int(info.get("row", 0)), column=int(info.get("column", 0)),
                rowspan=int(info.get("rowspan", 1)), columnspan=int(info.get("columnspan", 1)),
                sticky=info.get("sticky", "we"), padx=info.get("padx", 0), pady=info.get("pady", 0))
        try:
            # Expand horizontally by default
            parent.grid_columnconfigure(int(info.get("column", 0)), weight=1)
        except Exception:
            pass
        try:
            # Keep original width hint if it was set
            w = int(entry.cget("width"))
            if w > 0:
                rf.configure(width=w)
        except Exception:
            pass
        # Mark the new entry to avoid future upgrades
        rf.entry._rounded_applied = True
        # Destroy original
        try:
            entry.destroy()
        except Exception:
            pass
    except Exception:
        pass


def _upgrade_inputs_in(widget):
    """Recursively traverse a container and replace plain Entries with RoundedField,
    skipping anything under LoginFrame. Safe to call multiple times.
    """
    try:
        for child in list(widget.winfo_children()):
            # If this subtree is the login frame, skip entirely
            if isinstance(child, LoginFrame):
                continue
            # Recurse first
            _upgrade_inputs_in(child)
            # Then check if child is an Entry
            try:
                import tkinter.ttk as _ttk
                if isinstance(child, (tk.Entry, _ttk.Entry)):
                    _replace_entry_with_rounded(child)
            except Exception:
                pass
    except Exception:
        pass


# ---- Auto-upgrade entries in non-login tabs to RoundedField -----------------

def _is_inside_login(widget):
    try:
        from tkinter import Misc
        w = widget
        # Walk up the parents until root
        while isinstance(w, tk.Misc) and w is not None:
            if isinstance(w, LoginFrame):
                return True
            w = w.master
    except Exception:
        pass
    return False


def _stringvar_from_entry(entry):
    """Fetch existing textvariable from an Entry (if any) or mirror its content into a new StringVar."""
    try:
        name = str(entry.cget("textvariable") or "")
        if name:
            # Bind to the same underlying Tcl variable name
            return tk.StringVar(master=entry, name=name)
    except Exception:
        pass
    # Fall back to a new variable seeded with current text
    try:
        return tk.StringVar(value=entry.get())
    except Exception:
        return tk.StringVar()


def _replace_entry_with_rounded(entry):
    """Replace a tk/ttk Entry with a RoundedField in the same grid cell.
    Skips anything inside the login/register frame.
    """
    try:
        if _is_inside_login(entry):
            return
        if getattr(entry, "_rounded_applied", False):
            return
        info = entry.grid_info()
        if not info:
            # Only support grid-managed widgets for now
            return
        parent = entry.master
        var = _stringvar_from_entry(entry)
        show = ""
        try:
            show = entry.cget("show") or ""
        except Exception:
            show = ""
        # Create the rounded field with persistent glow
        rf = RoundedField(parent, textvariable=var, show=(show or None), height=44, always_glow=True)
        # Match the original grid placement
        rf.grid(row=int(info.get("row", 0)), column=int(info.get("column", 0)),
                rowspan=int(info.get("rowspan", 1)), columnspan=int(info.get("columnspan", 1)),
                sticky=info.get("sticky", "we"), padx=info.get("padx", 0), pady=info.get("pady", 0))
        try:
            # Expand horizontally by default
            parent.grid_columnconfigure(int(info.get("column", 0)), weight=1)
        except Exception:
            pass
        try:
            # Keep original width hint if it was set
            w = int(entry.cget("width"))
            if w > 0:
                rf.configure(width=w)
        except Exception:
            pass
        # Mark the new entry to avoid future upgrades
        rf.entry._rounded_applied = True
        # Destroy original
        try:
            entry.destroy()
        except Exception:
            pass
    except Exception:
        pass


def _upgrade_inputs_in(widget):
    """Recursively traverse a container and replace plain Entries with RoundedField,
    skipping anything under LoginFrame. Safe to call multiple times.
    """
    try:
        for child in list(widget.winfo_children()):
            # If this subtree is the login frame, skip entirely
            if isinstance(child, LoginFrame):
                continue
            # Recurse first
            _upgrade_inputs_in(child)
            # Then check if child is an Entry
            try:
                import tkinter.ttk as _ttk
                if isinstance(child, (tk.Entry, _ttk.Entry)):
                    _replace_entry_with_rounded(child)
            except Exception:
                pass
    except Exception:
        pass


class LoginFrame(ttk.Frame):
    def __init__(self, master, on_success, on_cancel):
        super().__init__(master, style="LoginShell.TFrame")
        self.configure(padding=0)
        self.on_success = on_success
        self.on_cancel = on_cancel
        self.mode = "register" if auth_user_count() == 0 else "login"
        self._password_visible = False
        self._confirm_visible = False

        # --- HERO (full-height filled gradient panel) ------------------------
        self.columnconfigure(0, weight=0, minsize=520)   # fixed left panel
        self.columnconfigure(1, weight=0)
        self.columnconfigure(2, weight=1)

        hero_canvas = tk.Canvas(self, highlightthickness=0, bd=0)
        hero_canvas.grid(row=0, column=0, sticky="nsew")
        hero_canvas.configure(width=520)          # match the reference width
        hero_canvas.grid_propagate(False)
        self.hero_canvas = hero_canvas
        self._hero_grad_start, self._hero_grad_end = ("#4c8df6", "#1f3fb5")

        # Canvas-based hero content (so gradient fill remains visible)
        self._hero_badge_id = None
        self._hero_title_id = None
        self._hero_summary_id = None
        self._hero_point_ids = []
        self._hero_footer_id = None

        # store current strings; _update_hero() will populate and then paint
        self._hero_title_text = ""
        self._hero_summary_text = ""
        self._hero_points_text = []
        self._hero_footer_text = ""

        hero_canvas.bind("<Configure>", self._on_hero_resize)
        self._draw_hero_gradient()

        divider = ttk.Frame(self, style="LoginCardDivider.TFrame", width=1)
        divider.grid(row=0, column=1, sticky="ns", pady=36, padx=(0, 0))

        form = ttk.Frame(self, style="LoginInner.TFrame")
        form.grid(row=0, column=2, sticky="nsew", padx=(32, 24))
        form.columnconfigure(0, weight=1)

        header = ttk.Frame(form, style="LoginHeader.TFrame")
        header.grid(row=0, column=0, sticky="we")
        header.columnconfigure(0, weight=1)
        self.title_lbl = ttk.Label(header, text="S3 / MinIO Manager", style="LoginTitle.TLabel", anchor="w")
        self.title_lbl.grid(row=0, column=0, sticky="we")
        # Removed moon toggle for a cleaner header
        header.bind("<Configure>", lambda e: self._set_title_wrap(e.width))

        self.subtitle_lbl = ttk.Label(form, text="", style="LoginSubtitle.TLabel")
        self.subtitle_lbl.grid(row=1, column=0, sticky="w", pady=(12, 18))

        self.welcome_lbl = ttk.Label(form, text="👋 Welcome back, operator", style="LoginWelcome.TLabel")
        self.welcome_lbl.grid(row=2, column=0, sticky="w", pady=(0, 22))

        ttk.Label(form, text="Username", style="LoginLabel.TLabel").grid(row=3, column=0, sticky="w")
        self.username_var = tk.StringVar()
        user_field = RoundedField(form, textvariable=self.username_var, height=44, always_glow=True, max_width=INPUT_MAX_WIDTH)
        user_field.grid(row=4, column=0, sticky="we", pady=(8, 22))
        self.username_entry = user_field.entry
        self.username_status = ttk.Label(user_field.trailing_holder, text="", style="LoginCheckIcon.TLabel")
        self.username_status.pack(side="right")

        ttk.Label(form, text="Password", style="LoginLabel.TLabel").grid(row=5, column=0, sticky="w")
        self.password_var = tk.StringVar()
        pass_field = RoundedField(form, textvariable=self.password_var, show="•", height=44, always_glow=True, max_width=INPUT_MAX_WIDTH)
        pass_field.grid(row=6, column=0, sticky="we", pady=(8, 10))
        self.password_entry = pass_field.entry
        # Remove show/hide toggle for password for a cleaner design

        self.confirm_label = ttk.Label(form, text="Confirm Password", style="LoginLabel.TLabel")
        self.confirm_var = tk.StringVar()
        confirm_field = RoundedField(form, textvariable=self.confirm_var, show="•", height=44, always_glow=True, max_width=INPUT_MAX_WIDTH)
        self.confirm_entry = confirm_field.entry
        self.confirm_toggle = ttk.Label(confirm_field.trailing_holder, text="Show", style="LoginLink.TLabel", anchor="center", padding=(0,0,0,0))
        self.confirm_toggle.bind("<Button-1>", lambda *_: self._toggle_confirm_visibility())
        confirm_field.place_right(self.confirm_toggle)
        self.confirm_row = confirm_field

        # Premium multi‑color strength bar (thin, gradient‑like)
        self.strength_canvas = tk.Canvas(
            form,
            height=10,
            highlightthickness=0,
            bd=0,
            background=THEME_PALETTE.get("SURFACE", "#111318"),
        )
        self.strength_canvas.grid(row=9, column=0, sticky="we", pady=(8, 12))

        self.message_var = tk.StringVar()
        self.message_label = ttk.Label(form, textvariable=self.message_var, style="LoginMessage.TLabel")
        self.message_label.grid(row=10, column=0, sticky="we", pady=(2, 0))

        buttons = ttk.Frame(form, style="LoginInner.TFrame")
        buttons.grid(row=12, column=0, sticky="we", pady=(12, 12))
        buttons.columnconfigure(0, weight=1)
        buttons.columnconfigure(1, weight=0)

        # Keep me signed in
        self.keep_signed_var = tk.BooleanVar(value=True)
        keep_row = ttk.Frame(form, style="LoginInner.TFrame")
        keep_row.grid(row=11, column=0, sticky="w", pady=(6, 0))
        ttk.Checkbutton(keep_row, text="Keep me signed in", variable=self.keep_signed_var, style="Section.TCheckbutton").pack(side="left")

        self.primary_btn = ttk.Button(buttons, text="Sign In ✓", style="PrimaryLarge.TButton", command=self._submit)
        self.primary_btn.grid(row=0, column=0, sticky="we")

        self.quit_btn = ttk.Button(buttons, text="Quit", style="GhostSmall.TButton", command=self.on_cancel)
        self.quit_btn.grid(row=0, column=1, sticky="e", padx=(14, 0))

        self.toggle_btn = ttk.Label(form, text="", style="LoginLink.TLabel", cursor="hand2")
        self.toggle_btn.grid(row=13, column=0, sticky="w", pady=(8, 0))
        self.toggle_btn.bind("<Button-1>", lambda *_: self._toggle_mode())

        self.footnote_var = tk.StringVar()
        ttk.Label(form, textvariable=self.footnote_var, style="LoginFootnote.TLabel").grid(row=14, column=0, sticky="w", pady=(10, 0))

        self.bind_all("<Return>", lambda *_: self._submit())
        self.bind_all("<Escape>", lambda *_: self.on_cancel())

        self._refresh_usernames()
        self._update_mode()
        self.after(100, self.username_entry.focus_set)
        self.username_var.trace_add("write", lambda *_: self._on_username_change())
        self.password_var.trace_add("write", lambda *_: self._on_password_change())

    def _refresh_usernames(self):
        # Do not auto‑fill usernames; keep the field empty for privacy
        if self.mode == "register":
            self.username_var.set("")

    def _update_mode(self):
        if self.mode == "login":
            self.toggle_btn.config(text="Need access? Create an account →")
            try:
                self.primary_btn.configure(text="Sign In ✓")
            except Exception:
                pass
            if self.confirm_label.winfo_manager():
                self.confirm_label.grid_remove()
            if self.confirm_row.winfo_manager():
                self.confirm_row.grid_remove()
            self.subtitle_lbl.config(text="Sign in with your workspace credentials to continue.")
            self.footnote_var.set("Having trouble? Ask an administrator to reset your local credentials.")
        else:
            self.toggle_btn.config(text="Back to sign in ↩")
            try:
                self.primary_btn.configure(text="Register & Sign In")
            except Exception:
                pass
            self.confirm_label.grid(row=7, column=0, sticky="w", pady=(8, 0))
            self.confirm_row.grid(row=8, column=0, sticky="we", pady=(8, 14))
            self.subtitle_lbl.config(text="Create a secure account.")
            self.footnote_var.set("Passwords must be at least 8 characters. We hash them locally with PBKDF2 before storing.")
        self.message_var.set("")
        self._update_hero()
        self._on_username_change()
        self._on_password_change()

    def _toggle_mode(self):
        self.mode = "register" if self.mode == "login" else "login"
        self.password_var.set("")
        self.confirm_var.set("")
        if self._password_visible:
            self._password_visible = False
            self.password_entry.config(show="•")
            self.password_toggle.config(text="Show")
        if self._confirm_visible:
            self._confirm_visible = False
            self.confirm_entry.config(show="•")
            self.confirm_toggle.config(text="Show")
        self._refresh_usernames()
        self._update_mode()
        self.after(50, self.username_entry.focus_set)

    def _on_hero_resize(self, event):
        # Redraw the background and reflow the canvas text
        self._draw_hero_gradient()
        self._paint_hero_text()

    def _draw_hero_gradient(self):
        canvas = getattr(self, "hero_canvas", None)
        if not canvas:
            return

        # Clear previous background layer
        canvas.delete("hero_bg")

        # Match the card surface
        try:
            canvas.configure(background=THEME_PALETTE.get("SURFACE", "#1b1d22"),
                             highlightthickness=0, bd=0)
        except Exception:
            pass

        w = max(canvas.winfo_width(), 1)
        h = max(canvas.winfo_height(), 1)

        # No more rounded corners: fill rectangular
        pad = 0      # no inner padding; fill reaches the edge

        # --- Premium blue vertical gradient (fully filled, rectangular)
        top_blue = "#1f3fb5"   # start
        mid_blue = "#1f3fb5"   # mid richness
        bot_blue = "#1f3fb5"   # end

        # Set gradient coordinates slightly outside canvas to avoid black  borders
        gx1, gy1 = -2, -2
        gx2, gy2 = w + 2, h + 2
        steps = max(120, gy2 - gy1)

        # Remove any canvas border that may still show at the edges
        canvas.configure(highlightthickness=0, borderwidth=0)

        def left_inset(y):
            # Disable rounded corner logic: always return 0 (rectangular)
            return 0

        # Draw thin horizontal strips; all edges perfectly rectangular
        for i in range(steps):
            t = i / (steps - 1)
            # two-stage blend for depth
            if t < 0.5:
                c = _blend(top_blue, mid_blue, t / 0.5)
            else:
                c = _blend(mid_blue, bot_blue, (t - 0.5) / 0.5)

            y1 = int(gy1 + i * (gy2 - gy1) / steps)
            y2 = int(gy1 + (i + 1) * (gy2 - gy1) / steps)
            x_inset_left = left_inset((y1 + y2) * 0.5)
            x1 = int(gx1 + x_inset_left)   # always 0 inset
            x2 = int(gx2)                  # right edge perfectly straight
            canvas.create_rectangle(x1, y1, x2, y2, fill=c, outline=c, tags="hero_bg")

        # Keep background behind text
        canvas.tag_lower("hero_bg")
        self._paint_hero_text()

    def _paint_hero_text(self):
        """Lay out hero copy directly on the canvas so the gradient fill stays visible."""
        c = self.hero_canvas
        if not c:
            return
        c.delete("hero_text")

        pad = 28
        w = max(c.winfo_width(), 1)
        # Badge
        badge_bg = _blend(THEME_PALETTE.get("ACCENT", "#4c8df6"), "#ffffff", 0.18)
        badge_fg = _blend(THEME_PALETTE.get("ACCENT", "#4c8df6"), "#000000", 0.15)
        # Draw a small rounded pill for the badge
        bx1, by1, bx2, by2, br = pad, pad, min(w - pad, pad + 160), pad + 32, 12
        c.create_rectangle(bx1+br, by1, bx2-br, by2, fill=badge_bg, outline="", tags="hero_text")
        c.create_arc(bx1, by1, bx1+2*br, by2, start=90, extent=180, style="pieslice", fill=badge_bg, outline="", tags="hero_text")
        c.create_arc(bx2-2*br, by1, bx2, by2, start=270, extent=180, style="pieslice", fill=badge_bg, outline="", tags="hero_text")
        c.create_text(bx1+14, by1+16, anchor="w", text="S3 / MINIO OPS", fill="#f5f7ff",
                      font=("SF Pro Text", 10, "bold"), tags="hero_text")

        # Title
        title_y = by2 + 28
        self._hero_title_id = c.create_text(
            pad, title_y, anchor="nw", width=w - pad*2,
            text=self._hero_title_text, fill=THEME_PALETTE.get("TEXT", "#e6ebf3"),
            font=("SF Pro Display", 36, "bold"), tags="hero_text"
        )

        # Summary
        sum_y = c.bbox(self._hero_title_id)[3] + 18 if self._hero_title_id else title_y + 60
        self._hero_summary_id = c.create_text(
            pad, sum_y, anchor="nw", width=w - pad*2,
            text=self._hero_summary_text,
            fill=_blend(THEME_PALETTE.get("SUBTLE", "#9aa3b2"), THEME_PALETTE.get("TEXT", "#e6ebf3"), 0.45),
            font=("SF Pro Text", 12), tags="hero_text"
        )

        # Bullets
        y = c.bbox(self._hero_summary_id)[3] + 16 if self._hero_summary_id else sum_y + 40
        self._hero_point_ids = []
        for i, line in enumerate(self._hero_points_text):
            item = c.create_text(
                pad, y, anchor="nw", width=w - pad*2,
                text=f"• {line}",
                fill=_blend(THEME_PALETTE.get("SUBTLE", "#9aa3b2"), THEME_PALETTE.get("TEXT", "#e6ebf3"), 0.55),
                font=("SF Pro Text", 12), tags="hero_text"
            )
            self._hero_point_ids.append(item)
            y = c.bbox(item)[3] + 10

        # Footer
        self._hero_footer_id = c.create_text(
            pad, y + 18, anchor="nw", width=w - pad*2,
            text=self._hero_footer_text,
            fill=_blend(THEME_PALETTE.get("SUBTLE", "#9aa3b2"), THEME_PALETTE.get("TEXT", "#e6ebf3"), 0.65),
            font=("SF Pro Text", 11), tags="hero_text"
        )

    @staticmethod
    def _hex_to_rgb(value):
        value = value.lstrip("#")
        return tuple(int(value[i:i+2], 16) for i in (0, 2, 4))

    def _set_title_wrap(self, width):
        try:
            # Reserve space for the moon toggle and some padding
            wrap = max(360, int(width) - 120)
            self.title_lbl.config(wraplength=wrap)
            # Responsive title sizing: slightly shrink on tighter widths
            try:
                style = ttk.Style(self)
                size = 40 if width > 760 else (38 if width > 680 else (36 if width > 600 else 32))
                style.configure("LoginTitle.TLabel", font=("SF Pro Display", size, "bold"))
            except Exception:
                pass
        except Exception:
            pass

    def _toggle_theme(self):
        global _initial_settings
        # Flip preference
        current_dark = str(os.environ.get("UI_DARK", _initial_settings.get("UI_DARK", "1"))).lower() not in ("0", "false", "no")
        new_dark = not current_dark
        os.environ["UI_DARK"] = "1" if new_dark else "0"
        # Persist to the same settings JSON used by s3.py
        try:
            cfg = load_s3_settings()
            cfg["UI_DARK"] = "1" if new_dark else "0"
            save_s3_settings(cfg)
            _initial_settings = cfg
        except Exception:
            pass
        # Re-apply theme styles; widgets reuse the same style names
        try:
            _, palette = apply_theme(self.winfo_toplevel())
            THEME_PALETTE.clear(); THEME_PALETTE.update(palette)
        except Exception:
            pass
        # Redraw elements that paint directly
        try:
            self._draw_hero_gradient()
            self._on_password_change()
        except Exception:
            pass

    def _update_hero(self):
        if self.mode == "login":
            self._hero_title_text = "Welcome back.\nReady to deploy?"
            self._hero_summary_text = "Manage S3/MinIO from one desktop — no shell needed."
            self._hero_points_text = [
                "Upload files and folders with resumable progress.",
                "Download objects with auto‑retry and metrics.",
                "List buckets & objects; filter and inspect metadata.",
                "Delete objects or entire buckets with safety checks.",
            ]
            self._hero_footer_text = "Configure endpoints, credentials, and defaults in Settings."
        else:
            self._hero_title_text = "Welcome.\nReady to deploy?"
            self._hero_summary_text = "Create an account to begin. Then manage S3/MinIO from one desktop — no shell needed."
            self._hero_points_text = [
                "Upload files and folders with resumable progress.",
                "Download objects with auto‑retry and metrics.",
                "List buckets & objects; filter and inspect metadata.",
                "Delete objects or entire buckets with safety checks.",
            ]
            self._hero_footer_text = "Configure endpoints, credentials, and defaults in Settings."
        # Repaint texts onto the canvas
        self._paint_hero_text()

    def _on_username_change(self):
        # Keep messaging generic; do not display the typed username
        if self.mode == "login":
            self.welcome_lbl.config(text="👋 Welcome back")
        else:
            self.welcome_lbl.config(text="🛡️ Create your account")
        username = self.username_var.get().strip()
        self.username_status.config(text="✓" if username else "")

    def _password_strength_score(self, password):
        score = 0
        if not password:
            return 0
        length = len(password)
        score += min(length * 6, 40)
        if re.search(r"[A-Z]", password):
            score += 15
        if re.search(r"[a-z]", password):
            score += 15
        if re.search(r"\d", password):
            score += 15
        if re.search(r"[^\w\s]", password):
            score += 15
        if length >= 12:
            score += 10
        return min(score, 100)

    def _on_password_change(self):
        score = self._password_strength_score(self.password_var.get())
        # update the premium multi‑segment bar
        try:
            self._draw_strength(score)
        except Exception:
            pass

    def _draw_strength(self, score):
        canvas = getattr(self, "strength_canvas", None)
        if not canvas:
            return
        canvas.delete("all")
        w = max(canvas.winfo_width(), 1)
        h = max(canvas.winfo_height(), 8)
        trough = THEME_PALETTE.get("SECTION_BG", "#22262e")
        # background trough
        canvas.create_rectangle(0, h//2 - 3, w, h//2 + 3, fill=trough, width=0)
        # draw three segments from orange -> lime -> green
        colors = ["#f59e0b", "#a3e635", "#22c55e"]
        segs = [0.33, 0.66, 1.0]
        filled = (score / 100.0) * w
        start_x = 0
        for idx, stop in enumerate(segs):
            end_x = w * stop
            draw_to = min(filled, end_x)
            if draw_to > start_x:
                canvas.create_rectangle(start_x, h//2 - 3, draw_to, h//2 + 3, fill=colors[idx], width=0)
            start_x = end_x

    def _toggle_password_visibility(self):
        self._password_visible = not self._password_visible
        if self._password_visible:
            self.password_entry.config(show="")
            self.password_toggle.config(text="Hide")
        else:
            self.password_entry.config(show="•")
            self.password_toggle.config(text="Show")

    def _toggle_confirm_visibility(self):
        self._confirm_visible = not self._confirm_visible
        if self._confirm_visible:
            self.confirm_entry.config(show="")
            self.confirm_toggle.config(text="Hide")
        else:
            self.confirm_entry.config(show="•")
            self.confirm_toggle.config(text="Show")

    def _submit(self):
        username = self.username_var.get().strip().lower()
        password = self.password_var.get()
        if self.mode == "login":
            if not username or not password:
                self.message_var.set("Enter both username and password.")
                self._shake()
                return
            user_id = auth_verify_user(username, password)
            if user_id is None:
                self.message_var.set("Invalid username or password.")
                self._shake()
                return
            self._success_pulse(lambda: self._finalize(user_id, username))
        else:
            confirm = self.confirm_var.get()
            if len(username) < 3:
                self.message_var.set("Username must be at least 3 characters.")
                return
            if not username.replace("_", "").isalnum():
                self.message_var.set("Use letters, numbers, and underscores only.")
                return
            if len(password) < 8:
                self.message_var.set("Password must be at least 8 characters.")
                return
            if password != confirm:
                self.message_var.set("Passwords do not match.")
                return
            if auth_get_user(username):
                self.message_var.set("Username already exists. Choose another.")
                return
            auth_create_user(username, password)
            user_id = auth_verify_user(username, password)
            if user_id is None:
                self.message_var.set("Unexpected error during registration.")
                return
            messagebox.showinfo("Account created", "Your account has been created.", parent=self)
            self._success_pulse(lambda: self._finalize(user_id, username))

    def _finalize(self, user_id, username):
        self.unbind_all("<Return>")
        self.unbind_all("<Escape>")
        # Save session if requested
        try:
            cfg = load_s3_settings()
            keep = getattr(self, "keep_signed_var", None)
            if keep is not None and keep.get():
                token = secrets.token_hex(16)
                import datetime as _dt
                expires = (_dt.datetime.now() + _dt.timedelta(days=7)).isoformat()
                cfg["SESSION"] = {"username": username, "token": token, "expires_at": expires}
            else:
                cfg.pop("SESSION", None)
            save_s3_settings(cfg)
        except Exception:
            pass
        self.on_success(user_id, username)

    def _shake(self):
        try:
            win = self.winfo_toplevel()
            win.update_idletasks()
            geom = win.geometry()
            base = geom.split("+")
            if len(base) >= 3:
                size = base[0]
                x = int(base[1]); y = int(base[2])
                for dx in (10, -10, 8, -8, 6, -6, 4, -4, 2, -2, 0):
                    win.geometry(f"{size}+{x+dx}+{y}")
                    win.update_idletasks()
                    win.after(12)
        except Exception:
            pass

    def _success_pulse(self, on_done, steps=10, duration_ms=300):
        """Quick success pulse on primary button; safe for both ttk and custom rounded button."""
        # If using custom rounded button, just flash hover state briefly
        try:
            from tkinter import Canvas as _TkCanvas
            if isinstance(self.primary_btn, RoundedButton) or isinstance(self.primary_btn, _TkCanvas):
                try:
                    self.primary_btn._set_hover(True)
                except Exception:
                    pass
                self.after(max(120, duration_ms // 2), lambda: (setattr(self.primary_btn, "_hover", False), self.primary_btn._redraw(), on_done()))
                return
        except Exception:
            pass

        try:
            style = ttk.Style(self)
            base = THEME_PALETTE.get("ACCENT", "#4c8df6")
            success = "#3ddc84"
            pulse_style = "AccentPulse.TButton"
            # inherit font if available
            font_val = style.lookup("Accent.TButton", "font") or ("Arial", 12, "bold")
            style.configure(pulse_style, padding=(15,12), font=font_val, background=base, foreground="white", borderwidth=0)
            self.primary_btn.configure(style=pulse_style)

            total_steps = max(2, int(steps)); delay = max(12, int(duration_ms / total_steps))
            def hex_to_rgb(h):
                h = h.lstrip('#'); return (int(h[0:2],16), int(h[2:4],16), int(h[4:6],16))
            def rgb_to_hex(r,g,b):
                return f"#{r:02x}{g:02x}{b:02x}"
            r1,g1,b1 = hex_to_rgb(base); r2,g2,b2 = hex_to_rgb(success)

            def frame(i=0, forward=True):
                t = i / (total_steps-1)
                r = int(r1 + (r2 - r1) * t); g = int(g1 + (g2 - g1) * t); b = int(b1 + (b2 - b1) * t)
                style.configure(pulse_style, background=rgb_to_hex(r,g,b))
                if forward and i < total_steps-1:
                    self.after(delay, lambda: frame(i+1, True))
                elif forward:
                    self.after(delay//2, lambda: frame(total_steps-2, False))
                elif i > 0:
                    self.after(delay//2, lambda: frame(i-1, False))
                else:
                    try:
                        self.primary_btn.configure(style="Accent.TButton")
                    except Exception:
                        pass
                    on_done()

            # Disable inputs briefly during pulse
            try:
                self.primary_btn.configure(state="disabled")
                self.username_entry.configure(state="disabled")
                self.password_entry.configure(state="disabled")
                if self.mode != "login":
                    self.confirm_entry.configure(state="disabled")
            except Exception:
                pass
            frame(0, True)
        except Exception:
            on_done()

import tkinter as tk
from tkinter import ttk

def create_topbar(parent, username, on_logout):
    """
    Gradient top bar with app title (left) and user info + logout (right).
    Uses a Canvas for a smooth vertical blue gradient and an overlay frame
    for the interactive controls.
    """
    # Wrapper frame so the caller still receives a normal widget
    topbar = ttk.Frame(parent, style="Topbar.TFrame")
    topbar.pack(fill="x", side="top")
    topbar.update_idletasks()

    # Canvas draws the gradient background
    canvas = tk.Canvas(topbar, height=54, highlightthickness=0, bd=0)
    canvas.pack(fill="x", expand=True)
    canvas._grad_from = "#3a5aff"
    canvas._grad_mid  = "#4a6aff"
    canvas._grad_to   = "#5b8cfe"

    # Overlay frame hosts the controls
    inner = ttk.Frame(topbar, style="TopbarInner.TFrame")
    inner.place(relx=0, rely=0, relwidth=1, relheight=1)

    # Left App Title
    lbl_title = ttk.Label(
        inner,
        text="S3 / MinIO Manager",
        style="TopbarTitle.TLabel",
        anchor="w"
    )
    lbl_title.pack(side="left", padx=(16, 0))

    # Right side: user avatar + name + logout
    right = ttk.Frame(inner, style="TopbarInner.TFrame")
    right.pack(side="right", padx=(12, 12))

    # Avatar (rounded pill look)
    avatar = ttk.Label(
        right,
        text="👤",
        style="TopbarAvatar.TLabel",
        anchor="center",
        width=2
    )
    avatar.pack(side="left", padx=(0, 6))

    # Username
    safe_name = (username or "").strip()
    if not safe_name:
        safe_name = "User"
    lbl_username = ttk.Label(
        right,
        text=safe_name.capitalize(),
        style="TopbarUser.TLabel",
        anchor="center"
    )
    lbl_username.pack(side="left", padx=(0, 10))
    # Expose for updates
    topbar._user_label = lbl_username

    # Logout (canvas-based pill to avoid white hover flash)
    logout_btn = RoundedPill(
        right,
        text="⎋ Logout",
        radius=6,
        pad_x=4,
        pad_y=2,
        command=on_logout,
    )
    logout_btn.configure(width=90)
    logout_btn.pack(side="left", padx=16, pady=6)
    # Ensure logout button is only packed, not gridded anywhere
    # If you see any logout_btn.grid(...) below, remove or comment it out.
    topbar._logout_btn = logout_btn

    def _draw_topbar_gradient(event=None):
        w = max(canvas.winfo_width(), 1)
        h = max(canvas.winfo_height(), 1)
        canvas.delete("grad")
        steps = max(64, h)
        # Rounded ends: draw a subtle rounded rect mask look by padding
        pad_y = 0
        y1 = pad_y
        y2 = h - pad_y
        for i in range(steps):
            t = i / (steps - 1)
            # two‑stop blend for a richer gradient
            if t < 0.5:
                # from start -> mid
                tt = t / 0.5
                col = _blend(canvas._grad_from, canvas._grad_mid, tt)
            else:
                # from mid -> end
                tt = (t - 0.5) / 0.5
                col = _blend(canvas._grad_mid, canvas._grad_to, tt)
            y = int(y1 + t * (y2 - y1))
            canvas.create_rectangle(0, y, w, y + 1, outline=col, fill=col, tags="grad")

    # initial paint + bind resize
    _draw_topbar_gradient()
    canvas.bind("<Configure>", _draw_topbar_gradient)

    # Keep references for future theme changes if needed
    topbar._gradient_canvas = canvas
    topbar._overlay = inner
    return topbar


def setup_custom_styles(root):
    style = ttk.Style(root)

    # Base containers
    style.configure("Topbar.TFrame", background=THEME_PALETTE.get("BG", "#0f1115"))
    style.configure("TopbarInner.TFrame", background="", relief="flat")

    # Title on the left
    style.configure(
        "TopbarTitle.TLabel",
        font=("SF Pro Display", 13, "bold"),
        foreground="#F5F7FF",
        background=""
    )

    # Avatar pill (light overlay to stand out on gradient)
    style.configure(
        "TopbarAvatar.TLabel",
        font=("SF Pro Text", 12, "bold"),
        foreground="#ffffff",
        background=_blend(THEME_PALETTE.get("ACCENT", "#5b8cfe"), "#000000", 0.25),
        padding=(8, 4)
    )

    # Username
    style.configure(
        "TopbarUser.TLabel",
        font=("SF Pro Text", 11),
        foreground="#E6EBF3",
        background=""
    )

    # (Logout button style for TopbarLogout.TButton removed; now using canvas-based pill)

def _start_login(root, card_factory):
    result = {"done": False}

    # Helper: lightweight fade-in for auto-login
    def _fade_in(win, duration_ms=240, steps=12):
        try:
            win.attributes("-alpha", 0.0)
        except Exception:
            return
        step = 1.0 / max(1, int(steps))
        delay = max(8, int(duration_ms / max(1, int(steps))))

        def tick(alpha=0.0):
            try:
                win.attributes("-alpha", max(0.0, min(1.0, alpha)))
            except Exception:
                return
            if alpha < 1.0:
                win.after(delay, lambda: tick(alpha + step))
        win.after(delay, tick)

    # Fast-path: auto login via session before creating overlays
    try:
        cfg = load_s3_settings()
        sess = (cfg or {}).get("SESSION") or {}
        username = sess.get("username")
        exp = sess.get("expires_at")
        if username and exp:
            dt = datetime.datetime.fromisoformat(exp)
            if dt > datetime.datetime.now():
                row = auth_get_user(username)
                if row:
                    CURRENT_USER["id"] = row[0]
                    CURRENT_USER["username"] = username
                    # Subtle fade-in once the main UI starts
                    _fade_in(root)
                    return True
    except Exception:
        pass

    login_background = ttk.Frame(root, style="LoginBackground.TFrame")
    login_background.pack(fill="both", expand=True)

    card_frame = card_factory(login_background)
    # Center using place so we can resize responsively
    card_frame.place(relx=0.5, rely=0.5, anchor="center")
    card_frame.pack_propagate(False)
    card_inner = getattr(card_frame, "_card_inner", card_frame)
    card_inner.pack_propagate(False)

    def _resize_login_card(event=None):
        try:
            login_background.update_idletasks()
            sw = max(login_background.winfo_width(), root.winfo_screenwidth())
            sh = max(login_background.winfo_height(), root.winfo_screenheight())
        except Exception:
            sw = root.winfo_screenwidth(); sh = root.winfo_screenheight()
        margin = 56
        avail_w = max(600, sw - margin * 2)
        avail_h = max(420, sh - margin * 2)
        min_w, min_h = 860, 560
        max_w, max_h = 1120, 820
        width = max(min_w, min(max_w, int(avail_w * 0.82)))
        height = max(min_h, min(max_h, int(avail_h * 0.8)))
        try:
            card_frame.configure(width=width, height=height)
        except Exception:
            pass
        # keep it centered
        try:
            card_frame.place_configure(relx=0.5, rely=0.5, anchor="center")
        except Exception:
            pass

    login_background.bind("<Configure>", _resize_login_card)
    root.after(50, _resize_login_card)

    def _success(uid, uname):
        # Save session is already handled inside LoginFrame._finalize -> on_success
        CURRENT_USER["id"] = uid
        CURRENT_USER["username"] = uname

        # Signal success and break out of the login loop
        result["done"] = True
        try:
            # closing the login UI
            for widget in root.winfo_children():
                widget.destroy()
        except Exception:
            pass

        # Quit the local mainloop started by _start_login so caller can continue
        try:
            root.after(0, root.quit)
        except Exception:
            pass

        # Local logout handler (does not rely on later definitions)
        def __logout():
            if not messagebox.askyesno("Log out", "Log out and return to sign-in?", parent=root):
                return
            try:
                cfg = load_s3_settings()
                cfg.pop("SESSION", None)
                save_s3_settings(cfg)
            except Exception:
                pass
            show_login_overlay(root, card)


    def _cancel():
        result["done"] = False
        root.quit()
        root.destroy()
        sys.exit(0)

    # (Auto-login already handled above.)

    login_frame = LoginFrame(card_inner, _success, _cancel)
    login_frame.pack(fill="both", expand=True)
    root.update_idletasks()
    root.deiconify()
    root.mainloop()
    try:
        login_background.destroy()
    except Exception:
        pass
    return result["done"]

def show_login_modal(parent, card_factory):
    """Display the LoginFrame in a modal Toplevel and return True on success."""
    result = {"ok": False}
    win = tk.Toplevel(parent)
    win.title("Sign in")
    try:
        win.transient(parent)
        win.grab_set()
    except Exception:
        pass
    container = card_factory(win)
    container.pack(expand=True, padx=60, pady=60)
    container.configure(width=900, height=520)
    container.pack_propagate(False)

    def _success(uid, uname):
        CURRENT_USER["id"] = uid
        CURRENT_USER["username"] = uname
        result["ok"] = True
        try:
            win.destroy()
        except Exception:
            pass

    def _cancel():
        result["ok"] = False
        try:
            win.destroy()
        except Exception:
            pass

    lf = LoginFrame(container, _success, _cancel)
    lf.pack(fill="both", expand=True)
    _center_window(win)
    parent.wait_window(win)
    return bool(result["ok"])

def show_login_overlay(parent, card_factory):
    """Render a full-window login overlay that blocks the underlying UI until success.
    Cancel attempts prompt to quit the app instead of leaving the UI unlocked.
    """
    overlay = ttk.Frame(parent, style="LoginBackground.TFrame")
    # Use place() so the overlay sits above existing packed widgets
    overlay.place(x=0, y=0, relwidth=1, relheight=1)
    try:
        overlay.lift()
        overlay.grab_set()
        parent.update_idletasks()
    except Exception:
        pass
    card_frame = card_factory(overlay)
    card_frame.place(relx=0.5, rely=0.5, anchor="center")
    card_frame.pack_propagate(False)
    card_inner = getattr(card_frame, "_card_inner", card_frame)
    card_inner.pack_propagate(False)

    def _resize_overlay_card(event=None):
        try:
            overlay.update_idletasks()
            sw = max(overlay.winfo_width(), parent.winfo_screenwidth())
            sh = max(overlay.winfo_height(), parent.winfo_screenheight())
        except Exception:
            sw = parent.winfo_screenwidth(); sh = parent.winfo_screenheight()
        margin = 56
        avail_w = max(600, sw - margin * 2)
        avail_h = max(420, sh - margin * 2)
        min_w, min_h = 860, 560
        max_w, max_h = 1120, 820
        width = max(min_w, min(max_w, int(avail_w * 0.82)))
        height = max(min_h, min(max_h, int(avail_h * 0.8)))
        try:
            card_frame.configure(width=width, height=height)
            card_frame.place_configure(relx=0.5, rely=0.5, anchor="center")
        except Exception:
            pass

    overlay.bind("<Configure>", _resize_overlay_card)
    parent.after(50, _resize_overlay_card)

    def _success(uid, uname):
        CURRENT_USER["id"] = uid
        CURRENT_USER["username"] = uname
        try:
            overlay.grab_release()
        except Exception:
            pass
        try:
            overlay.destroy()
        except Exception:
            pass
        _refresh_header_info()

    def _cancel():
        if messagebox.askyesno("Quit", "Exit the application?", parent=parent):
            try:
                parent.destroy()
            except Exception:
                pass
            sys.exit(0)
        # Otherwise ignore cancel and keep the overlay

    lf = LoginFrame(card_inner, _success, _cancel)
    lf.pack(fill="both", expand=True)

def _refresh_header_info():
    env_bits = []
    if os.environ.get("AWS_REGION"):
        env_bits.append(f"region: {os.environ.get('AWS_REGION')}")
    if os.environ.get("AWS_S3_ENDPOINT"):
        env_bits.append(os.environ.get("AWS_S3_ENDPOINT"))

    # Recreate the premium topbar if it was destroyed by a geometry mix-up
    def _exists(w):
        try:
            return w is not None and w.winfo_exists()
        except Exception:
            return False

    try:
        root_win = globals().get("root")
        if not _exists(globals().get("HEADER_TOPBAR")) and root_win is not None:
            try:
                setup_custom_styles(root_win)
            except Exception:
                pass
            try:
                uname = (CURRENT_USER.get("username") or "User").strip()
                globals()["HEADER_TOPBAR"] = create_topbar(root_win, username=uname, on_logout=lambda: show_login_overlay(root_win, card))
            except Exception:
                pass
            # Ensure there is a dedicated body container below the topbar
            if not _exists(globals().get("APP_BODY")) and root_win is not None:
                body = ttk.Frame(root_win)
                body.pack(fill="both", expand=True)  # all app content goes here, not on root
                globals()["APP_BODY"] = body

        # Keep username in sync
        if _exists(globals().get("HEADER_TOPBAR")) and CURRENT_USER.get("username"):
            try:
                HEADER_TOPBAR._user_label.configure(text=CURRENT_USER["username"].strip().capitalize() or "User")
            except Exception:
                pass
    except Exception:
        pass

    # Small info label (legacy)
    try:
        if HEADER_INFO_LABEL is not None:
            HEADER_INFO_LABEL.config(text="  |  ".join(env_bits))
    except Exception:
        pass

    # Status bar
    try:
        statusbar.config(text="Ready")
    except Exception:
        pass
# ---------------- Main UI factory (fallback) ----------------
# We keep a *minimal* fallback to avoid NameError if your real
# create_dashboard hasn't been imported yet. The fallback returns
# an empty container so it won't collide with the real UI.
if 'create_dashboard' not in globals():
    def create_dashboard(parent):
        """Silent fallback: just return an empty container frame."""
        from tkinter import ttk as _ttk
        container = _ttk.Frame(parent)
        container.pack(fill="both", expand=True)
        # Do not add any extra widgets here — the real app will
        # populate this area. We still upgrade entries defensively
        # in case downstream code adds them later.
        try:
            _upgrade_inputs_in(container)
        except Exception:
            pass
        return container

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


def _center_window(win, fallback=(480, 320)):
    try:
        win.update_idletasks()
        width = win.winfo_width()
        height = win.winfo_height()
        if width <= 1 or height <= 1:
            width, height = fallback
        screen_w = win.winfo_screenwidth()
        screen_h = win.winfo_screenheight()
        x = max((screen_w - width) // 2, 0)
        y = max((screen_h - height) // 3, 0)
        if width and height:
            win.geometry(f"{int(width)}x{int(height)}+{x}+{y}")
        else:
            win.geometry(f"+{x}+{y}")
    except Exception:
        pass

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

    # Read persisted preference first, then environment
    pref = None
    try:
        pref = str(_initial_settings.get("UI_DARK")) if isinstance(_initial_settings, dict) and "UI_DARK" in _initial_settings else None
    except Exception:
        pref = None
    env_flag = os.environ.get("UI_DARK")
    base = env_flag if env_flag is not None else (pref if pref is not None else "1")
    dark = str(base).lower() not in ("0", "false", "no")

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
        BG      = "#0f1115"; SURFACE = "#171a20"; RAISED  = "#1f232b"
        TEXT    = "#eef1f7"; SUBTLE  = "#9aa3b2"; ACCENT  = "#5b8cfe"
        TEXT = "#e6ebf3"
        TEXTAREA_BG = "#0c0e12"; STATUS_BG = "#15181f"
    else:
        BG      = "#f6f7fb"; SURFACE = "#ffffff"; RAISED  = "#ffffff"
        TEXT    = "#1a2130"; SUBTLE  = "#5b667a"; ACCENT  = "#3a6df0"
        TEXTAREA_BG = "#ffffff"; STATUS_BG = "#eff2f9"
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
    FONT_HERO = pick_font(DISPLAY_STACK, 26, "bold")
    FONT_BADGE = pick_font(TEXT_STACK, 9, "bold")
    FONT_PROGRESS = pick_font(TEXT_STACK, 11, "bold")
    FONT_ICON = pick_font(DISPLAY_STACK, 24, "bold")
    ACCENT_GLOW = _blend_hex(ACCENT, "#ffffff", 0.35)
    ACCENT_SHADOW = _blend_hex(ACCENT, "#000000", 0.25)
    INPUT_BG = _blend_hex(SURFACE, BG, 0.22 if dark else 0.06)
    INPUT_BG_FOCUS = _blend_hex(INPUT_BG, ACCENT, 0.35)

    root.configure(bg=BG)
    style.configure(".", background=BG, foreground=TEXT, fieldbackground=SURFACE, highlightthickness=0)
    style.configure("TLabel", background=BG, foreground=TEXT, font=FONT_TEXT)
    style.configure("Muted.TLabel", foreground=SUBTLE)
    style.configure("Header.TLabel", font=FONT_HEADER)
    style.configure("Small.TLabel", font=FONT_SMALL, foreground=SUBTLE)
    style.configure(
        "Card.TFrame",
        background=SURFACE,
        relief="flat",
        bordercolor=_blend_hex(SURFACE, BG, 0.28),
        borderwidth=1,
        padding=(22, 22),
    )
    style.configure("Toolbar.TFrame", background=BG)
    style.configure("TEntry", fieldbackground=RAISED, foreground=TEXT, insertcolor=TEXT, bordercolor=RAISED)
    style.map("TEntry", bordercolor=[("focus", ACCENT)])
    accent_idle = "#3a5aff"
    accent_active = "#4e6cff"
    style.configure("Accent.TButton", padding=(15,12), font=FONT_TEXT_BOLD,
                    background=accent_idle, foreground="white", borderwidth=0)
    style.map(
        "Accent.TButton",
        background=[("active", accent_active), ("disabled", _blend_hex(accent_idle, BG, 0.4))],
        foreground=[("disabled", _blend_hex("#ffffff", BG, 0.4))]
    )
    ghost_bg = _blend_hex(SURFACE, BG, 0.18 if dark else 0.04)
    ghost_bg_active = _blend_hex(ghost_bg, ACCENT, 0.18 if dark else 0.35)
    style.configure("Ghost.TButton", padding=(12,10), font=FONT_TEXT,
                    background=ghost_bg, foreground=TEXT, bordercolor=_blend_hex(ghost_bg, BG, 0.35), borderwidth=1)
    style.map(
        "Ghost.TButton",
        background=[("active", ghost_bg_active)],
        foreground=[("active", TEXT)],
        bordercolor=[("active", _blend_hex(ghost_bg_active, ACCENT, 0.2))]
    )
    # Larger primary and smaller ghost variants for auth actions
    style.configure("PrimaryLarge.TButton", padding=(20,14), font=pick_font(DISPLAY_STACK, 14, "bold"),
                    background=accent_idle, foreground="white", borderwidth=0)
    style.map("PrimaryLarge.TButton", background=[("active", accent_active)])
    style.configure("GhostSmall.TButton", padding=(8,6), font=FONT_TEXT,
                    background=ghost_bg, foreground=TEXT, bordercolor=_blend_hex(ghost_bg, BG, 0.35), borderwidth=1)
    style.map("GhostSmall.TButton", background=[("active", ghost_bg_active)])
    style.configure("Neutral.TButton", padding=(12,8), font=FONT_TEXT,
                    background=RAISED, foreground=TEXT)
    style.map("Neutral.TButton", background=[("active", _blend_hex(RAISED, ACCENT, 0.12)), ("disabled", _blend_hex(RAISED, BG, 0.35))])
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
        thickness=16,
        relief="flat",
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
    hero_top = _blend_hex(ACCENT, "#162a6a" if dark else "#ffffff", 0.28 if dark else 0.44)
    hero_bottom = _blend_hex(ACCENT, "#3b60ff" if dark else "#b3c3ff", 0.64 if dark else 0.2)
    hero_muted = _blend_hex("#f6f7fb" if dark else "#ffffff", hero_top, 0.42 if dark else 0.24)
    badge_bg = _blend_hex(hero_top, "#ffffff", 0.18 if dark else 0.3)
    badge_fg = "#f5f7ff" if dark else "#ffffff"
    style.configure("LoginBackground.TFrame", background=BG)
    style.configure("ElevatedCard.TFrame", background=SURFACE, padding=0)
    style.configure("LoginCard.TFrame", background=SURFACE, padding=(0,0))
    style.configure("LoginShell.TFrame", background=SURFACE, padding=0)
    style.configure("LoginHero.TFrame", background=hero_top, padding=(42, 48))
    # Make hero panel match input-field surface; the glow ring is painted on canvas
    style.configure("LoginHero.TFrame", background=SURFACE, padding=(42, 48))
    style.configure("LoginHeroContent.TFrame", background=SURFACE)

    style.configure(
        "LoginHeroHeading.TLabel",
        background=SURFACE,
        foreground=TEXT,
        font=("SF Pro Display", 36, "bold"),
        wraplength=320,
        justify="left",
    )
    style.configure(
        "LoginHeroText.TLabel",
        background=SURFACE,
        foreground=_blend_hex(SUBTLE, TEXT, 0.45),
        font=FONT_SMALL,
        wraplength=280,
        justify="left",
    )
    style.configure(
        "LoginHeroFootnote.TLabel",
        background=SURFACE,
        foreground=_blend_hex(SUBTLE, TEXT, 0.6),
        font=FONT_SMALL,
        wraplength=260,
        justify="left",
    )
    style.configure("LoginHeroIcon.TLabel", background=SURFACE, foreground=TEXT, font=("Segoe UI Emoji", 20))
    style.configure(
        "LoginBadge.TLabel",
        background=_blend_hex(SURFACE, ACCENT, 0.18 if dark else 0.28),
        foreground=_blend_hex(ACCENT, "#ffffff", 0.15 if dark else 0.0),
        font=FONT_BADGE,
        padding=(14, 6),
    )
    # Slightly tighter top/bottom padding to fit footnotes
    style.configure("LoginInner.TFrame", background=SURFACE, padding=(40, 64, 56, 40))
    style.configure("LoginHeader.TFrame", background=SURFACE)
    style.configure("LoginTitle.TLabel", background=SURFACE, foreground=TEXT, font=("SF Pro Display", 36, "bold"), wraplength=560, justify="left")
    style.configure("LoginToggle.TLabel", background=_blend_hex(SURFACE, BG, 0.12 if dark else 0.04), foreground=_blend_hex(SUBTLE, "#ffffff", 0.1), font=FONT_ICON, padding=(10,8))
    style.configure("LoginToggle.TLabel", relief="flat")
    style.configure("LoginSubtitle.TLabel", background=SURFACE, foreground=_blend_hex(SUBTLE, TEXT, 0.45), font=FONT_SMALL, wraplength=380, justify="left")
    style.configure("LoginWelcome.TLabel", background=SURFACE, foreground=_blend_hex(TEXT, SUBTLE, 0.15), font=FONT_TEXT)
    style.configure("LoginLabel.TLabel", background=SURFACE, foreground=_blend_hex(TEXT, SUBTLE, 0.2), font=FONT_SMALL_BOLD)
    style.configure("LoginCheckIcon.TLabel", background=SURFACE, foreground="#3ddc84", font=(DISPLAY_STACK[-1], 14, "bold"))
    # Make error messages more legible and less cramped
    style.configure("LoginMessage.TLabel", background=SURFACE, foreground="#ff6f73", font=FONT_SMALL,
                    justify="left", wraplength=520)
    style.configure("LoginLink.TLabel", background=SURFACE, foreground=ACCENT, font=FONT_SMALL_BOLD, cursor="hand2")
    style.map("LoginLink.TLabel", foreground=[("active", ACCENT_GLOW)])
    style.configure("LoginInput.TFrame", background=SURFACE, padding=(0,0))
    style.configure(
        "Login.TEntry",
        fieldbackground=SURFACE,
        background=SURFACE,
        bordercolor=SURFACE,
        lightcolor=SURFACE,
        darkcolor=SURFACE,
        relief="flat",
        padding=10,
        foreground=TEXT,
        insertcolor=TEXT,
    )
    style.map(
        "Login.TEntry",
        fieldbackground=[("focus", SURFACE)],
        bordercolor=[("focus", SURFACE)],
        lightcolor=[("focus", SURFACE)],
        darkcolor=[("focus", SURFACE)],
    )
    divider_bg = _blend_hex(SURFACE, hero_top, 0.24 if dark else 0.16)
    style.configure("LoginCardDivider.TFrame", background=divider_bg, width=2)
    # Footnote can be long; allow wider wrapping so it doesn't cut off
    style.configure("LoginFootnote.TLabel", background=SURFACE, foreground=_blend_hex(SUBTLE, TEXT, 0.6), font=FONT_SMALL,
                    wraplength=520, justify="left")
    strength_trough = _blend_hex(SURFACE, BG, 0.3 if dark else 0.18)
    style.configure(
        "StrengthWeak.Horizontal.TProgressbar",
        troughcolor=strength_trough,
        bordercolor=strength_trough,
        background="#ff6b6b",
        lightcolor="#ffa8a8",
        darkcolor="#d64545",
        thickness=12
    )
    style.configure(
        "StrengthMedium.Horizontal.TProgressbar",
        troughcolor=strength_trough,
        bordercolor=strength_trough,
        background="#f7b731",
        lightcolor="#ffd56b",
        darkcolor="#c28b07",
        thickness=12
    )
    style.configure(
        "StrengthStrong.Horizontal.TProgressbar",
        troughcolor=strength_trough,
        bordercolor=strength_trough,
        background="#3ddc84",
        lightcolor="#8af7b8",
        darkcolor="#25a05a",
        thickness=12
    )

    def card(parent):
        # Simple elevated frame without canvas outlines to avoid "grid" look
        outer = ttk.Frame(parent, style="Card.TFrame", padding=(18,18,18,18))
        outer.grid_columnconfigure(0, weight=1)
        outer._card_inner = outer
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
        "HERO_GRADIENT": (hero_top, hero_bottom),
        "CARD_SHADOW": _blend_hex(BG, "#000000", 0.35 if dark else 0.2),
    }
    return card, palette

_initial_settings = load_s3_settings()

# ---------------- GUI root ----------------
root = tk.Tk()
root.title("S3 / MinIO Manager")
root.geometry("1280x780")
root.minsize(960, 600)
_center_window(root)

card, palette = apply_theme(root)
THEME_PALETTE.clear()
THEME_PALETTE.update(palette)

if not _start_login(root, card):
    sys.exit(0)

HEADER_TOPBAR = create_topbar(
    root,
    username=CURRENT_USER.get("username") or "User",
    on_logout=lambda: show_login_overlay(root, card)
)
APP_BODY = ttk.Frame(root)
APP_BODY.pack(fill="both", expand=True)
_refresh_header_info()
create_dashboard(APP_BODY)

root.update_idletasks()
_set_initial_window_size(root)
root.deiconify()




notebook = ttk.Notebook(root)
notebook.pack(fill="both", expand=True, padx=12, pady=(0,10))

# --- Upgrade plain inputs in all tabs to RoundedField (excludes login/register) ---

def _apply_rounded_to_current_tab(*_):
    try:
        sel = notebook.select()
        if sel:
            tab = notebook.nametowidget(sel)
            _upgrade_inputs_in(tab)
    except Exception:
        pass

# Run once after the UI settles and also on every tab change
root.after(600, lambda: _upgrade_inputs_in(root))
notebook.bind("<<NotebookTabChanged>>", _apply_rounded_to_current_tab)

# --- Upgrade plain inputs in all tabs to RoundedField (excludes login/register) ---

def _apply_rounded_to_current_tab(*_):
    try:
        sel = notebook.select()
        if sel:
            tab = notebook.nametowidget(sel)
            _upgrade_inputs_in(tab)
    except Exception:
        pass

# Run once after the UI settles and also on every tab change
root.after(600, lambda: _upgrade_inputs_in(root))
notebook.bind("<<NotebookTabChanged>>", _apply_rounded_to_current_tab)


statusbar = ttk.Label(root, text="Ready", style="Small.TLabel", anchor="w")
statusbar.pack(fill="x", padx=16, pady=(0,12))

cancel_event = threading.Event()
PADX = 10; PADY = 8
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
up_key.trace_add("write", _mark_up_key_touched)
up_file = tk.StringVar()
up_bucket.trace_add("write", lambda *_: _refresh_upload_button(reschedule=False))
up_key.trace_add("write",    lambda *_: _refresh_upload_button(reschedule=False))
up_file.trace_add("write",   lambda *_: _refresh_upload_button(reschedule=False))
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
    from tkinter import filedialog
    f = filedialog.askopenfilename()
    if not f:
        return

    # set the path field
    try:
        up_file.set(f)
    except Exception:
        pass

    # auto-fill the object key if user hasn't manually edited it (or it's empty)
    base = os.path.basename(f)
    global _UP_KEY_TOUCHED, _UP_KEY_SET_PROGRAMMATICALLY
    try:
        current = up_key.get().strip()
    except Exception:
        current = ""

    if (not _UP_KEY_TOUCHED) or (not current):
        _UP_KEY_SET_PROGRAMMATICALLY = True
        try:
            up_key.set(base)
        finally:
            _UP_KEY_SET_PROGRAMMATICALLY = False

    # optional: focus the Start button
    try:
        up_btn_start.focus_set()
    except Exception:
        pass
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

s_info_section = ttk.Frame(s_card, style="Info.TFrame", padding=(22,18))
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
s_bottom_spacer = ttk.Frame(s_card, style="Section.TFrame", height=28)
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
        try:
            if hasattr(control, "winfo_exists") and not control.winfo_exists():
                return
        except Exception:
            pass
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
    _provider_snapshot[provider] = _current_provider_state()
    _refresh_configuration_status()

def _on_endpoint_change():
    """Invoked by endpoint StringVar traces. Intentionally no-op so we do not
    rebuild the Settings form while Tk is still processing widget changes."""
    pass

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
cfg_custom_endpoint.trace_add("write", lambda *_: _on_endpoint_change())
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
    global _upload_busy
    _upload_busy = True

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
    global _upload_busy
    _upload_busy = False
    _refresh_upload_button(reschedule=False)
    _reset_progress_metrics(up_status)
    up_status.config(text="Cancelling…")
    statusbar.config(text="Cancelling…")
    up_meta_label.config(text="Cancelling upload…")

up_btn_start.config(command=upload_start)
up_btn_cancel.config(command=upload_cancel)
# Start periodic validation for the Start Upload button
_refresh_upload_button(reschedule=True)

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
    global _upload_busy
    _upload_busy = False
    start_btn.config(state="normal")
    cancel_btn.config(state="disabled")
    statusbar.config(text="Ready")
    _refresh_upload_button(reschedule=False)
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