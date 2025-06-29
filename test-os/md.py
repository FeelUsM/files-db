import tkinter as tk
from tkinter import ttk
from tkhtmlview import HTMLLabel
import markdown

def update_preview(event=None):
    md_text = text_editor.get("1.0", tk.END)
    html = markdown.markdown(md_text)
    html_editor.delete("1.0",tk.END)
    html_editor.insert("1.0",html)
    html_view.set_html(html)

# Окно
root = tk.Tk()
root.title("Markdown Editor")
root.geometry("800x600")

# Панель
paned = ttk.PanedWindow(root, orient=tk.HORIZONTAL)
paned.pack(fill=tk.BOTH, expand=True)

# Левая часть — Text
text_editor = tk.Text(paned, wrap="word")
text_editor.insert("1.0", "# Markdown Editor\n\nНапиши что-нибудь...")
text_editor.bind("<KeyRelease>", update_preview)
paned.add(text_editor)

html_editor = tk.Text(paned, wrap="word")
paned.add(html_editor)

# Правая часть — HTML preview
preview_frame = tk.Frame(paned)
html_view = HTMLLabel(preview_frame, html="", background="white")
html_view.pack(fill=tk.BOTH, expand=True, padx=1, pady=1)
paned.add(preview_frame)

# Первый рендер
update_preview()

# Запуск
root.mainloop()
