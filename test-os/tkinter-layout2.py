import tkinter as tk
from tkinter import ttk

main_win = tk.Tk()
main_win.title("changes monitor")
main_win.minsize(200,150)

main_win.grid_rowconfigure(1, weight=1)  # середина
main_win.grid_columnconfigure(0, weight=1)

# Верхняя метка
top_frame = tk.Frame(main_win)
top_frame.grid(row=0, column=0, sticky='ew', padx=5, pady=5)
a_label = ttk.Label(top_frame, text='321')
a_label.pack(side=tk.LEFT)

# Центральная часть (растягивается)
panewindow = ttk.PanedWindow(main_win, orient=tk.HORIZONTAL)
panewindow.grid(row=1, column=0, sticky='nsew', padx=4, pady=4)
text2 = tk.Text(panewindow, wrap=tk.WORD)
panewindow.add(text2, weight=1)

# Нижняя метка
bottom_frame = tk.Frame(main_win)
bottom_frame.grid(row=2, column=0, sticky='ew', padx=5, pady=5)
b_label = ttk.Label(bottom_frame, text='123')
b_label.pack(side=tk.LEFT)

main_win.mainloop()
