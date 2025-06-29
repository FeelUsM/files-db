import tkinter as tk
from pystray import Icon as TrayIcon, Menu as TrayMenu, MenuItem as TrayMenuItem
from PIL import Image, ImageDraw
import threading
import sys

# Функция для создания иконки
def create_image():
    image = Image.new('RGB', (64, 64), color=(255, 0, 0))
    draw = ImageDraw.Draw(image)
    draw.rectangle((16, 16, 48, 48), fill=(0, 0, 255))
    return image

# Главное окно
root = tk.Tk()
root.title("Tray Example")
root.geometry("300x200")

# Функция, вызываемая при сворачивании окна
def on_minimize(event):
    if root.state() == 'iconic':
        root.withdraw()  # скрываем окно
        show_tray_icon()

# Возврат окна
def show_window(icon=None, item=None):
    tray_icon.stop()
    root.after(0, root.deiconify)

# Выход из приложения
def quit_app(icon=None, item=None):
    tray_icon.stop()
    root.destroy()
    sys.exit()

# Создание иконки в трее
def show_tray_icon():
    global tray_icon
    image = Image.open("icon.png")  # Используем файл, а не нарисованную иконку
    menu = TrayMenu(
        TrayMenuItem("Показать", show_window),
        TrayMenuItem("Выход", quit_app)
    )
    tray_icon = TrayIcon("TrayApp", image, menu=menu)
    print("Launching tray icon")
    threading.Thread(target=tray_icon.run, daemon=True).start()

# Обработка события "сворачивание"
root.bind("<Unmap>", on_minimize)

# Старт GUI
tk.Label(root, text="Сверни окно — оно уйдёт в трей").pack(pady=50)
root.mainloop()
