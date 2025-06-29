import tkinter as tk
from tkinter import ttk

class ResizableTextPanels:
    def __init__(self, root):
        self.root = root
        self.root.title("Резиновые текстовые поля")
        
        # Создаем панель для разделителя и текстовых полей
        self.panewindow = ttk.PanedWindow(root, orient=tk.HORIZONTAL)
        self.panewindow.pack(fill=tk.BOTH, expand=True)
        
        # Первое текстовое поле
        self.text1 = tk.Text(self.panewindow, wrap=tk.WORD)
        self.panewindow.add(self.text1, weight=1)
        
        # Второе текстовое поле
        self.text2 = tk.Text(self.panewindow, wrap=tk.WORD)
        self.panewindow.add(self.text2, weight=1)
        
        # Добавляем текст для демонстрации
        self.text1.insert(tk.END, "Это левое текстовое поле.\nВы можете перемещать разделитель.")
        self.text2.insert(tk.END, "Это правое текстовое поле.\nПопробуйте перетащить разделитель между полями.")

if __name__ == "__main__":
    root = tk.Tk()
    app = ResizableTextPanels(root)
    root.mainloop()