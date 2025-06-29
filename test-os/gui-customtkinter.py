import os
import customtkinter as ctk
from tkinter import ttk

class FileExplorer(ctk.CTk):
    def __init__(self):
        super().__init__()
        self.title("File Explorer (CustomTkinter)")
        self.geometry("800x600")
        
        # Настройка темы
        ctk.set_appearance_mode("System")  # "Light" или "Dark"
        ctk.set_default_color_theme("blue")  # "green", "dark-blue" и др.
        
        self.current_file = None
        self.create_widgets()
        self.load_initial_directory()
    
    def create_widgets(self):
        # Главный фрейм
        self.main_frame = ctk.CTkFrame(self)
        self.main_frame.pack(fill="both", expand=True, padx=10, pady=10)
        
        # Панель инструментов
        self.toolbar = ctk.CTkFrame(self.main_frame, height=40)
        self.toolbar.pack(fill="x", pady=(0, 5))
        
        # Кнопки
        self.refresh_btn = ctk.CTkButton(
            self.toolbar, 
            text="Refresh", 
            width=80,
            command=self.refresh_tree
        )
        self.refresh_btn.pack(side="left", padx=5)
        
        self.home_btn = ctk.CTkButton(
            self.toolbar,
            text="Home",
            width=80,
            command=self.go_home
        )
        self.home_btn.pack(side="left", padx=5)
        
        # Разделитель с Treeview и содержимым
        self.splitter = ctk.CTkFrame(self.main_frame)
        self.splitter.pack(fill="both", expand=True)
        
        # Дерево файлов (используем стандартный ttk.Treeview)
        self.tree_frame = ctk.CTkFrame(self.splitter, width=300)
        self.tree_frame.pack(side="left", fill="y")
        
        self.tree = ttk.Treeview(
            self.tree_frame,
            columns=("type",),
            show="tree headings",
            selectmode="browse"
        )
        self.tree.heading("#0", text="Name", anchor="w")
        self.tree.heading("type", text="Type", anchor="w")
        
        # Стилизация Treeview под CustomTkinter
        style = ttk.Style()
        style.theme_use("default")
        style.configure(
            "Treeview",
            background="#2b2b2b",
            foreground="white",
            fieldbackground="#2b2b2b",
            borderwidth=0
        )
        style.map("Treeview", background=[("selected", "#3b8ed0")])
        
        self.tree_scroll = ctk.CTkScrollbar(
            self.tree_frame,
            orientation="vertical",
            command=self.tree.yview
        )
        self.tree.configure(yscrollcommand=self.tree_scroll.set)
        
        self.tree.pack(side="left", fill="both", expand=True)
        self.tree_scroll.pack(side="right", fill="y")
        
        # Содержимое файла
        self.content_frame = ctk.CTkFrame(self.splitter)
        self.content_frame.pack(side="right", fill="both", expand=True, padx=(5, 0))
        
        self.path_label = ctk.CTkLabel(
            self.content_frame,
            text="No file selected",
            anchor="w",
            justify="left"
        )
        self.path_label.pack(fill="x", padx=5, pady=(0, 5))
        
        self.textbox = ctk.CTkTextbox(
            self.content_frame,
            wrap="word",
            font=("Consolas", 12)
        )
        self.textbox.pack(fill="both", expand=True)
        
        # Привязка событий
        self.tree.bind("<<TreeviewOpen>>", self.on_tree_open)
        self.tree.bind("<Double-1>", self.on_double_click)
    
    def load_initial_directory(self):
        """Загружает начальную директорию"""
        self.load_directory(os.path.expanduser("~"))  # Домашняя директория
    
    def load_directory(self, path):
        """Загружает содержимое директории"""
        self.tree.delete(*self.tree.get_children())
        
        try:
            for item in os.listdir(path):
                full_path = os.path.join(path, item)
                if os.path.isdir(full_path):
                    # Для папок добавляем узел с возможностью раскрытия
                    node = self.tree.insert(
                        "",
                        "end",
                        text=item,
                        values=("Directory",),
                        open=False
                    )
                    # Добавляем фиктивный узел для отображения "+"
                    self.tree.insert(node, "end", text="loading...")
                else:
                    # Для файлов просто добавляем элемент
                    self.tree.insert(
                        "",
                        "end",
                        text=item,
                        values=("File",)
                    )
        except PermissionError:
            self.tree.insert("", "end", text="Permission denied", values=("Error",))
    
    def on_tree_open(self, event):
        """Обработчик раскрытия узла"""
        item = self.tree.focus()
        path = self.get_full_path(item)
        
        if os.path.isdir(path):
            self.tree.delete(*self.tree.get_children(item))
            self.load_subdirectory(item, path)
    
    def load_subdirectory(self, parent, path):
        """Загружает поддиректорию"""
        try:
            for item in os.listdir(path):
                full_path = os.path.join(path, item)
                if os.path.isdir(full_path):
                    node = self.tree.insert(
                        parent,
                        "end",
                        text=item,
                        values=("Directory",),
                        open=False
                    )
                    # Добавляем фиктивный узел для отображения "+"
                    self.tree.insert(node, "end", text="loading...")
                else:
                    self.tree.insert(
                        parent,
                        "end",
                        text=item,
                        values=("File",)
                    )
        except PermissionError:
            self.tree.insert(parent, "end", text="Permission denied", values=("Error",))
    
    def on_double_click(self, event):
        """Обработчик двойного клика"""
        item = self.tree.focus()
        path = self.get_full_path(item)
        
        if os.path.isfile(path):
            self.show_file_content(path)
    
    def get_full_path(self, item):
        """Возвращает полный путь к элементу"""
        path = []
        while item:
            path.append(self.tree.item(item, "text"))
            item = self.tree.parent(item)
        return os.path.join(*reversed(path))
    
    def show_file_content(self, path):
        """Показывает содержимое файла"""
        self.current_file = path
        self.path_label.configure(text=f"Selected: {path}")
        
        try:
            with open(path, "r", encoding="utf-8") as f:
                content = f.read()
                self.textbox.delete("1.0", "end")
                self.textbox.insert("1.0", content)
        except UnicodeDecodeError:
            try:
                with open(path, "r", encoding="cp1251") as f:
                    content = f.read()
                    self.textbox.delete("1.0", "end")
                    self.textbox.insert("1.0", content)
            except Exception as e:
                self.textbox.delete("1.0", "end")
                self.textbox.insert("1.0", f"Cannot read file: {str(e)}")
        except Exception as e:
            self.textbox.delete("1.0", "end")
            self.textbox.insert("1.0", f"Error reading file: {str(e)}")
    
    def refresh_tree(self):
        """Обновляет дерево"""
        if self.tree.get_children():
            root_item = self.tree.get_children()[0]
            path = self.get_full_path(root_item)
            self.load_directory(path)
    
    def go_home(self):
        """Переходит в домашнюю директорию"""
        self.load_directory(os.path.expanduser("~"))

if __name__ == "__main__":
    app = FileExplorer()
    app.mainloop()