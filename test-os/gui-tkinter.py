import os
import tkinter as tk
from tkinter import ttk, scrolledtext, messagebox

class FileExplorer:
    def __init__(self, root):
        self.root = root
        self.root.title("File Explorer")
        self.root.geometry("800x600")
        
        # Основные переменные
        self.current_file = None
        
        # Создаем интерфейс
        self.create_widgets()
        
        # Загружаем начальную директорию
        self.load_directory(os.getcwd())
    
    def create_widgets(self):
        # Фрейм для дерева файлов
        self.tree_frame = ttk.Frame(self.root)
        self.tree_frame.pack(side=tk.LEFT, fill=tk.Y, padx=5, pady=5)
        
        # Кнопка обновления
        self.refresh_btn = ttk.Button(
            self.tree_frame, 
            text="Refresh", 
            command=self.refresh_tree
        )
        self.refresh_btn.pack(fill=tk.X)
        
        # Дерево файлов
        self.tree = ttk.Treeview(self.tree_frame)
        self.tree.pack(expand=True, fill=tk.BOTH)
        
        # Настраиваем столбцы и заголовок
        self.tree["columns"] = ("type",)
        self.tree.heading("#0", text="Name", anchor=tk.W)
        self.tree.heading("type", text="Type", anchor=tk.W)
        
        # Привязываем двойной клик
        self.tree.bind("<Double-1>", self.on_double_click)
        
        # Фрейм для содержимого файла
        self.content_frame = ttk.Frame(self.root)
        self.content_frame.pack(side=tk.RIGHT, expand=True, fill=tk.BOTH, padx=5, pady=5)
        
        # Путь к файлу
        self.file_path_label = ttk.Label(
            self.content_frame, 
            text="No file selected",
            anchor=tk.W
        )
        self.file_path_label.pack(fill=tk.X)
        
        # Текст с содержимым файла
        self.file_content = scrolledtext.ScrolledText(
            self.content_frame,
            wrap=tk.WORD,
            state=tk.DISABLED
        )
        self.file_content.pack(expand=True, fill=tk.BOTH)
    
    def load_directory(self, path):
        """Загружает содержимое директории в дерево"""
        self.tree.delete(*self.tree.get_children())
        self.add_node("", path, "Directory")
    
    def add_node(self, parent, path, node_type):
        """Добавляет узел в дерево"""
        node = self.tree.insert(
            parent, 
            tk.END, 
            text=os.path.basename(path), 
            values=(node_type,),
            open=False
        )
        
        if os.path.isdir(path):
            # Для директорий добавляем пустой узел (будет заполнен при раскрытии)
            self.tree.insert(node, tk.END, text="Loading...")
        
        return node
    
    def on_double_click(self, event):
        """Обработчик двойного клика по узлу"""
        item = self.tree.focus()
        path = self.get_full_path(item)
        
        if os.path.isdir(path):
            # Для директорий - раскрываем/закрываем
            if self.tree.item(item, "open"):
                self.tree.item(item, open=False)
                self.tree.delete(*self.tree.get_children(item))
                # Добавляем placeholder для возможного повторного раскрытия
                self.tree.insert(item, tk.END, text="Loading...")
            else:
                self.tree.item(item, open=True)
                self.load_subdirectories(item, path)
        else:
            # Для файлов - показываем содержимое
            self.show_file_content(path)
    
    def load_subdirectories(self, parent, path):
        """Загружает поддиректории"""
        self.tree.delete(*self.tree.get_children(parent))
        
        try:
            for item in os.listdir(path):
                full_path = os.path.join(path, item)
                if os.path.isdir(full_path):
                    self.add_node(parent, full_path, "Directory")
                else:
                    self.add_node(parent, full_path, "File")
        except PermissionError:
            self.tree.insert(parent, tk.END, text="Permission denied")
        except Exception as e:
            self.tree.insert(parent, tk.END, text=f"Error: {str(e)}")
    
    def get_full_path(self, item):
        """Возвращает полный путь к выбранному узлу"""
        path = []
        while item:
            path.append(self.tree.item(item, "text"))
            item = self.tree.parent(item)
        return os.path.join(*reversed(path))
    
    def show_file_content(self, path):
        """Показывает содержимое файла"""
        self.current_file = path
        self.file_path_label.config(text=f"Selected: {path}")
        
        self.file_content.config(state=tk.NORMAL)
        self.file_content.delete(1.0, tk.END)
        
        try:
            with open(path, 'r', encoding='utf-8') as f:
                content = f.read()
                self.file_content.insert(tk.END, content)
        except UnicodeDecodeError:
            try:
                with open(path, 'r', encoding='cp1251') as f:
                    content = f.read()
                    self.file_content.insert(tk.END, content)
            except Exception as e:
                self.file_content.insert(tk.END, f"Cannot read file: {str(e)}")
        except Exception as e:
            self.file_content.insert(tk.END, f"Error reading file: {str(e)}")
        
        self.file_content.config(state=tk.DISABLED)
    
    def refresh_tree(self):
        """Обновляет дерево файлов"""
        if self.tree.get_children():
            root_item = self.tree.get_children()[0]
            path = self.get_full_path(root_item)
            self.load_directory(path)

if __name__ == "__main__":
    root = tk.Tk()
    app = FileExplorer(root)
    root.mainloop()