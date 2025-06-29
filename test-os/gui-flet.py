import os
import flet as ft

class FileExplorer:
    def __init__(self, page: ft.Page):
        self.page = page
        self.page.title = "File Explorer (Flet)"
        self.page.theme_mode = ft.ThemeMode.SYSTEM
        self.current_file = None
        self.current_path = os.path.expanduser("~")
        self.setup_ui()

    def setup_ui(self):
        # Создаем AppBar
        self.page.appbar = ft.AppBar(
            title=ft.Text("File Explorer"),
            actions=[
                ft.IconButton(ft.icons.REFRESH, on_click=self.refresh_tree),
                ft.IconButton(ft.icons.HOME, on_click=self.go_home)
            ]
        )

        # Создаем разделитель с деревом файлов и содержимым
        self.path_label = ft.Text(f"Current path: {self.current_path}")
        
        self.tree_view = ft.Column(
            expand=True,
            scroll=ft.ScrollMode.AUTO,
            spacing=0
        )
        
        self.file_content = ft.TextField(
            multiline=True,
            expand=True,
            read_only=True,
            border=ft.InputBorder.NONE
        )
        
        self.content_view = ft.Column([
            ft.Text("File content:", weight=ft.FontWeight.BOLD),
            self.file_content
        ], expand=True)
        
        splitter = ft.Row([
            ft.Container(
                content=self.tree_view,
                width=300,
                padding=10,
                border=ft.border.all(1, ft.colors.OUTLINE)
            ),
            ft.VerticalDivider(width=1),
            ft.Container(
                content=self.content_view,
                expand=True,
                padding=10
            )
        ], expand=True)
        
        self.page.add(
            self.path_label,
            splitter
        )
        
        self.load_directory(self.current_path)

    def load_directory(self, path):
        """Загружает содержимое директории"""
        self.current_path = path
        self.path_label.value = f"Current path: {self.path_label}"
        self.tree_view.controls.clear()
        
        try:
            # Кнопка для перехода на уровень выше
            if path != os.path.expanduser("~"):
                parent_path = os.path.dirname(path)
                self.tree_view.controls.append(
                    ft.ListTile(
                        leading=ft.Icon(ft.icons.ARROW_UPWARD),
                        title=ft.Text(".. (Parent directory)"),
                        on_click=lambda e: self.load_directory(parent_path)
                    )
                )
            
            # Содержимое текущей директории
            for item in sorted(os.listdir(path)):
                full_path = os.path.join(path, item)
                if os.path.isdir(full_path):
                    self.tree_view.controls.append(
                        ft.ListTile(
                            leading=ft.Icon(ft.icons.FOLDER),
                            title=ft.Text(item),
                            on_click=lambda e, p=full_path: self.load_directory(p)
                        )
                    )
                else:
                    self.tree_view.controls.append(
                        ft.ListTile(
                            leading=ft.Icon(ft.icons.INSERT_DRIVE_FILE),
                            title=ft.Text(item),
                            on_click=lambda e, p=full_path: self.show_file_content(p)
                        )
                    )
        except PermissionError:
            self.tree_view.controls.append(
                ft.Text("Permission denied", color=ft.colors.ERROR)
            )
        
        self.page.update()

    def show_file_content(self, path):
        """Показывает содержимое файла"""
        self.current_file = path
        try:
            with open(path, "r", encoding="utf-8") as f:
                content = f.read()
                self.file_content.value = content
        except UnicodeDecodeError:
            try:
                with open(path, "r", encoding="cp1251") as f:
                    content = f.read()
                    self.file_content.value = content
            except Exception as e:
                self.file_content.value = f"Cannot read file: {str(e)}"
        except Exception as e:
            self.file_content.value = f"Error reading file: {str(e)}"
        
        self.page.update()

    def refresh_tree(self, e):
        """Обновляет дерево файлов"""
        self.load_directory(self.current_path)

    def go_home(self, e):
        """Переходит в домашнюю директорию"""
        self.load_directory(os.path.expanduser("~"))

def main(page: ft.Page):
    page.window_width = 800
    page.window_height = 600
    FileExplorer(page)

ft.app(target=main)