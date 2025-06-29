import os
import sys
from PySide6.QtWidgets import (QApplication, QMainWindow, QTreeView, QSplitter, 
                            QTextEdit, QFileSystemModel, QVBoxLayout, QWidget,
                            QLabel, QToolBar)
from PySide6.QtGui import QAction
from PySide6.QtCore import QDir, Qt

class FileExplorer(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("File Explorer")
        self.setGeometry(100, 100, 800, 600)
        
        self.current_file = None
        self.init_ui()
        
    def init_ui(self):
        # Создаем модель файловой системы
        self.model = QFileSystemModel()
        self.model.setRootPath(QDir.rootPath())
        self.model.setFilter(QDir.AllEntries | QDir.NoDotAndDotDot | QDir.AllDirs | QDir.Files)
        
        # Создаем древовидное представление
        self.tree = QTreeView()
        self.tree.setModel(self.model)
        self.tree.setRootIndex(self.model.index(QDir.currentPath()))
        self.tree.setAnimated(False)
        self.tree.setIndentation(20)
        self.tree.setSortingEnabled(True)
        self.tree.doubleClicked.connect(self.on_double_click)
        
        # Создаем панель инструментов
        toolbar = QToolBar()
        self.addToolBar(toolbar)
        
        # Кнопка обновления
        refresh_action = QAction("Refresh", self)
        refresh_action.triggered.connect(self.refresh_tree)
        toolbar.addAction(refresh_action)
        
        # Кнопка для перехода в домашнюю директорию
        home_action = QAction("Home", self)
        home_action.triggered.connect(self.go_home)
        toolbar.addAction(home_action)
        
        # Виджет для отображения пути к файлу
        self.path_label = QLabel("No file selected")
        self.path_label.setAlignment(Qt.AlignLeft)
        self.path_label.setStyleSheet("padding: 5px;")
        
        # Текстовое поле для содержимого файла
        self.text_edit = QTextEdit()
        self.text_edit.setReadOnly(True)
        
        # Создаем layout для правой панели
        right_panel = QWidget()
        right_layout = QVBoxLayout()
        right_layout.addWidget(self.path_label)
        right_layout.addWidget(self.text_edit)
        right_panel.setLayout(right_layout)
        
        # Разделитель для дерева и содержимого
        splitter = QSplitter(Qt.Horizontal)
        splitter.addWidget(self.tree)
        splitter.addWidget(right_panel)
        splitter.setSizes([200, 600])
        
        # Устанавливаем центральный виджет
        central_widget = QWidget()
        central_layout = QVBoxLayout()
        central_layout.addWidget(splitter)
        central_widget.setLayout(central_layout)
        self.setCentralWidget(central_widget)
    
    def on_double_click(self, index):
        """Обработчик двойного клика по элементу"""
        path = self.model.filePath(index)
        
        if os.path.isfile(path):
            self.show_file_content(path)
    
    def show_file_content(self, path):
        """Показывает содержимое файла"""
        self.current_file = path
        self.path_label.setText(f"Selected: {path}")
        
        try:
            with open(path, 'r', encoding='utf-8') as f:
                content = f.read()
                self.text_edit.setPlainText(content)
        except UnicodeDecodeError:
            try:
                with open(path, 'r', encoding='cp1251') as f:
                    content = f.read()
                    self.text_edit.setPlainText(content)
            except Exception as e:
                self.text_edit.setPlainText(f"Cannot read file: {str(e)}")
        except Exception as e:
            self.text_edit.setPlainText(f"Error reading file: {str(e)}")
    
    def refresh_tree(self):
        """Обновляет дерево файлов"""
        current_index = self.tree.currentIndex()
        if current_index.isValid():
            path = self.model.filePath(current_index)
            self.model.setRootPath("")  # Сброс модели
            self.model.setRootPath(path)  # Перезагрузка
    
    def go_home(self):
        """Переходит в домашнюю директорию пользователя"""
        home_path = QDir.homePath()
        self.tree.setRootIndex(self.model.index(home_path))

if __name__ == "__main__":
    app = QApplication(sys.argv)
    window = FileExplorer()
    window.show()
    sys.exit(app.exec_())