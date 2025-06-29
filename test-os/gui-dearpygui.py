import dearpygui.dearpygui as dpg
import os

# Создаем контекст Dear PyGui
dpg.create_context()

# Переменные для хранения информации о выбранном файле
selected_file = ""
selected_file_content = ""

def update_file_tree(sender, app_data, user_data):
    """Обновляет дерево файлов для указанного пути"""
    path = user_data
    with dpg.tree_node(label=os.path.basename(path), parent="file_tree"):
        for item in os.listdir(path):
            full_path = os.path.join(path, item)
            if os.path.isdir(full_path):
                # Для папок создаем узел с возможностью раскрытия
                with dpg.tree_node(label=item):
                    dpg.add_button(label="Load", callback=update_file_tree, user_data=full_path)
            else:
                # Для файлов добавляем кнопку для просмотра содержимого
                dpg.add_button(label=item, callback=show_file_content, user_data=full_path)

def show_file_content(sender, app_data, user_data):
    """Показывает содержимое выбранного файла"""
    global selected_file, selected_file_content
    selected_file = user_data
    try:
        with open(selected_file, 'r', encoding='utf-8') as f:
            selected_file_content = f.read()
    except Exception as e:
        selected_file_content = f"Error reading file: {str(e)}"
    
    # Обновляем текст в окне просмотра
    dpg.set_value("file_content_text", selected_file_content)
    dpg.set_value("file_path_text", f"Selected: {selected_file}")

def refresh_file_tree():
    """Обновляет все дерево файлов"""
    dpg.delete_item("file_tree", children_only=True)
    update_file_tree(None, None, os.getcwd())

# Создаем основное окно
with dpg.window(label="File Explorer", tag="primary_window"):
    dpg.add_text("File Explorer", color=(0, 255, 0))
    
    # Добавляем кнопку обновления
    dpg.add_button(label="Refresh", callback=refresh_file_tree)
    
    # Добавляем текстовое поле для отображения пути к файлу
    dpg.add_text("No file selected", tag="file_path_text")
    
    # Создаем разделитель
    dpg.add_separator()
    
    # Создаем группу с двумя колонками
    with dpg.group(horizontal=True):
        # Левая колонка - дерево файлов
        with dpg.child_window(width=300):
            with dpg.tree_node(label="File Tree", tag="file_tree"):
                pass  # Дерево будет заполнено при обновлении
        
        # Правая колонка - содержимое файла
        with dpg.child_window():
            dpg.add_text("File Content:")
            dpg.add_input_text(
                multiline=True, 
                width=-1, 
                height=-1, 
                tag="file_content_text",
                readonly=True
            )

# Инициализируем дерево файлов текущей директорией
refresh_file_tree()

# Создаем viewport и запускаем приложение
dpg.create_viewport(title='File Explorer', width=800, height=600)
dpg.setup_dearpygui()
dpg.show_viewport()
dpg.set_primary_window("primary_window", True)
dpg.start_dearpygui()
dpg.destroy_context()