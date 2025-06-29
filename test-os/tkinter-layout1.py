import tkinter as tk

root = tk.Tk()
root.title("Панель с кнопками и текстовым полем")

# Создаем фрейм для верхней панели
top_frame = tk.Frame(root)
top_frame.pack(side=tk.TOP, fill=tk.X, padx=5, pady=5)

# Создаем 5 колонок
columns = [
    [f"Кнопка 1-1"],  # 1 кнопка
    [f"Кнопка 2-1", f"Кнопка 2-2"],  # 2 кнопки
    [f"Кнопка 3-1", f"Кнопка 3-2", f"Кнопка 3-3"],  # 3 кнопки
    [f"Кнопка 4-1", f"Кнопка 4-2", f"Кнопка 4-3"],  # 3 кнопки
    [f"Кнопка 5-1", f"Кнопка 5-2"]  # 2 кнопки
]

# Создаем кнопки в каждой колонке
for i, buttons in enumerate(columns, 1):
    column_frame = tk.Frame(top_frame)
    column_frame.pack(side=tk.LEFT, fill=tk.Y, padx=5, expand=True)
    
    for j, button_text in enumerate(buttons, 1):
        btn = tk.Button(column_frame, text=button_text)
        btn.pack(side=tk.TOP, fill=tk.X, pady=2)

# Создаем текстовое поле внизу
text_field = tk.Text(root, height=15)
text_field.pack(side=tk.BOTTOM, fill=tk.BOTH, expand=True, padx=5, pady=5)

root.mainloop()