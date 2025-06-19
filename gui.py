import tkinter as tk
from tkinter import ttk
import customtkinter as ctk

from pprint import pprint
from filesdb import filesdb
#fdb = filesdb('root.db', server_in='fifo')


# добавляем отделы
#def by_path(path):
#    x = root
#    for q in path:
#        x = x[q]
#    return x
#
#def insert(path,elem): # в элемент по адресу path[:-1] добавляем элемент по адресу path
#    #path = json.loads(spath)
#    spath = json.dumps(path)
#    parent_spath = '' if len(path)==0 else json.dumps(path[:-1])
#    key = '' if len(path)==0 else repr(path[-1])+' : '
#    #elem = by_path(path)
#    if type(elem)==list:
#        if len(elem)==0:
#            tree.insert(parent_spath, tk.END, iid=spath, text=key+"[]")
#        else:
#            tree.insert(parent_spath, tk.END, iid=spath, text=key+"[ ... ]")
#    elif type(elem)==dict:
#        if len(elem.keys())==0:
#            tree.insert(parent_spath, tk.END, iid=spath, text=key+"{}")
#        else:
#            tree.insert(parent_spath, tk.END, iid=spath, text=key+"{ ... }")
#    elif type(elem)==str or type(elem)==int or type(elem)==float:
#        tree.insert(parent_spath, tk.END, iid=spath, text=key+repr(elem))
#    elif elem is True:
#        tree.insert(parent_spath, tk.END, iid=spath, text=key+'true')
#    elif elem is False:
#        tree.insert(parent_spath, tk.END, iid=spath, text=key+'false')
#    elif elem is None:
#        tree.insert(parent_spath, tk.END, iid=spath, text=key+'null')
#    else:
#        print(f'unknown type at {spath} : {type(elem)}')
#
#
#def on_treeview_click(event):
#    # Получаем объект Treeview
#    tree = event.widget
#    # Получаем индекс выделенного элемента
#    selected_item = tree.identify_row(event.y)
#    
#    # Если у элемента нет детей
#    if not tree.get_children(selected_item):
#        print("Добавляем элемент с данными: ",selected_item)
#        path = json.loads(selected_item)
#        elem = by_path(path)
#        if type(elem)==dict:
#            for k,v in elem.items():
#                insert(path+[k],v)
#            tree.item(selected_item, open=True) 
#        if type(elem)==list:
#            for k,v in enumerate(elem):
#                insert(path+[k],v)
#            tree.item(selected_item, open=True) 
#    else:
#        tree.item(selected_item, open=not tree.item(selected_item, 'open')) 
#    #tree.selection_set(selected_item)
#    return "break"

main_win = tk.Tk()
main_win.title("changes monitor")
main_win.minsize(200,150)

main_win.grid_rowconfigure(1, weight=1)  # середина
main_win.grid_columnconfigure(0, weight=1)

top_frame = tk.Frame(main_win)
top_frame.grid(row=0, column=0, sticky='ew', padx=5, pady=5)

panewindow = ttk.PanedWindow(main_win, orient=tk.HORIZONTAL)
panewindow.grid(row=1, column=0, sticky='nsew', padx=4, pady=4)

bottom_frame = tk.Frame(main_win)
bottom_frame.grid(row=2, column=0, sticky='ew', padx=5, pady=5)


def set_equal_sash():
    total_width = panewindow.winfo_width()
    panewindow.sashpos(0, total_width // 2)
main_win.update_idletasks()  # Убедиться, что окно отрисовано и имеет размеры
main_win.after(100, set_equal_sash)  # Установить sash немного позже, чтобы размеры гарантированно применились

# =============================================
treeframe = tk.Frame(panewindow)
#treeframe.pack(expand=True)
panewindow.add(treeframe, weight=1)

treeview = ttk.Treeview(treeframe)
treeview.pack(side=tk.LEFT, expand=1, fill=tk.BOTH)

# Создаем вертикальную полосу прокрутки
tree_scrollbar = tk.Scrollbar(treeframe, orient="vertical", command=treeview.yview)
tree_scrollbar.pack(side=tk.RIGHT, fill=tk.Y)

# Привязываем Treeview к полосам прокрутки
treeview.configure(yscrollcommand=tree_scrollbar.set)

text2 = tk.Text(panewindow, wrap=tk.WORD)
panewindow.add(text2, weight=1)

text2.insert(tk.END, "Это правое текстовое поле.\nПопробуйте перетащить разделитель между полями.")

# =============================================
first_frame = tk.Frame(top_frame)
first_frame.pack(side=tk.LEFT, fill=tk.Y, padx=5, expand=True)

position = {"anchor":tk.NW}
 
python = "Python"
java = "Java"
javascript = "JavaScript"
 
lang = tk.StringVar(value=java)    # по умолчанию будет выбран элемент с value=java
def lang_changes(*args):
	if lang.get()==python:
		treeview.selection_remove(treeview.selection())
	if lang.get()==java:
		treeview.item(1, open=True) # программно развернуть
	if lang.get()==javascript:
		treeview.item(1, open=False) # программно развернуть

lang.trace_add("write", lang_changes)
 
header = ttk.Label(first_frame,textvariable=lang)
header.pack(**position)
  
python_btn = ttk.Radiobutton(first_frame,text=python, value=python, variable=lang)
python_btn.pack(**position)
  
javascript_btn = ttk.Radiobutton(first_frame,text=javascript, value=javascript, variable=lang)
javascript_btn.pack(**position)
 
java_btn = ttk.Radiobutton(first_frame,text=java, value=java, variable=lang)
java_btn.pack(**position)

# =============================================
# Переменная для хранения iid выделенной строки
selected_iid = tk.IntVar(value=-1)  # -1 означает "ничего не выбрано"
is_expanded = tk.BooleanVar(value=False) 

selected_label = ttk.Label(bottom_frame,textvariable=selected_iid)
selected_label.pack(side=tk.LEFT)
  
expanded_label = ttk.Label(bottom_frame,textvariable=is_expanded)
expanded_label.pack(side=tk.LEFT)
  
# =============================================

# Добавляем данные (как в вашем примере)
treeview.insert('', tk.END, iid=1, text="1", tags=('color1',))
treeview.insert(1, tk.END, iid=2, text="2", tags=('color2',))
treeview.insert(1, tk.END, iid=3, text="3", tags=('color3',))
treeview.insert(2, tk.END, iid=4, text="4", tags=('color4',))
treeview.insert(2, tk.END, iid=5, text="5", tags=('color5',))

# Настраиваем цвета для тегов
treeview.tag_configure('color1', background='#FFDDDD')  # светло-красный
treeview.tag_configure('color2', background='#DDFFDD')  # светло-зеленый
treeview.tag_configure('color3', background='#DDDDFF')  # светло-синий
treeview.tag_configure('color4', background='#FFFFDD')  # светло-желтый
treeview.tag_configure('color5', background='#FFDDFF')  # светло-розовый

# Функция для обновления переменной при выборе строки
def update_selected_iid(event):
	selected = treeview.selection()  # Получаем выделенные элементы (кортеж)
	assert len(selected) <=1
	if selected:  # Если что-то выбрано
	    selected_iid.set(int(selected[0]))  # Берём первый элемент (если multi-select, можно доработать)
	    #x = treeview.item(selected[0], 'open')
	    #is_expanded.set(x)
	    #print('update_selected_iid',selected[0], x)
	else:
	    selected_iid.set(-1)  # Если ничего не выбрано
treeview.bind("<<TreeviewSelect>>", update_selected_iid)

def check_expanded(iid,event):
	x = treeview.item(iid, "open")
#	print(iid,x,event,
#event.char,
#event.delta,
#event.height,
#event.keycode,
#event.keysym,
#event.keysym_num,
#event.num,
#event.send_event,
#event.serial,
#event.state,
#event.time,
#event.type,
#event.widget,
#event.width)
	is_expanded.set(x)
def on_tree_open_close(event):
	iid = treeview.focus()  # Получаем iid элемента, с которым произошло событие
	treeview.after(10, lambda: check_expanded(iid, event))
treeview.bind("<<TreeviewOpen>>", on_tree_open_close)
treeview.bind("<<TreeviewClose>>", on_tree_open_close)

print(treeview.exists(1))
print(treeview.exists(100))
treeview.delete(1)
print(treeview.exists(1))
treeview.insert('', tk.END, iid=1, text="1", tags=('color1',))
print(treeview.exists(1))
main_win.mainloop()