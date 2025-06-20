import winreg
from pprint import pprint

def get_registry_key(key, sub_key_path=[], depth=0, max_depth=3):
    try:
        with winreg.OpenKey(key, '\\'.join(sub_key_path)) as reg_key:
            i = 0
            while True:
                try:
                    sub_key_name = winreg.EnumKey(reg_key, i)
                    sub_key_path_full = sub_key_path + [sub_key_name]
                    if depth < max_depth:
                        yield from get_registry_key(key, sub_key_path_full, depth + 1, max_depth)
                    else:
                        yield sub_key_path_full, None, None
                    i += 1
                except OSError:
                    break
            i = 0
            while True:
                try:
                    value_name, value_data, value_type = winreg.EnumValue(reg_key, i)
                    if type(value_data) is bytes:
                        value_data = repr(value_data)
                    yield (sub_key_path + [value_name]), value_data, value_type
                    i += 1
                except OSError:
                    break
    except WindowsError as e:
        print(e)

def registry_to_dict(hive, max_depth=3):
    registry_dict = {}
    for path_parts, value, vtype in get_registry_key(hive, [], 0, max_depth):
        current_level = registry_dict
        for part in path_parts[:-1]:
            if part not in current_level:
                current_level[part] = {}
            current_level = current_level[part]
        if value is not None:
            current_level[path_parts[-1]] = (value,vtype)
    return registry_dict

# Основные ветки реестра
hives = {
    "HKEY_CLASSES_ROOT": winreg.HKEY_CLASSES_ROOT,
    "HKEY_CURRENT_USER": winreg.HKEY_CURRENT_USER,
    "HKEY_LOCAL_MACHINE": winreg.HKEY_LOCAL_MACHINE,
    "HKEY_USERS": winreg.HKEY_USERS,
    "HKEY_CURRENT_CONFIG": winreg.HKEY_CURRENT_CONFIG
}

#registry_to_dict(winreg.HKEY_CURRENT_USER, max_depth=0)
#exit()

full_registry = {}

from datetime import datetime

start = datetime.now()

for hive_name, hive in hives.items():
    print(f"Чтение {hive_name}...")
    full_registry[hive_name] = registry_to_dict(hive, max_depth=100500)  # Уменьшите max_depth для тестирования

# Сохранение в файл (опционально)
print("Прочитано!",datetime.now()-start)

start = datetime.now()
import json
with open('registry_backup.json', 'w', encoding='utf-8') as f:
    json.dump(full_registry, f, indent=4, ensure_ascii=False)


print("Реестр сохранён в registry_backup.json", datetime.now()-start)
input()
