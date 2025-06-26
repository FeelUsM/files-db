import os
from datetime import datetime
import stat as STAT
import sys
#sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
#from junction import win_link
import json
#from collections import deafaultdict
import ctypes

start_path = 'C:\\'

#root = {}
types = {}

#dirs = start_path.split(os.sep)
#r = root
#for d in dirs:
#	r[d] = {}
#	r = r[d]



# проходит по всему дереву файлов и у каждого объекта получает атрибуты
# todo: сделать получение атрибутов через FindFirstFileW()
# для каждой комбинации атрибутов сохраняет не более 20 объектов
# и сохраняет это в types.json в формате {атрибут:[список путей]}




for path, dirs, files in os.walk(start_path, followlinks=False):
#	r = root
#	for d in (dd:=path.split(os.sep)):
#		r = r[d]
	if path.count('\\')<4:
		print(datetime.now().strftime("%H:%M:%S"),path)

	for f in dirs+files:
		p = path+os.sep+f
		attrs = ctypes.windll.kernel32.GetFileAttributesW(p)
		if attrs not in types:
			types[attrs] = [p]
			print(hex(attrs),p)
		elif len(types[attrs])<20:
			types[attrs].append(p)
			print(hex(attrs),p)

print('writing file')

#with open('FS.txt', "w") as json_file:
#	json.dump(root, json_file, indent=2)
with open('types.json', "w") as json_file:
	json.dump(types, json_file, indent=2)