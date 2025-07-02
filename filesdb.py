import os
import stat as STAT
import sqlite3
from tqdm import tqdm
from time import time, sleep
from datetime import datetime
from contextlib import closing
from traceback import print_exception, extract_stack, extract_tb, format_list
import inspect
import subprocess
import yaml
import socket # todo replace to zmq
from pprint import pprint
import sys
import threading
from queue import Queue

from my_stat import get_username_by_uid, get_groupname_by_gid, os_readlink, os_stat, STAT_DENIED, external_path, Stat

from typing import Optional, List, Any, Final, Self, Tuple, TextIO, Callable, cast, Set

import re
import codecs

class CustomIncrementalDecoder(codecs.IncrementalDecoder):
    def decode(self, b, final=False):
        return b.decode("utf8", errors="surrogatepass")

def decode_custom(b, errors='strict'):
	return b.decode("utf8", errors="surrogatepass"), len(b)
class CustomStreamReader(codecs.StreamReader):
    def decode(self, b, errors='strict'):
        return b.decode("utf8", errors="surrogatepass"), len(b)
if sys.platform == 'win32':
	ESCAPE_SYMBOL = '/'
else:
	ESCAPE_SYMBOL = '\\'
def surrogate_escape(s):
	outs = ''
	while m := re.search('[\ud800-\udfff]', s):
		assert m.end() - m.start() ==1
		outs += s[:m.start()] + ESCAPE_SYMBOL + 'u' + hex(ord(s[m.start()]))[2:]
		s = s[m.end():]
	outs+=s
	return outs
def surrogate_escape_encode(s):
	return surrogate_escape(s).encode('utf8')
class CustomIncrementalEncoder(codecs.IncrementalEncoder):
    def encode(self, s, final=False):
        return surrogate_escape_encode(s)
def encode_custom(s, errors='strict'):
	return surrogate_escape_encode(s), len(s)
class CustomStreamWriter(codecs.StreamWriter):
    def encode(self, s, errors='strict'):
        return surrogate_escape_encode(s), len(s)
def search(name):
    if name == "utf8_custom":
        return codecs.CodecInfo(
            name="utf8-custom",
            encode=lambda s, errors='strict': (surrogate_escape_encode(s), len(s)),
            decode=decode_custom,
            incrementalencoder=CustomIncrementalEncoder,
            incrementaldecoder=CustomIncrementalDecoder,
            streamreader=CustomStreamReader,
            streamwriter=CustomStreamWriter
        )
    else:
    	print('codecs serch result is None for',name)

codecs.register(search)
# error: Item "TextIO" of "TextIO | Any" has no attribute "reconfigure"  [union-attr]
sys.stdout.reconfigure(encoding='utf8-custom') # type: ignore[union-attr]
sys.stderr.reconfigure(encoding='utf8-custom') # type: ignore[union-attr]

def decode_row_factory(cursor, row):
    # применить декодер к каждому элементу
    return tuple(
        col.decode('utf8', errors="surrogatepass") if isinstance(col, bytes) else col
        for col in row
    )
# con.row_factory = decode_row_factory # in constructor
# используем слабую типизацию sqlite 
# todo CUR.execute('PRAGMA encoding = "...";')
def str_adapter(s: str) -> str|bytes:
    try:
        s.encode("utf8")
        return s
    except Exception:
        return s.encode("utf8", errors="surrogatepass")
sqlite3.register_adapter(str, str_adapter)


class NullContextManager(object):
    def __init__(self, dummy_resource=None):
        self.dummy_resource = dummy_resource
    def __enter__(self):
        return self.dummy_resource
    def __exit__(self, *args):
        pass


assert len(os.sep)==1
def internal_path(path : str) -> str:
	'''
	преобразует путь из представления, понятного ОС во внутреннее представление:
	элементы пути отделяются ровно одним os.sep
	путь начинается с os.sep
	в конце пути os.sep отсутствует
	'''
	#if path is None:
	#	path = os.getcwd()
	#path = os.path.abspath(path)
	assert os.path.isabs(path)
	path1 = path.replace(os.sep+os.sep,os.sep)
	while path1!=path:
		path = path1
		path1 = path.replace(os.sep+os.sep,os.sep)
	path = path1
	if path[-1]==os.sep:
		path = path[:-1]
	if sys.platform == 'win32':
		path = os.sep+path
	return path

def normalize_path(path : Optional[str]) -> str:
	if path is None: path = os.getcwd()
	return internal_path(os.path.abspath(path))

def access2str(st_mode : int) -> str:
	mode = STAT.S_IMODE(st_mode)
	assert mode < 2**11, mode
	s = ''
	for i in range(6,-1,-3):
		s+= 'r' if mode & 2**(i+2) else '-'
		s+= 'w' if mode & 2**(i+1) else '-'
		s+= 'x' if mode & 2**(i+0) else '-'
	if mode & 2**9:
		s = s[:-1]+ ('t' if mode & 1 else 'T')
	if mode & 2**10:
		s = s[:5]+ ('s' if mode & 16 else 'S') + s[6:]
	return s

# dirs:type
TFILE : Final = 0
TDIR : Final = 1
TLINK : Final = 2
TOTHER : Final = 3 # встречаются всякие сокеты, именованные каналы. Не смотря на то, что в /sys, /dev, /proc, /run - не лезем

def typ2str(x : int) -> str:
	assert 0<=x<=3
	return '-' if x==TFILE else \
			'd'if x==TDIR else \
			'l'if x==TLINK else \
			'o'#if x==TOTHER
def is_link (mode : int) -> bool: return STAT.S_ISLNK(mode)
def is_dir  (mode : int) -> bool: return STAT.S_ISDIR(mode)
def is_file (mode : int) -> bool: return STAT.S_ISREG(mode)
def is_other(mode : int) -> bool: return STAT.S_ISCHR(mode) or STAT.S_ISBLK(mode) or\
					STAT.S_ISFIFO(mode) or STAT.S_ISSOCK(mode) or\
					STAT.S_ISDOOR(mode) or STAT.S_ISPORT(mode) or\
					STAT.S_ISWHT(mode) or mode == STAT_DENIED # у папки не может отсутствовать stat
def simple_type(mode : int) -> int:
	typ = TLINK if is_link(mode) else\
		TDIR if is_dir(mode) else\
		TFILE if is_file(mode) else\
		TOTHER if is_other(mode) else \
		None
	if typ is None:
		raise Exception('unknown type')
	return typ


ECREAT : Final = 1 # в этом случае все старые записи == -1
EMODIF : Final = 2
EMOVE : Final = 3
EDEL : Final = 4

def etyp2str(etyp : int) -> str:
	assert 1<=etyp<=4
	return 'C' if etyp==ECREAT else\
			'M'if etyp==EMODIF else\
			'V'if etyp==EMOVE else\
			'D'#if etyp==EDEL

# dirs:modified
NOT_MODIFIED = 0
STAT_MODIFIED = 1
PRE_ROOT_DIR_MODIF = 2 # такие папки не обходим и соостестсвенно, изменилась они или нет - не имеет смысла
HASH_MODIFIED = 4

# hist:static_found
FOUND_BY_WATCHDOG = 0
FOUND_BY_WALK = 1
FOUND_BY_CACHECOMPARE = 2

FIELDS_STAT = '''st_mode,st_ino,st_dev,st_nlink,st_uid,st_gid,st_size,
	atime,mtime,ctime,disk_size,sys_attrs,stat_time'''
FIELDS_STAT_SET = '''st_mode =?,st_ino =?,st_dev =?,st_nlink =?,st_uid =?,st_gid =?,st_size =?,
	atime =?,mtime =?,ctime =?,disk_size =?,sys_attrs =?,stat_time =?'''
FIELDS_STAT_QUESTIONS = '?,?,?,?,?,?,?,?,?,?,?,?,?'
def stat2tuple(stat: Stat) -> tuple:
	return (stat.st_mode,stat.st_ino,stat.st_dev,stat.st_nlink,stat.st_uid,stat.st_gid,stat.st_size,
		stat.atime,stat.mtime,stat.ctime,stat.disk_size,stat.sys_attrs,stat.stat_time)
def tuple2stat(st_mode:int, st_ino:str, st_dev:str, st_nlink:int, st_uid:str, st_gid:str, st_size:int,
		atime:float, mtime:float, ctime:float, disk_size:int, sys_attrs:str, stat_time:float) -> Stat:
	return Stat(st_mode=st_mode,st_ino=st_ino,st_dev=st_dev,st_nlink=st_nlink,st_uid=st_uid,st_gid=st_gid,st_size=st_size,
		st_atime=atime,st_mtime=mtime,st_ctime=ctime,disk_size=disk_size,sys_attrs=sys_attrs,stat_time=stat_time)

class filesdb:
	__slots__ = ['VERBOSE','last_notification','FILES_DB','ROOT_DIRS','CON','CUR','keyboard_thr','commit_thr','walk_thr','q','username']
	VERBOSE : float # = 0.5
	# 0.5 - сообщать об изменениях объектов, которые не имеют владельцев
	# 1   - сообщать о записываемых событиях
	# 1.2 - сообщать обо всех событиях
	# 1.4 - сообщать о несоответствяих ФС, её образа и событий
	# 1.5 - сообщать о событиях
	# 2   - stat_eq и все функции событий
	# 3   - owner_save
	last_notification : float # = time()
	username : str

	def notify(self : Self, thr : float, *args, **kwargs) -> None:
		assert type(thr) in (int,float)
		if self.VERBOSE>=thr:
			print(f'level_{thr}:',*args, **kwargs)
			if __name__=='__main__' and time() > self.last_notification+2:
				self.last_notification = time()
				sep = kwargs['sep'] if 'sep' in kwargs else ' '
				message = surrogate_escape(sep.join(str(x) for x in args))
				if sys.platform == 'win32':
					import plyer # type: ignore
					from plyer import notification
					notification.notify(
						title='filesdb',
						message=message,
						app_name='filesdb',
						#app_icon='path/to/the/icon.{}'.format(
						# On Windows, app_icon has to be a path to a file in .ICO format.
						#'ico' if platform == 'win' else 'png'
						#)
					)
				else:
					if os.getuid()==0: # type: ignore[attr-defined]
						try:
							title = 'filesdb:'
							# Получаем DBUS_SESSION_BUS_ADDRESS
							dbus_address = subprocess.check_output(
								f"grep -z DBUS_SESSION_BUS_ADDRESS /proc/$(pgrep -u {self.username} plasmashell | head -1)/environ | tr '\\0' '\\n' | sed 's/DBUS_SESSION_BUS_ADDRESS=//'",
								#f"grep -z DBUS_SESSION_BUS_ADDRESS /proc/$(pgrep -u {self.username} gnome-session | head -n1)/environ | tr '\\0' '\\n' | sed 's/DBUS_SESSION_BUS_ADDRESS=//'",
								shell=True, text=True
							).strip()
							# Отправляем уведомление через sudo
							subprocess.run(
								["sudo", "-u", self.username, f"DBUS_SESSION_BUS_ADDRESS={dbus_address}", "notify-send", title, message],
								check=True
							)
						except subprocess.CalledProcessError as e:
							print(f"Ошибка при отправке уведомления: {e}")
					else:
						os.system('notify-send filesdb: "'+message+'"')

	def raise_notify(self : Self, e : Optional[Exception], *args, **kwargs) -> None:
		'''
		исключение после которого можно прожолжить работу, сделав уведомление
		но если в интерактивном режиме - то лучше упасть с остановкой
		'''
		if __name__=='__main__':
			print_exception(type(e), e, e.__traceback__ if e is not None else None, chain=True) # type: ignore[arg-type]
			print()
			print('The above exception was the direct cause of the following exception:')
			print()
			print("Traceback (most recent call last):")
			print("".join(format_list(extract_stack()[:-1])), end="")
			self.notify(0,*args,**kwargs)
			print("--------------------------")
		else:
			elocal = args[0] if len(args)==1 and isinstance(args[0],Exception) else Exception(*args)
			if e is None: raise elocal
			else:         raise elocal from e

	def set_VERBOSE(self : Self , x : float) -> None:
		self.VERBOSE = x

	def get_VERBOSE(self : Self) -> None:
		self.notify(0, self.VERBOSE)

	FILES_DB : str
	ROOT_DIRS : List[str]
	CON : sqlite3.Connection
	CUR : sqlite3.Cursor

	keyboard_thr: threading.Thread
	commit_thr: threading.Thread
	walk_thr: threading.Thread
	q : Queue # = Queue()

	# -------------
	# схема данных
	# -------------

	def _create_tables(self : Self) -> None:
		with self.CON:
		# -------- dirs -------
			self.CUR.execute('''CREATE TABLE dirs (
				parent_id INTEGER NOT NULL,                  /* id папки, в которой лежит данный объект */
				name      TEXT    NOT NULL,                  /* имя объекта в папке */
				id        INTEGER PRIMARY KEY AUTOINCREMENT, /* идентификатор объекта во всей БД */
				type      INTEGER NOT NULL,                  /* TFILE, TDIR, TLINK, TOTHER */
				modified  INTEGER NOT NULL,                  /* параметр обхода:
					0 - заходим при полном обходе
					1 - заходим при обходе модифицированных объектов
					2 - по таблице заходим всегда, но в ФС никогда не просматриваем (и даже stat не делаем) "pre-root-dir" 
					4 - надо пересчитать хеш */
			UNIQUE(parent_id, name)
			)
			''')
			self.CUR.execute('CREATE INDEX id_dirs ON dirs (id)')
			self.CUR.execute('CREATE INDEX parname_dirs ON dirs (parent_id, name)')	

		# -------- stat --------
			self.CUR.execute('''CREATE TABLE stat  (
				id         INTEGER PRIMARY KEY,
				type       INTEGER NOT NULL,
				
				st_mode    INTEGER, /*  unix format */
				st_ino     TEXT,
				st_dev     TEXT,
				st_nlink   INTEGER, /* жесткие ссылки */
				st_uid     TEXT,
				st_gid     TEXT,
				st_size    INTEGER,
				atime      REAL,
				mtime      REAL,
				ctime      REAL, /* время с момента изменения структуры stat */
				disk_size  INTEGER,
				sys_attrs  TEXT,
				stat_time  REAL, /* время, когда узнали stat у ОС */
				
				sum_size   INTEGER,
				data       TEXT, /* 
					для файлов - хэш, 
					для папок - хэш = сумма хэшей вложенных объектов (mod 2^128), 
					для симлинков - сама ссылка
					если не читается - NULL*/
				hash_time  REAL,

				owner      INTEGER,
				UNIQUE(st_ino,st_dev) /* при вставке ловим sqlite3.IntegrityError с .sqlite_errorname=='SQLITE_CONSTRAINT_UNIQUE' */
				/* множество NULL значений допустимо */
			)
			''')
			self.CUR.execute('CREATE INDEX id_stat ON stat (id)')
			self.CUR.execute('CREATE INDEX ino_dev ON stat (st_ino,st_dev)')

		# -------- deleted --------
			# для запоминания owner-ов удалённых файлов
			# и чтобы fid-ы не росли, если какой-то файл многократно удаляется и снова создаётся
			# можно было бы использовать hist для этих целей, но там каждый файл не в единственном экземпляре,
			# и особенно, если мы не хотим сохранять события о файле, а он постоянно удаляется и создаётся
			# todo добавить время удаления, чтобы можно было удалять инфу об очень давно удалённых файлах
			self.CUR.execute('''CREATE TABLE deleted  (
				parent_id INTEGER NOT NULL, /* старая запись из dirs */
				name      TEXT    NOT NULL, /* старая запись из dirs */
				id        INTEGER NOT NULL, /* старая запись из dirs */
				owner     INTEGER, /* при создании/восстановлении имеет преимущество перед owner-ом родительской папки */
			UNIQUE(id),
			UNIQUE(parent_id,name)
			)
			''')
			self.CUR.execute('CREATE INDEX id_deleted ON deleted (id)')
			self.CUR.execute('CREATE INDEX parname_deleted ON deleted (parent_id,name)')

		# -------- hist --------
			self.CUR.execute('''CREATE TABLE hist(
				parent_id    INTEGER NOT NULL, /* старая запись из dirs */
				name         TEXT    NOT NULL, /* старая запись из dirs */
				id           INTEGER NOT NULL, /* на id может быть несколько записей */
				type         INTEGER NOT NULL,
				event_type   INTEGER NOT NULL, /* ECREAT, EMODIF, EMOVE, EDEL */
				
				/* сохраняем stat, который был до события */
				st_mode    INTEGER, /*  unix format */
				st_ino     TEXT,
				st_dev     TEXT,
				st_nlink   INTEGER, /* жесткие ссылки */
				st_uid     TEXT,
				st_gid     TEXT,
				st_size    INTEGER,
				atime      REAL,
				mtime      REAL,
				ctime      REAL, /* время с момента изменения структуры stat */
				disk_size  INTEGER,
				sys_attrs  TEXT,
				stat_time  REAL, /* время, когда узнали stat у ОС */

				sum_size   INTEGER,
				data       TEXT, /* 
					для файлов - хэш, 
					для папок - хэш = сумма хэшей вложенных объектов (mod 2^128), 
					для симлинков - сама ссылка
					если не читается - NULL*/
				hash_time  REAL,

				time         REAL    NOT NULL, /* время события */
				static_found INTEGER NOT NULL /* 
					0 - обнаружено watchdog-ом, 
					1 - обнаружено статитсеским обходом дерева каталогов 
					2 - обнаружено путём сравнения хешей
				*/
			)
			''')
			self.CUR.execute('CREATE INDEX id_hist ON hist (id)')
			self.CUR.execute('CREATE INDEX time_hist ON hist (time)')

		# -------- owners --------
			self.CUR.execute('''CREATE TABLE owners  (
				id    INTEGER PRIMARY KEY AUTOINCREMENT,
				name  TEXT    NOT NULL, /* например система-код система-логи программа-код, программа-конфиг, программа-данные, человек-проект */
				save  INTEGER NOT NULL, /* bool - сохранять ли данные об изменении этого объекта в hist */
			UNIQUE(name)
			)
			''')
			self.CUR.execute('CREATE INDEX id_owners ON owners (id)')
			self.CUR.execute('CREATE INDEX name_owners ON owners (name)')

	def check_integrity(self : Self) -> None:
		'''
		проверяет
		присутствуют таблицы: dirs, stat, deleted, hist, owners
		PRE_ROOT_DIR не может быть STAT_MODIFIED или HASH_MODIFIED
		PRE_ROOT_DIR - директория
		у каждого существует родитель
			для dirs в dirs
			для deleted в dirs или deleted
			для PRE_ROOT_DIR в PRE_ROOT_DIR
		у всех из dirs (кроме root_dir) есть обаз из stat и наоборот
		для всех из stat, deleted у кого owner is not None есть owner в owners
		типы в dirs и stat должны сопадать
		dirs.type  stat.type = simple_type(stat.st_mode)
			todo проверка всех констант (dirs.type, dirs.modified, hist.type, hist.event_type, hist.static_found, owners.save)
		в dirs и deleted нет общих id
		в dirs и deleted нет общих (parent_id, name)
		не директория и STAT_MODIFIED => st_mode==STAT_DENIED
		не директория и HASH_MODIFIED => data==NULL
		data==NULL => HASH_MODIFIED
		st_mode==STAT_DENIED => STAT_MODIFIED и type=TOTHER
		'''

		# присутствуют таблицы: dirs, stat, deleted, hist, owners
		tables = {x[0] for x in self.CUR.execute('SELECT name FROM sqlite_master')}
		assert 'dirs' in tables, "table dirs not found"
		assert 'stat' in tables, "table stat not found"
		assert 'deleted' in tables, "table deleted not found"
		assert 'hist' in tables, "table hist not found"
		assert 'owners' in tables, "table owners not found"

		assert PRE_ROOT_DIR_MODIF==2 and STAT_MODIFIED==1 and HASH_MODIFIED==4

		# PRE_ROOT_DIR не может быть STAT_MODIFIED или HASH_MODIFIED
		n = self.CUR.execute('SELECT id FROM dirs WHERE modified&2/*PRE_ROOT_DIR_MODIF*/ AND (modified&1/*STAT_MODIFIED*/ OR modified&4/*HASH_MODIFIED*/)',(TDIR,)).fetchall()
		assert len(n)==0, f'PRE_ROOT_DIR is STAT_MODIFIED or HASH_MODIFIED: {n}'

		# PRE_ROOT_DIR - директория
		n = self.CUR.execute('SELECT id FROM dirs WHERE modified=2/*PRE_ROOT_DIR_MODIF*/ AND type!=?',(TDIR,)).fetchall()
		assert len(n)==0, f'PRE_ROOT_DIR is not dir: {n}'

		# у каждого существует родитель:
		# 	для dirs в dirs
		n = self.CUR.execute('SELECT parent_id FROM dirs WHERE NOT parent_id IN (SELECT id FROM dirs) AND parent_id !=0').fetchall()
		assert len(n)==0, f'lost parents in dirs: {n}'
		
		#	для pre-root_dir в pre-root_dir
		n = self.CUR.execute('''SELECT parent_id FROM dirs WHERE modified = 2/*PRE_ROOT_DIR_MODIF*/ AND (
			NOT parent_id IN (SELECT id FROM dirs WHERE modified = 2/*PRE_ROOT_DIR_MODIF*/) AND parent_id !=0)''').fetchall()
		assert len(n)==0, f'lost parents in pre-root_dirs: {n}'

		#	для deleted в dirs или deleted
		n = self.CUR.execute('''SELECT parent_id FROM deleted WHERE 
			NOT parent_id IN (SELECT dirs.id FROM dirs WHERE dirs.modified != 2/*PRE_ROOT_DIR_MODIF*/)
			AND NOT parent_id IN (SELECT deleted.id FROM deleted)''').fetchall()
		assert len(n)==0, f'lost parents in deleted: {n}'
		# директория из ROOT_DIRS не может быть удалена => deleted.parent_id не может находится среди pre_root_dirs

		# у всех из dirs (кроме pre-root_dir) есть обаз из stat и наоборот
		n1 = self.CUR.execute('SELECT id FROM dirs WHERE modified != 2/*PRE_ROOT_DIR_MODIF*/ AND NOT id IN (SELECT id FROM stat)').fetchall()
		n2 = self.CUR.execute('SELECT id FROM stat WHERE NOT id IN (SELECT id FROM dirs WHERE modified != 2/*PRE_ROOT_DIR_MODIF*/)').fetchall()
		assert len(n1)==0 and len(n2)==0, f'mismatch root_dirs and stat: {n1}, {n2}'

		# для всех из stat, deleted у кого owner is not None есть owner в owners
		n = self.CUR.execute('SELECT owner FROM stat WHERE owner NOT NULL AND NOT owner IN (SELECT id FROM owners)').fetchall()
		assert len(n)==0, f'lost owners from stat: {n}'
		n = self.CUR.execute('SELECT owner FROM deleted WHERE owner NOT NULL AND NOT owner IN (SELECT id FROM owners)').fetchall()
		assert len(n)==0, f'lost owners from deleted: {n}'

		# типы в dirs и stat должны сопадать
		n = self.CUR.execute('SELECT dirs.id, dirs.type, stat.type FROM dirs JOIN stat ON dirs.id=stat.id WHERE dirs.type != stat.type').fetchall()
		assert len(n)==0, f'mismatch types: {n}'

		# dirs.type  stat.type = simple_type(stat.st_mode)
		assert STAT.S_IFMT(0o177777)==0o170000, hex(STAT.S_IFMT(0o177777))
		n = self.CUR.execute('''SELECT id, type, st_mode FROM stat WHERE 
			st_mode&0xf000==? AND type!=? OR
			st_mode&0xf000==? AND type!=? OR
			st_mode&0xf000==? AND type!=? OR
			st_mode&0xf000!=? AND st_mode&0xf000!=? AND st_mode&0xf000!=? AND type!=?
			''',(STAT.S_IFREG,TFILE, STAT.S_IFLNK,TLINK, STAT.S_IFDIR,TDIR, STAT.S_IFREG,STAT.S_IFLNK,STAT.S_IFDIR, TOTHER)).fetchall()
		assert len(n)==0, f'mismatch types: {n}'
		#assert t2==simple_type(mode), (t2,simple_type(mode))

		# в dirs и deleted нет общих id
		n = self.CUR.execute('SELECT dirs.id FROM dirs JOIN deleted ON dirs.id=deleted.id').fetchall()
		assert len(n)==0, f'common ids in dirs and deleted: {n}'

		# в dirs и deleted нет общих (parent_id, name)
		n = self.CUR.execute('SELECT dirs.id, deleted.id, dirs.parent_id, dirs.name FROM dirs JOIN deleted ON dirs.parent_id=deleted.parent_id AND dirs.name=deleted.name').fetchall()
		assert len(n)==0, f'common (parent_id, name) in dirs and deleted: {n}'

		assert PRE_ROOT_DIR_MODIF==2 and STAT_MODIFIED==1 and HASH_MODIFIED==4

		# не директория и STAT_MODIFIED => st_mode==STAT_DENIED
		n = self.CUR.execute('SELECT id FROM stat  WHERE type!=? AND modified&1/*STAT_MODIFIED*/ AND stat.st_mode!=?',(
			TDIR,STAT_DENIED)).fetchall()
		assert len(n)==0, f'не директория и STAT_MODIFIED => st_mode==STAT_DENIED: {n}'

		# не директория и HASH_MODIFIED => data==NULL
		n = self.CUR.execute('SELECT id FROM stat WHERE type!=? AND dirs.modified&4/*HASH_MODIFIED*/ AND stat.data!=NULL',(
			TDIR,)).fetchall()
		assert len(n)==0, f'не директория и STAT_MODIFIED => st_mode==STAT_DENIED: {n}'

		# для всех из hist есть образ в stat или deleted
		# hist_ids = {x[0] for x in self.CUR.execute('SELECT id FROM hist').fetchall()}
		# assert hist_ids <= (notroot_dirs_ids|deleted_ids), f'hist enty with unknown id: {hist_ids-(notroot_dirs_ids|deleted_ids)}'
		# если создать файл, переименовать его и удалить, а потом повторить, то id первого файла затрётся в deleted и больше не будет существовать ни в dirs ни в deleted
		# при этом на каждое событие в ФС мы не будем осуществлять просмотр hist для удаления старых записей
		# к тому же в hist присутствует и parent_id и name, так что восстановить расположение объекта в ФС будет не сложно

		# у каждого существует родитель:	для hist в dirs или deleted
		# hist_parents = {x[0] for x in self.CUR.execute('SELECT parent_id FROM hist WHERE parent_id!=-1').fetchall()}
		# assert hist_parents <= (notroot_dirs_ids|deleted_ids), f'lost parents in hist: {hist_parents-(notroot_dirs_ids|deleted_ids)}'
		# целостность hist во время фоновой работы проверять не будем, сделаем потом отдельено check_hist, clean_hist ...

	# --------------
	# общие функции образа ФС
	# --------------

	def path2ids(self : Self, path : str, cursor : Optional[sqlite3.Cursor] =None) -> List[Optional[int]]:
		'''
		преобразовывает путь(внутренний) в последовательность id-ов всех родительских папок
		Если в какой-то момент не удалось найти очередную папку - последовательность будет заканчиваться Nane-ом
		id объекта, задаваемого путём находится в последнй ячейке массива
		'''
		if cursor is None: cursor = self.CUR
		ids : List[Optional[int]] = []
		cur_id = 0
		for name in path.split(os.sep):
			if name=='': continue
			n = cursor.execute('SELECT id FROM dirs WHERE parent_id = ? AND name = ?',(cur_id,name)).fetchone()
			if n is None:
				return ids+[None]
				#raise Exception(f"can't find {name} in {cur_id}")
			cur_id = n[0]
			ids.append(cur_id)
		return ids
	def id2path(self : Self, fid : int, cursor : Optional[sqlite3.Cursor] =None) -> str:
		'''
		преобразовывает id в путь(внутренний)
		'''
		if cursor is None: cursor = self.CUR
		path = ''
		while fid!=0:
			n = cursor.execute('SELECT parent_id, name FROM dirs WHERE id = ? ',(fid,)).fetchone()
			assert n is not None
			path = os.sep+n[1]+path
			fid = n[0]
		return path

	# unused method
	def is_modified(self :Self, fid : int, bits:int, cursor : Optional[sqlite3.Cursor] =None) -> int: #  =STAT_MODIFIED
		'''
		просто замена одному запросу в БД
		'''
		if cursor is None: cursor = self.CUR
		n = cursor.execute('SELECT modified FROM dirs WHERE id = ?',(fid,)).fetchone()
		if n is None: raise Exception(f"can't find fid {fid}")
		return n[0]&bits
	def set_modified(self : Self, fid : int, bits:int, cursor : Optional[sqlite3.Cursor] =None) -> None: #  =STAT_MODIFIED|HASH_MODIFIED
		'''
		выставляет modified в объект и в его родителя, если тот ещё не, и так рекурсивно (вплоть до PRE_ROOT_DIR_MODIF) todo переделать в цикл
		'''
		if cursor is None: cursor = self.CUR
		if fid==0: return
		n = cursor.execute('SELECT parent_id, modified FROM dirs WHERE id = ?',(fid,)).fetchone()
		if n is None: raise Exception(f"can't find fid {fid}")
		if not (n[1]&PRE_ROOT_DIR_MODIF or (bits&STAT_MODIFIED and bits&HASH_MODIFIED and  n[1]&STAT_MODIFIED and n[1]&HASH_MODIFIED) or \
			(bits&STAT_MODIFIED and not bits&HASH_MODIFIED and  n[1]&STAT_MODIFIED) or (not bits&STAT_MODIFIED and bits&HASH_MODIFIED and n[1]&HASH_MODIFIED) ):
				# not n[1]&PRE_ROOT_DIR_MODIF and (bits&(STAT_MODIFIED|HASH_MODIFIED))&~n[1]&(STAT_MODIFIED|HASH_MODIFIED)
			cursor.execute('UPDATE dirs SET modified = 1 WHERE id = ?',(fid,))
			self.set_modified(n[0], bits, cursor=cursor)

	def update_stat(self : Self, fid : int, stat : Stat, cursor : Optional[sqlite3.Cursor] =None) -> None:
		'''
		по fid-у заполняет stat-поля в stat
		'''
		if cursor is None: cursor = self.CUR
		try:
			cursor.execute(f'UPDATE stat SET {FIELDS_STAT_SET} WHERE id = ?', (*stat2tuple(stat),fid))
		except sqlite3.IntegrityError as e:
			if e.sqlite_errorname=='SQLITE_CONSTRAINT_UNIQUE':
				self.notify(0,f'found same files with st_ino={stat.st_ino} and st_dev={stat.st_dev}')
				stat.st_dev = None
				cursor.execute(f'UPDATE stat SET {FIELDS_STAT_SET} WHERE id = ?',(*stat2tuple(stat),fid))
			else:
				raise
	def get_stat(self : Self, fid : int, cursor : Optional[sqlite3.Cursor] =None) -> Stat:
		'''
		по fid-у возвращает stat-поля из stat в виде объекта
		'''
		if cursor is None: cursor = self.CUR
		return tuple2stat(*cursor.execute(f'SELECT {FIELDS_STAT} FROM stat WHERE id = ?',(fid,)).fetchone())

	# --------------------
	# инициализация БД
	# --------------------

	def _create_root(self : Self, path : str, cursor : Optional[sqlite3.Cursor] =None) -> int:
		'''
		создает корневые директории в дереве dirs (помечает родительские директории к path как pre-root-dir)
		возвращает fid созданной директории
		'''
		if cursor is None: cursor = self.CUR
		ids = self.path2ids(path,cursor)
		assert ids[-1] is None
		fid = 0 if len(ids)==1 else ids[-2]

		# рассчитываем, что src_path - обсолютный путь, не симлинк, не содержит // типа '/a//b/c'
		pathl = path.split(os.sep)

		#print(ids,fid,pathl)
		for name in pathl[len(ids):-1]:
			cursor.execute('INSERT INTO dirs (parent_id, name, modified, type) VALUES (?, ?, ?, ?)',(fid, name, PRE_ROOT_DIR_MODIF, TDIR))
			(fid,) = cursor.execute('SELECT id FROM dirs WHERE parent_id =? AND name=?',(fid,name)).fetchone()
			assert fid is not None
		stat = os_stat(path)
		typ = simple_type(stat.st_mode)
		if stat.st_mode == STAT_DENIED:
			self.notify(0,f'root dir is not accessible: {path}')
		elif typ!=TDIR:
			self.notify(0,f'root dir is not dir: {path}')
		self.CUR.execute('INSERT INTO dirs (parent_id, name, modified, type) VALUES (?, ?, ?, ?)', 
			(fid, pathl[-1], HASH_MODIFIED|(STAT_MODIFIED if stat.st_mode == STAT_DENIED else 0), typ))
		(fid,) = self.CUR.execute('SELECT id FROM dirs WHERE parent_id = ? AND name = ?',(fid, pathl[-1])).fetchone()
		assert fid is not None
		self.CUR.execute('INSERT INTO stat (id,type) VALUES (?,?)', (fid,typ))
		self.update_stat(fid,stat,self.CUR)
		return fid

	def _init_cur(self : Self, root_dirs : List[str]) -> None:
		'''
		обходит ФС из root_dirs и заполняет таблицы dirs и stat
		'''
		with self.CON:
			t = time()
			self.notify(0,'walk root_dirs:')
			for root_dir in tqdm(root_dirs):
				#self.notify(0,root_dir)
				self._create_root(internal_path(root_dir),self.CUR)
				for root, dirs, files in os.walk(root_dir): # проходит по тем папкам, которые может открыть
					pathids = self.path2ids(internal_path(root),self.CUR)
					assert pathids[-1] is not None
					if time()-t>10:
						print('...',root)
						t = time()
					#self.notify(0,root,pathids,dirs)
					# при выполнении stat TFILE/TDIR может быть заменён на TLINK или TOTHER
					for name in dirs+files:
						path = root+os.sep+name
						stat = os_stat(path)
						typ = simple_type(stat.st_mode)
						if stat.st_mode == STAT_DENIED:
							self.notify(0,f'file is not accessible: {path}')
						self.CUR.execute('INSERT INTO dirs (parent_id, name, modified, type) VALUES (?, ?, ?, ?)', 
							(pathids[-1], name, HASH_MODIFIED|(STAT_MODIFIED if stat.st_mode == STAT_DENIED else 0), typ))
						(fid,) = self.CUR.execute('SELECT id FROM dirs WHERE parent_id = ? AND name = ?',(pathids[-1], name)).fetchone()
						assert fid is not None
						self.CUR.execute('INSERT INTO stat (id,type) VALUES (?,?)', (fid,typ))
						self.update_stat(fid,stat,self.CUR)

	def init_db(self : Self) -> None:
		'''
		создаёт и инициализирует таблицы
		'''
		self._create_tables()
		self._init_cur(self.ROOT_DIRS)

	# ---------------------
	# общие функции событий
	# ---------------------

	def id2path_d(self : Self, fid : int, cursor : Optional[sqlite3.Cursor] =None) -> Tuple[str, bool]:
		'''
		то же что id2path(), только ещё ищет в deleted
		возвращает (path, deleted: Bool)
		'''
		if cursor is None: cursor = self.CUR
		path : List[str] = []
		deleted = False
		while fid!=0:
			n = cursor.execute('SELECT parent_id, name FROM dirs WHERE id = ?',(fid,)).fetchone()
			if n is None:
				deleted = True
				n = cursor.execute('SELECT parent_id, name FROM deleted WHERE id = ?',(fid,)).fetchone()
				if n is None:
					raise Exception(f"can't find fid {fid}")
			(fid, name) = n
			path.insert(0,name)
		path.insert(0,'')
		return os.sep.join(path), deleted
	def path2ids_d(self : Self, path : str, cursor : Optional[sqlite3.Cursor] =None) -> Tuple[List[Optional[int]], bool]:
		'''
		то же что path2ids(), только ещё ищет в deleted
		возвращает (ids, deleted: Bool)
		'''
		if cursor is None: cursor = self.CUR
		ids : List[Optional[int]] = []
		cur_id = 0
		deleted = False
		for name in path.split(os.sep):
			if name=='': continue
			n = cursor.execute('SELECT id FROM dirs WHERE parent_id = ? AND name = ?',(cur_id,name)).fetchone()
			if n is None:
				deleted = True
				n = cursor.execute('SELECT id FROM deleted WHERE parent_id = ? AND name = ?',(cur_id,name)).fetchone()
				if n is None:
					return ids+[None], deleted
					#raise Exception(f"can't find {name} in {cur_id}")
			cur_id = n[0]
			ids.append(cur_id)
		return ids, deleted

	def any2id(self : Self, fid : None|int|str) -> int:
		if fid is None:
			fid = os.getcwd()
		if type(fid) is str:
			fid = self.path2ids(normalize_path(fid))[-1]
			if fid is None: raise Exception('path does not exist')
		assert type(fid) is int and self.CUR.execute('SELECT id FROM dirs WHERE id==?',(fid,)).fetchone() is not None
		return fid
	def any2id_d(self : Self, fid : None|int|str) -> Tuple[int,bool]:
		if fid is None:
			fid = os.getcwd()
		if type(fid) is str: 
			ids,d = self.path2ids_d(os.path.abspath(fid))
			if ids[-1] is None: raise Exception('path does not exist')
			return ids[-1], d
		else:
			if self.CUR.execute('SELECT id FROM deleted WHERE id==?',(fid,)).fetchone() is not None:
				return int(fid), True # int() for mypy
			else:
				assert type(fid) is int and self.CUR.execute('SELECT id FROM dirs WHERE id==?',(fid,)).fetchone() is not None
				return fid, False

	def owner_save(self : Self, fid : int, cursor : Optional[sqlite3.Cursor] =None) -> Tuple[int, bool]:
		'''
		определяет владельца и надо ли сохранять события, связанные с этим файлом
		'''
		if cursor is None: cursor = self.CUR
		self.notify(3.1,'owner_save',fid)
		(owner,) = cursor.execute('SELECT owner FROM stat WHERE id = ?',(fid,)).fetchone()
		if owner is not None:
			(save,) = cursor.execute('SELECT save FROM owners WHERE id = ?',(owner,)).fetchone()
		else:
			save = True
		return (owner,save)

	def add_event(self : Self, fid : int, typ : None|int, etyp : int, static_found : int, owner : None|int, cursor : Optional[sqlite3.Cursor] =None) -> None:
		'''
		создает запись в hist
		если событие ECREAT: заполняет большинство полей NULL
		иначе: копирует данные из dirs, stat
			опционально если указан typ: проверяет, чтобы он равнялся старому типу
		'''
		ltime = time()

		if cursor is None: cursor = self.CUR
		if etyp==ECREAT:
			cursor.execute(f'''INSERT INTO hist (parent_id, name, id, type, event_type, {FIELDS_STAT}, time,static_found) VALUES 
				(-1,'',?,?,?, {FIELDS_STAT_QUESTIONS}, ?,?)''', 
				(fid,typ,etyp, *stat2tuple(Stat(st_mode=-1)), ltime,static_found))
		else:
			(parent_id, name, otyp,) = cursor.execute('SELECT parent_id, name, type FROM dirs WHERE id = ?',(fid,)).fetchone()
			if typ is not None:
				assert typ == otyp , (typ, otyp)
			else:
				typ = otyp
			# просто часть данных копируем а часть заполняем вручную
			stat = stat2tuple(self.get_stat(fid,cursor))
			cursor.execute(f'''INSERT INTO hist (parent_id, name, id, type, event_type, {FIELDS_STAT}, time, static_found) 
				VALUES (?,?,?,?,?,{FIELDS_STAT_QUESTIONS},?,?)''',
				(parent_id, name, fid, typ, etyp, *stat, ltime, static_found)
			)

		if self.VERBOSE>=1 or owner is None and self.VERBOSE>0:
			self.notify(0,datetime.fromtimestamp(ltime), etyp2str(etyp), fid, typ2str(typ) if typ is not None else None, self.id2path_d(fid,cursor)[0])

	def modify(self : Self, fid : int, stat : Stat, static_found : int, cursor : Optional[sqlite3.Cursor] =None) -> None:
		'''
		известно, что объект fid изменился, известен его новый stat
		'''
		if cursor is None: cursor = self.CUR

		(parent_id, name, typ) = cursor.execute('SELECT parent_id, name, type FROM dirs WHERE id = ?',(fid,)).fetchone()
		if typ != simple_type(stat.st_mode):
			self.notify(0.5, f'changed type of {fid} {self.id2path(fid,cursor)}')
			owner, save = self.owner_save(fid,cursor)
			self.delete(fid, static_found, cursor)
			self.create(parent_id, name, stat, static_found, cursor, owner, save)
			return

		self.update_stat(fid,stat,cursor)
		# помечаем HASH_MODIFIED, снимаем STAT_MODIFIED (если stat.st_mode!=STAT_DENIED)
		# ставим HASH_MODIFIED|STAT_MODIFIED у его родителя
		self.set_modified(parent_id, HASH_MODIFIED|STAT_MODIFIED, cursor)
		mod = HASH_MODIFIED|(STAT_MODIFIED if stat.st_mode==STAT_DENIED or typ==TDIR else 0)
		cursor.execute('UPDATE dirs SET modified=? WHERE id = ?',(mod,fid))

		(owner,save) = self.owner_save(fid,cursor)
		if save or self.VERBOSE>=1.2:
			# cохранить старый stat
			# условие для папки - если изменился её stat (st_atime, st_mtime не учитываем)
			# условие для файла - если с предыдущего обновления прошло больше 10 сек
			if simple_type(stat.st_mode)==TDIR:
				save1 = (stat != self.get_stat(fid,cursor))
			else:
				save1 = True
				n = cursor.execute('SELECT time FROM hist WHERE id = ? ORDER BY time DESC LIMIT 1',(fid,)).fetchone()
				if n is not None: # раньше этот файл уже обновлялся
					save = abs(n[0] - time())>10
			if save and save1:
				self.add_event(fid, simple_type(stat.st_mode), EMODIF, static_found, owner, cursor)
			elif save1:
				self.notify(1.2,'modify',fid, self.id2path(fid, cursor), static_found)
		
	def create(self : Self, parent_id : int, name : str, stat : Stat, static_found : bool|int, cursor : Optional[sqlite3.Cursor] =None, 
			owner : Optional[int]=None, save : Optional[bool]=None
	) -> int:
		'''
		создается объект, родительская директория которого уже существует
		save, owner определяются родительской папкой или из таблицы deleted
		возвращает fid созданного объекта
		'''
		if cursor is None: cursor = self.CUR
		if owner is None or save is None:
			(owner,save) = self.owner_save(parent_id,cursor)
		self.set_modified(parent_id, HASH_MODIFIED|STAT_MODIFIED, cursor)
		typ = simple_type(stat.st_mode)
		mod = HASH_MODIFIED|(STAT_MODIFIED if stat.st_mode==STAT_DENIED or typ==TDIR else 0)

		n = cursor.execute('SELECT id, owner FROM deleted WHERE parent_id =? AND name=?',(parent_id,name)).fetchone()
		if n is None: # раньше НЕ удалялся
			cursor.execute('INSERT INTO dirs (parent_id, name, modified, type) VALUES (?, ?, ?, ?)',
						   (parent_id, name, mod, typ))
			(fid,) = cursor.execute('SELECT id FROM dirs WHERE parent_id =? AND name=?',(parent_id,name)).fetchone()
		else:
			fid,owner1 = n
			if owner1 is not None:
				n = cursor.execute('SELECT save FROM owners WHERE id = ?',(owner1,)).fetchone()
				if n is not None:
					owner = owner1
					(sav,) = n
					if save: save = sav
			cursor.execute('DELETE FROM deleted WHERE parent_id =? AND name=?',(parent_id,name))
			cursor.execute('INSERT INTO dirs (parent_id, name, id, modified, type) VALUES (?, ?, ?, ?, ?)',
						   (parent_id, name, fid, mod, typ))
			
		# обновить stat в cur
		cursor.execute('INSERT INTO stat (id,type,owner) VALUES (?,?,?)',(fid,simple_type(stat.st_mode),owner))
		self.update_stat(fid,stat,cursor)

		if save:
			self.add_event(fid, simple_type(stat.st_mode), ECREAT, static_found, owner, cursor)
		else:
			self.notify(1.2, 'create',parent_id, self.id2path(parent_id, cursor), name, static_found, owner, save)

		return fid
		
	def delete(self : Self, fid : int, static_found : bool|int, cursor : Optional[sqlite3.Cursor] =None) -> None:
		'''
		удаляем существующий объект fid
		а также его потомков, если они существуют
		'''
		if cursor is None: cursor = self.CUR
		(owner,save) = self.owner_save(fid,cursor)

		def my_walk(did : int) -> None:
			n = cursor.execute('SELECT name,id,type FROM dirs WHERE parent_id = ? ',(did,)).fetchall()
			for name,fid,ftype in n:
				if ftype==TDIR:
					my_walk(fid)
				(owner,) = cursor.execute('SELECT owner FROM stat WHERE id = ?', (fid,)).fetchone()
				if save:
					self.add_event(fid, None, EDEL, static_found, owner, cursor)

				cursor.execute('INSERT INTO deleted VALUES (?,?,?,?)',(did,name,fid,owner))

				cursor.execute('DELETE FROM stat WHERE id = ?',(fid,))
				cursor.execute('DELETE FROM dirs WHERE id = ?',(fid,))
				
		my_walk(fid)
		if save:
			self.add_event(fid, None, EDEL, static_found, owner, cursor)
		else:
			self.notify(1.2, 'delete',fid, self.id2path(fid, cursor),static_found)

		(owner,) = cursor.execute('SELECT owner FROM stat WHERE id = ?', (fid,)).fetchone()
		(did,name) = cursor.execute('SELECT parent_id, name FROM dirs WHERE id = ?', (fid,)).fetchone()
		cursor.execute('INSERT INTO deleted VALUES (?,?,?,?)',(did,name,fid,owner))

		cursor.execute('DELETE FROM stat WHERE id = ?',(fid,))
		cursor.execute('DELETE FROM dirs WHERE id = ?',(fid,))

	# --------------------------------
	# функции статического обновления
	# --------------------------------

	def update_hashes(self : Self, with_all : bool =False) -> None:
		import hashlib
		# todo calc only unknown hashes
		with self.CON:
			with closing(self.CON.cursor()) as cursor:
				ids:List[Tuple[int]]
				if with_all:
					ids = cursor.execute('SELECT id FROM dirs WHERE type = ?',(TFILE,)).fetchall()
				else:
					ids = cursor.execute('SELECT id FROM dirs WHERE type = ? AND modified = STAT_MODIFIED',(TFILE,)).fetchall()
				cnt = 0
				print('calc hashes:')
				for fid1 in (tqdm(ids) if __name__!="__main__" else ids):
					fid = fid1[0]
					path = None
					path = external_path(self.id2path(fid,cursor))
					try:
						f = open(path,'rb')
					except FileNotFoundError:
						self.delete(fid, True, cursor)
						continue
					except PermissionError:
						hsh = None
					except Exception as e:
						self.raise_notify(e,fid,path)
						continue
					else:
						with f:
							hsh = hashlib.md5(f.read()).hexdigest()
					(ohash,) = cursor.execute('SELECT data FROM stat WHERE id = ?',(fid,)).fetchone()
					if ohash is not None and ohash!=hsh:
						self.modify(fid, os_stat(path), FOUND_BY_CACHECOMPARE, cursor)
					cursor.execute('UPDATE stat SET data = ? WHERE id = ?',(hsh,fid))
					cursor.execute('UPDATE dirs SET modified = 0 WHERE id = ?',(fid,))
					cnt+=1
					if cnt%1000000==0:
						print('COMMIT update_hashes')
						cursor.execute('COMMIT')

		# обновить симлинки, директории, сынтегрировать хеши
		with self.CON:
			with closing(self.CON.cursor()) as cursor:
				def my_walk(did : int, root : bool) -> int|None:
					n = cursor.execute('SELECT name,id,type,modified FROM dirs WHERE parent_id = ? ',(did,)).fetchall()
					hsh : int|None = 0
					for name,fid,ftype,modified in n:
						if ftype==TFILE:
							try:
								(lhsh,) = cursor.execute('SELECT data FROM stat WHERE id = ?',(fid,)).fetchone()
							except Exception as e:
								lhsh = None
								self.raise_notify(e,fid)
						elif ftype==TLINK:
							try:
								lnk = os_readlink(external_path(self.id2path(fid,cursor)))
							except FileNotFoundError:
								self.delete(fid, True, cursor)
								continue
							except PermissionError:
								hsh = None
							else:
								(olink,) = cursor.execute('SELECT data FROM stat WHERE id = ?',(fid,)).fetchone()
								if olink is not None and olink!=lnk:
									self.modify(fid, os_stat(self.id2path(fid)), FOUND_BY_CACHECOMPARE, cursor)
								lhsh = hashlib.md5(lnk.encode()).hexdigest()
								cursor.execute('UPDATE stat SET data = ? WHERE id = ?',(lnk,fid))
								cursor.execute('UPDATE dirs SET modified = 0 WHERE id = ?',(fid,))
						elif ftype==TDIR:
							if with_all or modified!=0:
								lhsh = str(my_walk(fid,modified==PRE_ROOT_DIR_MODIF))
							else:
								(lhsh,) = cursor.execute('SELECT data FROM stat WHERE id = ?',(fid,)).fetchone()
						elif ftype==TOTHER:
							lhsh = hex( 0 )[2:].zfill(32)
							cursor.execute('UPDATE dirs SET modified = 0 WHERE id = ?',(fid,))
						else:
							assert False, (name,fid,ftype)

						if lhsh is None:
							hsh = None
						if hsh is not None:
							hsh += int(lhsh, 16)

					if hsh is not None:
						shsh = hex( hsh%(2**128) )[2:].zfill(32)
						if not root:
							cursor.execute('UPDATE stat SET data = ? WHERE id = ?',(shsh,did))
							cursor.execute('UPDATE dirs SET modified = 0 WHERE id = ?',(did,))
					return hsh
				my_walk(0,True)

	def walk_stat1(self : Self, with_all : bool, did : int, *, progress : Optional[Callable[[],None]]=None, path : str='', typ : int=TDIR, modified : int=0) -> bool:
		# path, typ, modified - внутренние рекурсивные параметры, не предназначенные для внешнего вызова
		# only_modified === not with_all
		# если это не pre-root-dir
		#	если это папака
		#		просматриваем дочерние объeкты, какие есть и какие должны быть
		#		удаляем удалённые
		#		просматриваем которые остались(с учётом only_modified)
		#		создаём новые и просматриваем их(modified=3)
		#	если modified!=3
		#		делаем stat текущего пути, сравниваем с имеющимся
		#		если разные stat, есть созданные/удалённые, есть различия в дочерних - modified(); return True
		# если pre-root-dir
		#	просматриваем дочерние объeкты, какие есть(с учётом only_modified)
		this_modified = False
		if did!=0 and modified!=2:
			if typ==TDIR:
				children = self.CUR.execute('SELECT name,id,type,modified FROM dirs WHERE parent_id = ?',(did,)).fetchall()
				real_children = os.listdir(external_path(path))
				children2 = []
				# удаляем удалённые
				for (name,fid,ctyp,cmodified) in children:
					if name in real_children:
						children2.append((name,fid,ctyp,cmodified))
						real_children.remove(name)
					else:
						this_modified = True
						self.delete(fid, True, self.CUR)
					if progress is not None: progress()
				# просматриваем которые остались(с учётом only_modified)
				for (name,fid,ctyp,cmodified) in children2:
					if with_all or cmodified:
						this_modified |= self.walk_stat1(with_all, fid, progress=progress, path=path+os.sep+name, typ=ctyp, modified=cmodified)
				# создаём новые и просматриваем их(modified=3)
				for name in real_children:
					this_modified = True
					cpath = path+os.sep+name
					try:
						cstat = os_stat(cpath)
					except FileNotFoundError:
						print(cpath,"found new item but can't stat it")
						continue
					fid = self.create(did, name, cstat, True, self.CUR)
					self.walk_stat1(with_all, fid, progress=progress, path=cpath, typ=simple_type(cstat.st_mode), modified=3)
			if modified!=3:
				try:
					stat = os_stat(path)
					this_modified |= (stat != self.get_stat(did,self.CUR))
					if this_modified:
						self.modify(did, stat, True, self.CUR)
				except FileNotFoundError:
					print(path,"item may be alreay deleted")
		else:
			for (name,fid,ctyp,cmodified) in self.CUR.execute('SELECT name,id,type,modified FROM dirs WHERE parent_id = ?',(did,)).fetchall():
				this_modified |= self.walk_stat1(with_all, fid, progress=progress, path=path+os.sep+name, typ=ctyp, modified=cmodified)
		return this_modified

	def walk_stat(self : Self, with_all : bool, did : int) -> None:
		'''
		основная цель: найти изменения, которые не были пойманы watchdog-ом
		with_all === not only_modified
		обходим модифицированные объекты в БД, сравнимаем их с ФС
		или обходим все объекты ФС (в зависимости от only_modified)
		изменения заносим в журнал
		и помечаем их как модифицированнные
		'''
		if with_all:
			total = self.CUR.execute('SELECT COUNT(*) AS count FROM dirs WHERE modified!=2').fetchone()
		else:
			total = self.CUR.execute('SELECT COUNT(*) AS count FROM dirs WHERE modified==1').fetchone()
		total = 0 if total is None else total[0]
		with self.CON:
			with (tqdm(total=total, desc="Progress") if __name__!="__main__" else NullContextManager(None)) as pbar:
				count = 0
				def progress() -> None:
					nonlocal count
					count+=1
					if __name__!="__main__" and total>100 and count % (total // 100)==0:
						pbar.update(total // 100)
				self.walk_stat1(with_all, did, progress=progress)

	def walk_stat_all(self : Self) -> None:
		self.walk_stat(True, 0)
	def walk_stat_modified(self : Self) -> None:
		self.walk_stat(False, 0)

	# --------------------------------
	# функции динамического обновления
	# --------------------------------

	def create_parents(self : Self, path : str, cursor : Optional[sqlite3.Cursor] =None, ids=None) -> Tuple[int, str, int, bool]:
		if cursor is None: cursor = self.CUR
		self.notify(2, 'create_parents',path,cursor,ids)
		if ids is None:
			ids = self.path2ids(path,cursor)
			
		# рассчитываем, что src_path - абсолютный путь, не симлинк, не содержит // типа '/a//b/c'
		pathl = path.split(os.sep)
		#assert ids[-1] is None # при вызове из move() это может быть не так
		fid = ids[-2]
		(owner,save) = self.owner_save(fid,cursor)

		parent_path = os.sep.join(pathl[:len(ids)])
		for name in pathl[len(ids):-1]:
			parent_path+= (os.sep+name)
			lstat = os_stat(parent_path) # FileNotFoundError будет пойман в области watchdog-а
			assert simple_type(lstat.st_mode)==TDIR, simple_type(lstat.st_mode)
			fid = self.create(fid, name, lstat, True, cursor, owner, save)

		return fid, pathl[-1], owner, save

	def create1(self : Self, ids : List[None|int], src_path : str, stat : Stat, is_directory : bool, cursor : Optional[sqlite3.Cursor] =None) -> None:
		if cursor is None: cursor = self.CUR
		self.notify(2, 'created1',ids, src_path, stat, is_directory, cursor)
		(fid, name, owner, save) = self.create_parents(src_path,cursor,ids)
		self.create(fid, name, stat, False, cursor, owner, save)

	def move_deleted(self : Self, ofid : int, nfid : int, cursor : Optional[sqlite3.Cursor] =None) -> None:
		'''
		в deleted у потомков ofid меняет предка на nfid
		а если происходит коллизия по имени с dirs - удаляет его и применяется рекурсивно
		'''
		if cursor is None: cursor = self.CUR
		forbidden_names = set(cursor.execute(
			'SELECT name FROM dirs WHERE parent_id = ? UNION SELECT name FROM deleted WHERE parent_id = ?',(nfid,nfid)).fetchall())
		for (name,fid) in cursor.execute('SELECT name, id FROM deleted WHERE parent_id = ?',(ofid,)).fetchall():
			if (name,) in forbidden_names:
				cursor.execute('DELETE FROM deleted WHERE id = ?',(fid,))
				self.move_deleted(fid,
					cursor.execute(
						'SELECT id FROM dirs WHERE parent_id=? AND name=? UNION SELECT id FROM deleted WHERE parent_id=? AND name=?'
						,(nfid,name,nfid,name)).fetchone()[0],
					cursor)
			else:
				cursor.execute('UPDATE deleted SET parent_id=? WHERE id = ?',(nfid,fid))

	def move(self : Self, fid : int, dest_path : str, cursor : Optional[sqlite3.Cursor] =None) -> None:
		'''
		существующий объект fid перемещается на новое место
		фактически у него изменяется только parent_id, name
		Если требуется, создаются необходиме родительские директории для целевого пути
		'''
		if cursor is None: cursor = self.CUR
		self.notify(2, 'moved',fid, dest_path, cursor)
		(parent_id, name, _, _) = self.create_parents(dest_path,cursor)
		(owner,save) = self.owner_save(fid,cursor)
		if save:
			self.add_event(fid, None, EMOVE, False, owner, cursor)
		if (n:=cursor.execute('SELECT id FROM dirs WHERE parent_id=? AND name=?',(parent_id, name)).fetchone()) is not None and n[0]!=fid:
			self.delete(n[0],False)
		cursor.execute('UPDATE dirs SET parent_id = ?, name = ? WHERE id = ?',(parent_id, name, fid))
		n = cursor.execute('SELECT id FROM deleted WHERE parent_id=? AND name=?',(parent_id, name)).fetchone()
		if n is not None:
			cursor.execute('DELETE FROM deleted WHERE parent_id=? AND name=?',(parent_id, name))
			self.move_deleted(n[0], fid, cursor)

	def modified(self : Self, src_path : str, stat : Stat, is_directory : bool, is_synthetic : bool, cursor : Optional[sqlite3.Cursor] =None) -> None:
		if cursor is None: cursor = self.CUR
		self.notify(2, 'modified',src_path, stat, is_directory, is_synthetic, cursor)
		src_path = internal_path(src_path)
		if is_synthetic:
			print('synthetic modified',src_path, is_directory, datetime.fromtimestamp(time()))
			return
		ids = self.path2ids(src_path,cursor)
		if ids[-1] is None:
			self.notify(1.4, 'do modified as created',src_path, datetime.fromtimestamp(time()))
			return self.create1(ids, src_path, stat, is_directory,cursor)
		return self.modify(ids[-1], stat, False, cursor)

	def created(self : Self, src_path : str, stat : Stat, is_directory : bool, is_synthetic : bool, cursor : Optional[sqlite3.Cursor] =None) -> None:
		if cursor is None: cursor = self.CUR
		self.notify(2, 'created',src_path, stat, is_directory, is_synthetic, cursor)
		src_path = internal_path(src_path)
		if is_synthetic:
			print('synthetic created',src_path, is_directory, datetime.fromtimestamp(time()))
			return
		ids = self.path2ids(src_path,cursor)
		if ids[-1] is not None:
			# если было удалено, но это не было зафиксировано, а потом создалось - считаем, что просто изменилось
			self.notify(1.4, 'do created as modified',src_path, datetime.fromtimestamp(time()))
			return self.modify(ids[-1], stat, False, cursor)
		return self.create1(ids, src_path, stat, is_directory,cursor)

	def deleted(self : Self, src_path : str, is_directory : bool, is_synthetic : bool, cursor : Optional[sqlite3.Cursor] =None) -> None:
		if cursor is None: cursor = self.CUR
		self.notify(2, 'deleted',src_path, is_directory, is_synthetic, cursor)
		src_path = internal_path(src_path)
		if is_synthetic:
			print('synthetic deleted',src_path, is_directory, datetime.fromtimestamp(time()))
			return
		ids = self.path2ids(src_path,cursor)
		if ids[-1] is None:
			self.notify(1.4, 'error in deleted: unknown object:',src_path)
			return
		self.delete(ids[-1], False, cursor)

	def moved(self : Self, src_path : str, dest_path : str, stat : Stat, is_directory : bool, is_synthetic : bool, cursor : Optional[sqlite3.Cursor] =None) -> None:
		if cursor is None: cursor = self.CUR
		self.notify(2, 'moved',src_path, dest_path, stat, is_directory, is_synthetic, cursor)
		src_path = internal_path(src_path)
		dest_path = internal_path(dest_path)
		if is_synthetic:
			#print('synthetic moved', is_directory, datetime.fromtimestamp(time()))
			#print('\t'+src_path)
			#print('\t'+dest_path)
			return
		ids = self.path2ids(src_path,cursor)
		if ids[-1] is None:
			self.notify(1.4, 'do moved as created',src_path, dest_path, time())
			return self.created(dest_path, stat, is_directory, is_synthetic, cursor)
		self.move(ids[-1],dest_path, cursor)

	# --------------------------------
	# интерфейсные функции
	# --------------------------------
	def send2server(self : Self, name : str, *args, **kwargs) -> None:
		assert self.server_in is not None
		if len(name)==1:
			message = name+'\n'
		else:
			message = yaml.dump([name,list(args),kwargs], default_flow_style=True, sort_keys=False).replace('\n','')[1:-1]+'\n'
		if type(self.server_in) is socket.socket:
			self.server_in.sendall(message.encode())
		else:
			print(message, file=self.server_in, end='')
			self.server_in.flush()

	def reset_modified(self : Self) -> None:
		'''
		сбрасывает все modified флаги
		'''
		if self.server_in is not None: return self.send2server(inspect.stack()[0][3])
		with self.CON:
			self.CUR.execute('UPDATE dirs SET modified =0 WHERE modified==1')

	def create_owner(self : Self, name : str, save : bool) -> int:
		'''
		создает владельца, возвращает его id
		'''
		if self.server_in is not None: 
			self.send2server(inspect.stack()[0][3], name, save)
			sleep(1)
			return self.CUR.execute('SELECT id FROM owners WHERE name = ?',(name,)).fetchone()[0]
		with self.CON:
			self.CUR.execute('INSERT INTO owners (name, save) VALUES (?, ?)', (name,save))
			return self.CUR.execute('SELECT id FROM owners WHERE name = ?',(name,)).fetchone()[0]

	def update_owner(self : Self, name : str, save : bool) -> int:
		'''
		у существующего владельца обновляет параметр save, возвращает его id
		'''
		if self.server_in is not None: 
			self.send2server(inspect.stack()[0][3], name, save)
			sleep(1)
			return self.CUR.execute('SELECT id FROM owners WHERE name = ?',(name,)).fetchone()[0]
		with self.CON:
			self.CUR.execute('UPDATE owners SET save = ? WHERE name = ?', (save,name))
			return self.CUR.execute('SELECT id FROM owners WHERE name = ?',(name,)).fetchone()[0]

	def credate_owner(self : Self, name : str, save : bool) -> int:
		'''
		создаёт владельца, а если он уже существует, то только обновляет его параметр save. Возвращает его id
		'''
		if self.server_in is not None: 
			self.send2server(inspect.stack()[0][3], name, save)
			sleep(1)
			return self.CUR.execute('SELECT id FROM owners WHERE name = ?',(name,)).fetchone()[0]
		with self.CON:
			self.CUR.execute('''INSERT INTO owners (name, save) VALUES (?, ?) ON CONFLICT(name) DO UPDATE SET
				name = excluded.name,    save = excluded.save ''', (name,save))
			return self.CUR.execute('SELECT id FROM owners WHERE name = ?',(name,)).fetchone()[0]

	def del_owner(self : Self, owner : str) -> None:
		'''
		удаляет владельца и все упоминания о нём из таблиц stat, deleted
		'''
		if self.server_in is not None: return self.send2server(inspect.stack()[0][3], owner)
		with self.CON:
			self.CUR.execute('UPDATE stat SET owner = NULL WHERE stat.owner = (SELECT owners.id FROM owners WHERE owners.name = ?)',(owner,))
			self.CUR.execute('UPDATE deleted  SET owner  = NULL WHERE deleted.owner  = (SELECT owners.id FROM owners WHERE owners.name = ?)',(owner,))
			self.CUR.execute('DELETE FROM owners WHERE name = ?',(owner,))
		
	def del_owner_hist(self : Self, owner : str) -> None:
		'''
		удаляет владельца и все упоминания о нём из таблиц stat, deleted
		а также удалает из hist все записи, в которых упоминается этот владелец
		'''
		if self.server_in is not None: return self.send2server(inspect.stack()[0][3], owner)
		with self.CON:
			self.CUR.execute('DELETE FROM hist WHERE hist.id IN (SELECT stat.id FROM stat JOIN owners ON stat.owner=owners.id WHERE owners.name = ?)',(owner,))
			self.CUR.execute('DELETE FROM hist WHERE hist.id IN (SELECT deleted.id FROM deleted JOIN owners ON deleted.owner=owners.id WHERE owners.name = ?)',(owner,))
			self.CUR.execute('UPDATE stat SET owner = NULL WHERE stat.owner = (SELECT owners.id FROM owners WHERE owners.name = ?)',(owner,))
			self.CUR.execute('UPDATE deleted  SET owner = NULL WHERE deleted.owner  = (SELECT owners.id FROM owners WHERE owners.name = ?)',(owner,))
			self.CUR.execute('DELETE FROM owners WHERE name = ?',(owner,))

	def del_hist_owner(self : Self, owner : str, interval=None) -> None:
		'''
		удалает из hist все записи, в которых упоминается этот владелец
		'''
		#todo interval
		if self.server_in is not None: return self.send2server(inspect.stack()[0][3], owner, interval=interval)
		with self.CON:
			self.CUR.execute('DELETE FROM hist WHERE hist.id IN (SELECT stat.id FROM stat JOIN owners ON stat.owner=owners.id WHERE owners.name = ?)',(owner,))
			self.CUR.execute('DELETE FROM hist WHERE hist.id IN (SELECT deleted.id FROM deleted JOIN owners ON deleted.owner=owners.id WHERE owners.name = ?)',(owner,))

	def del_hist_id(self : Self, fid : int, interval=None) -> None:
		'''
		удаляет из истории все записи про заданный объект
		'''
		#todo interval
		if self.server_in is not None: return self.send2server(inspect.stack()[0][3], fid, interval=interval)
		fid = self.any2id_d(fid)[0]
		with self.CON:
			self.CUR.execute('DELETE FROM hist WHERE id = ?',(fid,))

	def del_hist_id_recursive(self : Self, fid : int, interval=None) -> None:
		'''
		удаляет из истории все записи про заданный объект и его дочерние объекты
		'''
		#todo interval
		if self.server_in is not None: return self.send2server(inspect.stack()[0][3], fid, interval=interval)
		fid = self.any2id_d(fid)[0]
		with self.CON:
			self.CUR.execute('DELETE FROM hist WHERE id = ?',(fid,))
			fids = self.CUR.execute('SELECT id FROM dirs WHERE parent_id = ? JOIN SELECT id FROM deleted  WHERE parent_id = ?',(fid,fid)).fetchall()
			while len(fids)>0:
				self.CUR.executemany('DELETE FROM hist WHERE id = ?',fids)
				fids2 = []
				for (fid,) in fids:
					fids2+= self.CUR.execute('SELECT id FROM dirs WHERE parent_id = ?',(fid,)).fetchall()
					fids2+= self.CUR.execute('SELECT id FROM deleted  WHERE parent_id = ?',(fid,)).fetchall()
				fids = fids2

	def rename_owner(self : Self, oname : str, name : str) -> None:
		if self.server_in is not None: return self.send2server(inspect.stack()[0][3], oname, name)
		with self.CON:
			self.CUR.execute('UPDATE owners SET name = ? WHERE name = ?',(name,oname))

	def set_owner(self : Self, path : str, owner : str, *, replace_inner : bool =False, in_deleted : bool =True) -> int:
		'''
		если такого owner-а еще нет - он создаётся
		save - надо ли в будущем сохранять события изменений этих файлов
			если None - не обновлять owner-а
		replace_inner:
			если True - устанавливает owner-а для всех вложенных объектов
			если False - только для тех вложенных, у которых еще нет owner-а или он такой как у объекта path
		in_deleted - устанавливать ли owner-а для удалённых объектов
		возвращает fid файла
		'''
		if self.server_in is not None: self.send2server(inspect.stack()[0][3], path, owner, replace_inner=replace_inner, in_deleted=in_deleted); return self.any2id_d(path)[0]
		with self.CON:
			with closing(self.CON.cursor()) as cursor:
				# oid - owner-id, который будем устанавливать
				if owner is not None:
					try:
						(oid,) = cursor.execute('SELECT id FROM owners WHERE name = ?',(owner,)).fetchone()
					except TypeError:
						print('set_owner: cannot find owner:',repr(owner))
						raise
				else:
					oid = None

				fid,d = self.any2id_d(path)
				if not d:
					(oldoid,) = cursor.execute('SELECT owner FROM stat WHERE id = ?',(fid,)).fetchone()
					cursor.execute('UPDATE stat SET owner = ? WHERE id = ?',(oid,fid))
				else:
					(oldoid,) = cursor.execute('SELECT owner FROM deleted WHERE id = ?',(fid,)).fetchone()
					cursor.execute('UPDATE deleted SET owner = ? WHERE id = ?',(oid,fid))

				def my_walk(did : int) -> None:
					#self.notify(0,'my_walk',did)
					if True:
						if replace_inner:
							n = cursor.execute('SELECT name,id,type FROM dirs WHERE parent_id = ? ',(did,)).fetchall()
						elif oldoid is None:
							n = cursor.execute('''SELECT dirs.name, dirs.id, dirs.type FROM dirs JOIN stat ON dirs.id=stat.id
								WHERE dirs.parent_id = ? AND stat.owner ISNULL''',(did,)).fetchall()
						else:
							n = cursor.execute('''SELECT dirs.name, dirs.id, dirs.type FROM dirs JOIN stat ON dirs.id=stat.id
								WHERE dirs.parent_id = ? AND stat.owner = ?''',(did,oldoid)).fetchall()
						for name,fid,ftype in n:
							cursor.execute('UPDATE stat SET owner = ? WHERE id = ?',(oid,fid))
							my_walk(fid)
					if in_deleted:
						if replace_inner:
							n = cursor.execute('SELECT name,id FROM deleted WHERE parent_id = ? ',(did,)).fetchall()
						elif oldoid is None:
							n = cursor.execute('''SELECT name,id FROM deleted
								WHERE parent_id = ? AND owner ISNULL''',(did,)).fetchall()
						else:
							n = cursor.execute('''SELECT name,id FROM deleted
								WHERE parent_id = ? AND owner = ?''',(did,oldoid)).fetchall()
						for name,fid in n:
							cursor.execute('UPDATE deleted SET owner = ? WHERE id = ?',(oid,fid))
							my_walk(fid)
				my_walk(fid)
				return fid

	def set_create_owner(self : Self, path : str, owner : str, save : bool, *, del_hist : bool =False, replace_inner : bool =False, in_deleted : bool =True) -> None:
		if self.server_in is not None: return self.send2server(inspect.stack()[0][3], path, owner, save, del_hist=del_hist, replace_inner=replace_inner, in_deleted=in_deleted)
		self.create_owner(owner, save)
		self.set_owner(path, owner, replace_inner=replace_inner, in_deleted=in_deleted)
		if del_hist: self.del_hist_owner(owner)

	def set_credate_owner(self : Self, path : str, owner : str, save : bool, *, del_hist : bool =False, replace_inner : bool =False, in_deleted : bool=True) -> None:
		if self.server_in is not None: return self.send2server(inspect.stack()[0][3], path, owner, save, del_hist=del_hist, replace_inner=replace_inner, in_deleted=in_deleted)
		self.credate_owner(owner, save)
		self.set_owner(path, owner, replace_inner=replace_inner, in_deleted=in_deleted)
		if del_hist: self.del_hist_owner(owner)

	def help(self : Self) -> None:
		if self.server_in is not None: return self.send2server(inspect.stack()[0][3])
		print('''
		syntax: yaml list of
			fun_name, args
		or
			fun_name, args, kwargs
		without brackets. For example:
			set_VERBOSE, [1]
			set_owner, ["/home", "users"], {replace_inner=False, in_deleted=True}

		set_VERBOSE(x)
		get_VERBOSE()
		check_integrity()
		update_hashes(with_all=False)

		create_owner(name, save)
		update_owner(name, save)
		credate_owner(name, save)
		del_owner(owner)
		del_owner_hist(owner)
		del_hist_owner(owner, interval=None)
		del_hist_id(id, interval=None)
		rename_owner(oname, name)
		set_owner(path, owner, *, replace_inner=False, in_deleted=True)
		set_create_owner(path, owner, save, *, del_hist=False, replace_inner=False, in_deleted=True)
		set_credate_owner(path, owner, save, *, del_hist=False, replace_inner=False, in_deleted=True)
		help()
			''')

	def watch(self : Self, do_stat : bool = True) -> None:
		'''запускает watchdog, который ловит события файловой системы
		также может выполнять команды из stdin'''
		# взаимодействуем с ФС
		from watchdog.events import FileSystemEvent, FileSystemEventHandler
		from watchdog.observers import Observer

		self.check_integrity()
		if do_stat:
			#self.notify(0,'walk_stat_all started')
			self.walk_stat_all()
			#self.notify(0,'walk_stat_all finished')

		# todo список полностью игнорируемых путей
		# todo сделать временный журнал ошибок, в котором последующие ошибки могут отменять предыдущие
		#if 0:
		#	def modified(src_path, stat, is_directory, is_synthetic, cursor):
		#		print('modified',src_path)
		#	
		#	def created(src_path, stat, is_directory, is_synthetic, cursor):
		#		print('created',src_path)
		#	
		#	def deleted(src_path, is_directory, is_synthetic, cursor):
		#		print('deleted',src_path)
		#	
		#	def moved(src_path, dest_path, stat, is_directory, is_synthetic, cursor):
		#		print('moved',src_path)
			
		def my_event_handler(event: FileSystemEvent) -> None:
			assert isinstance(event.src_path, str)
			assert isinstance(event.dest_path, str)
			if event.event_type=='event closed_no_write':
				self.notify(1.5, 'pass closed_no_write',event.src_path)
				pass
			elif event.event_type=='opened':
				self.notify(1.5, 'event pass opened',event.src_path)
				pass
			elif event.event_type=='modified' or event.event_type=='closed':
				self.notify(1.5, 'event modified',event.src_path)
				try:
					stat = os_stat(event.src_path)
				except FileNotFoundError as e:
					self.notify(1.4, 'error in modified event:', type(e), e, event.src_path, event.is_directory, event.is_synthetic)
				else:
					self.modified(event.src_path, stat, event.is_directory, event.is_synthetic, self.CUR)
				
			elif event.event_type=='created':
				self.notify(1.5, 'event created',event.src_path)
				try:
					stat = os_stat(event.src_path)
				except FileNotFoundError as e:
					self.notify(1.4, 'error in created event:', type(e), e, event.src_path, event.is_directory, event.is_synthetic)
				else:
					self.created(event.src_path, stat, event.is_directory, event.is_synthetic, self.CUR)
			elif event.event_type=='deleted':
				self.notify(1.5, 'event deleted',event.src_path)
				self.deleted(event.src_path, event.is_directory, event.is_synthetic, self.CUR)
			elif event.event_type=='moved':
				self.notify(1.5, 'event moved',event.src_path,event.dest_path)
				try:
					stat = os_stat(event.dest_path)
				except FileNotFoundError as e:
					self.notify(1.4, 'do moved as deleted:', type(e), e, event.src_path, event.dest_path, event.is_directory, event.is_synthetic)
					self.deleted(event.src_path, event.is_directory, event.is_synthetic, self.CUR)
				else:
					self.moved(event.src_path, event.dest_path, stat, event.is_directory, event.is_synthetic, self.CUR)
			else:
				self.raise_notify(None,'event '+repr(event))

		self.q = Queue()

		self_q = self.q
		class MyEventHandler(FileSystemEventHandler):
			def on_any_event(self : Self, event: FileSystemEvent) -> None:
				if event.event_type=='closed_no_write':
					pass
				elif event.event_type=='opened':
					pass
				else:
					#print('put',event.event_type,event.src_path)
					self_q.put(event)
		def observe(root_dirs : List[str]):
			event_handler = MyEventHandler()  # Создаем обработчик с временным значением shared_data
			observer = Observer()

			for dr in root_dirs:
				observer.schedule(event_handler, dr, recursive=True)
			observer.start()
			return observer
		observer = observe(self.ROOT_DIRS)
		self.notify(0,
			f"Observer '{Observer.__name__}' started...", # type: ignore[attr-defined]
			 threading.current_thread().name, datetime.fromtimestamp(time()))


		def keyboard_monitor() -> None:
			x= ''
			while x!='q':
				try:
					x = input()
				except EOFError:
					x = 'q'
				self.q.put(x)
		if self.keyboard_thr is None or not self.keyboard_thr.is_alive():
			self.keyboard_thr = threading.Thread(target = keyboard_monitor, args=tuple(), name='keyboard_thr', daemon=True)
			self.keyboard_thr.start()
		else:
			self.notify(1.5,'keep old keyboard_thr')

		stopped = False
		def commit_monitor() -> None:
			while not stopped:
				sleep(60)
				self.q.put('u')
		if self.commit_thr is None or not self.commit_thr.is_alive():
			self.commit_thr = threading.Thread(target = commit_monitor, args=tuple(), name='commit_thr', daemon=True)
			self.commit_thr.start()
		else:
			self.notify(1.5,'keep old commit_thr')

		try:
			while True:
				event = self.q.get()
				if isinstance(event,FileSystemEvent):
					my_event_handler(event)
				elif type(event) is str:
					if event=='':
						pass
					if event=='q':
						if self.CON.in_transaction:
							#print('COMMIT event=="q"')
							self.CUR.execute('COMMIT')
							stopped = True
						break
					elif event=='u':
						if self.CON.in_transaction:
							#print('COMMIT event=="u"')
							self.CUR.execute('COMMIT')
							self.check_integrity()
					else:
						try:
							eventmes = event
							print('got: ', repr(eventmes))
							event = yaml.safe_load('['+event+']')
							#print('parsed:',event)
						except yaml.YAMLError as e:
							print(e)
						else:
							if self.CON.in_transaction:
								#print('COMMIT before command')
								self.CUR.execute('COMMIT')
							try:
								if len(event)==2: event.append({})
								fun, args, kwargs = event
								fun = getattr(self,fun)
								fun(*args,**kwargs)
								print('-----------------------')
							except Exception as e:
								self.raise_notify(e,'type: "help, []" for more information about syntax')
								print('got_: ', repr(eventmes))
							if self.CON.in_transaction:
								print('COMMIT after command')
								self.CUR.execute('COMMIT')
				else:
					self.notify(0,'unknown type:',type(event))
				self.q.task_done()
		except Exception as e:
			if __name__=='__main__':
				print_exception(type(e), e, e.__traceback__, chain=True)
				print()
				print('The above exception was the direct cause of the following exception:')
				print()
				print("Traceback (most recent call last):")
				print("".join(format_list(extract_stack()[:-1])), end="")
				self.notify(0,'!!! TOTAL FAIL !!!')
				os.abort()
			else:
				raise e
		finally:
			observer.stop()  # Останавливаем Observer
			observer.join()  # Ждем завершения потока
			self.notify(0,'watcher stopped')

	# --------------------------------
	# мониторинговые функции
	# --------------------------------

	@staticmethod
	def _unpack_interval(interval):
		if interval is None:
			tstart,tend = None, None
		else:
			tstart,tend = interval
		if tstart is None: tstart = 0
		if tend is None: tend = time()+1000
		return tstart,tend

	class InfoFid:
		def __init__(self,
				parent_id: int,
				name	:str,
				fid		:int,
				typ		:int, # TFILE/TDIR/TLINK/TOTHER
				modified:int, # 0 нет, 1 да, 2 это pre-root-dir
				deleted	:int|bool,
				path	:str,
				ids		:List[int], # path в формате списка id-ов
				data	:str|None, # hash/link
				stat	:Stat|None,
				count_static:int, # количество изменений, найденных статически
				count	:int, # количество изменений этого файла за указанный интервал
				count_all : int, # количество изменений за всё время
				oname	:str|None, # имя владельца
				save	:bool, # сохраняем или игнорируем события связанные с этим файлом
				oid		:int|None)->None:
			self.parent_id: int = parent_id
			self.name	:str = name
			self.fid	:int = fid
			self.typ	:int = typ
			self.modified:int = modified
			self.deleted:int|bool = deleted
			self.path	:str = path
			self.ids	:List[int] = ids
			self.data	:str|None = data
			self.stat	:Stat|None = stat
			self.count_static:int = count_static
			self.count	:int = count
			self.count_all :int = count_all
			self.oname	:str|None = oname
			self.save	:bool = save
			self.oid	:int|None = oid

	def info_fid(self : Self, fid : int, *, interval : None|Tuple[None|float, None|float] =None) -> "filesdb.InfoFid":
		'''
		возращает инфу об одном файле, подсчитывает количество упоминаний в истории за заданный промежуток времени
		'''
		if fid==0:
			return self.InfoFid(
				parent_id=0,
				name	='',
				fid		=0,
				typ		=TDIR,
				modified=2,
				deleted	=0,
				path	='/',
				ids		=[0],
				data	=None,
				stat	=None,
				count_static=0,
				count	=0,
				count_all=0,
				oname	=None,
				save	=True,
				oid		=None,
			)
		tstart,tend = self._unpack_interval(interval)

		count:int
		count_static:int
		count_all : int

		n = self.CUR.execute('SELECT parent_id,name,id,type,modified FROM dirs WHERE id = ?',(fid,)).fetchone()
		if n is not None:
			(parent_id,name,fid,typ,modified) = n
			deleted = False
			path = self.id2path(fid)
			ids = cast(List[int], self.path2ids(path))

			if modified!=2: # pre-root-dir
				(data, oid) = self.CUR.execute('SELECT data, owner FROM stat WHERE id = ?',(fid,)).fetchone()
				stat = self.get_stat(fid)

				nn = self.CUR.execute(
					'SELECT COUNT(*), SUM(CASE WHEN static_found>0 THEN 1 ELSE 0 END) FROM hist WHERE id==? AND ?<=time AND time<=?'
					,(fid,tstart,tend)).fetchone()
				count, count_static = nn if nn is not None else (0, 0)
				nn = self.CUR.execute('SELECT COUNT(*) FROM hist WHERE id==?',(fid, )).fetchone()
				count_all = nn[0] if nn is not None else 0

				if oid is not None:
					(oname,save) = self.CUR.execute('SELECT name, save FROM owners WHERE id = ?',(oid,)).fetchone()
				else:
					(oname,save) = (None, True)
			else:
				(data, oid, stat, count_static, count, count_all, oname, save) = (None, None, None, 0, 0, 0, None, None)
		else:
			n = self.CUR.execute('SELECT parent_id,name,id,owner FROM deleted WHERE id = ?',(fid,)).fetchone()
			if n is None: self.raise_notify(None, f"can't find {fid} in dirs and in deleted")
			(parent_id,name,fid,oid) = n
			deleted = True
			modified = 0
			path = self.id2path_d(fid)[0]
			ids = cast(List[int], self.path2ids_d(path)[0])

			n = self.CUR.execute(f'''SELECT data, {FIELDS_STAT}, type
				FROM hist WHERE id = ? ORDER BY time DESC LIMIT 1''',(fid,)).fetchone()
			if n is not None:
				data = n[0] if n[0]!='' and n[0]!=-1 else None
				stat = tuple2stat(*n[1:-1]) if n[1]!=-1 else None
				typ = n[-1] if n[-1]!=-1 else None
			else:
				data = None
				stat = None
				typ = None

			nn = self.CUR.execute('SELECT COUNT(*), SUM(CASE WHEN static_found>0 THEN 1 ELSE 0 END) FROM hist WHERE id==? AND ?<=time AND time<=?',(fid,tstart,tend)).fetchone()
			count, count_static = nn if nn is not None else (0, 0)
			nn = self.CUR.execute('SELECT COUNT(*) FROM hist WHERE id==?',(fid,)).fetchone()
			count_all = nn[0] if nn is not None else 0

			if oid is not None:
				(oname,save) = self.CUR.execute('SELECT name, save FROM owners WHERE id = ?',(oid,)).fetchone()
			else:
				(oname,save) = (None, True)
		return self.InfoFid(
			parent_id=parent_id,
			name	=name,
			fid		=fid,
			typ		=typ,
			modified=modified,
			deleted	=deleted,
			path	=path,
			ids		=ids,
			data	=data,
			stat	=stat,
			count_static=count_static,
			count	=count,
			count_all=count_all,
			oname	=oname,
			save	=save,
			oid		=oid,
		)

	@staticmethod
	def format_info(info : "filesdb.InfoFid", *, info_lev : int=1, path_indent : None|str =None, nest_reducer : int=0, abs_path : None|bool =None, show_owner : bool =True) -> List[str]:
		'''
		info_lev=0	права и дату модификации не показывает, показывает полный путь
		info_lev=1	путь короткий, показывает права и дату модификации
		info_lev=2	путь короткий, показывает права и дату модификации и uid, gid, size, data
		path_indent	если не None показывает только имя, но перед ним делает отступы в количестве глубины вложенности
		если abs_path не None - path_indent игнорируется
		'''
		out_data : List[str] = []

		if abs_path is None:
			abs_path = info_lev==0
		else:
			path_indent = None

		# Modified-count-static
		s = ''
		if info.modified!=2:
			if info.deleted:
				s+='D'
			if info.modified:
				s+='M'
			if info.count>0:
				s+=str(info.count - info.count_static)
				if info.count_static>0:
					s+='+'+str(info.count_static)
		out_data.append(s)

		# root-fid
		s = ''
		if info.modified==2:
			s+='R'
		elif info.typ is not None:
			s+= typ2str(info.typ)
		else:
			s+='?'
		s+=f'{info.fid:7}'
		out_data.append(s)

		# access
		if info_lev>0:
			if info.stat is None:
				s = '??????????'
			else:
				#    -. Обычный или исполняемый документ
				#    d. Папка.
				#    l. Символьная ссылка.
				#    p. ФИФО.
				#    b. Блочное устройство.
				#    s. Сокет.
				#    c. Символьное устройство.
				typ = typ2str(info.typ)
				if typ=='o':
					if STAT.S_ISFIFO(info.stat.st_mode):   typ = 'p'
					elif STAT.S_ISBLK(info.stat.st_mode):  typ = 'b'
					elif STAT.S_ISSOCK(info.stat.st_mode): typ = 's'
					elif STAT.S_ISCHR(info.stat.st_mode):  typ = 'c'
					else:                     typ = '?'
				s = typ+access2str(info.stat.st_mode)
			out_data.append(s)

		# modif-time
		if info_lev>0:
			if info.stat is None:
				out_data.append('????-??-?? ??:??:??.??????')	
			else:
				out_data.append(str(datetime.fromtimestamp(info.stat.mtime)))

		# uid, gid, size
		if info_lev==2:
			if info.stat is not None:
				out_data.append(get_username_by_uid(info.stat.st_uid) if info.stat.st_uid is not None else '?')
				out_data.append(get_groupname_by_gid(info.stat.st_gid)if info.stat.st_gid is not None else '?')
				out_data.append(str(info.stat.st_size))
			else:
				out_data.append('user-?')
				out_data.append('group-?')
				out_data.append('size-?')

		# path-name
		if path_indent is not None:
			out_data.append(path_indent*(len(info.ids)-1-abs(nest_reducer)) + info.name)
		elif abs_path:
			out_data.append(info.path)
		else:
			out_data.append(info.name)

		# data
		if info_lev==2:
			if info.data is not None and info.typ is not None:
				out_data.append(('->' if info.typ==TLINK else '')+str(info.data))
			else:
				out_data.append('???')

		# owner
		if show_owner:
			out_data.append(f'<={info.oname}({"+" if info.save else "-"})' if info.oname is not None else '')

		return out_data

	def ls(self : Self, fid_in : None|str|int =None,*,info_lev : int =1) -> None:
		'''
		показывает папку и её содержимое
		'''
		fid = self.any2id(fid_in)
		print(*self.format_info(self.info_fid(fid), info_lev=0),sep='\t')
		for (fid,) in self.CUR.execute('SELECT id FROM dirs WHERE parent_id = ?',(fid,)).fetchall():
			print(*self.format_info(self.info_fid(fid), info_lev=info_lev),sep='\t')

	def ls_r(self : Self, fid_in : None|str|int =None,*,
			info_lev : int =1, show_deleted : bool =True, 
			where : str|Tuple[List[int],List[int]]|List[int] ='all', interval : None|Tuple[None|float, None|float] =None
	) -> None:
		'''
		where:
		'all' - всё показывать
		'hist_owner'
		'hist_noowner'
		'modified'
		(*,*) -> (fids,fidsd)
		list/set -> fids -> fidsd
		'''
		assert where in ['all', 'hist_owner', 'hist_noowner', 'modified'] if type(where) is str else True
		fids: List[int]|Set[int]
		fidsd: List[int]|Set[int]
		tstart,tend = self._unpack_interval(interval)

		fid = self.any2id(fid_in)
		print(*self.format_info(self.info_fid(fid), info_lev=0),sep='\t')
		nest_reducer = (len(self.path2ids(self.id2path(fid))))
		if where=='hist_owner': 
			fids                   = set(self.CUR.execute(
				'SELECT stat.id    FROM stat    JOIN hist ON stat.id   ==hist.id WHERE stat.owner    NOT NULL AND ?<=hist.time AND hist.time<=?',(tstart,tend)).fetchall())
			if show_deleted: fidsd = set(self.CUR.execute(
				'SELECT deleted.id FROM deleted JOIN hist ON deleted.id==hist.id WHERE deleted.owner NOT NULL AND ?<=hist.time AND hist.time<=?',(tstart,tend)).fetchall())
		if where=='hist_noowner': 
			fids                   = set(self.CUR.execute(
				'SELECT stat.id    FROM stat    JOIN hist ON stat.id   ==hist.id WHERE stat.owner    IS NULL AND ?<=hist.time AND hist.time<=?',(tstart,tend)).fetchall())
			if show_deleted: fidsd = set(self.CUR.execute(
				'SELECT deleted.id FROM deleted JOIN hist ON deleted.id==hist.id WHERE deleted.owner IS NULL AND ?<=hist.time AND hist.time<=?',(tstart,tend)).fetchall())
		if where=='modified': 
			fids                   = set(self.CUR.execute(
				'SELECT dirs.id    FROM dirs    JOIN hist ON dirs.id   ==hist.id WHERE dirs.modified  >0     AND ?<=hist.time AND hist.time<=?',(tstart,tend)).fetchall())
			if show_deleted: fidsd = set()
		if type(where) is tuple:
			(fids,fidsd) = where
		if isinstance(where, list) or isinstance(where, set):
			fids = fidsd = where
		#print(fids)
		#print(fidsd)

		count = 0
		def my_walk(did : int, parents : List[int]):
			nonlocal count
			printed = False
			if True:
				for (fid,) in self.CUR.execute('SELECT id FROM dirs WHERE parent_id = ?',(did,)).fetchall():
					if where=='all' or (fid,) in fids:
						info = self.info_fid(fid,interval=interval)
						if interval is None or info.count>0:
							printed = True
							count+=1
							for pid in parents: # вывод невыведенных парентов
								print(*self.format_info(self.info_fid(pid,interval=interval), info_lev=info_lev, path_indent='  ', nest_reducer=nest_reducer),sep='\t')
							parents = []
							print(*self.format_info(info, info_lev=info_lev, path_indent='  ', nest_reducer=nest_reducer),sep='\t')
							my_walk(fid,[])
						else:
							if my_walk(fid,parents+[fid]):
								printed = True
								parents = []
					else:
						if my_walk(fid,parents+[fid]):
							printed = True
							parents = []
			if show_deleted:
				for (fid,) in self.CUR.execute('SELECT id FROM deleted WHERE parent_id = ?',(did,)).fetchall():
					if where=='all' or (fid,) in fidsd:
						info = self.info_fid(fid,interval=interval)
						if interval is None or info.count>0:
							printed = True
							count+=1
							for pid in parents: # вывод ненвыведенных парентов
								print(*self.format_info(self.info_fid(pid,interval=interval), info_lev=info_lev, path_indent='  ', nest_reducer=nest_reducer),sep='\t')
							parents = []
							print(*self.format_info(self.info_fid(fid), info_lev=info_lev, path_indent='  ', nest_reducer=nest_reducer),sep='\t')
							my_walk(fid,[])
						else:
							if my_walk(fid,parents+[fid]):
								printed = True
								parents = []
					else:
						if my_walk(fid,parents+[fid]):
							printed = True
							parents = []
			return printed
		my_walk(fid,[])
		print('total objects number:',count)#, 'checked objects:',len(fids),'+',len(fidsd))

	def list_owners(self : Self, path : None|str =None, show_deleted : bool =True, owner=None) -> None:
		# todo если задан owner - показывает только его
		# выводить только если owner и owner родителя не совпадают
		# deleted - с пометками
		count = 0
		if path is None:
			for dr in self.ROOT_DIRS:
				self.list_owners(dr, show_deleted)
			return
		path = os.path.abspath(path)
		# format: save owner fid deleted path
		(fids,deleted) = self.path2ids_d(path,self.CUR)
		fid = fids[-1]
		if fid is None:
			self.raise_notify(None, 'path does not exist')
		assert fid is not None
		def my_walk(did : int , deleted : bool , downer : int|None, depth : int) -> None:
			nonlocal count
			if not deleted:
				for (owner, fid) in self.CUR.execute(
					'SELECT stat.owner, stat.id FROM stat JOIN dirs ON dirs.id=stat.id WHERE dirs.parent_id = ?',(did,)).fetchall():
					if owner!=downer:
						count+=1
						print(*self.format_info(self.info_fid(fid), info_lev=0, abs_path=True),sep='\t')
					my_walk(fid,False,owner,depth+1)
			if show_deleted:
				for (owner, fid) in self.CUR.execute('SELECT owner, id FROM deleted WHERE parent_id = ?''',(did,)).fetchall():
					if owner!=downer:
						count+=1
						print(*self.format_info(self.info_fid(fid), info_lev=0, abs_path=True),sep='\t')
					my_walk(fid,True,owner,depth+1)
		if show_deleted or not deleted:
			info = self.info_fid(fid)
			count+=1
			print(*self.format_info(info, info_lev=0, abs_path=True),sep='\t')
			my_walk(fid,deleted,info.oid,0)
		print('total objects number:',count)

	def unused_owners(self : Self) -> None:
		for (oid, oname) in self.CUR.execute('''SELECT owners.id, owners.name  FROM owners WHERE owners.id NOT IN 
				(SELECT stat.owner AS id FROM stat WHERE stat.owner NOT NULL UNION SELECT deleted.owner AS id FROM deleted WHERE deleted.owner NOT NULL)'''):
			print(oid, oname, sep='\t')

	def all_info(self : Self, interval : None|Tuple[None|float, None|float] =None, show_deleted : bool=True):
		print('----- modified with no owner ----')
		for path in self.ROOT_DIRS:
			self.ls_r(path,info_lev=1, show_deleted=True, where='hist_noowner',interval=interval)
		print('----- modified with owner ----')
		for path in self.ROOT_DIRS:
			self.ls_r(path,info_lev=1, show_deleted=True, where='hist_owner',interval=interval)
		print('----- with owner ----')
		self.list_owners()
		print('----- unused owners ----')
		self.unused_owners()

	def hist_id(self : Self, fid : int) -> None:
		print(self.id2path_d(fid,self.CUR))
		# todo
		#with closing(self.CON.execute('SELECT * FROM dirs WHERE id = ?',(fid,))) as cursor:
		#	list(self.print_fid(cursor))
		for (parent_id, name, typ, etyp, data, _time, static_found) in \
			self.CUR.execute('SELECT parent_id, name, type, event_type, data, time, static_found FROM hist WHERE id = ? ORDER BY time DESC',(fid,)).fetchall():
				if etyp==ECREAT: etyp = 'C'
				elif etyp==EDEL: etyp = 'D'
				elif etyp==EMOVE: etyp= 'V'
				elif etyp==EMODIF:etyp= 'M'
				else: assert False, etyp
				if etyp=='V':
					print(etyp+' '+('S' if static_found else 'W')+' '+str(datetime.fromtimestamp(_time)),
						 self.id2path_d(parent_id,self.CUR)[0]+os.sep+name)
				else:
					print(etyp+' '+('S' if static_found else 'W')+' '+str(datetime.fromtimestamp(_time)))

	# ------------------------------------
	# инициализация приложения/библиотеки
	# ------------------------------------

	def read_root_dirs(self : Self) -> List[str]:
		'''из базы данных считывает, какие папки отмечены для слежения'''
		with self.CON:
			root_dirs = []
			def walk(did : int, path : str) -> None:
				n = self.CUR.execute('SELECT id, name, modified FROM dirs WHERE parent_id = ?',(did,)).fetchall()
				for (fid, name, modified) in n:
					if modified==2:
						walk(fid, path+os.sep+name)
					else:
						root_dirs.append(external_path(path+os.sep+name))
			walk(0, '')
			return root_dirs

	@staticmethod
	def get_root_dirs() -> List[str]:
		'''определяет, за какими папками надо на самом деле следить, если указано следить за всей файловой системой'''
		if sys.platform == 'win32': return ['C:\\']
		dirs = []
		for rd in os.listdir(path='/'):
			if rd in ['media','cdrom','mnt','proc','sys','dev','run']:
				continue
			mode = os.stat('/'+rd,follow_symlinks=False).st_mode
			if STAT.S_ISDIR(mode) and not STAT.S_ISLNK(mode):
				dirs.append('/'+rd)
		return dirs

	def __init__(self : Self, files_db : str, root_dirs : Optional[List[str]|str] = None, nohash : bool = False, nocheck : bool =False, server_in : Optional[str] =None) -> None:
		'''
		инициализирует FILES_DB, ROOT_DIRS; открывает сединение CON, CUR (по умолчанию только для чтения)
		files_db - имя файла базы данных
		root_dirs - если задан - пытается создать базу данных с этими наблюдаемыми директориями, 
			иначе self.ROOT_DIRS ссчитывает из базы данных
		nohash - не вычислять хэши при инициализации
		nocheck - не проверять целостность БД
		режим сервера:
			может принимать команды через stdin
		режим клиента
			может читать БД
			для записи в БД отправляет команды на сервер
			server_in - имя UNIX сокета или FIFO чтобы отправлять управляющие команды
		'''
		self.VERBOSE = 0.5
		#self.keyboard_thr = None
		#self.commit_thr = None
		self.last_notification = time()
		#self.q = None
		self.username = 'feelus' # todo получать из аргументов или из среды

		try:
			self.CON.cursor().close()
		except Exception:
			pass
		else:
			self.CON.close()
			#raise Exception('close existing connection before opening new one')

		ro = server_in is not None
		self.server_in : Optional[TextIO] = None
		if server_in is not None:
			if STAT.S_ISSOCK(os.stat(server_in).st_mode):
				sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) # type: ignore[attr-defined]
				sock.connect(server_in)
				self.server_in = sock.makefile('w',encoding='utf-8')
			else:
				self.server_in = open(server_in, "w")

		if root_dirs is None:
			# loading existring db
			if not os.path.isfile(files_db):
				raise Exception(f'database {files_db} does not exist. Create it with root_dirs argument')
			self.FILES_DB = files_db
			if ro:
				self.CON = sqlite3.connect('file:'+self.FILES_DB+'?mode=ro', uri=True)
				print(f'connected in readonly mode to {self.FILES_DB}')
			else:
				self.CON = sqlite3.connect(self.FILES_DB)
				db_mode = self.CON.execute("PRAGMA journal_mode=WAL;").fetchone()
				assert db_mode==('wal',), db_mode
				print(f'connected in readwrite mode to {self.FILES_DB}')
			self.CON.row_factory = decode_row_factory
			self.CUR = self.CON.cursor()

			if not nocheck and not ro:
				self.check_integrity()
			self.ROOT_DIRS = self.read_root_dirs()
		else:
			# creation new db
			if os.path.isfile(files_db):
				raise Exception(f'database {files_db} already exist. To open it run this function/file without root_dirs argument')
			if type(root_dirs) is str:
				root_dirs = [root_dirs]
			if root_dirs==['/']:
				root_dirs = self.get_root_dirs()
				print('root_dirs:',root_dirs)
			assert isinstance(root_dirs,list)
			root_dirs = [os.path.abspath(x) for x in root_dirs]
			self.FILES_DB = files_db
			self.CON = sqlite3.connect(self.FILES_DB)
			self.CON.row_factory = decode_row_factory
			self.CUR = self.CON.cursor()
			self.ROOT_DIRS = root_dirs
			print('ROOT_DIRS:',self.ROOT_DIRS)
			self.init_db() # nohash ...
			if not nocheck and not ro:
				self.check_integrity()
			if ro:
				self.CON.close()
				self.CON = sqlite3.connect('files:'+self.FILES_DB+'?mode=ro', uri=True)

	def __del__(self : Self) -> None:
		self.CON.close()
		if self.server_in is not None:
			self.server_in.close()
		if self.keyboard_thr is not None and self.keyboard_thr.is_alive():
			self.notify(1.5,f'filesdb({repr(self.FILES_DB)}): lost running keyboard thread')
		if self.commit_thr is not None and self.commit_thr.is_alive():
			self.notify(1.5,f'filesdb({repr(self.FILES_DB)}): lost running commit thread')

	def execute(self : Self,*args,**kwargs) -> sqlite3.Cursor:
		with self.CON:
			return self.CUR.execute(*args,**kwargs)

if __name__ == "__main__":
	import sys

	if len(sys.argv)==1 or sys.argv[1]=='--help' or sys.argv[1]=='-h' :
		print(f'''run:\n  {sys.argv[0]} [--nohash] files.db [dir...]''')
		exit(0)

	import sys
	nohash : bool
	if sys.argv[1]=='--nohash':
		nohash = True
		del sys.argv[1]
	else:
		nohash = False

	root_dirs : Optional[List[str]] = sys.argv[2:]
	if root_dirs is None or len(root_dirs)==0: root_dirs = None
	print(sys.argv[1],root_dirs,nohash)
	fdb = filesdb(sys.argv[1],root_dirs,nohash)
	fdb.watch()
