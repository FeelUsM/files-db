import os
import stat as STAT
import sqlite3
from tqdm import tqdm
import hashlib
from time import time, sleep
from datetime import datetime
from contextlib import closing
from traceback import print_exception, extract_stack, extract_tb, format_list

class AttrDict(dict):
	def __getattr__(self, key):
		if key not in self:
			raise AttributeError(key) # essential for testing by hasattr
		return self[key]
	def __setattr__(self, key, value):
		self[key] = value
def make_dict(**kwargs):
	return AttrDict(kwargs)

class NullContextManager(object):
    def __init__(self, dummy_resource=None):
        self.dummy_resource = dummy_resource
    def __enter__(self):
        return self.dummy_resource
    def __exit__(self, *args):
        pass

# cur_dirs:modified
# 2 - pre-root-dir
# 1 - modified
# 0 - not modified

# hist:static_found
# 0 - обнаружено watchdog-ом, 
# 1 - обнаружено статитсеским обходом дерева каталогов 
# 2 - обнаружено путём сравнения хешей

# cur_dirs:type
MFILE = 0
MDIR = 1
MLINK = 2
MOTHER = 3 # встречаются всякие сокеты, именованные каналы. Не смотря на то, что в /sys, /dev, /proc, /run - не лезем

def os_stat(path,follow_symlinks=False):
	return os.stat(path,follow_symlinks=follow_symlinks)
def typ2str(x):
	assert 0<=x<=3
	return '-' if x==MFILE else \
			'd'if x==MDIR else \
			'l'if x==MLINK else \
			'o'#if x==MOTHER
def is_link(mode): return STAT.S_ISLNK(mode)
def is_dir(mode):  return STAT.S_ISDIR(mode)
def is_file(mode): return STAT.S_ISREG(mode)
def is_other(mode):return STAT.S_ISCHR(mode) or STAT.S_ISBLK(mode) or\
					STAT.S_ISFIFO(mode) or STAT.S_ISSOCK(mode) or\
					STAT.S_ISDOOR(mode) or STAT.S_ISPORT(mode) or\
					STAT.S_ISWHT(mode)
def simple_type(mode):
	typ = MLINK if STAT.S_ISLNK(mode) else\
		MDIR if STAT.S_ISDIR(mode) else\
		MFILE if STAT.S_ISREG(mode) else\
		MOTHER if STAT.S_ISCHR(mode) or STAT.S_ISBLK(mode) or\
			STAT.S_ISFIFO(mode) or STAT.S_ISSOCK(mode) or\
			STAT.S_ISDOOR(mode) or STAT.S_ISPORT(mode) or\
			STAT.S_ISWHT(mode) else \
		None
	if typ is None:
		raise Exception('unknown type')
	return typ


ECREAT = 1 # в этом случае все старые записи == -1
EMODIF = 2
EMOVE = 3
EDEL = 4

def etyp2str(etyp):
	assert 1<=etyp<=4
	return 'C' if etyp==ECREAT else\
			'M'if etyp==EMODIF else\
			'V'if etyp==EMOVE else\
			'D'#if etyp==EDEL

def stat_eq(stat, ostat):
	'''
	сравнивает два stat-а на равенство
	если это директории: должно совпадать всё кроме access_time и modification_time
	иначе: должно совпадать всё кроме access_time
	'''
	if stat.st_mode != ostat.st_mode:
		#if VERBOSE>=2: print('st_mode')
		return False
	if stat.st_ino != ostat.st_ino:
		#if VERBOSE>=2: print('st_ino')
		return False
	if stat.st_dev != ostat.st_dev:
		#if VERBOSE>=2: print('st_dev')
		return False
	if stat.st_nlink != ostat.st_nlink:
		#if VERBOSE>=2: print('st_nlink')
		return False
	if stat.st_uid != ostat.st_uid:
		#if VERBOSE>=2: print('st_uid')
		return False
	if stat.st_gid != ostat.st_gid:
		#if VERBOSE>=2: print('st_gid')
		return False
	if stat.st_size != ostat.st_size:
		#if VERBOSE>=2: print('st_size')
		return False
	if stat.st_ctime != ostat.st_ctime:
		#if VERBOSE>=2: print('st_ctime',datetime.fromtimestamp(ostat.st_ctime),datetime.fromtimestamp(stat.st_ctime))
		return False
	if simple_type(stat.st_mode)!=MDIR and stat.st_mtime != ostat.st_mtime:
		#if VERBOSE>=2: print('st_mtime')
		return False
	if stat.st_blocks != ostat.st_blocks:
		#if VERBOSE>=2: print('st_blocks')
		return False
	if stat.st_blksize != ostat.st_blksize:
		#if VERBOSE>=2: print('st_blksize')
		return False
	return True

def normalize_path(path):
	return path.replace('//','/').replace('//','/').replace('//','/').replace('//','/')

import pwd
def get_username_by_uid(uid):
	try:
		return pwd.getpwuid(uid).pw_name
	except KeyError:
		return None  # Если UID не существует
import grp
def get_groupname_by_gid(gid):
	try:
		return grp.getgrgid(gid).gr_name
	except KeyError:
		return None  # Если UID не существует
def access2str(st_mode):
	mode = STAT.S_IMODE(st_mode)
	assert mode < 2**10, mode
	s = ''
	for i in range(6,-1,-3):
		s+= 'r' if mode & 2**(i+2) else '-'
		s+= 'w' if mode & 2**(i+1) else '-'
		s+= 'x' if mode & 2**(i+0) else '-'
	if mode & 2**9:
		return s[:-1]+ ('t' if mode & 1 else 'T')
	return s


class filesdb:

	VERBOSE = 0.5
	# 0.5 - сообщать об изменениях объектов, которые не имеют владельцев
	# 1   - сообщать о записываемых событиях
	# 1.2 - сообщать обо всех событиях
	# 1.4 - сообщать о несоответствяих ФС, её образа и событий
	# 1.5 - сообщать о событиях
	# 2   - stat_eq и все функции событий
	# 3   - owner_save

	def notify(self, thr, *args, **kwargs):
		assert type(thr) in (int,float)
		if self.VERBOSE>=thr:
			print(*args, **kwargs)
			if __name__=='__main__':
				sep = kwargs['sep'] if 'sep' in kwargs else ' '
				os.system('notify-send "filesdb: '+sep.join(str(x) for x in args)+'"')

	def raise_notify(self,e,*args):
		'''
		исключение после которого можно прожолжить работу, сделав уведомление
		но если в интерактивном режиме - то лучше упасть с остановкой
		'''
		if __name__=='__main__':
			print_exception(type(e), e, e.__traceback__, chain=True)
			print()
			print('The above exception was the direct cause of the following exception:')
			print()
			print("Traceback (most recent call last):")
			print("".join(format_list(extract_stack()[:-2])), end="")
			print(*args)
			print("--------------------------")
		else:
			elocal = args[0] if len(args)==1 and isinstance(args[0],Exception) else Exception(*args)
			if e is None: raise elocal
			else:         raise elocal from e

	def set_VERBOSE(self,x):
		self.VERBOSE = x

	def get_VERBOSE(self):
		self.notify(0, self.VERBOSE)

	FILES_DB = None
	ROOT_DIRS = None
	CON = None
	CUR = None

	keyboard_thr = None
	commit_thr = None

	# -------------
	# схема данных
	# -------------

	def _create_tables(self):
		with self.CON:
			self.CUR.execute('''CREATE TABLE cur_dirs (
				parent_id INTEGER NOT NULL,                  /* id папки, в которой лежит данный объект */
				name      TEXT    NOT NULL,                  /* имя объекта в папке */
				id        INTEGER PRIMARY KEY AUTOINCREMENT, /* идентификатор объекта во всей БД */
				type      INTEGER NOT NULL,                  /* MFILE, MDIR, MLINK, MOTHER */
				modified  INTEGER NOT NULL,                  /* параметр обхода:
					0 - заходим при полном обходе
					1 - заходим приобходе модифицированных объектов
					2 - по таблице заходим всегда, но в ФС никогда не просматриваем (и даже stat не делаем) "pre-root-dir" */
			UNIQUE(parent_id, name)
			)
			''')
			self.CUR.execute('CREATE INDEX id_cur_dirs ON cur_dirs (id)')
			self.CUR.execute('CREATE INDEX parname_cur_dirs ON cur_dirs (parent_id, name)')	

			self.CUR.execute(''' CREATE TABLE cur_stat  (
				id         INTEGER PRIMARY KEY,
				type       INTEGER NOT NULL,
				
				st_mode    INTEGER, /* поля stat */
				st_ino     INTEGER,
				st_dev     INTEGER,
				st_nlink   INTEGER,
				st_uid     INTEGER,
				st_gid     INTEGER,
				st_size    INTEGER,
				st_atime   REAL,
				st_mtime   REAL,
				st_ctime   REAL,
				st_blocks  INTEGER,
				st_blksize INTEGER,
				
				data       TEXT, /* для файлов - хэш, для папок - хэш = сумма хэшей вложенных объектов (mod 2^32), для симлинков - сама ссылка */
				owner      INTEGER
			)
			''')
			self.CUR.execute('CREATE INDEX id_cur_stat ON cur_stat (id)')

			# для запоминания owner-ов удалённых файлов
			# и чтобы fid-ы не росли, если какой-то файл многократно удаляется и снова создаётся
			# можно было бы использовать hist для этих целей, но там каждый файл не в единственном экземпляре,
			# и особенно, если мы не хотим сохранять события о файле, а он постоянно удаляется и создаётся
			# todo добавить время удаления, чтобы можно было удалять инфу об очень давно удалённых файлах
			self.CUR.execute('''
			CREATE TABLE deleted  (
				parent_id INTEGER NOT NULL, /* старая запись из cur_dirs */
				name      TEXT    NOT NULL, /* старая запись из cur_dirs */
				id        INTEGER NOT NULL, /* старая запись из cur_dirs */
				owner     INTEGER, /* при создании/восстановлении имеет преимущество перед owner-ом родительской папки */
			UNIQUE(id),
			UNIQUE(parent_id,name)
			)
			''')
			self.CUR.execute('CREATE INDEX id_deleted ON deleted (id)')
			self.CUR.execute('CREATE INDEX parname_deleted ON deleted (parent_id,name)')

			self.CUR.execute('''
			CREATE TABLE hist(
				parent_id    INTEGER NOT NULL, /* старая запись из cur_dirs */
				name         TEXT    NOT NULL, /* старая запись из cur_dirs */
				id           INTEGER NOT NULL, /* на id может быть несколько записей */
				type         INTEGER NOT NULL,
				event_type   INTEGER NOT NULL, /* ECREAT, EMODIF, EMOVE, EDEL */
				
				st_mode      INTEGER, /* старая запись из cur_stat */
				st_ino       INTEGER,
				st_dev       INTEGER,
				st_nlink     INTEGER,
				st_uid       INTEGER,
				st_gid       INTEGER,
				st_size      INTEGER,
				st_atime     REAL,
				st_mtime     REAL,
				st_ctime     REAL,
				st_blocks    INTEGER,
				st_blksize   INTEGER,

				data         TEXT,    /* старая запись из cur_stat */

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

			self.CUR.execute('''CREATE TABLE owners  (
				id    INTEGER PRIMARY KEY AUTOINCREMENT,
				name  TEXT    NOT NULL, /* например система-код система-логи программа-код, программа-конфиг, программа-данные, человек-проект */
				save  INTEGER NOT NULL, /* bool - сохранять ли данные об изменении этого объекта в hist */
				name1 TEXT, /* если у объекта несколько владельцев, то для каждой группы владельцев свой id, имена через запятую */
				name2 TEXT, /* а здесь имена каждого владельца по отдельности */
				name3 TEXT,
				name4 TEXT,
				name5 TEXT,
			UNIQUE(name)
			)
			''')
			self.CUR.execute('CREATE INDEX id_owners ON owners (id)')
			self.CUR.execute('CREATE INDEX name_owners ON owners (name)')

	def check_integrity(self):
		'''
		проверяет
		присутствуют таблицы: cur_dirs, cur_stat, deleted, hist, owners
		у каждого существует родитель
			для cur_dirs в cur_dirs
			для deleted в cur_dirs или deleted
			для hist в cur_dirs или deleted
			для pre-root_dir в pre-root_dir
			для modified в modified или pre-root_dir
		у всех из cur_dirs (кроме root_dir) есть обаз из cur_stat и наоборот
		для всех из hist есть образ в cur_stat или deleted
		для всех из cur_stat, deleted, hist у кого owner is not None есть owner в owners
		cur_dirs.type  cur_stat.type = simple_type(cur_stat.st_mode)
		todo проверка всех констант (cur_dirs.type, cur_dirs.modified, hist.type, hist.event_type, hist.static_found, owners.save)
		'''

		# присутствуют таблицы: cur_dirs, cur_stat, deleted, hist, owners
		tables = {x[0] for x in self.CUR.execute('SELECT name FROM sqlite_master')}
		assert 'cur_dirs' in tables, "table cur_dirs not found"
		assert 'cur_stat' in tables, "table cur_stat not found"
		assert 'deleted' in tables, "table deleted not found"
		assert 'hist' in tables, "table hist not found"
		assert 'owners' in tables, "table owners not found"

		# у каждого существует родитель
		# 	для cur_dirs в cur_dirs
		cur_dirs_parents = {x[0] for x in self.CUR.execute('SELECT parent_id FROM cur_dirs').fetchall()}
		cur_dirs_ids = {x[0] for x in self.CUR.execute('SELECT id FROM cur_dirs').fetchall()}
		assert cur_dirs_parents <= (cur_dirs_ids|{0}), f'lost parents in cur_dirs: {cur_dirs_parents-(cur_dirs_ids|{0})}'

		#	для pre-root_dir в pre-root_dir
		root_dirs_parents = {x[0] for x in self.CUR.execute('SELECT parent_id FROM cur_dirs WHERE modified = 2').fetchall()}
		root_dirs_ids = {x[0] for x in self.CUR.execute('SELECT id FROM cur_dirs WHERE modified = 2').fetchall()}
		assert root_dirs_parents <= (root_dirs_ids|{0}), f'lost parents in pre-root_dirs: {root_dirs_parents-(root_dirs_ids|{0})}'

		#	для modified в modified или pre-root_dir
		m_dirs_parents = {x[0] for x in self.CUR.execute('SELECT parent_id FROM cur_dirs WHERE modified = 1').fetchall()}
		m_dirs_ids = {x[0] for x in self.CUR.execute('SELECT id FROM cur_dirs WHERE modified = 1').fetchall()}
		assert m_dirs_parents <= (m_dirs_ids|root_dirs_ids|{0}), f'lost parents in modified: {m_dirs_parents-(m_dirs_ids|root_dirs_ids|{0})}'

		# у всех из cur_dirs (кроме pre-root_dir) есть обаз из cur_stat и наоборот
		notroot_dirs_ids = {x[0] for x in self.CUR.execute('SELECT id FROM cur_dirs WHERE modified != 2').fetchall()}
		stat_ids = {x[0] for x in self.CUR.execute('SELECT id FROM cur_stat').fetchall()}
		assert notroot_dirs_ids == stat_ids, f'mismatch root_dirs and stat: {notroot_dirs_ids - stat_ids}, {stat_ids - notroot_dirs_ids}'

		#	для deleted в cur_dirs или deleted
		deleted_parents = {x[0] for x in self.CUR.execute('SELECT parent_id FROM deleted').fetchall()}
		deleted_ids = {x[0] for x in self.CUR.execute('SELECT id FROM deleted').fetchall()}
		assert deleted_parents <= (notroot_dirs_ids|deleted_ids), f'lost parents in deleted: {deleted_parents-(notroot_dirs_ids|deleted_ids)}'

		# 	для hist в cur_dirs или deleted
		hist_parents = {x[0] for x in self.CUR.execute('SELECT parent_id FROM hist WHERE parent_id!=-1').fetchall()}
		assert hist_parents <= (notroot_dirs_ids|deleted_ids), f'lost parents in hist: {hist_parents-(notroot_dirs_ids|deleted_ids)}'

		# для всех из hist есть образ в cur_stat или deleted
		hist_ids = {x[0] for x in self.CUR.execute('SELECT id FROM hist').fetchall()}
		assert hist_ids <= (notroot_dirs_ids|deleted_ids), f'hist enty with unknown id: {hist_ids-(notroot_dirs_ids|deleted_ids)}'

		# для всех из cur_stat, deleted, hist у кого owner is not None есть owner в owners
		stat_owners = {x[0] for x in self.CUR.execute('SELECT owner FROM cur_stat WHERE owner NOT NULL').fetchall()}
		deleted_owners = {x[0] for x in self.CUR.execute('SELECT owner FROM deleted WHERE owner NOT NULL').fetchall()}
		owners = {x[0] for x in self.CUR.execute('SELECT id FROM owners').fetchall()}
		assert (stat_owners|deleted_owners) <= owners, f'lost owners : {(stat_owners|deleted_owners) - owners}'

		# cur_dirs.type  cur_stat.type = simple_type(cur_stat.st_mode)
		for (t1, t2, mode) in self.CUR.execute('SELECT cur_dirs.type, cur_stat.type, cur_stat.st_mode FROM cur_dirs JOIN cur_stat ON cur_dirs.id=cur_stat.id').fetchall():
			assert t1==t2 , (t1,t2)
			if mode is not None:
				assert t2==simple_type(mode), (t2,simple_type(mode))
	# --------------
	# общие функции образа ФС
	# --------------

	def path2ids(self,path,cursor=None):
		'''
		преобразовывает путь в последовательность id-ов всех родительских папок
		Если в какой-то момент не удалось найти очередную папку - последовательность будет заканчиваться Nane-ом
		id объекта, задаваемого путём находится в последнй ячейке массива
		'''
		if cursor is None: cursor = self.CUR
		ids = []
		cur_id = 0
		for name in path.split('/'):
			if name=='': continue
			n = cursor.execute('SELECT id FROM cur_dirs WHERE parent_id = ? AND name = ?',(cur_id,name)).fetchone()
			if n is None:
				return ids+[None]
				#raise Exception(f"can't find {name} in {cur_id}")
			cur_id = n[0]
			ids.append(cur_id)
		return ids
	def id2path(self,fid,cursor=None):
		'''
		преобразовывает id в путь
		'''
		if cursor is None: cursor = self.CUR
		path = ''
		while fid!=0:
			n = cursor.execute('SELECT parent_id, name FROM cur_dirs WHERE id = ? ',(fid,)).fetchone()
			assert n is not None
			path = '/'+n[1]+path
			fid = n[0]
		return path

	# unused method
	def is_modified(self, fid, cursor=None):
		'''
		просто замена одному запросу в БД
		'''
		if cursor is None: cursor = self.CUR
		n = cursor.execute('SELECT modified FROM cur_dirs WHERE id = ?',(fid,)).fetchone()
		if n is None: raise Exception(f"can't find fid {fid}")
		return n[0]==1
	def set_modified(self, fid, cursor=None):
		'''
		выставляет modified в объект и в его родителя, если тот ещё не, и так рекурсивно
		'''
		if cursor is None: cursor = self.CUR
		if fid==0: return
		n = cursor.execute('SELECT parent_id, modified FROM cur_dirs WHERE id = ?',(fid,)).fetchone()
		if n is None: raise Exception(f"can't find fid {fid}")
		if n[1]==0:
			#print('set_modified', fid)
			cursor.execute('UPDATE cur_dirs SET modified = 1 WHERE id = ?',(fid,))
			self.set_modified(n[0], cursor=None)
		
	def update_stat(self, fid, stat, cursor=None):
		'''
		по fid-у заполняет stat-поля в cur_stat
		'''
		if cursor is None: cursor = self.CUR
		(typ,) = cursor.execute('SELECT type FROM cur_stat WHERE id = ?',(fid,)).fetchone()
		assert typ == simple_type(stat.st_mode), (fid,typ, simple_type(stat.st_mode), stat.st_mode)
		cursor.execute('''UPDATE cur_stat SET
			st_mode=?,st_ino=?,st_dev=?,st_nlink=?,st_uid=?,st_gid=?,st_size=?,
			st_atime=?,st_mtime=?,st_ctime=?,st_blocks=?,st_blksize=? WHERE id = ?''',
			(stat.st_mode,stat.st_ino,stat.st_dev,stat.st_nlink,stat.st_uid,stat.st_gid,stat.st_size,
			stat.st_atime,stat.st_mtime,stat.st_ctime,stat.st_blocks,stat.st_blksize, fid)
		)
	def get_stat(self, fid, cursor=None):
		'''
		по fid-у возвращает stat-поля из cur_stat в виде объекта
		'''
		if cursor is None: cursor = self.CUR
		(st_mode,st_ino,st_dev,st_nlink,st_uid,st_gid,st_size,
			st_atime,st_mtime,st_ctime,st_blocks,st_blksize) = \
		cursor.execute('''SELECT
			st_mode,st_ino,st_dev,st_nlink,st_uid,st_gid,st_size,
			st_atime,st_mtime,st_ctime,st_blocks,st_blksize
			FROM cur_stat WHERE id = ?''',(fid,)
		).fetchone()
		return make_dict(st_mode=st_mode,st_ino=st_ino,st_dev=st_dev,st_nlink=st_nlink,st_uid=st_uid,st_gid=st_gid,st_size=st_size,
						   st_atime=st_atime,st_mtime=st_mtime,st_ctime=st_ctime,st_blocks=st_blocks,st_blksize=st_blksize)

	# --------------------
	# инициализация БД
	# --------------------

	def _create_root(self, path,cursor=None):
		'''
		создает корневые директории в дереве cur_dirs (помечает родительские директории к path как pre-root-dir)
		'''
		if cursor is None: cursor = self.CUR
		ids = self.path2ids(path,cursor)
		assert ids[-1] is None
		fid = 0 if len(ids)==1 else ids[-2]

		# рассчитываем, что src_path - обсолютный путь, не симлинк, не содержит // типа '/a//b/c'
		path0 = path
		path = path.split('/')

		#print(ids,fid,path)
		for name in path[len(ids):-1]:
			cursor.execute('INSERT INTO cur_dirs (parent_id, name, modified, type) VALUES (?, ?, 2, ?)',(fid, name, MDIR))
			(fid,) = cursor.execute('SELECT id FROM cur_dirs WHERE parent_id =? AND name=?',(fid,name)).fetchone()
		try:
			stat = os_stat(path0)
		except Exception as e:
			self.notify(0,path,type(e),e)
			if name in dirs:
				self.CUR.executemany('INSERT INTO cur_dirs (parent_id, name, modified, type) VALUES (?, ?, 0, ?)', (fid, path[-1], MDIR))
				(fid,) = self.CUR.execute('SELECT id FROM cur_dirs WHERE parent_id = ? AND name = ?',(fid, path[-1])).fetchone()
				self.CUR.execute('INSERT INTO cur_stat (id,type) VALUES (?,?)', (fid,MDIR))
				self.notify('blindly create dir')
		else:
			self.CUR.execute('INSERT INTO cur_dirs (parent_id, name, modified, type) VALUES (?, ?, 0, ?)', (fid, path[-1], simple_type(stat.st_mode)))
			(fid,) = self.CUR.execute('SELECT id FROM cur_dirs WHERE parent_id = ? AND name = ?',(fid, path[-1])).fetchone()
			self.CUR.execute('INSERT INTO cur_stat (id,type) VALUES (?,?)', (fid,simple_type(stat.st_mode)))
			self.update_stat(fid,stat,self.CUR)
		return fid

	def _init_cur(self, root_dirs):
		'''
		обходит ФС из root_dirs и заполняет таблицу cur_dirs
		'''
		with self.CON:
			self.notify(0,'walk root_dirs:')
			for root_dir in tqdm(root_dirs):
				#self.notify(0,root_dir)
				self._create_root(root_dir,self.CUR)
				for root, dirs, files in os.walk(root_dir):
					pathids = self.path2ids(root,self.CUR)
					assert pathids[-1] is not None
					#self.notify(0,root,pathids,dirs)
					# при выполнении stat MFILE/MDIR может быть заменён на MLINK или MOTHER
					for name in dirs+files:
						try:
							stat = os_stat(root+'/'+name)
						except Exception as e:
							self.notify(0,root+'/'+name,type(e),e)
							if name in dirs:
								self.CUR.executemany('INSERT INTO cur_dirs (parent_id, name, modified, type) VALUES (?, ?, 0, ?)', (pathids[-1], name, MDIR))
								(fid,) = self.CUR.execute('SELECT id FROM cur_dirs WHERE parent_id = ? AND name = ?',(pathids[-1], name)).fetchone()
								self.CUR.execute('INSERT INTO cur_stat (id,type) VALUES (?,?)', (fid,MDIR))
								self.notify(0,'blindly create dir')
						else:
							self.CUR.execute('INSERT INTO cur_dirs (parent_id, name, modified, type) VALUES (?, ?, 0, ?)', (pathids[-1], name, simple_type(stat.st_mode)))
							(fid,) = self.CUR.execute('SELECT id FROM cur_dirs WHERE parent_id = ? AND name = ?',(pathids[-1], name)).fetchone()
							self.CUR.execute('INSERT INTO cur_stat (id,type) VALUES (?,?)', (fid,simple_type(stat.st_mode)))
							self.update_stat(fid,stat,self.CUR)

	def init_db(self, nohash):
		'''
		создаёт и инициализирует таблицы
		'''
		self._create_tables()
		self._init_cur(self.ROOT_DIRS)
		if not nohash:
			self.update_hashes(True,None)

	# ---------------------
	# общие функции событий
	# ---------------------

	def id2path_hist(self,fid,cursor=None):
		'''
		то же что id2path(), только ещё ищет в deleted
		возвращает (path, deleted: Bool)
		'''
		if cursor is None: cursor = self.CUR
		path = []
		deleted = False
		while fid!=0:
			n = cursor.execute('SELECT parent_id, name FROM cur_dirs WHERE id = ?',(fid,)).fetchone()
			if n is None:
				deleted = True
				n = cursor.execute('SELECT parent_id, name FROM deleted WHERE id = ?',(fid,)).fetchone()
				if n is None:
					raise Exception(f"can't find fid {fid}")
			(fid, name) = n
			path.insert(0,name)
		path.insert(0,'')
		return '/'.join(path), deleted
	def path2ids_hist(self,path,cursor=None):
		'''
		то же что path2ids(), только ещё ищет в deleted
		возвращает (ids, deleted: Bool)
		'''
		if cursor is None: cursor = self.CUR
		ids = []
		cur_id = 0
		deleted = False
		for name in path.split('/'):
			if name=='': continue
			n = cursor.execute('SELECT id FROM cur_dirs WHERE parent_id = ? AND name = ?',(cur_id,name)).fetchone()
			if n is None:
				deleted = True
				n = cursor.execute('SELECT id FROM deleted WHERE parent_id = ? AND name = ?',(cur_id,name)).fetchone()
				if n is None:
					return ids+[None]
					#raise Exception(f"can't find {name} in {cur_id}")
			cur_id = n[0]
			ids.append(cur_id)
		return ids, deleted

	def any2id(self,fid):
		if fid is None:
			fid = os.getcwd()
		if type(fid) is str:
			fid = self.path2ids(normalize_path(os.path.abspath(fid)))[-1]
			if fid is None: raise Exception('path does not exist')
		return fid
	def any2id_hist(self,fid):
		if fid is None:
			fid = os.getcwd()
		if type(fid) is str: 
			fid = self.path2ids_hist(normalize_path(os.path.abspath(fid)))[0][-1]
			if fid is None: raise Exception('path does not exist')
		return fid

	def owner_save(self,fid,cursor=None):
		'''
		определяет владельца и надо ли сохранять события, связанные с этим файлом
		'''
		if cursor is None: cursor = self.CUR
		self.notify(3.1,'owner_save',fid)
		(owner,) = cursor.execute('SELECT owner FROM cur_stat WHERE id = ?',(fid,)).fetchone()
		if owner is not None:
			(save,) = cursor.execute('SELECT save FROM owners WHERE id = ?',(owner,)).fetchone()
		else:
			save = True
		return (owner,save)

	def add_event(self, fid, typ, etyp, static_found, owner, cursor=None):
		'''
		создает запись в hist
		если событие ECREAT: заполняет большинство полей -1
		иначе: копирует данные из fur_dirs, cur_stat
			опционально если указан typ: проверяет, чтобы он равнялся старому типу
		'''
		ltime = time()

		if cursor is None: cursor = self.CUR
		if etyp==ECREAT:
			cursor.execute('''INSERT INTO hist (
					parent_id, name,
					id, type, event_type,
					st_mode,st_ino,st_dev,st_nlink,st_uid,st_gid,st_size,st_atime,st_mtime,st_ctime,st_blocks,st_blksize,
					data,
					time,static_found
				) VALUES (-1,'',?,?,?,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,NULL,?,?)''',
						   (fid,typ,etyp,ltime,static_found))
		else:
			(otyp,) = cursor.execute('SELECT type FROM cur_dirs WHERE id = ?',(fid,)).fetchone()
			if typ is not None:
				assert typ == otyp , (typ, otyp)
			else:
				typ = otyp
			# просто часть данных копируем а часть заполняем вручную
			cursor.execute('''INSERT INTO hist (parent_id, name, id, type, event_type,
				st_mode,st_ino,st_dev,st_nlink,st_uid,st_gid,st_size,
				st_atime,st_mtime,st_ctime,st_blocks,st_blksize,data,
				time,static_found)
				SELECT t1.parent_id, t1.name, ?, ?, ?,
				t2.st_mode,t2.st_ino,t2.st_dev,t2.st_nlink,t2.st_uid,t2.st_gid,t2.st_size,
				t2.st_atime,t2.st_mtime,t2.st_ctime,t2.st_blocks,t2.st_blksize,t2.data,
				?,?
				FROM cur_dirs AS t1
				JOIN cur_stat AS t2
				ON 1=1
				WHERE t1.id = ? AND t2.id = ?
				''',
						   (fid,typ,etyp,ltime,static_found,fid,fid)
			)
		if self.VERBOSE>=1 or owner is None and self.VERBOSE>0:
			self.notify(0,datetime.fromtimestamp(ltime), etyp2str(etyp), static_found, fid, typ2str(typ), self.id2path_hist(fid,cursor)[0])

	def modify(self, fid, stat, static_found, cursor=None):
		'''
		известно, что объект fid изменился, известен его новый stat
		'''
		if cursor is None: cursor = self.CUR

		self.set_modified(fid, cursor)
		self.update_stat(fid,stat,cursor)

		(owner,save) = self.owner_save(fid,cursor)
		if save or self.VERBOSE>=1.2:
			# cохранить старый stat
			# условие для папки - если изменился её stat (st_atime, st_mtime не учитываем)
			# условие для файла - если с предыдущего обновления прошло больше 10 сек
			if simple_type(stat.st_mode)==MDIR:
				save1 = not stat_eq(stat,self.get_stat(fid,cursor))
			else:
				save1 = True
				n = cursor.execute('SELECT time FROM hist WHERE id = ? ORDER BY time DESC LIMIT 1',(fid,)).fetchone()
				if n is not None: # раньше этот файл уже обновлялся
					save = abs(n[0] - time())>10
			if save and save1:
				self.add_event(fid, simple_type(stat.st_mode), EMODIF, static_found, owner, cursor)
			elif save1:
				self.notify(1.2,'modify',fid, self.id2path(fid, cursor), static_found)
		
	def create(self, parent_id, name, stat, static_found, cursor=None, owner=None, save=None):
		'''
		создается объект, родительская директория которого уже существует
		save, owner определяются родительской папкой или 
		возвращает fid созданного объекта
		'''
		if cursor is None: cursor = self.CUR
		if owner is None or save is None:
			(owner,save) = self.owner_save(parent_id,cursor)
		self.set_modified(parent_id, cursor)
		n = cursor.execute('SELECT id, owner FROM deleted WHERE parent_id =? AND name=?',(parent_id,name)).fetchone()
		if n is None: # раньше НЕ удалялся
			cursor.execute('INSERT INTO cur_dirs (parent_id, name, modified, type) VALUES (?, ?, 1, ?)',
						   (parent_id, name, simple_type(stat.st_mode)))
			(fid,) = cursor.execute('SELECT id FROM cur_dirs WHERE parent_id =? AND name=?',(parent_id,name)).fetchone()
		else:
			fid,owner1 = n
			if owner1 is not None:
				n = cursor.execute('SELECT save FROM owners WHERE id = ?',(owner1,))
				if n is not None:
					owner = owner1
					(sav,) = n
					if save: save = sav
			cursor.execute('DELETE FROM deleted WHERE parent_id =? AND name=?',(parent_id,name))
			cursor.execute('INSERT INTO cur_dirs (parent_id, name, id, modified, type) VALUES (?, ?, ?, 1, ?)',
						   (parent_id, name, fid, simple_type(stat.st_mode)))
			
		# обновить stat в cur
		cursor.execute('INSERT INTO cur_stat (id,type,owner) VALUES (?,?,?)',(fid,simple_type(stat.st_mode),owner))
		self.update_stat(fid,stat,cursor)

		if save:
			self.add_event(fid, simple_type(stat.st_mode), ECREAT, static_found, owner, cursor)
		else:
			self.notify(1.2, 'create',parent_id, self.id2path(parent_id, cursor), name, static_found, owner, save)

		return fid
		
	def delete(self, fid, static_found, cursor=None):
		'''
		удаляем существующий объект fid
		а также его потомков, если они существуют
		'''
		if cursor is None: cursor = self.CUR
		(owner,save) = self.owner_save(fid,cursor)

		def my_walk(did):
			n = cursor.execute('SELECT name,id,type FROM cur_dirs WHERE parent_id = ? ',(did,)).fetchall()
			for name,fid,ftype in n:
				if ftype==MDIR:
					my_walk(fid)
				(owner,) = cursor.execute('SELECT owner FROM cur_stat WHERE id = ?', (fid,)).fetchone()
				if save:
					self.add_event(fid, None, EDEL, static_found, owner, cursor)

				cursor.execute('''INSERT INTO deleted VALUES (?,?,?,?) ON CONFLICT DO UPDATE SET
				parent_id=excluded.parent_id, name=excluded.name, id=excluded.id, owner=excluded.owner''',(did,name,fid,owner))

				cursor.execute('DELETE FROM cur_stat WHERE id = ?',(fid,))
				cursor.execute('DELETE FROM cur_dirs WHERE id = ?',(fid,))
				
		my_walk(fid)
		if save:
			self.add_event(fid, None, EDEL, static_found, owner, cursor)
		else:
			self.notify(1.2, 'delete',fid, self.id2path(fid, cursor),static_found)

		(owner,) = cursor.execute('SELECT owner FROM cur_stat WHERE id = ?', (fid,)).fetchone()
		(did,name) = cursor.execute('SELECT parent_id, name FROM cur_dirs WHERE id = ?', (fid,)).fetchone()
		cursor.execute('''INSERT INTO deleted VALUES (?,?,?,?) ON CONFLICT DO UPDATE SET
				parent_id=excluded.parent_id, name=excluded.name, id=excluded.id, owner=excluded.owner''',(did,name,fid,owner))

		cursor.execute('DELETE FROM cur_stat WHERE id = ?',(fid,))
		cursor.execute('DELETE FROM cur_dirs WHERE id = ?',(fid,))

	# --------------------------------
	# функции статического обновления
	# --------------------------------

	def update_hashes(self, with_all=False, modify='self.modify'):
		# todo calc only unknown hashes
		if modify == 'self.modify': modify = self.modify

		with self.CON:
			with closing(self.CON.cursor()) as cursor:
				if with_all:
					ids = cursor.execute('SELECT id FROM cur_dirs WHERE type = ?',(MFILE,)).fetchall()
				else:
					ids = cursor.execute('SELECT id FROM cur_dirs WHERE type = ? AND modified = 1',(MFILE,)).fetchall()
				cnt = 0
				print('calc hashes:')
				for fid in (tqdm(ids) if __name__!="__main__" else ids):
					fid = fid[0]
					path = None
					try:
						path = self.id2path(fid,cursor)
						hsh = hashlib.md5(open(path,'rb').read()).hexdigest()
					except FileNotFoundError:
						self.set_modified(fid, cursor)
					except Exception as e:
						self.raise_notify(e,fid,path)
					else:
						if modify is not None:
							(ohash,) = cursor.execute('SELECT data FROM cur_stat WHERE id = ?',(fid,)).fetchone()
							if ohash is not None and ohash!=hsh:
								modify(fid, os_stat(path), 2, cursor)
						cursor.execute('UPDATE cur_stat SET data = ? WHERE id = ?',(hsh,fid))
						cursor.execute('UPDATE cur_dirs SET modified = 0 WHERE id = ?',(fid,))
						cnt+=1
						if cnt%1000000==0:
							cursor.execute('COMMIT')

		# обновить симлинки, директории, сынтегрировать хеши
		with self.CON:
			with closing(self.CON.cursor()) as cursor:
				def my_walk(did,root):
					n = cursor.execute('SELECT name,id,type,modified FROM cur_dirs WHERE parent_id = ? ',(did,)).fetchall()
					hsh = 0
					for name,fid,ftype,modified in n:
						if ftype==MFILE:
							try:
								(lhsh,) = cursor.execute('SELECT data FROM cur_stat WHERE id = ?',(fid,)).fetchone()
							except Exception as e:
								lhsh = None
								self.raise_notify(e,fid)
						elif ftype==MLINK:
							try:
								lnk = os.readlink(self.id2path(fid,cursor))
								if modify is not None:
									(olink,) = cursor.execute('SELECT data FROM cur_stat WHERE id = ?',(fid,)).fetchone()
									if olink is not None and olink!=lnk:
										modify(fid, os_stat(self.id2path(fid)), 2, cursor)
								lhsh = hashlib.md5(lnk.encode()).hexdigest()
								cursor.execute('UPDATE cur_stat SET data = ? WHERE id = ?',(lnk,fid))
								cursor.execute('UPDATE cur_dirs SET modified = 0 WHERE id = ?',(fid,))
							except FileNotFoundError:
								self.set_modified(fid, cursor)
								lhsh = None
						elif ftype==MDIR:
							if with_all or modified!=0:
								lhsh = my_walk(fid,modified==2)
							else:
								(lhsh,) = cursor.execute('SELECT data FROM cur_stat WHERE id = ?',(fid,)).fetchone()
						elif ftype==MOTHER:
							lhsh = hex( 0 )[2:].zfill(32)
							cursor.execute('UPDATE cur_dirs SET modified = 0 WHERE id = ?',(fid,))
						else:
							assert False, (name,fid,ftype)

						if lhsh is None:
							hsh = None
						if hsh is not None:
							hsh += int(lhsh, 16)

					if hsh is not None:
						hsh = hex( hsh%(2**32) )[2:].zfill(32)
						if not root:
							cursor.execute('UPDATE cur_stat SET data = ? WHERE id = ?',(hsh,did))
							cursor.execute('UPDATE cur_dirs SET modified = 0 WHERE id = ?',(did,))
					return hsh
				my_walk(0,True)

	def walk_stat1(self, with_all, did, *, progress=None, path='', typ=MDIR, modified=0):
		# path, typ, modified - внутренние рекурсивные параметры, не предназначенные для внешнего вызова
		# если это не pre-root-dir
		#	если это папака
		#		просматриваем дочерние объeкты, какие есть и какие должны быть
		#		удаляем удалённые
		#		просматриваем которые остались(с учётом only_modified)
		#		создаём новые и просматриваем их(modified=3)
		#	если modified!=3
		#		делаем stat, сравниваем с имеющимся
		#		если разные stat, есть созданные/удалённые, есть различия в дочерних - modified(); return True
		# если pre-root-dir
		#	просматриваем дочерние объeкты, какие есть(с учётом only_modified)
		this_modified = False
		if did!=0 and modified!=2:
			if typ==MDIR:
				children = self.CUR.execute('SELECT name,id,type,modified FROM cur_dirs WHERE parent_id = ?',(did,)).fetchall()
				real_children = os.listdir(path)
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
						this_modified |= self.walk_stat1(with_all, fid, progress=progress, path=path+'/'+name, typ=ctyp, modified=cmodified)
				# создаём новые и просматриваем их(modified=3)
				for name in real_children:
					this_modified = True
					cpath = path+'/'+name
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
					this_modified |= not stat_eq(stat,self.get_stat(did,self.CUR))
					if this_modified:
						self.modify(did, stat, True, self.CUR)
				except FileNotFoundError:
					print(path,"item may be alreay deleted")
		else:
			for (name,fid,ctyp,cmodified) in self.CUR.execute('SELECT name,id,type,modified FROM cur_dirs WHERE parent_id = ?',(did,)).fetchall():
				this_modified |= self.walk_stat1(with_all, fid, progress=progress, path=path+'/'+name, typ=ctyp, modified=cmodified)
		return this_modified

	def walk_stat(self, with_all, did):
		'''
		основная цель: найти изменения, которые не были пойманы watchdog-ом
		with_all === not only_modified
		обходим модифицированные объекты в БД, сравнимаем их с ФС
		или обходим все объекты ФС (в зависимости от only_modified)
		изменения заносим в журнал
		и помечаем их как модифицированнные
		'''
		if with_all:
			total = self.CUR.execute('SELECT COUNT(*) AS count FROM cur_dirs WHERE modified!=2').fetchone()
		else:
			total = self.CUR.execute('SELECT COUNT(*) AS count FROM cur_dirs WHERE modified==1').fetchone()
		total = 0 if total is None else total[0]
		with self.CON:
			with (tqdm(total=total, desc="Progress") if __name__!="__main__" else NullContextManager(None)) as pbar:
				count = 0
				def progress():
					nonlocal count
					count+=1
					if __name__!="__main__" and count % (total // 100)==0:
						pbar.update(total // 100)
				self.walk_stat1(with_all, did, progress=progress)

	def walk_stat_all(self):
		self.walk_stat(True, 0)
	def walk_stat_modified(self):
		self.walk_stat(False, 0)

	# --------------------------------
	# функции динамического обновления
	# --------------------------------

	def create_parents(self, path, cursor=None, ids=None):
		if cursor is None: cursor = self.CUR
		self.notify(2, 'create_parents',path,cursor,ids)
		if ids is None:
			ids = self.path2ids(path,cursor)
			
		# рассчитываем, что src_path - обсолютный путь, не симлинк, не содержит // типа '/a//b/c'
		path = path.split('/')

		fid = ids[-2]
		(owner,save) = self.owner_save(fid,cursor)

		parent_path = '/'.join(path[:len(ids)])
		for name in path[len(ids):-1]:
			parent_path+= ('/'+name)
			lstat = os_stat(parent_path) # FileNotFoundError будет пойман в области watchdog-а
			assert simple_type(lstat.st_mode)==MDIR, simple_type(lstat.st_mode)
			fid = self.create(fid, name, lstat, True, cursor, owner, save)

		return fid, path[-1], owner, save

	def create1(self, ids, src_path, stat, is_directory, cursor=None):
		if cursor is None: cursor = self.CUR
		self.notify(2, 'created1',ids, src_path, stat, is_directory, cursor)
		(fid, name, owner, save) = self.create_parents(src_path,cursor,ids)
		self.create(fid, name, stat, False, cursor, owner, save)

	def move(self, fid, dest_path, cursor=None):
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
		if (n:=cursor.execute('SELECT id FROM cur_dirs WHERE parent_id=? AND name=?',(parent_id, name)).fetchone()) is not None and n[0]!=fid:
			self.delete(n[0],False)
		cursor.execute('UPDATE cur_dirs SET parent_id = ?, name = ? WHERE id = ?',(parent_id, name, fid))

	def modified(self, src_path, stat, is_directory, is_synthetic, cursor=None):
		if cursor is None: cursor = self.CUR
		self.notify(2, 'modified',src_path, stat, is_directory, is_synthetic, cursor)
		src_path = normalize_path(src_path)
		if is_synthetic:
			self.notify(0,'synthetic modified',src_path, is_directory)
			return
		ids = self.path2ids(src_path,cursor)
		if ids[-1] is None:
			self.notify(1.4, 'do modified as created',src_path, datetime.fromtimestamp(time()))
			return self.create1(ids, src_path, stat, is_directory,cursor)
		return self.modify(ids[-1], stat, False, cursor)

	def created(self, src_path, stat, is_directory, is_synthetic, cursor=None):
		if cursor is None: cursor = self.CUR
		self.notify(2, 'created',src_path, stat, is_directory, is_synthetic, cursor)
		src_path = normalize_path(src_path)
		if is_synthetic:
			self.notify(0,'synthetic created',src_path, is_directory)
			return
		ids = self.path2ids(src_path,cursor)
		if ids[-1] is not None:
			# если было удалено, но это не было зафиксировано, а потом создалось - считаем, что просто изменилось
			self.notify(1.4, 'do created as modified',src_path, datetime.fromtimestamp(time()))
			return self.modify(ids[-1], stat, False, cursor)
		return self.create1(ids, src_path, stat, is_directory,cursor)

	def deleted(self, src_path, is_directory, is_synthetic, cursor=None):
		if cursor is None: cursor = self.CUR
		self.notify(2, 'deleted',src_path, is_directory, is_synthetic, cursor)
		src_path = normalize_path(src_path)
		if is_synthetic:
			self.notify(0,'synthetic deleted',src_path, is_directory)
			return
		ids = self.path2ids(src_path,cursor)
		if ids[-1] is None:
			self.notify(1.4, 'error in deleted: unknown object:',src_path)
			return
		self.delete(ids[-1], False, cursor)

	def moved(self, src_path, dest_path, stat, is_directory, is_synthetic, cursor=None):
		if cursor is None: cursor = self.CUR
		self.notify(2, 'moved',src_path, dest_path, stat, is_directory, is_synthetic, cursor)
		src_path = normalize_path(src_path)
		dest_path = normalize_path(dest_path)
		if is_synthetic:
			self.notify(0,'synthetic moved',src_path, dest_path, is_directory)
			return
		ids = self.path2ids(src_path,cursor)
		if ids[-1] is None:
			self.notify(1.4, 'do moved as created',src_path, dest_path, time())
			return self.created(dest_path, stat, is_directory, is_synthetic, cursor)
		self.move(ids[-1],dest_path, cursor)

	# --------------------------------
	# интерфейсные функции
	# --------------------------------
	def reset_modified(self):
		'''
		сбрасывает все modified флаги
		'''
		with self.CON:
			self.CUR.execute('UPDATE cur_dirs SET modified =0 WHERE modified==1')

	def create_owner(self, name, save):
		'''
		создает владельца, возвращает его id
		'''
		with self.CON:
			self.CUR.execute('INSERT INTO owners (name, save) VALUES (?, ?)', (name,save))
			return self.CUR.execute('SELECT id FROM owners WHERE name = ?',(name,)).fetchone()[0]

	def update_owner(self, name, save):
		'''
		у существующего владельца обновляет параметр save, возвращает его id
		'''
		with self.CON:
			self.CUR.execute('UPDATE owners SET save = ? WHERE name = ?', (save,name))
			return self.CUR.execute('SELECT id FROM owners WHERE name = ?',(name,)).fetchone()[0]

	def credate_owner(self, name, save):
		'''
		создаёт владельца, а если он уже существует, то только обновляет его параметр save. Возвращает его id
		'''
		with self.CON:
			self.CUR.execute('''INSERT INTO owners (name, save) VALUES (?, ?) ON CONFLICT(name) DO UPDATE SET
				name = excluded.name,    save = excluded.save ''', (name,save))
			return self.CUR.execute('SELECT id FROM owners WHERE name = ?',(name,)).fetchone()[0]

	def del_owner(self, owner):
		'''
		удаляет владельца и все упоминания о нём из таблиц cur_stat, deleted
		'''
		with self.CON:
			self.CUR.execute('UPDATE cur_stat SET owner = NULL WHERE cur_stat.owner = (SELECT owners.id FROM owners WHERE owners.name = ?)',(owner,))
			self.CUR.execute('UPDATE deleted  SET owner  = NULL WHERE deleted.owner  = (SELECT owners.id FROM owners WHERE owners.name = ?)',(owner,))
			self.CUR.execute('DELETE FROM owners WHERE name = ?',(owner,))
		
	def del_owner_hist(self, owner):
		'''
		удаляет владельца и все упоминания о нём из таблиц cur_stat, deleted
		а также удалает из hist все записи, в которых упоминается этот владелец
		'''
		with self.CON:
			self.CUR.execute('DELETE FROM hist WHERE hist.id IN (SELECT cur_stat.id FROM cur_stat JOIN owners ON cur_stat.owner=owners.id WHERE owners.name = ?)',(owner,))
			self.CUR.execute('DELETE FROM hist WHERE hist.id IN (SELECT deleted.id FROM deleted JOIN owners ON deleted.owner=owners.id WHERE owners.name = ?)',(owner,))
			self.CUR.execute('UPDATE cur_stat SET owner = NULL WHERE cur_stat.owner = (SELECT owners.id FROM owners WHERE owners.name = ?)',(owner,))
			self.CUR.execute('UPDATE deleted  SET owner = NULL WHERE deleted.owner  = (SELECT owners.id FROM owners WHERE owners.name = ?)',(owner,))
			self.CUR.execute('DELETE FROM owners WHERE name = ?',(owner,))

	def del_hist_owner(self, owner, interval=None):
		'''
		удалает из hist все записи, в которых упоминается этот владелец
		'''
		#todo interval
		with self.CON:
			self.CUR.execute('DELETE FROM hist WHERE hist.id IN (SELECT cur_stat.id FROM cur_stat JOIN owners ON cur_stat.owner=owners.id WHERE owners.name = ?)',(owner,))
			self.CUR.execute('DELETE FROM hist WHERE hist.id IN (SELECT deleted.id FROM deleted JOIN owners ON deleted.owner=owners.id WHERE owners.name = ?)',(owner,))

	def del_hist_id(self, fid, interval=None):
		'''
		удаляет из истории все записи про заданный объект
		'''
		#todo interval
		fid = any2id_hist(fid)
		with self.CON:
			self.CUR.execute('DELETE FROM hist WHERE id = ?)',(fid,))

	def del_hist_id_recursive(self, fid, interval=None):
		'''
		удаляет из истории все записи про заданный объект и его дочерние объекты
		'''
		#todo interval
		fid = any2id_hist(fid)
		with self.CON:
			self.CUR.execute('DELETE FROM hist WHERE id = ?)',(fid,))
			fids = self.CUR.execute('SELECT id FROM cur_dirs WHERE parent_id = ? JOIN SELECT id FROM deleted  WHERE parent_id = ?',(fid,fid)).fetchall()
			while len(fids)>0:
				self.CUR.executemany('DELETE FROM hist WHERE id = ?)',fids)
				fids2 = []
				for (fid,) in fids:
					fids2+= self.CUR.execute('SELECT id FROM cur_dirs WHERE parent_id = ?',(fid,)).fetchall()
					fids2+= self.CUR.execute('SELECT id FROM deleted  WHERE parent_id = ?',(fid,)).fetchall()
				fids = fids2

	def rename_owner(self, oname, name):
		with self.CON:
			self.CUR.execute('UPDATE owners SET name = ? WHERE name = ?',(name,oname))

	def set_owner(self, path, owner, *, replace_inner=False, in_deleted=True):
		'''
		если такого owner-а еще нет - он создаётся
		save - надо ли в будущем сохранять события изменений этих файлов
			если None - не обновлять owner-а
		replace_inner:
			если True - устанавливает owner-а для всех вложенных объектов
			если False - только для тех вложенных, у которых еще нет owner-а или он такой как у объекта path
		in_deleted - устанавливать ли owner-а для удалённых объектов
		'''
		with self.CON:
			with closing(self.CON.cursor()) as cursor:
				# oid - owner-id, который будем устанавливать
				if owner is not None:
					(oid,) = cursor.execute('SELECT id FROM owners WHERE name = ?',(owner,)).fetchone()
				else:
					oid = None

				fid = self.any2id_hist(path)
				(oldoid,) = cursor.execute('SELECT owner FROM cur_stat WHERE id = ?',(fid,)).fetchone()
				cursor.execute('UPDATE cur_stat SET owner = ? WHERE id = ?',(oid,fid))

				def my_walk(did):
					#self.notify(0,'my_walk',did)
					if replace_inner:
						n = cursor.execute('SELECT name,id,type FROM cur_dirs WHERE parent_id = ? ',(did,)).fetchall()
					elif oldoid is None:
						n = cursor.execute('''SELECT cur_dirs.name, cur_dirs.id, cur_dirs.type FROM cur_dirs JOIN cur_stat ON cur_dirs.id=cur_stat.id
							WHERE cur_dirs.parent_id = ? AND cur_stat.owner ISNULL''',(did,)).fetchall()
					else:
						n = cursor.execute('''SELECT cur_dirs.name, cur_dirs.id, cur_dirs.type FROM cur_dirs JOIN cur_stat ON cur_dirs.id=cur_stat.id
							WHERE cur_dirs.parent_id = ? AND cur_stat.owner = ?''',(did,oldoid)).fetchall()
					for name,fid,ftype in n:
						cursor.execute('UPDATE cur_stat SET owner = ? WHERE id = ?',(oid,fid))
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

	def set_create_owner(self, path, owner, save, *, del_hist=False, replace_inner=False, in_deleted=True):
		self.create_owner(owner, save)
		self.set_owner(path, owner, replace_inner=replace_inner, in_deleted=in_deleted)
		if del_hist: self.del_hist_owner(owner)

	def set_credate_owner(self, path, owner, save, *, del_hist=False, replace_inner=False, in_deleted=True):
		self.credate_owner(owner, save)
		self.set_owner(path, owner, replace_inner=replace_inner, in_deleted=in_deleted)
		if del_hist: self.del_hist_owner(owner)

	@staticmethod
	def help():
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
		update_hashes(with_all=False, modify=None)

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

	def watch(self, do_stat = True):
		'''запускает watchdog, который ловит события файловой системы
		также может выполнять команды из stdin'''
		# взаимодействуем с ФС
		from watchdog.events import FileSystemEvent, FileSystemEventHandler
		from watchdog.observers import Observer
		import threading
		from queue import Queue
		from time import time, sleep
		import sys

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
			if event.event_type=='closed_no_write':
				self.notify(1.5, 'pass closed_no_write',event.src_path)
				pass
			elif event.event_type=='opened':
				self.notify(1.5, 'pass opened',event.src_path)
				pass
			elif event.event_type=='modified' or event.event_type=='closed':
				self.notify(1.5, 'modified',event.src_path)
				try:
					stat = os_stat(event.src_path)
				except FileNotFoundError as e:
					self.notify(1.4, 'error in modified event:', type(e), e, event.src_path, event.is_directory, event.is_synthetic)
				else:
					self.modified(event.src_path, stat, event.is_directory, event.is_synthetic, self.CUR)
				
			elif event.event_type=='created':
				self.notify(1.5, 'created',event.src_path)
				try:
					stat = os_stat(event.src_path)
				except FileNotFoundError as e:
					self.notify(1.4, 'error in created event:', type(e), e, event.src_path, event.is_directory, event.is_synthetic)
				else:
					self.created(event.src_path, stat, event.is_directory, event.is_synthetic, self.CUR)
			elif event.event_type=='deleted':
				self.notify(1.5, 'deleted',event.src_path)
				self.deleted(event.src_path, event.is_directory, event.is_synthetic, self.CUR)
			elif event.event_type=='moved':
				self.notify(1.5, 'moved',event.src_path,event.dest_path)
				try:
					stat = os_stat(event.dest_path)
				except FileNotFoundError as e:
					self.notify(1.4, 'do moved as deleted:', type(e), e, event.src_path, event.dest_path, event.is_directory, event.is_synthetic)
					self.deleted(event.src_path, event.is_directory, event.is_synthetic, self.CUR)
				else:
					self.moved(event.src_path, event.dest_path, stat, event.is_directory, event.is_synthetic, self.CUR)
			else:
				self.raise_notify(None,event)

		q = Queue()

		class MyEventHandler(FileSystemEventHandler):
			def on_any_event(self, event: FileSystemEvent) -> None:
				if event.event_type=='closed_no_write':
					pass
				elif event.event_type=='opened':
					pass
				else:
					#print('put',event.event_type,event.src_path)
					q.put(event)
		def observe(root_dirs):
			event_handler = MyEventHandler()  # Создаем обработчик с временным значением shared_data
			observer = Observer()

			for dr in root_dirs:
				observer.schedule(event_handler, dr, recursive=True)
			observer.start()
			return observer
		observer = observe(self.ROOT_DIRS)
		self.notify(0,"All started...", threading.current_thread().name, datetime.fromtimestamp(time()))


		def keyboard_monitor():
			x= ''
			while x!='q':
				try:
					x = input()
				except EOFError:
					x = 'q'
				q.put(x)
		if self.keyboard_thr is None or not self.keyboard_thr.is_alive():
			self.keyboard_thr = threading.Thread(target = keyboard_monitor, args=tuple(), name='keyboard_thr', daemon=True)
			self.keyboard_thr.start()
		else:
			self.notify(1.5,'keep old keyboard_thr')

		stopped = False
		def commit_monitor():
			while not stopped:
				sleep(60)
				q.put('u')
		if self.commit_thr is None or not self.commit_thr.is_alive():
			self.commit_thr = threading.Thread(target = commit_monitor, args=tuple(), name='commit_thr', daemon=True)
			self.commit_thr.start()
		else:
			self.notify(1.5,'keep old commit_thr')

		try:
			while True:
				event = q.get()
				if isinstance(event,FileSystemEvent):
					my_event_handler(event)
				elif type(event) is str:
					if event=='q':
						if self.CON.in_transaction:
							self.CUR.execute('COMMIT')
							stopped = True
						break
					elif event=='u':
						if self.CON.in_transaction:
							self.CUR.execute('COMMIT')
							self.notify(1.2, 'COMMIT',datetime.fromtimestamp(time()))
							self.check_integrity()
					else:
						import yaml
						try:
							event = yaml.safe_load('['+event+']')
						except ParserError as e:
							print(e)
						else:
							if self.CON.in_transaction:
								self.CUR.execute('COMMIT')
							try:
								if len(event)==2: event.append({})
								fun, args, kwargs = event
								fun = getattr(self,fun)
								fun(*args,**kwargs)
								print('-----------------------')
							except Exception as e:
								self.raise_notify(e,'type: "help, []" for more information about syntax')
							if self.CON.in_transaction:
								self.CUR.execute('COMMIT')
				else:
					self.notify(0,'unknown type:',type(event))
				q.task_done()
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

	# --------------------------------
	# мониторинговые функции
	# --------------------------------

	def info_fid(self, fid, *, interval=None):
		'todo interval'
		if fid==0:
			return make_dict(
				parent_id=0,
				name	='',
				fid		=0,
				typ		=MDIR,
				modified=2,
				deleted	=0,
				path	='/',
				ids		=[0],
				data	=None,
				stat	=None,
				count_static=0,
				count	=0,
				oname	=None,
				save	=True,
				oid		=None,
			)
		if interval is None:
			tstart,tend = None, None
		else:
			tstart,tend = interval
		if tstart is None: tstart = 0
		if tend is None: tend = time()+100

		n = self.CUR.execute('SELECT parent_id,name,id,type,modified FROM cur_dirs WHERE id = ?',(fid,)).fetchone()
		if n is not None:
			(parent_id,name,fid,typ,modified) = n
			deleted = False
			path = self.id2path(fid)
			ids = self.path2ids(path)

			if modified!=2:
				(data, oid) = self.CUR.execute('SELECT data, owner FROM cur_stat WHERE id = ?',(fid,)).fetchone()
				stat = self.get_stat(fid)

				nn = self.CON.execute(
					'SELECT COUNT(*), SUM(CASE WHEN static_found>0 THEN 1 ELSE 0 END) FROM hist WHERE id==? AND ?<=time AND time<=? GROUP BY id '
					,(fid,tstart,tend)).fetchone()
				count, count_static = nn if nn is not None else (0, 0)

				if oid is not None:
					(oname,save) = self.CUR.execute('SELECT name, save FROM owners WHERE id = ?',(oid,)).fetchone()
				else:
					(oname,save) = (None, True)
			else:
				(data, oid, stat, count_static, count, oname, save) = (None, None, None, None, None, None, None)
		else:
			n = self.CUR.execute('SELECT parent_id,name,id,owner FROM deleted WHERE id = ?',(fid,)).fetchone()
			if n is None: self.raise_notify(None, f"can't find {fid} in cur_dirs and in deleted")
			(parent_id,name,fid,oid) = n
			deleted = True
			modified = 0
			path = self.id2path_hist(fid)[0]
			ids = self.path2ids_hist(path)[0]

			n = self.CUR.execute('''SELECT data, 
				st_mode,st_ino,st_dev,st_nlink,st_uid,st_gid,st_size,
				st_atime,st_mtime,st_ctime,st_blocks,st_blksize,
				type
				FROM hist WHERE id = ? ORDER BY time DESC LIMIT 1''',(fid,)).fetchone()
			if n is not None:
				data = n[0] if n[0]!='' and n[0]!=-1 else None
				stat = make_dict(st_mode=n[1],st_ino=n[2],st_dev=n[3],st_nlink=n[4],st_uid=n[5],st_gid=n[6],st_size=n[7],
				st_atime=n[8],st_mtime=n[9],st_ctime=n[10],st_blocks=n[11],st_blksize=n[12]) if n[1]!=-1 else None
				typ = n[13] if n[13]!=-1 else None
			else:
				data = None
				stat = None
				typ = None

			nn = self.CON.execute('SELECT COUNT(*), SUM(CASE WHEN static_found>0 THEN 1 ELSE 0 END) FROM hist WHERE id==? AND ?<=time AND time<=? GROUP BY id ',(fid,tstart,tend)).fetchone()
			count, count_static = nn if nn is not None else (0, 0)

			if oid is not None:
				(oname,save) = self.CUR.execute('SELECT name, save FROM owners WHERE id = ?',(oid,)).fetchone()
			else:
				(oname,save) = (None, True)
		return make_dict(
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
			oname	=oname,
			save	=save,
			oid		=oid,
		)

	@staticmethod
	def format_info(info, *, info_lev=1, path_indent=None, nest_reducer=0, abs_path=None, show_owner=True):
		'''
		info_lev=0	права и дату модификации не показывает, показывает полный путь
		info_lev=1	путь короткий, показывает права и дату модификации
		info_lev=2	путь короткий, показывает права и дату модификации и uid, gid, size, data
		path_indent	несли не None показывает только имя, но перед ним делает отступы в количестве глубины вложенности
		если abs_path не None - path_indent игнорируется
		'''
		out_data = []

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
		s+=str(info.fid)
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
				out_data.append(datetime.fromtimestamp(info.stat.st_mtime))

		# uid, gid, size
		if info_lev==2:
			if info.stat is not None:
				out_data.append(get_username_by_uid(info.stat.st_uid))
				out_data.append(get_groupname_by_gid(info.stat.st_gid))
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
				out_data.append(('->' if info.typ==MLINK else '')+str(info.data))
			else:
				out_data.append('???')

		# owner
		if show_owner:
			out_data.append(f'<={info.oname}({"+" if info.save else "-"})' if info.oname is not None else '')

		return out_data

	def ls(self, fid=None,*,info_lev=1):
		fid = self.any2id(fid)
		print(*self.format_info(self.info_fid(fid), info_lev=0),sep='\t')
		for (fid,) in self.CUR.execute('SELECT id FROM cur_dirs WHERE parent_id = ?',(fid,)).fetchall():
			print(*self.format_info(self.info_fid(fid), info_lev=info_lev),sep='\t')

	def ls_r(self, fid=None,*,info_lev=1, show_deleted=True, where='all', interval=None):
		'''
		where:
		'all' - всё показывать
		'hist_owner'
		'hist_noowner'
		'modified'
		(*,*) -> (fids,fidsd)
		list/set -> fids -> fidsd
		'''
		assert where in ['all', 'hist_owner', 'hist_noowner', 'modified']
		fid = self.any2id(fid)
		print(*self.format_info(self.info_fid(fid), info_lev=0),sep='\t')
		nest_reducer = (len(self.path2ids(self.id2path(fid))))
		if where=='hist_owner': 
			fids                   = set(self.CUR.execute('SELECT cur_stat.id FROM cur_stat JOIN hist ON cur_stat.id==hist.id WHERE cur_stat.owner NOT NULL').fetchall())
			if show_deleted: fidsd = set(self.CUR.execute('SELECT  deleted.id FROM deleted  JOIN hist ON deleted.id ==hist.id WHERE deleted.owner  NOT NULL').fetchall())
		if where=='hist_noowner': 
			fids                   = set(self.CUR.execute('SELECT cur_stat.id FROM cur_stat JOIN hist ON cur_stat.id==hist.id WHERE cur_stat.owner IS NULL').fetchall())
			if show_deleted: fidsd = set(self.CUR.execute('SELECT  deleted.id FROM deleted  JOIN hist ON deleted.id ==hist.id WHERE deleted.owner  IS NULL').fetchall())
		if where=='modified': 
			fids                   = set(self.CUR.execute('SELECT id FROM cur_dirs WHERE modified>0').fetchall())
			if show_deleted: fidsd = set()
		if type(where) is tuple:
			(fids,fidsd) = where
		if type(where) is list or type(where) is set:
			fids = fidsd = where
		#print(fids)
		#print(fidsd)

		count = 0
		def my_walk(did,parents):
			nonlocal count
			printed = False
			if True:
				for (fid,) in self.CUR.execute('SELECT id FROM cur_dirs WHERE parent_id = ?',(did,)).fetchall():
					if where=='all' or (fid,) in fids:
						info = self.info_fid(fid,interval=interval)
						if interval is None or info.count>0:
							printed = True
							count+=1
							for pid in parents: # вывод ненвыведенных парентов
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
		print('total objects number:',count)

	def list_owners(self, path=None, show_deleted=True, owner=None):
		# todo если задан owner - показывает только его
		# выводить только если owner и owner родителя не совпадают
		# deleted - с пометками
		if path is None:
			for dr in self.ROOT_DIRS:
				self.list_owners(dr, show_deleted)
			return
		path = os.path.abspath(path)
		# format: save owner fid deleted path
		(fid,deleted) = self.path2ids_hist(path,self.CUR)
		fid = fid[-1]
		if fid is None:
			self.raise_notify(None, 'path does not exist')
		if show_deleted or not deleted:
			print(*self.format_info(self.info_fid(fid), info_lev=0, abs_path=True),sep='\t')
		def my_walk(did,deleted,downer,depth):
			if not deleted:
				for (owner, fid) in self.CUR.execute(
					'SELECT cur_stat.owner, cur_stat.id FROM cur_stat JOIN cur_dirs ON cur_dirs.id=cur_stat.id WHERE cur_dirs.parent_id = ?',(did,)).fetchall():
					if owner!=downer:
						print(*self.format_info(self.info_fid(fid), info_lev=0, abs_path=True),sep='\t')
					my_walk(fid,False,owner,depth+1)
			if show_deleted:
				for (owner, fid) in self.CUR.execute('SELECT owner, id FROM deleted WHERE parent_id = ?''',(did,)).fetchall():
					if owner!=downer:
						print(*self.format_info(self.info_fid(fid), info_lev=0, abs_path=True),sep='\t')
					my_walk(fid,True,owner,depth+1)
		my_walk(fid,deleted,owner,0)

	def unused_owners(self):
		for (oid, oname) in self.CUR.execute('''SELECT owners.id, owners.name  FROM owners WHERE owners.id NOT IN 
				(SELECT cur_stat.owner AS id FROM cur_stat WHERE cur_stat.owner NOT NULL UNION SELECT deleted.owner AS id FROM deleted WHERE deleted.owner NOT NULL)'''):
			print(oid, oname, sep='\t')

	def all_info(self, interval=None, show_deleted=True):
		print('----- modified with no owner ----')
		for path in self.ROOT_DIRS:
			self.ls_r(path,info_lev=1, show_deleted=True, where='hist_noowner')
		print('----- modified with owner ----')
		for path in self.ROOT_DIRS:
			self.ls_r(path,info_lev=1, show_deleted=True, where='hist_owner',interval=interval)
		print('----- with owner ----')
		self.list_owners()
		print('----- unused owners ----')
		self.unused_owners()

	def hist_id(self, fid):
		print(self.id2path_hist(fid,self.CUR))
		with closing(self.CON.execute('SELECT * FROM cur_dirs WHERE id = ?',(fid,))) as cursor:
			list(self.print_fid(cursor))
		for (parent_id, name, typ, etyp, data, time, static_found) in \
			self.CUR.execute('SELECT parent_id, name, type, event_type, data, time, static_found FROM hist WHERE id = ? ORDER BY time DESC',(fid,)).fetchall():
				if etyp==ECREAT: etyp = 'C'
				elif etyp==EDEL: etyp = 'D'
				elif etyp==EMOVE: etyp = 'V'
				elif etyp==EMODIF:etyp= 'M'
				else: assert False, etyp
				if etyp=='V':
					print(etyp+' '+('S' if static_found else 'W')+' '+str(datetime.fromtimestamp(time)),
						 self.id2path_hist(parent_id,self.CUR)[0]+'/'+name)
				else:
					print(etyp+' '+('S' if static_found else 'W')+' '+str(datetime.fromtimestamp(time)))

	# ------------------------------------
	# инициализация приложения/библиотеки
	# ------------------------------------

	def read_root_dirs(self):
		'''из базы данных считывает, какие папки отмечены для слежения'''
		with self.CON:
			root_dirs = []
			def walk(did,path):
				n = self.CUR.execute('SELECT id, name, modified FROM cur_dirs WHERE parent_id = ?',(did,)).fetchall()
				for (fid, name, modified) in n:
					if modified==2:
						walk(fid, path+'/'+name)
					else:
						root_dirs.append(path+'/'+name)
			walk(0, '')
			return root_dirs

	@staticmethod
	def get_root_dirs():
		'''определяет, за какими папками надо на самом деле следить, если указано следить за всей файловой системой'''
		dirs = []
		for rd in os.listdir(path='/'):
			if rd in ['media','cdrom','mnt','proc','sys','dev','run']:
				continue
			mode = os.stat('/'+rd,follow_symlinks=False).st_mode
			if STAT.S_ISDIR(mode) and not STAT.S_ISLNK(mode):
				dirs.append('/'+rd)
		return dirs

	def __init__(self, files_db, root_dirs = None, nohash = False, ro = True, nocheck=False):
		'''инициализирует FILES_DB, ROOT_DIRS; открывает сединение CON, CUR (по умолчанию только для чтения)'''
		try:
			self.CON.cursor().close()
		except Exception as ex:
			pass
		else:
			self.CON.close()
			#raise Exception('close existing connection before opening new one')

		if root_dirs is None:
			# loading existring db
			if not os.path.isfile(files_db):
				raise Exception(f'database {files_db} does not exist. Create it with root_dirs argument')
			self.FILES_DB = files_db
			if ro:
				self.CON = sqlite3.connect('file:'+self.FILES_DB+'?mode=ro', uri=True)
			else:
				self.CON = sqlite3.connect(self.FILES_DB)
				db_mode = self.CON.execute("PRAGMA journal_mode=WAL;").fetchone()
				assert db_mode==('wal',), db_mode
			self.CUR = self.CON.cursor()


			if not nocheck:
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
			root_dirs = [os.path.abspath(x) for x in root_dirs]
			self.FILES_DB = files_db
			self.CON = sqlite3.connect(self.FILES_DB)
			self.CUR = self.CON.cursor()
			self.ROOT_DIRS = root_dirs
			self.init_db(nohash)
			if not nocheck:
				self.check_integrity()
			if ro:
				self.CON.close()
				self.CON = sqlite3.connect('files:'+self.FILES_DB+'?mode=ro', uri=True)

	def __del__(self):
		self.CON.close()
		if self.keyboard_thr is not None and self.keyboard_thr.is_alive():
			self.notify(1.5,f'filesdb({repr(self.FILES_DB)}): lost running keyboard thread')
		if self.commit_thr is not None and self.commit_thr.is_alive():
			self.notify(1.5,f'filesdb({repr(self.FILES_DB)}): lost running commit thread')

	def execute(self,*args,**kwargs):
		with self.CON:
			return self.CUR.execute(*args,**kwargs)

if __name__ == "__main__":
	import sys

	if len(sys.argv)==1 or sys.argv[1]=='--help' or sys.argv[1]=='-h' :
		print(f'''run:\n  {sys.argv[0]} [--nohash] files.db [dir...]''')
		exit(0)

	import sys
	if sys.argv[1]=='--nohash':
		nohash = True
		del sys.argv[1]
	else:
		nohash = False

	root_dirs = sys.argv[2:]
	if len(root_dirs)==0: root_dirs = None
	print(sys.argv[1],root_dirs,nohash)
	fdb = filesdb(sys.argv[1],root_dirs,nohash, ro=False)
	fdb.watch()
