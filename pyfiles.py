import os
import stat as STAT
import sqlite3
from tqdm import tqdm
import hashlib
from time import time
from datetime import datetime
from contextlib import closing

class AttrDict(dict):
	def __getattr__(self, key):
		if key not in self:
			raise AttributeError(key) # essential for testing by hasattr
		return self[key]
	def __setattr__(self, key, value):
		self[key] = value
def make_dict(**kwargs):
	return AttrDict(kwargs)


VERBOSE = 1

FILES_DB = None
ROOT_DIRS = None
CON = None
CUR = None

# -------------
# схема данных
# -------------

def create_tables():
	with CON:
		CUR.execute('''CREATE TABLE cur_dirs (
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
		CUR.execute('CREATE INDEX id_cur_dirs ON cur_dirs (id)')
		CUR.execute('CREATE INDEX parname_cur_dirs ON cur_dirs (parent_id, name)')	

		CUR.execute(''' CREATE TABLE cur_stat  (
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
		CUR.execute('CREATE INDEX id_cur_stat ON cur_stat (id)')

		# для запоминания owner-ов удалённых файлов
		# и чтобы fid-ы не росли, если какой-то файл многократно удаляется и снова создаётся
		# можно было бы использовать hist для этих целей, но там каждый файл не в единственном экземпляре,
		# и особенно, если мы не хотим сохранять события о файле, а он постоянно удаляется и создаётся
		# todo добавить время удаления, чтобы можно было удалять инфу об очень давно удалённых файлах
		CUR.execute('''
		CREATE TABLE deleted  (
			parent_id INTEGER NOT NULL, /* старая запись из cur_dirs */
			name      TEXT    NOT NULL, /* старая запись из cur_dirs */
			id        INTEGER NOT NULL, /* старая запись из cur_dirs */
			owner     INTEGER, /* при создании/восстановлении имеет преимущество перед owner-ом родительской папки */
		UNIQUE(id),
		UNIQUE(parent_id,name)
		)
		''')
		CUR.execute('CREATE INDEX id_deleted ON deleted (id)')
		CUR.execute('CREATE INDEX parname_deleted ON deleted (parent_id,name)')

		CUR.execute('''
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
			owner        INTEGER, /* старая запись из cur_stat */

			time         REAL    NOT NULL, /* время события */
			static_found INTEGER NOT NULL /* 0 - обнаружено watchdog-ом, 1 - обнаружено статитсеским обходом дерева каталогов */
		)
		''')
		CUR.execute('CREATE INDEX id_hist ON hist (id)')
		CUR.execute('CREATE INDEX time_hist ON hist (time)')

		CUR.execute('''CREATE TABLE owners  (
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
		CUR.execute('CREATE INDEX id_owners ON owners (id)')
		CUR.execute('CREATE INDEX name_owners ON owners (name)')

def check_integrity():
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
	для всех из cur_stat у кого owner is not None есть owner в owners
	cur_dirs.type  cur_stat.type = simple_type(cur_stat.st_mode)
	'''

	# присутствуют таблицы: cur_dirs, cur_stat, deleted, hist, owners
	tables = {x[0] for x in CUR.execute('SELECT name FROM sqlite_master')}
	assert 'cur_dirs' in tables, "table cur_dirs not found"
	assert 'cur_stat' in tables, "table cur_stat not found"
	assert 'deleted' in tables, "table deleted not found"
	assert 'hist' in tables, "table hist not found"
	assert 'owners' in tables, "table owners not found"

	# у каждого существует родитель
	# 	для cur_dirs в cur_dirs
	cur_dirs_parents = {x[0] for x in CUR.execute('SELECT parent_id FROM cur_dirs').fetchall()}
	cur_dirs_ids = {x[0] for x in CUR.execute('SELECT id FROM cur_dirs').fetchall()}
	assert cur_dirs_parents <= (cur_dirs_ids|{0}), f'lost parents in cur_dirs: {cur_dirs_parents-(cur_dirs_ids|{0})}'

	#	для pre-root_dir в pre-root_dir
	root_dirs_parents = {x[0] for x in CUR.execute('SELECT parent_id FROM cur_dirs WHERE modified = 2').fetchall()}
	root_dirs_ids = {x[0] for x in CUR.execute('SELECT id FROM cur_dirs WHERE modified = 2').fetchall()}
	assert root_dirs_parents <= (root_dirs_ids|{0}), f'lost parents in pre-root_dirs: {root_dirs_parents-(root_dirs_ids|{0})}'

	#	для modified в modified или pre-root_dir
	m_dirs_parents = {x[0] for x in CUR.execute('SELECT parent_id FROM cur_dirs WHERE modified = 1').fetchall()}
	m_dirs_ids = {x[0] for x in CUR.execute('SELECT id FROM cur_dirs WHERE modified = 1').fetchall()}
	assert m_dirs_parents <= (m_dirs_ids|root_dirs_ids|{0}), f'lost parents in modified: {m_dirs_parents-(m_dirs_ids|root_dirs_ids|{0})}'

	# у всех из cur_dirs (кроме pre-root_dir) есть обаз из cur_stat и наоборот
	notroot_dirs_ids = {x[0] for x in CUR.execute('SELECT id FROM cur_dirs WHERE modified != 2').fetchall()}
	stat_ids = {x[0] for x in CUR.execute('SELECT id FROM cur_stat').fetchall()}
	assert notroot_dirs_ids == stat_ids, f'mismatch root_dirs and stat: {notroot_dirs_ids - stat_ids}, {stat_ids - notroot_dirs_ids}'

	#	для deleted в cur_dirs или deleted
	deleted_parents = {x[0] for x in CUR.execute('SELECT parent_id FROM deleted').fetchall()}
	deleted_ids = {x[0] for x in CUR.execute('SELECT id FROM deleted').fetchall()}
	assert deleted_parents <= (notroot_dirs_ids|deleted_ids), f'lost parents in deleted: {deleted_parents-(notroot_dirs_ids|deleted_ids)}'

	# 	для hist в cur_dirs или deleted
	hist_parents = {x[0] for x in CUR.execute('SELECT parent_id FROM hist WHERE parent_id!=-1').fetchall()}
	assert hist_parents <= (notroot_dirs_ids|deleted_ids), f'lost parents in hist: {hist_parents-(notroot_dirs_ids|deleted_ids)}'

	# для всех из hist есть образ в cur_stat или deleted
	hist_ids = {x[0] for x in CUR.execute('SELECT id FROM hist').fetchall()}
	assert hist_ids <= (notroot_dirs_ids|deleted_ids), f'hist enty with unknown id: {hist_ids-(notroot_dirs_ids|deleted_ids)}'

	# для всех из cur_stat у кого owner is not None есть owner в owners
	stat_owners = {x[0] for x in CUR.execute('SELECT owner FROM cur_stat WHERE owner NOT NULL').fetchall()}
	owners = {x[0] for x in CUR.execute('SELECT id FROM owners').fetchall()}
	assert stat_owners <= owners, f'lost owner: {stat_owners - owners}'

	# cur_dirs.type  cur_stat.type = simple_type(cur_stat.st_mode)
	for (t1, t2, mode) in CUR.execute('SELECT cur_dirs.type, cur_stat.type, cur_stat.st_mode FROM cur_dirs JOIN cur_stat ON cur_dirs.id=cur_stat.id').fetchall():
		assert t1==t2 , (t1,t2)
		if mode is not None:
			assert t2==simple_type(mode), (t2,simple_type(mode))
# --------------
# общие функции образа ФС
# --------------

# cur_dirs:modified
# 2 - pre-root-dir
# 1 - modified
# 0 - not modified

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

def path2ids(path,cursor):
	'''
	преобразовывает путь в последовательность id-ов всех родительских папок
	Если в какой-то момент не удалось найти очередную папку - последовательность будет заканчиваться Nane-ом
	id объекта, задаваемого путём находится в последнй ячейке массива
	'''
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
def id2path(fid,cursor):
	'''
	преобразовывает id в путь
	'''
	path = ''
	while fid!=0:
		n = cursor.execute('SELECT parent_id, name FROM cur_dirs WHERE id = ? ',(fid,)).fetchone()
		assert n is not None
		path = '/'+n[1]+path
		fid = n[0]
	return path

def is_modified(fid, cursor):
	'''
	просто замена одному запросу в БД
	'''
	n = cursor.execute('SELECT modified FROM cur_dirs WHERE id = ?',(fid,)).fetchone()
	if n is None: raise Exception(f"can't find fid {fid}")
	return n[0]==1
def set_modified(fid, cursor):
	'''
	выставляет modified в объект и в его родителя, если тот ещё не, и так рекурсивно
	'''
	if fid==0: return
	n = cursor.execute('SELECT parent_id, modified FROM cur_dirs WHERE id = ?',(fid,)).fetchone()
	if n is None: raise Exception(f"can't find fid {fid}")
	if n[1]==0:
		#print('set_modified', fid)
		cursor.execute('UPDATE cur_dirs SET modified = 1 WHERE id = ?',(fid,))
		set_modified(n[0], cursor)
	
def update_stat(fid, stat, cursor):
	'''
	по fid-у заполняет stat-поля в cur_stat
	'''
	cursor.execute('''UPDATE cur_stat SET
		st_mode=?,st_ino=?,st_dev=?,st_nlink=?,st_uid=?,st_gid=?,st_size=?,
		st_atime=?,st_mtime=?,st_ctime=?,st_blocks=?,st_blksize=? WHERE id = ?''',
		(stat.st_mode,stat.st_ino,stat.st_dev,stat.st_nlink,stat.st_uid,stat.st_gid,stat.st_size,
		stat.st_atime,stat.st_mtime,stat.st_ctime,stat.st_blocks,stat.st_blksize, fid)
	)
def get_stat(fid, cursor):
	'''
	по fid-у возвращает stat-поля из cur_stat в виде объекта
	'''
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

def create_root(path,cursor):
	'''
	создает корневые директории в дереве cur_dirs (помечает родительские директории к path как pre-root-dir)
	'''
	ids = path2ids(path,cursor)
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
		print(path,type(e),e)
		if name in dirs:
			CUR.executemany('INSERT INTO cur_dirs (parent_id, name, modified, type) VALUES (?, ?, 0, ?)', (fid, path[-1], MDIR))
			(fid,) = CUR.execute('SELECT id FROM cur_dirs WHERE parent_id = ? AND name = ?',(fid, path[-1])).fetchone()
			CUR.execute('INSERT INTO cur_stat (id,type) VALUES (?,?)', (fid,MDIR))
			print('blindly create dir')
	else:
		CUR.execute('INSERT INTO cur_dirs (parent_id, name, modified, type) VALUES (?, ?, 0, ?)', (fid, path[-1], simple_type(stat.st_mode)))
		(fid,) = CUR.execute('SELECT id FROM cur_dirs WHERE parent_id = ? AND name = ?',(fid, path[-1])).fetchone()
		CUR.execute('INSERT INTO cur_stat (id,type) VALUES (?,?)', (fid,simple_type(stat.st_mode)))
		update_stat(fid,stat,CUR)
	return fid

def init_cur(root_dirs):
	'''
	обходит ФС из root_dirs и заполняет таблицу cur_dirs
	'''
	with CON:
		print('walk root_dirs:')
		for root_dir in tqdm(root_dirs):
			#print(root_dir)
			create_root(root_dir,CUR)
			for root, dirs, files in os.walk(root_dir):
				pathids = path2ids(root,CUR)
				assert pathids[-1] is not None
				#print(root,pathids,dirs)
				# при выполнении stat MFILE/MDIR может быть заменён на MLINK или MOTHER
				for name in dirs+files:
					try:
						stat = os_stat(root+'/'+name)
					except Exception as e:
						print(root+'/'+name,type(e),e)
						if name in dirs:
							CUR.executemany('INSERT INTO cur_dirs (parent_id, name, modified, type) VALUES (?, ?, 0, ?)', (pathids[-1], name, MDIR))
							(fid,) = CUR.execute('SELECT id FROM cur_dirs WHERE parent_id = ? AND name = ?',(pathids[-1], name)).fetchone()
							CUR.execute('INSERT INTO cur_stat (id,type) VALUES (?,?)', (fid,MDIR))
							print('blindly create dir')
					else:
						CUR.execute('INSERT INTO cur_dirs (parent_id, name, modified, type) VALUES (?, ?, 0, ?)', (pathids[-1], name, simple_type(stat.st_mode)))
						(fid,) = CUR.execute('SELECT id FROM cur_dirs WHERE parent_id = ? AND name = ?',(pathids[-1], name)).fetchone()
						CUR.execute('INSERT INTO cur_stat (id,type) VALUES (?,?)', (fid,simple_type(stat.st_mode)))
						update_stat(fid,stat,CUR)

def update_hashes(with_all=True, modify=None):
	# todo calc only unknown hashes

	with CON:
		with closing(CON.cursor()) as cursor:
			if with_all:
				ids = cursor.execute('SELECT id FROM cur_stat WHERE type = ?',(MFILE,)).fetchall()
			else:
				ids = cursor.execute('SELECT id FROM cur_stat WHERE type = ? AND modified = 1',(MFILE,)).fetchall()
			cnt = 0
			print('calc hashes:')
			for fid in tqdm(ids):
				fid = fid[0]
				path = None
				try:
					path = id2path(fid,cursor)
					hsh = hashlib.md5(open(path,'rb').read()).hexdigest()
					if modify is not None:
						(ohash,) = cursor.execute('SELECT data FROM cur_stat WHERE id = ?',(fid,)).fetchone()
						if ohash is not None and ohash!=hsh:
							modify(fid, os_stat(path), True, cursor)
					cursor.execute('UPDATE cur_stat SET data = ? WHERE id = ?',(hsh,fid))
					cursor.execute('UPDATE cur_dirs SET modified = 0 WHERE id = ?',(fid,))
					cnt+=1
					if cnt%1000000==0:
						cursor.execute('COMMIT')
				except FileNotFoundError:
					set_modified(fid, cursor)
				except Exception as e:
					print(fid,path,type(e),e)

	# обновить симлинки, директории, сынтегрировать хеши
	with CON:
		with closing(CON.cursor()) as cursor:
			def my_walk(did,root):
				n = cursor.execute('SELECT name,id,type,modified FROM cur_dirs WHERE parent_id = ? ',(did,)).fetchall()
				hsh = 0
				for name,fid,ftype,modified in n:
					if ftype==MFILE:
						try:
							(lhsh,) = cursor.execute('SELECT data FROM cur_stat WHERE id = ?',(fid,)).fetchone()
						except Exception as e:
							print(fid)
							raise e
					elif ftype==MLINK:
						try:
							lnk = os.readlink(id2path(fid,cursor))
							if modify is not None:
								(olink,) = cursor.execute('SELECT data FROM cur_stat WHERE id = ?',(fid,)).fetchone()
								if olink is not None and olink!=lnk:
									modify(fid, os_stat(id2path(fid)), True, cursor)
							lhsh = hashlib.md5(lnk.encode()).hexdigest()
							cursor.execute('UPDATE cur_stat SET data = ? WHERE id = ?',(lnk,fid))
						except FileNotFoundError:
							set_modified(fid, cursor)
							lhsh = None
					elif ftype==MDIR:
						if with_all or modified!=0:
							lhsh = my_walk(fid,modified==2)
						else:
							(lhsh,) = cursor.execute('SELECT data FROM cur_stat WHERE id = ?',(fid,)).fetchone()
					elif ftype==MOTHER:
						lhsh = hex( 0 )[2:].zfill(32)
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

def init_db(nohash):
	'''
	создаёт и инициализирует таблицы
	'''
	create_tables()
	init_cur(ROOT_DIRS)
	if not nohash:
		update_hashes()

# ---------------------
# общие функции событий
# ---------------------

def id2path_hist(fid,cursor):
	'''
	то же что id2path(), только ещё ищет в deleted
	возвращает (path, deleted: Bool)
	'''
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
def path2ids_hist(path,cursor):
	'''
	то же что path2ids(), только ещё ищет в deleted
	возвращает (ids, deleted: Bool)
	'''
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
	
def owner_save(fid,cursor):
	'''
	определяет владельца и надо ли сохранять события, связанные с этим файлом
	'''
	if VERBOSE>=3: print('owner_save',fid)
	(owner,) = cursor.execute('SELECT owner FROM cur_stat WHERE id = ?',(fid,)).fetchone()
	if owner is not None:
		(save,) = cursor.execute('SELECT save FROM owners WHERE id = ?',(owner,)).fetchone()
	else:
		save = True
	return (owner,save)

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
		if VERBOSE>=2: print('st_mode')
		return False
	if stat.st_ino != ostat.st_ino:
		if VERBOSE>=2: print('st_ino')
		return False
	if stat.st_dev != ostat.st_dev:
		if VERBOSE>=2: print('st_dev')
		return False
	if stat.st_nlink != ostat.st_nlink:
		if VERBOSE>=2: print('st_nlink')
		return False
	if stat.st_uid != ostat.st_uid:
		if VERBOSE>=2: print('st_uid')
		return False
	if stat.st_gid != ostat.st_gid:
		if VERBOSE>=2: print('st_gid')
		return False
	if stat.st_size != ostat.st_size:
		if VERBOSE>=2: print('st_size')
		return False
	if stat.st_ctime != ostat.st_ctime:
		if VERBOSE>=2: print('st_ctime',datetime.fromtimestamp(ostat.st_ctime),datetime.fromtimestamp(stat.st_ctime))
		return False
	if simple_type(stat.st_mode)!=MDIR and stat.st_mtime != ostat.st_mtime:
		if VERBOSE>=2: print('st_mtime')
		return False
	if stat.st_blocks != ostat.st_blocks:
		if VERBOSE>=2: print('st_blocks')
		return False
	if stat.st_blksize != ostat.st_blksize:
		if VERBOSE>=2: print('st_blksize')
		return False
	return True

def add_event(fid, typ, etyp, static_found, cursor):
	'''
	создает запись в hist
	если событие ECREAT: заполняет большинство полей -1
	иначе: копирует данные из fur_dirs, cur_stat
		опционально если указан typ: проверяет, чтобы он равнялся старому типу
	'''
	ltime = time()
	if etyp==ECREAT:
		if VERBOSE:
			print(datetime.fromtimestamp(ltime), etyp2str(etyp), static_found, fid, typ2str(typ), id2path_hist(fid,cursor)[0])
		cursor.execute('''INSERT INTO hist (
				parent_id, name,
				id, type, event_type,
				st_mode,st_ino,st_dev,st_nlink,st_uid,st_gid,st_size,st_atime,st_mtime,st_ctime,st_blocks,st_blksize,
				data,owner,
				time,static_found
			) VALUES (-1,'',?,?,?,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,?,?)''',
					   (fid,typ,etyp,ltime,static_found))
	else:
		(otyp,) = cursor.execute('SELECT type FROM cur_dirs WHERE id = ?',(fid,)).fetchone()
		if typ is not None:
			assert typ == otyp , (typ, otyp)
		else:
			typ = otyp
		if VERBOSE:
			print(datetime.fromtimestamp(ltime), etyp2str(etyp), static_found, fid, typ2str(otyp), id2path_hist(fid,cursor)[0])
		# просто часть данных копируем а часть заполняем вручную
		cursor.execute('''INSERT INTO hist (parent_id, name, id, type, event_type,
			st_mode,st_ino,st_dev,st_nlink,st_uid,st_gid,st_size,
			st_atime,st_mtime,st_ctime,st_blocks,st_blksize,data,owner,
			time,static_found)
			SELECT t1.parent_id, t1.name, ?, ?, ?,
			t2.st_mode,t2.st_ino,t2.st_dev,t2.st_nlink,t2.st_uid,t2.st_gid,t2.st_size,
			t2.st_atime,t2.st_mtime,t2.st_ctime,t2.st_blocks,t2.st_blksize,t2.data,t2.owner,
			?,?
			FROM cur_dirs AS t1
			JOIN cur_stat AS t2
			ON 1=1
			WHERE t1.id = ? AND t2.id = ?
			''',
					   (fid,typ,etyp,ltime,static_found,fid,fid)
		)
			# INSERT INTO table_target (col1, col2, col3, col4, col5)
			# SELECT t1.col1, t1.col2, t2.col3, t2.col4, ?
			# FROM table1 AS t1
			# JOIN table2 AS t2
			# ON 1=1
			# WHERE t1.id = ? AND t2.id = ?;

def modify(fid, stat, static_found, cursor):
	'''
	известно, что объект fid изменился, известен его новый stat
	'''
	if VERBOSE>=2: print('modify',fid, id2path(fid, cursor), static_found)#stat,

	set_modified(fid, cursor)
	update_stat(fid,stat,cursor)

	(owner,save) = owner_save(fid,cursor)
	if save:
		# cохранить старый stat
		# условие для папки - если изменился её stat (st_atime, st_mtime не учитываем)
		# условие для файла - если с предыдущего обновления прошло больше 10 сек
		if simple_type(stat.st_mode)==MDIR:
			save = not stat_eq(stat,get_stat(fid,cursor))
		else:
			save = True
			n = cursor.execute('SELECT time FROM hist WHERE id = ? ORDER BY time DESC LIMIT 1',(fid,)).fetchone()
			if n is not None: # раньше этот файл уже обновлялся
				save = abs(n[0] - time())>10
		if save:
			add_event(fid, simple_type(stat.st_mode), EMODIF, static_found, cursor)
	
def create(parent_id, name, stat, static_found, cursor, owner=None, save=None):
	'''
	создается объект, родительская директория которого уже существует
	как правило save, owner определяются родительской папкой
	возвращает fid созданного объекта
	'''
	if owner is None or save is None:
		(owner,save) = owner_save(parent_id,cursor)
	if VERBOSE>=2: print('create',parent_id, id2path(parent_id, cursor), name, static_found, owner, save)# stat,
	set_modified(parent_id, cursor)
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
	update_stat(fid,stat,cursor)

	if save:
		add_event(fid, simple_type(stat.st_mode), ECREAT, static_found, cursor)

	return fid
	
def delete(fid, static_found, cursor):
	'''
	удаляем существующий объект fid
	а также его потомков, если они существуют
	'''
	if VERBOSE>=2: print('delete',fid, id2path(fid, cursor),static_found)
	(owner,save) = owner_save(fid,cursor)

	def my_walk(did):
		n = cursor.execute('SELECT name,id,type FROM cur_dirs WHERE parent_id = ? ',(did,)).fetchall()
		for name,fid,ftype in n:
			if ftype==MDIR:
				my_walk(fid)
			if save:
				add_event(fid, None, EDEL, static_found, cursor)

			(owner,) = cursor.execute('SELECT owner FROM cur_stat WHERE id = ?', (fid,)).fetchone()
			cursor.execute('INSERT INTO deleted VALUES (?,?,?,?)',(did,name,fid,owner))

			cursor.execute('DELETE FROM cur_stat WHERE id = ?',(fid,))
			cursor.execute('DELETE FROM cur_dirs WHERE id = ?',(fid,))
			
	my_walk(fid)
	if save:
		add_event(fid, None, EDEL, static_found, cursor)

	(owner,) = cursor.execute('SELECT owner FROM cur_stat WHERE id = ?', (fid,)).fetchone()
	(did,name) = cursor.execute('SELECT parent_id, name FROM cur_dirs WHERE id = ?', (fid,)).fetchone()
	cursor.execute('INSERT INTO deleted VALUES (?,?,?,?)',(did,name,fid,owner))

	cursor.execute('DELETE FROM cur_stat WHERE id = ?',(fid,))
	cursor.execute('DELETE FROM cur_dirs WHERE id = ?',(fid,))

# --------------------------------
# функции статического обновления
# --------------------------------

def walk_stat(with_all, did, path='', typ=MDIR, modified=0):
	'''
	основная цель: найти изменения, которые не были пойманы watchdog-ом
	with_all === not only_modified
	обходим модифицированные объекты в БД, сравнимаем их с ФС
	или обходим все объекты ФС (в зависимости от only_modified)
	изменения заносим в журнал
	и помечаем их как модифицированнные
	'''
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
	with CON:
		if did!=0 and modified!=2:
			if typ==MDIR:
				children = CUR.execute('SELECT name,id,type,modified FROM cur_dirs WHERE parent_id = ?',(did,)).fetchall()
				real_children = os.listdir(path)
				children2 = []
				# удаляем удалённые
				for (name,fid,ctyp,cmodified) in children:
					if name in real_children:
						children2.append((name,fid,ctyp,cmodified))
						real_children.remove(name)
					else:
						this_modified = True
						delete(fid, True, CUR)
				# просматриваем которые остались(с учётом only_modified)
				for (name,fid,ctyp,cmodified) in children2:
					if with_all or cmodified:
						this_modified |= walk_stat(with_all, fid, path+'/'+name, ctyp, cmodified)
				# создаём новые и просматриваем их(modified=3)
				for name in real_children:
					this_modified = True
					cpath = path+'/'+name
					try:
						cstat = os_stat(cpath)
					except FileNotFoundError:
						print(cpath,"found new item but can't stat it")
						continue
					fid = create(did, name, cstat, True, CUR)
					walk_stat(with_all, fid, cpath, simple_type(cstat.st_mode), 3)
			if modified!=3:
				try:
					stat = os_stat(path)
					this_modified |= not stat_eq(stat,get_stat(did,CUR))
					if this_modified:
						modify(did, stat, True, CUR)
				except FileNotFoundError:
					print(path,"item may be alreay deleted")
		else:
			for (name,fid,ctyp,cmodified) in CUR.execute('SELECT name,id,type,modified FROM cur_dirs WHERE parent_id = ?',(did,)).fetchall():
				this_modified |= walk_stat(with_all, fid, path+'/'+name, ctyp, cmodified)
	return this_modified

def walk_stat_all():
	walk_stat(True, 0)
def walk_stat_modified():
	walk_stat(False, 0)

# --------------------------------
# функции динамического обновления
# --------------------------------

def create_parents(path,cursor,ids=None):
	if VERBOSE>=2: print('create_parents',path,cursor,ids)
	if ids is None:
		ids = path2ids(path,cursor)
		
	# рассчитываем, что src_path - обсолютный путь, не симлинк, не содержит // типа '/a//b/c'
	path = path.split('/')

	fid = ids[-2]
	(owner,save) = owner_save(fid,cursor)

	parent_path = '/'.join(path[:len(ids)])
	for name in path[len(ids):-1]:
		parent_path+= ('/'+name)
		lstat = os_stat(parent_path) # FileNotFoundError будет пойман в области watchdog-а
		assert simple_type(lstat.st_mode)==MDIR, simple_type(lstat.st_mode)
		fid = create(fid, name, lstat, True, cursor, owner, save)

	return fid, path[-1], owner, save

def create1(ids, src_path, stat, is_directory, cursor):
	if VERBOSE>=2: print('created1',ids, src_path, stat, is_directory, cursor)
	(fid, name, owner, save) = create_parents(src_path,cursor,ids)
	create(fid, name, stat, False, cursor, owner, save)

def move(fid, dest_path, cursor):
	'''
	существующий объект fid перемещается на новое место
	фактически у него изменяется только parent_id, name
	Если требуется, создаются необходиме родительские директории для целевого пути
	'''
	if VERBOSE>=2: print('moved',fid, dest_path, cursor)
	(parent_id, _, _, name) = create_parents(dest_path,cursor)
	(_,save) = owner_save(fid,cursor)
	if save:
		add_event(fid, None, EMOVE, False, cursor)
	cursor.execute('UPDATE cur_dirs SET parent_id = ?, name = ? WHERE id = ?',(parent_id, name, fid))

def normalize_path(path):
	return path.replace('//','/').replace('//','/').replace('//','/').replace('//','/')

def modified(src_path, stat, is_directory, is_synthetic, cursor):
	if VERBOSE>=2: print('modified',src_path, stat, is_directory, is_synthetic, cursor)
	src_path = normalize_path(src_path)
	if is_synthetic:
		print('synthetic modified',src_path, is_directory)
		return
	ids = path2ids(src_path,cursor)
	if ids[-1] is None:
		print('do modified as created',src_path, datetime.fromtimestamp(time()))
		return create1(ids, src_path, stat, is_directory,cursor)
	return modify(ids[-1], stat, False, cursor)

def created(src_path, stat, is_directory, is_synthetic, cursor):
	if VERBOSE>=2: print('created',src_path, stat, is_directory, is_synthetic, cursor)
	src_path = normalize_path(src_path)
	if is_synthetic:
		print('synthetic created',src_path, is_directory)
		return
	ids = path2ids(src_path,cursor)
	if ids[-1] is not None:
		# если было удалено, но это не было зафиксировано, а потом создалось - считаем, что просто изменилось
		print('do created as modified',src_path, datetime.fromtimestamp(time()))
		return modify(ids[-1], stat, False, cursor)
	return create1(ids, src_path, stat, is_directory,cursor)

def deleted(src_path, is_directory, is_synthetic, cursor):
	if VERBOSE>=2: print('deleted',src_path, is_directory, is_synthetic, cursor)
	src_path = normalize_path(src_path)
	if is_synthetic:
		print('synthetic deleted',src_path, is_directory)
		return
	ids = path2ids(src_path,cursor)
	if ids[-1] is None:
		print('deleted unknown object:',src_path)
		return
	delete(ids[-1], False, cursor)

def moved(src_path, dest_path, stat, is_directory, is_synthetic, cursor):
	if VERBOSE>=2: print('moved',src_path, dest_path, stat, is_directory, is_synthetic, cursor)
	src_path = normalize_path(src_path)
	dest_path = normalize_path(dest_path)
	if is_synthetic:
		print('synthetic moved',src_path, dest_path, is_directory)
		return
	ids = path2ids(src_path,cursor)
	if ids[-1] is None:
		print('do moved as created',src_path, dest_path, time())
		return created(dest_path, stat, is_directory, is_synthetic, cursor)
	move(ids[-1],dest_path, cursor)

# --------------------------------
# интерфейсные функции
# --------------------------------

def update_owner(name,save, strict = False):
	with CON:
		if strict:
			if CUR.execute('SELECT * FROM owners WHERE name = ?',(name,)).fetchone() is None:
				raise Exception('such owner does not exist')
		CUR.execute('''INSERT INTO owners (name, save) VALUES (?, ?) ON CONFLICT(name) DO UPDATE SET
			name = excluded.name,    save = excluded.save ''', (name,save))

def rename_owner(oname, name, stric = False):
	with CON:
		if strict:
			if CUR.execute('SELECT * FROM owners WHERE name = ?',(name,)).fetchone() is None:
				raise Exception('such owner does not exist')
		CUR.execute('UPDATE owners SET name = ? WHERE name = ?',(name,oname))

def delete_owner(owner, stric = False):
	with CON:
		if strict:
			if CUR.execute('SELECT * FROM owners WHERE name = ?',(name,)).fetchone() is None:
				raise Exception('such owner does not exist')
		CUR.execute('DELETE FROM owners WHERE name = ?',(owner,))

def set_owner(path, owner, *, save=None, update=False, in_deleted=False, del_hist=False):
	'''
	если такого owner-а еще нет - он создаётся
	save - надо ли в будущем сохранять события изменений этих файлов
		если None - не обновлять owner-а
	update:
		если True - устанавливает owner-а для всех вложенных объектов
		если False - только для тех вложенных, у которых еще нет owner-а или он такой как у объекта path
	in_deleted - устанавливать ли owner-а для удалённых объектов
	del_hist - удалить ли все события с этими файлами
	'''
	path = os.path.abspath(path)
	if save is not None and owner is not None:
		update_owner(owner,save,True)
	with CON:
		with closing(CON.cursor()) as cursor:
			# oid - owner-id, который будем устанавливать
			if owner is not None:
				(oid,) = cursor.execute('SELECT id FROM owners WHERE name = ?',(owner,)).fetchone()
			else:
				oid = None

			def my_walk(did):
				if del_hist:
					cursor.execute('DELETE FROM hist WHERE id = ?',(did,))
				if update:
					n = cursor.execute('SELECT name,id,type FROM cur_dirs WHERE parent_id = ? ',(did,)).fetchall()
				else:
					n = cursor.execute('''SELECT name, cur_dirs.id, cur_dirs.type FROM cur_dirs JOIN cur_stat ON cur_dirs.id=cur_stat.id
						WHERE cur_dirs.parent_id = ? AND cur_stat.owner ISNULL''',(did,)).fetchall()
				for name,fid,ftype in n:
					cursor.execute('UPDATE cur_stat SET owner = ? WHERE id = ?',(oid,fid))
					my_walk(fid)
				if in_deleted:
					if update:
						n = cursor.execute('SELECT name,id FROM deleted WHERE parent_id = ? ',(did,)).fetchall()
					else:
						n = cursor.execute('''SELECT name,id FROM deleted
							WHERE parent_id = ? AND owner ISNULL''',(did,)).fetchall()
					for name,fid in n:
						cursor.execute('UPDATE deleted SET owner = ? WHERE id = ?',(oid,fid))
						my_walk(fid)

			fid = path2ids(path,cursor)[-1]
			cursor.execute('UPDATE cur_stat SET owner = ? WHERE id = ?',(oid,fid))
			(typ,) = cursor.execute('SELECT type FROM cur_dirs WHERE id = ?',(fid,)).fetchone()
			if typ==MDIR:
				my_walk(fid)
			if del_hist:
				cursor.execute('DELETE FROM hist WHERE id = ?',(fid,))
			return fid

def help_owner():
	print('''
		update(name,save, stric = False)
		create(name,save, stric = False)
		rename(oname, name, stric = False)
		delete(owner, stric = False)
		set(path, owner, *, save=None, update=False, in_deleted=False, del_hist=False)
		hashes(with_all=True)
		help()
		''')

def watch(do_stat = True):
	'''запускает watchdog, который ловит события файловой системы
	также может выполнять команды из stdin'''
	# взаимодействуем с ФС
	from watchdog.events import FileSystemEvent, FileSystemEventHandler
	from watchdog.observers import Observer
	import threading
	from queue import Queue
	from time import time, sleep

	# в основном потоке переменные глобальные, в потоке watch - локальные и всегда передаётся cursor через аргументы
	db_mode = CUR.execute("PRAGMA journal_mode=WAL;").fetchone()
	assert db_mode==('wal',), db_mode

	if do_stat:
		print('start walk_stat_all')
		walk_stat_all()
		print('walk_stat_all finished')

	# todo список полностью игнорируемых путей
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
		
	def event_handler(event: FileSystemEvent) -> None:
		if event.event_type=='closed_no_write':
			if VERBOSE>=1: print('pass closed_no_write',event.src_path)
			pass
		elif event.event_type=='opened':
			if VERBOSE>=1: print('pass opened',event.src_path)
			pass
		elif event.event_type=='modified' or event.event_type=='closed':
			if VERBOSE>=1: print('modified',event.src_path)
			try:
				stat = os_stat(event.src_path)
			except FileNotFoundError as e:
				print('error in modified event:', type(e), e, event.src_path, event.is_directory, event.is_synthetic)
			else:
				modified(event.src_path, stat, event.is_directory, event.is_synthetic, CUR)
			
		elif event.event_type=='created':
			if VERBOSE>=1: print('created',event.src_path)
			try:
				stat = os_stat(event.src_path)
			except FileNotFoundError as e:
				print('error in created event:', type(e), e, event.src_path, event.is_directory, event.is_synthetic)
			else:
				created(event.src_path, stat, event.is_directory, event.is_synthetic, CUR)
		elif event.event_type=='deleted':
			if VERBOSE>=1: print('deleted',event.src_path)
			deleted(event.src_path, event.is_directory, event.is_synthetic, CUR)
		elif event.event_type=='moved':
			if VERBOSE>=1: print('moved',event.src_path,event.dest_path)
			try:
				stat = os_stat(event.dest_path)
			except FileNotFoundError:
				print('error in moved event:', type(e), e, event.src_path, event.dest_path, event.is_directory, event.is_synthetic)
				stat = make_dict(st_mode=None,st_ino=None,st_dev=None,st_nlink=None,st_uid=None,st_gid=None,st_size=None,
					   st_atime=None,st_mtime=None,st_ctime=None,st_blocks=None,st_blksize=None)
			moved(event.src_path, event.dest_path, stat, event.is_directory, event.is_synthetic, CUR)
		else:
			raise Exception(event)

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
	observer = observe(ROOT_DIRS)
	print("All started...", threading.current_thread().name, datetime.fromtimestamp(time()))


	def keyboard_monitor():
		x= ''
		while x!='q':
			try:
				x = input()
			except EOFError:
				x = 'q'
			q.put(x)
	keyboard_thr = threading.Thread(target = keyboard_monitor, args=tuple(), name='keyboard_thr', daemon=True)
	keyboard_thr.start()

	def commit_monitor():
		while True:
			sleep(60)
			q.put('u')
	commit_thr = threading.Thread(target = commit_monitor, args=tuple(), name='commit_thr', daemon=True)
	commit_thr.start()

	try:
		while True:
			event = q.get()
			if isinstance(event,FileSystemEvent):
				event_handler(event)
			elif type(event) is str:
				if event=='q':
					if CON.in_transaction:
						CUR.execute('COMMIT')
					break
				elif event=='u':
					if CON.in_transaction:
						CUR.execute('COMMIT')
						print('COMMIT',datetime.fromtimestamp(time()))
				else:
					import yaml
					try:
						event = yaml.safe_load('['+event+']')
					except ParserError as e:
						print(e)
					else:
						if len(event)==2: event.append({})
						fun, args, kwargs = event
						if CON.in_transaction:
							CUR.execute('COMMIT')
						try:
							if fun=='update': update_owner(*args,**kwargs)
							if fun=='create': update_owner(*args,**kwargs)
							if fun=='delete': delete_owner(*args,**kwargs)
							if fun=='rename': rename_owner(*args,**kwargs)
							if fun=='set': set_owner(*args,**kwargs)
							if fun=='hashes': update_hashes(*args,**kwargs, modify=modify)
							if fun=='help': help_owner(*args,**kwargs)
						except Exception as e:
							print(e)
						if CON.in_transaction:
							CUR.execute('COMMIT')
						print('-----------------------')
			else:
				print('unknown type:',type(event))
			q.task_done()
		print(1,CON)
	finally:
		observer.stop()  # Останавливаем Observer
		observer.join()  # Ждем завершения потока
	print(2,CON)

# --------------------------------
# мониторинговые функции
# --------------------------------

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
def access_mode(st_mode):
	mode = STAT.S_IMODE(st_mode)
	assert mode < 2**9
	s = ''
	for i in range(6,-1,-3):
		s+= 'r' if mode & 2**(i+2) else '-'
		s+= 'w' if mode & 2**(i+1) else '-'
		s+= 'x' if mode & 2**(i+0) else '-'
	return s
def print_fid(cursor, *,strict=True,short=True):
	for parent_id,name,fid,typ,modified in cursor:
		assert 0 <= modified <= 2
		n = CUR.execute('SELECT * FROM cur_stat WHERE id = ?',(fid,)).fetchone()
		if n is None:
			if strict:
				assert 1 <= modified <= 2
			if typ==MFILE:   typ = '-'
			elif typ==MDIR:  typ = 'd'
			elif typ==MLINK: typ = 'l'
			else:
				typ = 'o'
				assert typ==MOTHER
			print(('R' if modified==2 else 'M' if modified==1 else '-')+' '+str(fid),typ,name,sep='\t')
		else:
			(fid1,typ1,
			   st_mode,t_ino,st_dev,st_nlink,st_uid,st_gid,st_size,st_atime,st_mtime,st_ctime,st_blocks,st_blksize,
				data,owner) = n
			assert fid==fid1 and typ==typ1 and simple_type(st_mode)==typ, (fid,fid1,typ,typ1,simple_type(st_mode),typ)

	# fid drwxr-xr-x 3 root root 1785 Jun 29 10:11 Videos hash/link owner

	#    -. Обычный или исполняемый документ
	#    d. Папка.
	#    l. Символьная ссылка.
	#    p. ФИФО.
	#    b. Блочное устройство.
	#    s. Сокет.
	#    c. Символьное устройство.
			typ = typ2str(typ)
			if typ=='o':
				if STAT.S_ISFIFO(st_mode):   typ = 'p'
				elif STAT.S_ISBLK(st_mode):  typ = 'b'
				elif STAT.S_ISSOCK(st_mode): typ = 's'
				elif STAT.S_ISCHR(st_mode):  typ = 'c'
				else:                     typ = '?'
			if owner is not None:
				(owner,) = CUR.execute('SELECT name FROM owners WHERE id = ?',(owner,)).fetchone()
			if short:
				print(('M' if modified==1 else '-')+' '+\
					  str(fid),
					  typ+access_mode(st_mode),
					  datetime.fromtimestamp(st_mtime),
					  name,
					  '<='+owner if owner is not None else '',
					 sep='\t')
			else:
				print(('M' if modified==1 else '-')+' '+\
					  str(fid),
					  typ+access_mode(st_mode),
					  st_nlink,
					  get_username_by_uid(st_uid),
					  get_groupname_by_gid(st_gid),
					  st_size,
					  datetime.fromtimestamp(st_mtime),
					  name,
					  ('->' if typ=='l' else '')+str(data),
					  '<='+owner if owner is not None else '',
					 sep='\t')

		if typ=='d':
			yield fid

def ls(fid=None,*,strict=True,short=True):
	if fid is None:
		fid = os.getcwd()
	with CON:
		if type(fid) is str:
			if not fid.startswith('/'):
				fid = os.getcwd()+'/'+fid
			print(ids := path2ids(fid,CUR))
			fid = ids[-1]
		with closing(CON.execute('SELECT * FROM cur_dirs WHERE parent_id = ?',(fid,))) as cursor:
			list(print_fid(cursor, strict=strict, short=short))
def ls_r(fid=None,*,strict=True,short=True):
	if fid is None:
		fid = os.getcwd()
	with CON:
		if type(fid) is str:
			if not fid.startswith('/'):
				fid = os.getcwd()+'/'+fid
			print(ids := path2ids(fid,CUR))
			fid = ids[-1]
		else:
			print(id2path(fid,CUR),':')
		with closing(print_fid(CON.execute('SELECT * FROM cur_dirs WHERE parent_id = ?',(fid,)), strict=strict)) as cursor:
			for x in list(cursor):
				ls_r(x,strict=strict, short=short)

def list_modified():
	with CON:
		def my_walk(did,path):
			n = CUR.execute('SELECT name,id,type,modified FROM cur_dirs WHERE parent_id = ? AND (modified = 1 OR modified = 2)',(did,)).fetchall()
			for name,fid,ftype,modified in n:
				if ftype==MDIR and modified !=0:
					if modified==1:
						print(path+'/'+name+'/')
					my_walk(fid,path+'/'+name)
				else:
					print(path+'/'+name)
		my_walk(0,'')

def hist_byid(fid):
	print(id2path_hist(fid,CUR))
	with closing(CON.execute('SELECT * FROM cur_dirs WHERE id = ?',(fid,))) as cursor:
		list(print_fid(cursor))
	for (parent_id, name, fid, typ, etyp,
		 st_mode, st_ino, st_dev, st_nlink, st_uid, st_gid, st_size, st_atime, st_mtime, st_ctime, st_blocks, st_blksize,
		data, owner, time, static_found) in \
		CUR.execute('SELECT * FROM hist WHERE id = ? ORDER BY time DESC',(fid,)).fetchall():
			if etyp==ECREAT: etyp = 'C'
			elif etyp==EDEL: etyp = 'D'
			elif etyp==EMOVE: etyp = 'V'
			elif etyp==EMODIF:etyp= 'M'
			else: assert False, etyp
			if etyp=='V':
				print(etyp+' '+('S' if static_found else 'W')+' '+str(datetime.fromtimestamp(time)),
					 id2path_hist(parent_id,CUR)+'/'+name)
			else:
				print(etyp+' '+('S' if static_found else 'W')+' '+str(datetime.fromtimestamp(time)))

def ls_hist(path=None, static_found=None):
	'''
	
	'''
	if path is None:
		for dr in ROOT_DIRS:
			ls_hist(dr, static_found)
		return
	path = os.path.abspath(path)
	(fid,deleted) = path2ids_hist(path,CUR)
	fid = fid[-1]
	if fid is None:
		raise Exception('path does not exist')
	print('cnt\tid ?D\tpath')
	print('--------------------')
	def my_walk(did):
		if static_found:
			cnt = CON.execute('SELECT COUNT(*) AS count FROM hist WHERE id = ? AND static_found = 1 GROUP BY id ',(did,)).fetchone()
		else:
			cnt = CON.execute('SELECT COUNT(*) AS count FROM hist WHERE id = ? GROUP BY id ',(did,)).fetchone()
		if cnt is not None and cnt[0]>0:
			path, deleted = id2path_hist(did,CUR)
			print(cnt[0],str(did)+(' D' if deleted else '  '),path, sep = '\t')
		for (fid,) in CON.execute('SELECT id FROM cur_dirs WHERE parent_id = ?',(did,)).fetchall():
			my_walk(fid)
		for (fid,) in CON.execute('SELECT id FROM deleted WHERE parent_id = ?',(did,)).fetchall():
			my_walk(fid)
	my_walk(fid)

def list_owners(path=None, show_deleted=False):
	# выводить только если owner и owner родителя не совпадают
	# deleted - с пометками
	if path is None:
		for dr in ROOT_DIRS:
			list_owners(dr, show_deleted)
		return
	path = os.path.abspath(path)
	# format: save owner fid deleted path
	(fid,deleted) = path2ids_hist(path,CUR)
	fid = fid[-1]
	if fid is None:
		raise Exception('path does not exist')
	if deleted:
		n = CUR.execute('SELECT owners.save, owners.name, owners.id FROM owners JOIN deleted ON owners.id=deleted.owner WHERE deleted.id = ?',(fid,)).fetchone()
	else:
		n = CUR.execute('SELECT owners.save, owners.name, owners.id FROM owners JOIN cur_stat ON owners.id=cur_stat.owner WHERE cur_stat.id = ?',(fid,)).fetchone()
	if n is not None:
		(save, oname, owner) = n
	else:
		(save, oname, owner) = (True, None, None)
	if show_deleted or not deleted:
		print(save,oname,str(fid)+(' D' if deleted else '  '), path, sep='\t')
	def my_walk(did,deleted,downer):
		if not deleted:
			for (save, oname, owner, fid) in CUR.execute('''SELECT owners.save, owners.name, owners.id, cur_stat.id
					FROM owners JOIN cur_stat ON owners.id=cur_stat.owner JOIN cur_dirs ON cur_dirs.id=cur_stat.id  WHERE cur_dirs.parent_id = ?''',(did,)).fetchall():
				if owner!=downer:
					print(save,oname,str(fid)+'  ', id2path(fid), sep='\t')
				my_walk(fid,False,owner)
		if show_deleted:
			for (save, oname, owner, fid) in CUR.execute('''SELECT owners.save, owners.name, owners.id, deleted.id
					FROM owners JOIN deleted ON owners.id=deleted.owner WHERE deleted.parent_id = ?''',(did,)).fetchall():
				if owner!=downer:
					print(save,oname,str(fid)+' D', id2path_hist(fid)[0], sep='\t')
				my_walk(fid,True,owner)
	my_walk(fid,deleted,owner)

# ------------------------------------
# инициализация приложения/библиотеки
# ------------------------------------

def read_root_dirs():
	'''из базы данных считывает, какие папки отмечены для слежения'''
	with CON:
		root_dirs = []
		def walk(did,path):
			n = CUR.execute('SELECT id, name, modified FROM cur_dirs WHERE parent_id = ?',(did,)).fetchall()
			for (fid, name, modified) in n:
				if modified==2:
					walk(fid, path+'/'+name)
				else:
					root_dirs.append(path+'/'+name)
		walk(0, '')
		return root_dirs

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

def init_connction(files_db, root_dirs = None, nohash = False, ro = True):
	'''инициализирует FILES_DB, ROOT_DIRS; открывает сединение CON, CUR (по умолчанию только для чтения)'''
	global FILES_DB
	global ROOT_DIRS
	global CON
	global CUR

	try:
		CON.cursor().close()
	except Exception as ex:
		pass
	else:
		CON.close()
		#raise Exception('close existing connection before opening new one')

	if root_dirs is None:
		# loading existring db
		if not os.path.isfile(files_db):
			raise Exception(f'database {files_db} does not exist. Create it with root_dirs argument')
		FILES_DB = files_db
		if ro:
			CON = sqlite3.connect('files:'+FILES_DB+'?mode=ro', uri=True)
		else:
			CON = sqlite3.connect(FILES_DB)
		CUR = CON.cursor()
		check_integrity()
		ROOT_DIRS = read_root_dirs()
	else:
		# creation new db
		if os.path.isfile(files_db):
			raise Exception(f'database {files_db} already exist. To open it run this function/file without root_dirs argument')
		if type(root_dirs) is str:
			root_dirs = [root_dirs]
		if root_dirs==['/']:
			root_dirs = get_root_dirs()
		root_dirs = [os.path.abspath(x) for x in root_dirs]
		FILES_DB = files_db
		CON = sqlite3.connect(FILES_DB)
		CUR = CON.cursor()
		ROOT_DIRS = root_dirs
		init_db(nohash)
		check_integrity()
		if ro:
			CON.close()
			CON = sqlite3.connect('files:'+FILES_DB+'?mode=ro', uri=True)

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
	init_connection(sys.argv[1],root_dirs,nohash)
	CON.close()
	watch()




