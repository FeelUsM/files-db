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
	path = path.split('/')

	#print(ids,fid,path)
	for name in path[len(ids):-1]:
		cursor.execute('INSERT INTO cur_dirs (parent_id, name, modified, type) VALUES (?, ?, 2, ?)',(fid, name, MDIR))
		(fid,) = cursor.execute('SELECT id FROM cur_dirs WHERE parent_id =? AND name=?',(fid,name)).fetchone()
	cursor.execute('INSERT INTO cur_dirs (parent_id, name, modified, type) VALUES (?, ?, 0, ?)',(fid, path[-1], MDIR))
	(fid,) = cursor.execute('SELECT id FROM cur_dirs WHERE parent_id =? AND name=?',(fid,path[-1])).fetchone()
	return fid

def init_cur_dirs(root_dirs):
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
				CUR.executemany('INSERT INTO cur_dirs (parent_id, name, modified, type) VALUES (?, ?, 0, ?)', [(pathids[-1], x, MDIR) for x in dirs])
				CUR.executemany('INSERT INTO cur_dirs (parent_id, name, modified, type) VALUES (?, ?, 0, ?)', [(pathids[-1], x, MFILE) for x in files])

def init_stat():
	'''
	делает stat для всех объектов и заполняет cur_stat
	'''
	with CON:
		ids = CUR.execute('SELECT id FROM cur_dirs WHERE modified !=2').fetchall()
		cnt = 0
		print('stat files:')
		for (fid,) in tqdm(ids):
			path = None
			try:
				path = id2path(fid,CUR)
				stat = os.stat(path,follow_symlinks=False)
				if not is_link(stat.st_mode) and not is_dir(stat.st_mode) and not is_file(stat.st_mode) and not is_other(stat.st_mode):
					raise Exception('unknown type')

				CUR.execute('UPDATE cur_dirs SET type = ? WHERE id = ?',(simple_type(stat.st_mode),fid))
				CUR.execute('INSERT INTO cur_stat (id,type) VALUES (?,?)', (fid,simple_type(stat.st_mode)))
				update_stat(fid,stat,CUR)
				cnt += 1
				if cnt%1000000==0:
					CUR.execute('COMMIT')
			except FileNotFoundError as e:
				if VERBOSE>=2: print(e)
				set_modified(fid, CUR)
			except Exception as e:
				print(fid,path,type(e),e)

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
						(ohash) = cursor.execute('SELECT data FROM cur_stat WHERE id = ?',(fid,)).fetchone()
						if ohash is not None and ohash!=hsh:
							modify(fid, os.stat(path), True, cursor)
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
						(lhsh,) = cursor.execute('SELECT data FROM cur_stat WHERE id = ?',(fid,)).fetchone()
					elif ftype==MLINK:
						try:
							lnk = os.readlink(id2path(fid,cursor))
							if modify is not None:
								(olink,) = cursor.execute('SELECT data FROM cur_stat WHERE id = ?',(fid,)).fetchone()
								if olink is not None and olink!=lnk:
									modify(fid, os.stat(id2path(fid)), True, cursor)
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
	init_cur_dirs(ROOT_DIRS)
	init_stat()
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
						cstat = os.stat(cpath)
					except FileNotFoundError:
						print(cpath,"found new item but can't stat it")
						continue
					fid = create(did, name, cstat, True, CUR)
					walk_stat(with_all, fid, cpath, simple_type(cstat.st_mode), 3)
			if modified!=3:
				try:
					stat = os.stat(path)
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
		lstat = os.stat(parent_path, follow_symlinks=False) # FileNotFoundError будет пойман в области watchdog-а
		assert simple_type(lstat.st_mode)==MDIR, simple_type(lstat.st_mode)
		fid = create(fid, name, lstat, True, cursor, owner, save)

	return fid, path[-1], owner, save

def created1(ids, src_path, stat, is_directory, cursor):
	if VERBOSE>=2: print('created1',ids, src_path, stat, is_directory, cursor)
	(fid, name, owner, save) = create_parents(src_path,cursor,ids)
	create(fid, name, stat, False, cursor, owner, save)

def moved(fid, dest_path, cursor):
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
		return created1(ids, src_path, stat, is_directory,cursor)
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
	return created1(ids, src_path, stat, is_directory,cursor)

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
		return created1(ids, dest_path, stat, is_directory,cursor)
	moved1(ids[-1],dest_path, cursor)

# --------------------------------
# мониторинговые и интерфейсные функции
# --------------------------------

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
			pass
		elif event.event_type=='opened':
			pass
		elif event.event_type=='modified' or event.event_type=='closed':
			try:
				stat = os.stat(event.src_path,follow_symlinks=False)
				modified(event.src_path, stat, event.is_directory, event.is_synthetic, CUR)
			except FileNotFoundError as e:
				print('error in modified event:', e, event.src_path, event.is_directory, event.is_synthetic)
			
		elif event.event_type=='created':
			try:
				stat = os.stat(event.src_path,follow_symlinks=False)
				created(event.src_path, stat, event.is_directory, event.is_synthetic, CUR)
			except FileNotFoundError as e:
				print('error in created event:', e, event.src_path, event.is_directory, event.is_synthetic)
		elif event.event_type=='deleted':
			deleted(event.src_path, event.is_directory, event.is_synthetic, CUR)
		elif event.event_type=='moved':
			try:
				stat = os.stat(event.dest_path,follow_symlinks=False)
			except FileNotFoundError:
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
					break
				elif event=='u':
					if CON.in_transaction:
						CUR.execute('COMMIT')
						print('COMMIT',datetime.fromtimestamp(time()))
				else:
					print(222,event)
			else:
				print(type(event))
			q.task_done()
		print(1,CON)
	finally:
		observer.stop()  # Останавливаем Observer
		observer.join()  # Ждем завершения потока
	print(2,CON)


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
			CON = sqlite3.connect('files:'+FILES_DB+'?mode=ro')
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
			CON = sqlite3.connect('files:'+FILES_DB+'?mode=ro')

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




