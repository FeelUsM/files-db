import stat as STAT
import os
import ctypes

STAT_MAX = STAT.S_IFSOCK|STAT.S_IFLNK|STAT.S_IFREG|STAT.S_IFBLK|STAT.S_IFDIR|STAT.S_IFCHR|STAT.S_IFIFO|STAT.S_IFDOOR|STAT.S_IFPORT|STAT.S_IFWHT|\
	STAT.S_ISUID|STAT.S_ISGID|STAT.S_ISVTX|STAT.S_IRWXU|STAT.S_IRUSR|STAT.S_IWUSR|STAT.S_IXUSR|STAT.S_IRWXG|STAT.S_IRGRP|STAT.S_IWGRP|STAT.S_IXGRP|\
	STAT.S_IRWXO|STAT.S_IROTH|STAT.S_IWOTH|STAT.S_IXOTH|STAT.S_ENFMT|STAT.S_IREAD|STAT.S_IWRITE|STAT.S_IEXEC
STAT_DENIED = 1
while STAT_DENIED<=STAT_MAX:
	STAT_DENIED <<=1
print(hex(STAT_DENIED))

def external_path(path : str) -> str:
	'''
	повторное применеие этой функции допустимо
	'''
	if os.name=='nt':
		if path.count(os.sep)==1 and path[0]==os.sep:
			path+=os.sep
		if path[0]==os.sep:
			path = path[1:]
	return path

def make_stat(*,st_mode,st_ino,st_dev,st_nlink,st_uid,st_gid,st_size,st_atime,st_mtime,st_ctime) -> os.stat_result:
	return os.stat_result((st_mode,st_ino,st_dev,st_nlink,st_uid,st_gid,st_size,st_atime,st_mtime,st_ctime))

if os.name=='nt':
	import ctypes.wintypes as wintypes
	UNKNOWN_REPARSE_TAG = 0x10  # reserved value in microsoft
	FILE_ATTRIBUTE_DIRECTORY     = 0x00000010
	FILE_ATTRIBUTE_REPARSE_POINT = 0x00000400
	FILE_ATTRIBUTE_READONLY      = 0x00000001

	IO_REPARSE_TAG_SYMLINK = 0xA000000C
	IO_REPARSE_TAG_MOUNT_POINT = 0xA0000003
	IO_REPARSE_TAG_LX_SYMLINK = 0xA000001D
	IO_REPARSE_TAG_WCI_LINK_1 = 0xa0001027
	IO_REPARSE_TAG_WCI_1 = 0x90001018

	def os_stat(path : str) -> os.stat_result:
		path = external_path(path)
		try:
			stat = os.stat(path,follow_symlinks=False)
			st_file_attributes = stat.st_file_attributes
			st_reparse_tag     = stat.st_reparse_tag
			# todo получение uid, gid
        	# todo ... st_block, st_blksize = ... todo для sumsize
		except Exception:
			print('default os.stat error:',path)
			# получаем winattrs
			MAX_PATH = 260
			class WIN32_FIND_DATAW(ctypes.Structure):
				_fields_ = [
					("dwFileAttributes",   wintypes.DWORD),
					("ftCreationTime",     wintypes.FILETIME),
					("ftLastAccessTime",   wintypes.FILETIME),
					("ftLastWriteTime",    wintypes.FILETIME),
					("nFileSizeHigh",      wintypes.DWORD),
					("nFileSizeLow",       wintypes.DWORD),
					("dwReserved0",        wintypes.DWORD),
					("dwReserved1",        wintypes.DWORD),
					("cFileName",          wintypes.WCHAR * MAX_PATH),
					("cAlternateFileName", wintypes.WCHAR * 14),
					# Устаревшие поля (не присутствуют в winbase.h, но могут быть найдены в некоторых API/обёртках):
					("dwFileType",         wintypes.DWORD),     # Deprecated
					("dwCreatorType",      wintypes.DWORD),     # Deprecated
					("wFinderFlags",       wintypes.WORD),      # Deprecated
				]
			data = WIN32_FIND_DATAW()
			hFind = ctypes.windll.kernel32.FindFirstFileW(path, ctypes.byref(data))
			if hFind == ctypes.c_void_p(-1).value:
				print('cannot find file:',path)
				return make_stat(st_mode=STAT_DENIED,st_ino=0,st_dev=0,st_nlink=0,st_uid=0,st_gid=0,st_size=0,st_atime=0,st_mtime=0,st_ctime=0)
			ctypes.windll.kernel32.FindClose(hFind)
			assert data.cFileName == os.path.basename(path), (path, data.cFileName)

			mode = STAT_DENIED
			if data & FILE_ATTRIBUTE_DIRECTORY:
				mode |= STAT.S_IFDIR | STAT.S_IXUSR | STAT.S_IXGRP | STAT.S_IXOTH
			else:
				mode |= STAT.S_IFREG
			mode |= STAT.S_IRUSR | STAT.S_IRGRP | STAT.S_IROTH
			if not (data & FILE_ATTRIBUTE_READONLY):
				mode |= STAT.S_IWUSR | STAT.S_IWGRP | STAT.S_IWOTH
			m = path.lower()
			if m.endswith('.exe') or m.endswith('.cmd') or m.endswith('.bat') or m.endswith('.com'):
				STAT.S_IXUSR | STAT.S_IXGRP | STAT.S_IXOTH

			# устанавливаем всё кроме uid,gid,dev,ino # ,st_block, st_blksize
			stat = make_stat(
				st_mode    = mode,
				st_ino     = 0,
				st_dev     = 0,
				st_nlink   = 1,
				st_uid     = 0,
				st_gid     = 0,
				st_size    = data.nFileSizeLow + data.nFileSizeHigh* 2**32,
				st_atime   = data.ftLastAccessTime,
				st_mtime   = data.ftLastWriteTime,
				st_ctime   = data.ftCreationTime,
			)
			st_file_attributes = data.dwFileAttributes
			st_reparse_tag     = UNKNOWN_REPARSE_TAG if (data.dwFileAttributes & FILE_ATTRIBUTE_REPARSE_POINT) else 0

		# для IO_REPARSE_TAG_WCI_LINK_1 и IO_REPARSE_TAG_LX_SYMLINK сбросить STAT.S_IFWHT(0xffff) и установить STAT.S_IFLNK
		if st_file_attributes & FILE_ATTRIBUTE_REPARSE_POINT and st_reparse_tag in (IO_REPARSE_TAG_WCI_LINK_1,IO_REPARSE_TAG_LX_SYMLINK):
			mode = stat.st_mode&(STAT.S_IFMT(STAT_MAX)) | STAT.S_IFLNK
			return make_stat(st_mode= mode,
				st_ino=stat.st_ino,st_dev=stat.st_dev,st_nlink=stat.st_nlink,st_uid=stat.st_uid,st_gid=stat.st_gid,st_size=stat.st_size,
				st_atime=stat.st_atime,st_mtime=stat.st_mtime,st_ctime=stat.st_ctime)
		else:
			return stat

	def os_readlink(path: str) -> str:
		try:
			return os.readlink(path)
		except FileNotFoundError:
			raise
		except Exception:
			# получаем reparse_tag
			# для IO_REPARSE_TAG_WCI_LINK_1 и IO_REPARSE_TAG_LX_SYMLINK  - обрабатываем
			# если ошибка - raise
			WIN_OPEN_EXISTING                = 3
			WIN_FILE_SHARE_READ              = 0x00000001
			WIN_FILE_SHARE_WRITE             = 0x00000002
			WIN_FILE_SHARE_DELETE            = 0x00000004
			WIN_FILE_ATTRIBUTE_NORMAL        = 0x00000080
			WIN_FILE_FLAG_BACKUP_SEMANTICS   = 0x02000000
			WIN_FILE_FLAG_OPEN_REPARSE_POINT = 0x00200000

			handle = ctypes.windll.kernel32.CreateFileW( # https://learn.microsoft.com/ru-ru/windows/win32/api/fileapi/nf-fileapi-createfilea
				path,                  # lpFileName            file to open
				0,#GENERIC_READ,          # dwDesiredAccess       open for reading
				0, 
				None,                  # lpSecurityAttributes  default security
				WIN_OPEN_EXISTING,         # dwCreationDisposition existing file only
				WIN_FILE_FLAG_OPEN_REPARSE_POINT|WIN_FILE_FLAG_BACKUP_SEMANTICS, #FILE_FLAG_OPEN_REPARSE_POINT|FILE_FLAG_BACKUP_SEMANTICS,# FILE_ATTRIBUTE_NORMAL, # dwFlagsAndAttributes  normal file                             ????
				None                   # hTemplateFile         no attr. template
			)
			if handle == ctypes.c_void_p(-1).value: # INVALID_HANDLE_VALUE
				raise Exception(f'invalid handle value for {path}')

			class REPARSE_DATA_BUFFER(ctypes.Structure):
				_pack_ = 1
				_fields_ = [
					("ReparseTag", wintypes.ULONG),
					("ReparseDataLength", wintypes.USHORT),
					("Reserved", wintypes.USHORT),
					("GenericReparseBuffer", ctypes.c_byte * 0x3FF0)
				]

			buf = REPARSE_DATA_BUFFER()
			bytes_returned = wintypes.DWORD()

			FSCTL_GET_REPARSE_POINT = 0x000900A8
			success = ctypes.windll.kernel32.DeviceIoControl(
				handle,
				FSCTL_GET_REPARSE_POINT,
				None,
				0,
				ctypes.byref(buf),
				ctypes.sizeof(buf),
				ctypes.byref(bytes_returned),
				None
			)
			if not success:
				error = ctypes.windll.kernel32.GetLastError()
				ctypes.windll.kernel32.CloseHandle(handle)
				if error==6:
					raise Exception(f'ERROR_INVALID_HANDLE for {path}')
				else:
					raise Exception(f'unknown error in DeviceIoControl() for {path}')

			ctypes.windll.kernel32.CloseHandle(handle)

			tag = buf.ReparseTag

			if tag == IO_REPARSE_TAG_LX_SYMLINK:
				#print('tag: IO_REPARSE_TAG_LX_SYMLINK')
				raw_buf = bytearray(buf.GenericReparseBuffer[:buf.ReparseDataLength])
				target = raw_buf[4:].decode("utf-8", errors="replace")
				return target
			elif tag == IO_REPARSE_TAG_SYMLINK:
				raise Exception(f'IO_REPARSE_TAG_SYMLINK should be treated in os.symlink(), {path}')
			elif tag == IO_REPARSE_TAG_MOUNT_POINT:
				raise Exception(f'IO_REPARSE_TAG_MOUNT_POINT should be treated in os.symlink(), {path}')
			elif tag == IO_REPARSE_TAG_WCI_LINK_1: # Used by the Windows Container Isolation filter. Server-side interpretation only, not meaningful over the wire.
				return 'IO_REPARSE_TAG_WCI_LINK_1-'+bytes(buf.GenericReparseBuffer)[:buf.ReparseDataLength].hex()
			elif tag == IO_REPARSE_TAG_WCI_1: # Used by the Windows Container Isolation filter. Server-side interpretation only, not meaningful over the wire.
				raise Exception(f'IO_REPARSE_TAG_WCI_1 is not a symlink or junction: {path}')
			else:
				pprint(bytes(buf.GenericReparseBuffer)[:50])
				raise Exception(f"❓ Unknown reparse tag: {hex(tag)} (len={bytes_returned}) {path}")

else:
	def os_stat(path : str) -> os.stat_result:
		try:
			return os.stat(external_path(path),follow_symlinks=False)
		except Exception:
			return make_stat(st_mode=STAT_DENIED,st_ino=0,st_dev=0,st_nlink=0,st_uid=0,st_gid=0,st_size=0,st_atime=0,st_mtime=0,st_ctime=0)
	def os_readlink(path: str) -> str:
		return os.readlink(path)

def stat_eq(stat : os.stat_result, ostat : os.stat_result) -> bool:
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
	if not STAT.S_ISDIR(stat.st_mode) and stat.st_mtime != ostat.st_mtime:
		#if VERBOSE>=2: print('st_mtime')
		return False
	if os.name!='nt' and stat.st_blocks != ostat.st_blocks: # type: ignore[attr-defined]
		#if VERBOSE>=2: print('st_blocks')
		return False
	if os.name!='nt' and stat.st_blksize != ostat.st_blksize: # type: ignore[attr-defined]
		#if VERBOSE>=2: print('st_blksize')
		return False
	return True

if os.name == 'nt':
	def get_username_by_uid(uid : int) -> str:
		return 'dummy'
	def get_groupname_by_gid(gid : int) -> str:
		return 'dummy'
else:
	import pwd
	import grp
	def get_username_by_uid(uid : int) -> str:
		return pwd.getpwuid(uid).pw_name # type: ignore[attr-defined]
	def get_groupname_by_gid(gid : int) -> str:
		return grp.getgrgid(gid).gr_name # type: ignore[attr-defined]