import ctypes
import ctypes.wintypes as wintypes
import os
import os.path
import stat as STAT
from pprint import pprint

# читает types.json в формате {атрибут:[список путей]} (созданный в os-walk.py)
# ищет среди них тех, кто является reparse-point и пытается их разыменовать
# а кто не reparse-point и не директория - прочитать

def winattrs(path):
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
		raise Exception(f'cannot find {path} {hex(hFind)}')
	assert data.cFileName == os.path.basename(path), (path, data.cFileName)
	return data.dwFileAttributes

def winstat(path):
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
		raise Exception(f'cannot find {path} {hex(hFind)}')
	assert data.cFileName == os.path.basename(path), (path, data.cFileName)

	class AttrDict(dict):
		def __getattr__(self, key):
			if key not in self:
				raise AttributeError(key) # essential for testing by hasattr
			return self[key]
		def __setattr__(self, key, value):
			self[key] = value
	def make_dict(**kwargs):
		return AttrDict(kwargs)	
	mode = 0
	return make_dict(
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
		st_blocks  = 0,
		st_blksize = 0,
	)
	

#	> with what parameters is it better to call CreateFileW() so that it can open as many different files as possible for GetFinalPathNameByHandleW() 
	#> To maximize the ability of CreateFileW() to open various files for use with GetFinalPathNameByHandleW(), you should consider the following parameters:
	#
	#    dwDesiredAccess: Specify access modes that allow both reading and writing, such as GENERIC_READ | GENERIC_WRITE. 
	#       This will enable you to open files for both reading and writing operations.
	#    dwShareMode: Use FILE_SHARE_READ | FILE_SHARE_WRITE to allow other processes to read from and write to the file while it is open. 
	#       This increases the likelihood of successfully opening files that may be in use by other applications.
	#    dwCreationDisposition: Use OPEN_EXISTING to open a file that already exists. This is crucial for accessing existing files rather than creating new ones.
	#    lpSecurityAttributes: If security is a concern, you may specify security attributes that are appropriate for the files you intend to access. 
	#       However, this is optional and depends on your specific needs.
	#
	#By using these parameters, you can enhance the versatility of CreateFileW() in conjunction with GetFinalPathNameByHandleW().
	#
	#References:
	#
	#    Creating and Opening Files https://learn.microsoft.com/en-us/windows/win32/fileio/creating-and-opening-files
	#    Obtaining a File Name From a File Handle https://learn.microsoft.com/en-us/windows/win32/memory/obtaining-a-file-name-from-a-file-handle
	#
	#> what abou dwFlagsAndAttributes parameter? 
	#> For the dwFlagsAndAttributes parameter in CreateFileW(), you should consider using the following flags to enhance compatibility and access to a wide range of files:
	#
	#    FILE_ATTRIBUTE_NORMAL: This flag indicates that the file has no special attributes. It is a good default choice for general file access.
	#    FILE_FLAG_BACKUP_SEMANTICS: This flag allows you to open a directory for backup purposes, which can be useful if you need to access files within a directory.
	#    FILE_FLAG_OVERLAPPED: If you plan to perform asynchronous I/O operations, this flag can be beneficial.
	#    FILE_FLAG_NO_BUFFERING: This flag can be used if you want to bypass the system cache, which may be necessary for certain types of file operations.
	#
	#By combining these flags appropriately, you can increase the likelihood of successfully opening various types of files for use with GetFinalPathNameByHandleW().
	#References:
	#
	#    Creating and Opening Files https://learn.microsoft.com/en-us/windows/win32/fileio/creating-and-opening-files
	#    CreateFile Scenarios https://learn.microsoft.com/en-us/windows/win32/fileio/creating-and-opening-files#createfile-scenariosWIN_GENERIC_READ                 = 0x80000000
WIN_OPEN_EXISTING                = 3
WIN_FILE_SHARE_READ              = 0x00000001
WIN_FILE_SHARE_WRITE             = 0x00000002
WIN_FILE_SHARE_DELETE            = 0x00000004
WIN_FILE_ATTRIBUTE_NORMAL        = 0x00000080
WIN_FILE_FLAG_BACKUP_SEMANTICS   = 0x02000000
WIN_FILE_FLAG_OPEN_REPARSE_POINT = 0x00200000

def GetHandleByPath(path, shares_attrs, open_attrs):
	hFile = ctypes.windll.kernel32.CreateFileW( # https://learn.microsoft.com/ru-ru/windows/win32/api/fileapi/nf-fileapi-createfilea
		path,                  # lpFileName            file to open
		0,#GENERIC_READ,          # dwDesiredAccess       open for reading
		shares_attrs, 
		None,                  # lpSecurityAttributes  default security
		WIN_OPEN_EXISTING,         # dwCreationDisposition existing file only
		open_attrs, #FILE_FLAG_OPEN_REPARSE_POINT|FILE_FLAG_BACKUP_SEMANTICS,# FILE_ATTRIBUTE_NORMAL, # dwFlagsAndAttributes  normal file                             ????
		None                   # hTemplateFile         no attr. template
	)
	if hFile == ctypes.c_void_p(-1).value: # INVALID_HANDLE_VALUE
		raise Exception(f'invalid handle value for'+(f' (attrs={(attrs)}) {path}'if attrs!='' else ''))
	return hFile

def fsctl_get_reparse_point(path,attrs=None):
	if attrs is not None and attrs != '': attrs = hex(int(attrs)) # for debug
	# если attrs==None or attrs!='', ошибка будет содержать f' (attrs={(attrs)}) {path}'


	handle = GetHandleByPath(
		path, 
		0,
		WIN_FILE_FLAG_OPEN_REPARSE_POINT|WIN_FILE_FLAG_BACKUP_SEMANTICS
	)

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
			raise Exception(f'ERROR_INVALID_HANDLE'+(f' (attrs={(attrs)}) {path}'if attrs!='' else ''))
		else:
			raise Exception(f'unknown error in DeviceIoControl() {(error)}'+(f' (attrs={(attrs)}) {path}'if attrs!='' else ''))

	ctypes.windll.kernel32.CloseHandle(handle)

	tag = buf.ReparseTag

	IO_REPARSE_TAG_SYMLINK = 0xA000000C
	IO_REPARSE_TAG_MOUNT_POINT = 0xA0000003
	IO_REPARSE_TAG_LX_SYMLINK = 0xA000001D
	if tag == IO_REPARSE_TAG_LX_SYMLINK:
		#print('tag: IO_REPARSE_TAG_LX_SYMLINK')
		raw_buf = bytearray(buf.GenericReparseBuffer[:buf.ReparseDataLength])
		target = raw_buf[4:].decode("utf-8", errors="replace")
		return target

	elif tag == IO_REPARSE_TAG_SYMLINK:
		#print('tag: IO_REPARSE_TAG_SYMLINK',buf.ReparseDataLength,buf.Reserved)
		class SYMLINK(ctypes.Structure):
			_pack_ = 1
			_fields_ = [
				("SubstituteNameOffset", wintypes.USHORT),
				("SubstituteNameLength", wintypes.USHORT),
				("PrintNameOffset", wintypes.USHORT),
				("PrintNameLength", wintypes.USHORT),
				("Flags", wintypes.ULONG),
				("PathBuffer", ctypes.c_wchar * ((buf.ReparseDataLength-ctypes.sizeof(wintypes.USHORT)*4-ctypes.sizeof(wintypes.ULONG))//ctypes.sizeof(ctypes.c_wchar)))
			]
		slrb = SYMLINK.from_buffer(buf.GenericReparseBuffer)
		offset = slrb.PrintNameOffset // 2
		length = slrb.PrintNameLength // 2
		target = slrb.PathBuffer[offset:offset + length]
		return target

	elif tag == IO_REPARSE_TAG_MOUNT_POINT:
		#print('tag: IO_REPARSE_TAG_MOUNT_POINT')
		class MOUNT(ctypes.Structure):
			_pack_ = 1
			_fields_ = [
				("SubstituteNameOffset", wintypes.USHORT),
				("SubstituteNameLength", wintypes.USHORT),
				("PrintNameOffset", wintypes.USHORT),
				("PrintNameLength", wintypes.USHORT),
				("PathBuffer", ctypes.c_wchar * ((buf.ReparseDataLength-ctypes.sizeof(wintypes.USHORT)*4)//ctypes.sizeof(ctypes.c_wchar)))
			]
		mprb = MOUNT.from_buffer(buf.GenericReparseBuffer)
		offset = mprb.PrintNameOffset // 2
		length = mprb.PrintNameLength // 2
		target = mprb.PathBuffer[offset:offset + length]
		return target

	elif tag == 0xa0001027: # Used by the Windows Container Isolation filter. Server-side interpretation only, not meaningful over the wire.
		return 'IO_REPARSE_TAG_WCI_LINK_1-'+bytes(buf.GenericReparseBuffer)[:buf.ReparseDataLength].hex()

	elif tag == 0x90001018: # Used by the Windows Container Isolation filter. Server-side interpretation only, not meaningful over the wire.
		return None #'IO_REPARSE_TAG_WCI_1-'+bytes(buf.GenericReparseBuffer)[:buf.ReparseDataLength].hex()

	else:
		pprint(bytes(buf.GenericReparseBuffer)[:50])
		raise Exception(f"❓ Unknown reparse tag: {hex(tag)} (len={bytes_returned})"+(f' (attrs={(attrs)}) {path}'if attrs!='' else ''))

def GetFinalPathNameByHandle(path,attrs=None):
	if attrs is not None and attrs != '': attrs = hex(int(attrs)) # for debug
	hFile = GetHandleByPath(
		path, 
		WIN_FILE_SHARE_READ | WIN_FILE_SHARE_WRITE | WIN_FILE_SHARE_DELETE,   
		WIN_FILE_FLAG_BACKUP_SEMANTICS | WIN_FILE_ATTRIBUTE_NORMAL
	)
	# https://learn.microsoft.com/en-us/windows/win32/api/fileapi/nf-fileapi-getfinalpathnamebyhandlew
	FILE_NAME_NORMALIZED = 0 # Тип возвращаемого результата
	VOLUME_NAME_NT = 2
	bufsize = ctypes.windll.kernel32.GetFinalPathNameByHandleW(hFile, None, 0, FILE_NAME_NORMALIZED)
	if bufsize==0:
		error = ctypes.windll.kernel32.GetLastError()
		ctypes.windll.kernel32.CloseHandle(hFile)
		if error==6:
			raise Exception(f'ERROR_INVALID_HANDLE'+(f' (attrs={(attrs)}) {path}'if attrs!='' else ''))
		else:
			raise Exception(f'unknown error in GetFinalPathNameByHandleW() {(error)}'+(f' (attrs={(attrs)}) {path}'if attrs!='' else ''))
	buf = ctypes.create_unicode_buffer(bufsize+5)
	bufsize2 = ctypes.windll.kernel32.GetFinalPathNameByHandleW(hFile, buf, bufsize, FILE_NAME_NORMALIZED)
	if bufsize2==0:
		error = ctypes.windll.kernel32.GetLastError()
		ctypes.windll.kernel32.CloseHandle(hFile)
		raise Exception(f'unknown strange error in GetFinalPathNameByHandleW() {(error)}'+(f' (attrs={(attrs)}) {path}'if attrs!='' else ''))
	if bufsize2>bufsize:
		ctypes.windll.kernel32.CloseHandle(hFile)
		raise Exception(f'not anough buffer size {bufsize}->{bufsize2}'+(f' (attrs={(attrs)}) {path}'if attrs!='' else ''))
	path_str = buf.value
	if path_str.startswith("\\\\?\\"):
		path_str = path_str[4:]
	ctypes.windll.kernel32.CloseHandle(hFile)
	return path_str

def readlink(path,attrs=None):
	'''
	в случае если результат GetFinalPathNameByHandle совпадает с исходным путём - это всё-таки не симлинк
	в этом случае результат None
	'''
	if attrs is not None and attrs != '': attrs = hex(int(attrs)) # for debug
	#try:                   l1 = GetFinalPathNameByHandle(path,''); e1 = None
	#except Exception as e: l1 = None;                              e1 = e
	try:                   l2 = fsctl_get_reparse_point(path,'');  e2 = None
	except Exception as e: l2 = None;                              e2 = e
	try:                   l3 = os.readlink(path);                 e3 = None
	except Exception as e: l3 = None;                              e3 = e
	if e2 and e3: # and e1 
		raise Exception(f"fsctl_get_reparse_point:{type(e2)}{e2} | os.readlink:{type(e3)}{e3}"+f' (attrs={(attrs)}) {path}'if attrs!='' else '') # GetFinalPathNameByHandle:{type(e1)}{e1} | 
	#if l1 is not None and l2 is not None and l1!=l2:
		#print('different GetFinalPathNameByHandle and fsctl_get_reparse_point',l1,l2)
	if l2 is not None and l3 is not None and l2!=l3:
		print('different fsctl_get_reparse_point and os.readlink',l2,l3)
	#if l3 is not None and l1 is not None and l3!=l1:
		#print('different os.readlink and GetFinalPathNameByHandle',l3,l1)
	#if l1 is not None and l1 == path:
		#return None
	return l3 if l3 is not None else l2 # if l2 is not None else l1

def printlink(path,attrs=None):
	#try:                   print('GetFinalPathNameByHandle:',GetFinalPathNameByHandle(path))
	#except Exception as e: print('GetFinalPathNameByHandle error:',type(e),e)
	#try:                   print('fsctl_get_reparse_point:',fsctl_get_reparse_point(path))
	#except Exception as e: print('fsctl_get_reparse_point error:',type(e),e)
	#try:                   print('os.readlink:',os.readlink(path))
	#except Exception as e: print('os.readlink error:',type(e),e)
	print('--->',readlink(path,attrs))

if 0:
	path = \
	r'C:\swapfile.sys'
	#r'C:\hiberfil.sys'
	#r'C:\DumpStack.log.tmp'
	#r'C:\pagefile.sys'
	pprint(os.stat(path))
	print(hex(winattrs(path)))
	print(path,'->')
	#pprint(GetFinalPathNameByHandle(path))
	#pprint(fsctl_get_reparse_point(path))
	pprint(os.readlink(path))

	#for p,d,f in os.walk(r'C:\ProgramData\Microsoft\Windows\Containers\BaseImages\f56cb5fe-ed3d-4a79-bca5-42ede6dd16be\BaseLayer\Files\Windows\Media'):
	#	print(p)

	exit()


attr_consts = {
1 : 'FILE_ATTRIBUTE_READONLY',
2 : 'FILE_ATTRIBUTE_HIDDEN',
4 : 'FILE_ATTRIBUTE_SYSTEM',
16 : 'FILE_ATTRIBUTE_DIRECTORY',
32 : 'FILE_ATTRIBUTE_ARCHIVE',
64 : 'FILE_ATTRIBUTE_DEVICE',
128 : 'FILE_ATTRIBUTE_NORMAL',
256 : 'FILE_ATTRIBUTE_TEMPORARY',
512 : 'FILE_ATTRIBUTE_SPARSE_FILE',
1024 : 'FILE_ATTRIBUTE_REPARSE_POINT',
2048 : 'FILE_ATTRIBUTE_COMPRESSED',
4096 : 'FILE_ATTRIBUTE_OFFLINE',
8192 : 'FILE_ATTRIBUTE_NOT_CONTENT_INDEXED',
16384 : 'FILE_ATTRIBUTE_ENCRYPTED',
32768 : 'FILE_ATTRIBUTE_INTEGRITY_STREAM',
65536 : 'FILE_ATTRIBUTE_VIRTUAL',
131072 : 'FILE_ATTRIBUTE_NO_SCRUB_DATA',
#262144 : 'FILE_ATTRIBUTE_EA',
262144 : 'FILE_ATTRIBUTE_RECALL_ON_OPEN',
524288 : 'FILE_ATTRIBUTE_PINNED',
1048576 : 'FILE_ATTRIBUTE_UNPINNED',

4194304 : 'FILE_ATTRIBUTE_RECALL_ON_DATA_ACCESS',
}

# is_reparse_point STAT.S_ISLNK(cygwin if can't stat) issymlink isjunktion |
#    GetFinalPathNameByHandle(with WIN_FILE_FLAG_OPEN_REPARSE_POINT|WIN_FILE_FLAG_BACKUP_SEMANTICS/with WIN_FILE_FLAG_BACKUP_SEMANTICS | WIN_FILE_ATTRIBUTE_NORMAL)
#	fsctl_get_reparse_point os.symlink | dir /a
excluded_oattrs = [
		# True cygwin False False | same_path/ERROR_INVALID_HANDLE IO_REPARSE_TAG_WCI_LINK_1 not a symbolic link | [...]
	0x1621,0x1625,0x1627,0x1623, 0x3625, 0x3620, 0x1620, 
		# True False  False False | same_path                      IO_REPARSE_TAG_WCI_1      not a symbolic link | <DIR>
	0x415, 0x413, 0x2411, 0x2414, 0x414, 0x412, 0x2412, 0x2412, 0x411, 0x2410, 0x410, 0x2416, 0x416, 
		# True cygwin False False | same_path/ERROR_INVALID_HANDLE IO_REPARSE_TAG_LX_SYMLINK not a symbolic link | <JUNCTION>
	#0x420,
		# True cygwin True  False | same_path/ERROR_INVALID_HANDLE IO_REPARSE_TAG_SYMLINK	 real_link           | <SYMLINK>
	#0x420, # C:\msys64\home\FeelUs\_cxx
		# True cygwin False False | ERROR_INVALID_HANDLE           ERROR_INVALID_HANDLE      Отказано в доступе  | Файл не найден
	0x416, # C:\ProgramData\Microsoft\Windows\Containers\BaseImages\f56cb5fe-ed3d-4a79-bca5-42ede6dd16be\BaseLayer\Files\System Volume Information
		# not found
	0x2021, # C:\ProgramData\Safing\Portmaster\logs\notifier\2025-06-24-08-06-50.error.log
		# занят процессом, не симлинк
	-1
	]

def get_popen(*args):
	import subprocess

	# Run a command and capture its output
	process = subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
	stdout, stderr = process.communicate()
	stdout = stdout.decode(encoding='cp866')
	stderr = stderr.decode(encoding='cp866')
	return stdout + ('\n'+stderr if len(stderr)>0 else '')

def print_path(path):
	print('-------')
	print(path)
	dirout = get_popen('cmd','/c','dir','/a',path).split('\n')
	assert dirout[0].find('Том в устройстве C имеет метку SysGate')>=0, dirout
	assert dirout[1].find('Серийный номер тома: B49A-BE0C')>=0, dirout
	assert dirout[3].find('Содержимое папки')>=0, dirout
	print(dirout[5])

import json
with open('types.txt', 'r') as f:
	data = json.load(f)
	for oattrs,li in data.items():
		if int(oattrs) in excluded_oattrs:
			continue
		flag = False
		for path in li:

			try:
				stat = os.stat(path)
			except FileNotFoundError:
				flag = True
				print()
				print_path(path)
				print('not found')
			except OSError as e:
				attrs = winattrs(path)
				b1 = bool(attrs & FILE_ATTRIBUTE_REPARSE_POINT)

				b3 = os.path.islink(path)
				b4 = os.path.isjunction(path)
				if b1 or b3 or b4:
					flag = True
					print()
					print_path(path)
					#print(b1,'cygwin',b3,b4)
					#print(hex(int(oattrs))+'!=!=!=' if int(oattrs)!=attrs else '',hex(attrs),'stat():',e)
					try:
						printlink(path,attrs)
						#print('->',link)
					except Exception as e:
						print(e)
				else:
					flag = True
					print()
					print_path(path)
					print(hex(int(oattrs))+'!=!=!=' if int(oattrs)!=attrs else '',hex(attrs),e)

			else:

				attrs = winattrs(path)
				#print(hex(attrs))
				FILE_ATTRIBUTE_REPARSE_POINT = 0x400
				b1 = bool(attrs & FILE_ATTRIBUTE_REPARSE_POINT)

				stat = os.stat(path,follow_symlinks=False)
				#print(stat)
				#print(hex(stat.st_mode))
				b2 = STAT.S_ISLNK(stat.st_mode)

				b3 = os.path.islink(path)
				b4 = os.path.isjunction(path)
				if b1 or b2 or b3 or b4:
					if b3 or b4:
						#print()
						#print_path(path)
						#print(b1,b2,b3,b4)
						#print(hex(int(oattrs))+'!=!=!=' if int(oattrs)!=attrs else '',hex(attrs),stat)
						os.readlink(path)
					else:
						flag = True
						print()
						print_path(path)
						#print(b1,b2,b3,b4)
						#print(hex(int(oattrs))+'!=!=!=' if int(oattrs)!=attrs else '',hex(attrs),stat)
						try:
							printlink(path,attrs)
							#print('->',link)
						except Exception as e:
							print(e)
				else:
					try:
						if not os.path.isdir(path):
							with open(path,'rb') as lf:
								lf.read(100)
						else:
							os.listdir(path)
					except PermissionError:
						pass
					except Exception as e:
						flag = True
						print(type(e),e)
		lattrs = int(oattrs)
		for m,n in attr_consts.items():
			lattrs &= ~m
		if lattrs:
			flag = True
			print('unknown bits:',hex(lattrs))
			pprint(li)
		if flag:
			for m,n in attr_consts.items():
				if m & int(oattrs):
					print(n)
			print('-----------------------',hex(int(oattrs)),'-----------------------')
