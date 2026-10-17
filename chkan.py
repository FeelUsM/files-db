#!/usr/bin/env python3
"""chkan.py: снимки файловой системы и обратимые diff-ы между ними.

Запуск из папки, в которой лежит (или будет создана) ./.changes/:

	python3 chkan.py [-y] [-w] [папка]

root = папка, в которой лежит .changes/TAGS.txt (по умолчанию текущая).
Файлы в .changes/:
	TAGS.txt                           теги, исключения и команды @run
	scan_YYYYMMDD-HHMMSS.json          слепки (остаются первый и последний)
	diff_<от>_<до>.json                diff-ы между соседними слепками
"""

import argparse
import ctypes
import datetime
import errno
import glob
import json
import os
import re
import stat
import subprocess
import sys
import time
from collections import namedtuple

CHANGES_DIR = ".changes"
TAGS_FILE = "TAGS.txt"
YES_ANSWERS = ("y", "yes", "д", "да")
FORMAT = 4
LOG_FILE = "chkan-log.txt"
BACKUP_DIR = "format3"  # сюда уходят оригиналы при переходе с формата 3
TIME_FIELDS = ("mtime", "ctime", "btime")
ROOT_KEY = "."
NO_TAGS_KEY = "-"
SEPARATOR_KEY = "-" * 10
INLINE_LIMIT = 200

FIELD_ORDER = ("type", "dev", "ino", "size", "total", "mtime", "ctime", "btime", "uid", "gid", "mode", "target")
OPTIONAL_FIELDS = ("total", "target")  # есть не у всех объектов; null в set значит «поля нет»
CHANGE_OPS = ("change", "change_d")  # change_d: то же, что change, но для папок
INHERITED = ("dev", "uid", "gid")

TAG_RE = re.compile(r"^[\w.]+$")
TAG_TOKEN_RE = re.compile(r"^(?P<name>[\w.]+)(?P<ignore>\(ignore\))?$")
NO_RE = re.compile(r"^@no\((?P<name>[^()\s]*)\)$")
HASH_RE = re.compile(r"(^|\s)#")
COMMENT_RE = re.compile(r"(^|\s)//")  # комментарий: // в начале строки или после пробела (http://... не трогаем)
RUN_START = "@run("
RUN_NEXT_RE = re.compile(r"\)[ \t]*@run\(")
SCAN_RE = re.compile(r"^scan_(\d{8}-\d{6})\.json$")
DIFF_RE = re.compile(r"^diff_(\d{8}-\d{6})_(\d{8}-\d{6})\.json$")
TIME_RE = re.compile(r"^(?:(?:(?:(\d{4})-)?(\d{2})-)?(\d{2}) )?(\d{2}):(\d{2}):(\d{2})$")

TAGS_TEMPLATE = """\
// root = папка, в которой лежит .changes/
// Строка: путь<TAB># тег тег(ignore) @no(тег) @exclude @run(команда) // комментарий
// Вложенность задаётся табами. Спец-теги начинаются с @:
//   @exclude    не сканировать
//   @no(тег)    снять тег с этого пути и всего поддерева
//   @run(bash)  выполнить в конце; текущая папка = путь строки (без пути: root);
//               в окружении CHKAN_TAGS, CHKAN_DIFF, CHKAN_SCAN. @run пишите последним в строке.
// Пример:
//   .cache/	# @exclude
//   .changes/	# @exclude  (по умолчанию .changes сканируется как обычная папка)
//   projects/*/src	# work  (wildcard-ы: * ? [...] {a,b}; посмотреть во что раскрылись: chkan.py wildcard)
//   projects/	# work
//   	old/	# @no(work)
//   # noisy(ignore)	// строка без пути только помечает тег игнорируемым
//   # @run(echo $CHKAN_DIFF)
"""


_progress_disabled = False  # --no-progress


def msg(text="", end="\n"):
	"""Сообщение пользователю: всегда в stderr (в stdout только статистика)."""
	print(text, end=end, file=sys.stderr, flush=True)


def ask(prompt):
	"""Вопрос в stderr; пустой ответ и конец ввода считаются «нет»."""
	msg(prompt, end="")
	try:
		return input().strip().lower() in YES_ANSWERS
	except EOFError:
		return False


class ChkanError(Exception):
	pass


class DiffError(ChkanError):
	pass


# ---------------------------------------------------------------- пути


def join_path(parent, name):
	return name if parent == ROOT_KEY else parent + "/" + name


def is_under(path, prefix):
	return path == prefix or path.startswith(prefix + "/")


def make_excluder(prefixes):
	"""Функция path -> bool: лежит ли путь внутри одного из исключённых."""
	prefix_set = set(prefixes)

	def under(path):
		if not prefix_set:
			return False
		cur = path
		while True:
			if cur in prefix_set:
				return True
			i = cur.rfind("/")
			if i < 0:
				return False
			cur = cur[:i]

	return under


def canonical_exclude(paths):
	result = []
	for p in sorted(set(paths), key=lambda s: s.split("/")):
		if any(is_under(p, r) for r in result):
			continue
		result.append(p)
	return result


# ---------------------------------------------------------------- TAGS.txt


class TagNode:
	__slots__ = ("ops", "children")

	def __init__(self):
		self.ops = []
		self.children = {}


State = namedtuple("State", "node tags excluded")


def _apply_ops(node, tags, excluded):
	for kind, name in node.ops:
		if kind == "add":
			tags = tags | {name}
		elif kind == "cancel":
			tags = tags - {name}
		else:
			excluded = True
	return tags, excluded


class Resolver:
	"""Быстро считает теги путей, кеширует состояния папок."""

	def __init__(self, rules):
		self.rules = rules
		self.dirs = {ROOT_KEY: rules.root_state()}

	def _dir_state(self, path):
		st = self.dirs.get(path)
		if st is None:
			parent, _, name = path.rpartition("/")
			st = self.rules.child_state(self._dir_state(parent or ROOT_KEY), name)
			self.dirs[path] = st
		return st

	def tags(self, path):
		if path == ROOT_KEY:
			return self.dirs[ROOT_KEY].tags
		parent, _, name = path.rpartition("/")
		return self.rules.child_state(self._dir_state(parent or ROOT_KEY), name).tags


class Rules:
	def __init__(self, trie, ignore_tags, exclude, runs=(), wildcards=()):
		self.trie = trie
		self.ignore_tags = frozenset(ignore_tags)
		self.exclude = list(exclude)
		self.runs = list(runs)  # [(путь относительно root или '.', команда)] в порядке файла
		self.wildcards = list(wildcards)  # [(номер строки, полный путь с wildcard-ом, [во что раскрылся])]

	def root_state(self):
		tags, excluded = _apply_ops(self.trie, frozenset(), False)
		return State(self.trie, tags, excluded)

	def child_state(self, state, name):
		node = state.node.children.get(name) if state.node is not None else None
		if node is None:
			return State(None, state.tags, state.excluded)
		tags, excluded = _apply_ops(node, state.tags, state.excluded)
		return State(node, tags, excluded)

	def state_of(self, path):
		st = self.root_state()
		if path != ROOT_KEY:
			for seg in path.split("/"):
				st = self.child_state(st, seg)
		return st

	def tags_of(self, path):
		return self.state_of(path).tags

	def resolver(self):
		return Resolver(self)


def parse_tag_token(token, where):
	"""Один тег без пробелов -> (вид, значение, ignore): add / cancel / exclude."""
	if token == "@exclude":
		return "exclude", None, False
	if token.startswith("@no("):
		m = NO_RE.match(token)
		if not m:
			raise ChkanError(f"{where}: неверный {token!r}, ожидалось @no(тег)")
		name = m.group("name")
		if name.startswith("@"):
			raise ChkanError(f"{where}: спец-теги отменять нельзя: {token!r}")
		if not TAG_RE.match(name):
			raise ChkanError(f"{where}: недопустимое имя тега в {token!r} (буквы, цифры, _ и .)")
		return "cancel", name, False
	if token.startswith("@"):
		raise ChkanError(f"{where}: неизвестный спец-тег {token!r}")
	m = TAG_TOKEN_RE.match(token)
	if not m:
		raise ChkanError(f"{where}: недопустимый тег {token!r} (буквы, цифры, _ и .; допустима пометка (ignore))")
	return "add", m.group("name"), bool(m.group("ignore"))


def parse_tag_items(text, where):
	"""Часть строки после '#': теги, @exclude, @no(тег), @run(команда).

	Команда @run идёт до первой `)  @run(` (следующая команда) или до последней `)` в строке."""
	items = []
	i, n = 0, len(text)
	while i < n:
		if text[i].isspace():
			i += 1
			continue
		if text.startswith(RUN_START, i):
			start = i + len(RUN_START)
			m = RUN_NEXT_RE.search(text, start)
			if m:
				end, i = m.start(), m.end() - len(RUN_START)
			else:
				end = text.rfind(")", start)
				if end < 0:
					raise ChkanError(f"{where}: у @run нет закрывающей )")
				i = end + 1
			command = text[start:end].strip()
			if not command:
				raise ChkanError(f"{where}: пустая команда в @run")
			items.append(("run", command, False))
			continue
		j = i
		while j < n and not text[j].isspace():
			j += 1
		items.append(parse_tag_token(text[i:j], where))
		i = j
	return items


BRACE_RE = re.compile(r"\{([^{}]*)\}")


def expand_braces(text):
	"""a/{x,y}/f -> a/x/f, a/y/f (модуль glob фигурные скобки не знает)."""
	m = BRACE_RE.search(text)
	if not m:
		return [text]
	out = []
	for alt in m.group(1).split(","):
		out.extend(expand_braces(text[: m.start()] + alt + text[m.end():]))
	return list(dict.fromkeys(out))  # вложенные скобки раскрываются изнутри и дают повторы


def is_wildcard(path):
	return glob.has_magic(path) or bool(BRACE_RE.search(path))


def expand_path(path_text, parents, root, where):
	"""Путь из строки TAGS.txt -> список полных путей (кортежей имён) под каждым из родителей."""
	variants = expand_braces(path_text)
	result, seen = [], set()
	for parent in parents:
		for variant in variants:
			segs = [seg for seg in variant.split("/") if seg not in ("", ".")]
			if ".." in segs:
				raise ChkanError(f"{where}: .. в путях запрещено")
			if glob.has_magic("/".join(segs)):
				pattern = "/".join([glob.escape(name) for name in parent] + segs)
				found = [tuple(m.split("/")) for m in sorted(glob.glob(pattern, root_dir=root))]
			else:
				found = [parent + tuple(segs)]
			for path in found:
				if path not in seen:
					seen.add(path)
					result.append(path)
	return result


def parse_tags(text, root=".", strict=True):
	"""strict=False: wildcard без совпадений не ошибка (нужно команде wildcard, чтобы это показать)."""
	trie = TagNode()
	ignore_tags = set()
	exclude = []
	runs = []
	wildcards = []
	stack = []  # на каждый уровень вложенности: (полный путь как написан, [раскрытые пути])
	for lineno, raw in enumerate(text.splitlines(), 1):
		cm = COMMENT_RE.search(raw)
		line = (raw[: cm.start()] if cm else raw).rstrip()
		if not line.strip():
			continue
		where = f"{TAGS_FILE}:{lineno}"
		indent = len(line) - len(line.lstrip("\t"))
		body = line[indent:]
		if body[:1].isspace():
			raise ChkanError(f"{where}: отступы только табами")
		m = HASH_RE.search(body)
		if m:
			path_part, tags_part = body[: m.start()].strip(), body[m.end():]
		else:
			path_part, tags_part = body.strip(), ""
		parsed = parse_tag_items(tags_part, where)
		for kind, value, ignore in parsed:
			if ignore:
				ignore_tags.add(value)
		if not path_part:
			runs.extend((ROOT_KEY, value) for kind, value, _ in parsed if kind == "run")
			continue
		if path_part.startswith("/"):
			raise ChkanError(f"{where}: пути задаются относительно root, без начального /")
		if indent > len(stack):
			raise ChkanError(f"{where}: слишком глубокий отступ")
		del stack[indent:]
		parent_text, parents = stack[-1] if stack else ("", [()])
		own_text = "/".join(seg for seg in path_part.split("/") if seg not in ("", "."))
		full_text = "/".join(x for x in (parent_text, own_text) if x)
		paths = expand_path(path_part, parents, root, where)
		if not paths and strict and parents:
			raise ChkanError(f"{where}: по шаблону {full_text!r} ничего не найдено")
		stack.append((full_text, paths))
		if is_wildcard(full_text):
			wildcards.append((lineno, full_text, ["/".join(p) for p in paths]))
		for path in paths:
			node = trie
			for seg in path:
				node = node.children.setdefault(seg, TagNode())
			for kind, value, _ignore in parsed:
				if kind in ("add", "cancel", "exclude"):
					node.ops.append((kind, value))
				if kind == "exclude":
					if not path:
						raise ChkanError(f"{where}: нельзя исключить root")
					exclude.append("/".join(path))
				elif kind == "run":
					runs.append(("/".join(path) or ROOT_KEY, value))
	return Rules(trie, ignore_tags, canonical_exclude(exclude), runs, wildcards)


# ---------------------------------------------------------------- stat


class _Timestamp(ctypes.Structure):
	_fields_ = [("tv_sec", ctypes.c_int64), ("tv_nsec", ctypes.c_uint32), ("_pad", ctypes.c_int32)]


class _Statx(ctypes.Structure):
	_fields_ = [
		("stx_mask", ctypes.c_uint32),
		("stx_blksize", ctypes.c_uint32),
		("stx_attributes", ctypes.c_uint64),
		("stx_nlink", ctypes.c_uint32),
		("stx_uid", ctypes.c_uint32),
		("stx_gid", ctypes.c_uint32),
		("stx_mode", ctypes.c_uint16),
		("_pad0", ctypes.c_uint16),
		("stx_ino", ctypes.c_uint64),
		("stx_size", ctypes.c_uint64),
		("stx_blocks", ctypes.c_uint64),
		("stx_attributes_mask", ctypes.c_uint64),
		("stx_atime", _Timestamp),
		("stx_btime", _Timestamp),
		("stx_ctime", _Timestamp),
		("stx_mtime", _Timestamp),
		("stx_rdev_major", ctypes.c_uint32),
		("stx_rdev_minor", ctypes.c_uint32),
		("stx_dev_major", ctypes.c_uint32),
		("stx_dev_minor", ctypes.c_uint32),
		("stx_mnt_id", ctypes.c_uint64),
		("stx_dio_mem_align", ctypes.c_uint32),
		("stx_dio_offset_align", ctypes.c_uint32),
		("_spare", ctypes.c_uint64 * 12),
	]


AT_FDCWD = -100
AT_SYMLINK_NOFOLLOW = 0x100
STATX_BASIC_STATS = 0x7FF
STATX_BTIME = 0x800


def _init_statx():
	if not sys.platform.startswith("linux"):
		return None
	try:
		fn = ctypes.CDLL(None, use_errno=True).statx
	except (OSError, AttributeError):
		return None
	fn.argtypes = [ctypes.c_int, ctypes.c_char_p, ctypes.c_int, ctypes.c_uint, ctypes.POINTER(_Statx)]
	fn.restype = ctypes.c_int
	return fn


_statx_fn = _init_statx()


def _type_char(mode):
	if stat.S_ISDIR(mode):
		return "d"
	if stat.S_ISREG(mode):
		return "f"
	if stat.S_ISLNK(mode):
		return "l"
	return "o"


def _make_record(mode, dev, ino, size, mtime, ctime, btime, uid, gid, path):
	rec = {
		"type": _type_char(mode),
		"dev": dev,
		"ino": ino,
		"size": size,
		"mtime": mtime,
		"ctime": ctime,
		"btime": btime,
		"uid": uid,
		"gid": gid,
		"mode": "%04o" % (mode & 0o7777),
	}
	if rec["type"] == "l":
		rec["target"] = os.readlink(path)
	return rec


def _statx_record(path):
	buf = _Statx()
	rc = _statx_fn(AT_FDCWD, os.fsencode(path), AT_SYMLINK_NOFOLLOW, STATX_BASIC_STATS | STATX_BTIME, ctypes.byref(buf))
	if rc != 0:
		err = ctypes.get_errno()
		raise OSError(err, os.strerror(err), path)
	btime = None
	if buf.stx_mask & STATX_BTIME:
		btime = buf.stx_btime.tv_sec
	return _make_record(
		buf.stx_mode,
		os.makedev(buf.stx_dev_major, buf.stx_dev_minor),
		buf.stx_ino,
		buf.stx_size,
		buf.stx_mtime.tv_sec,
		buf.stx_ctime.tv_sec,
		btime,
		buf.stx_uid,
		buf.stx_gid,
		path,
	)


def _lstat_record(path):
	st = os.lstat(path)
	return _make_record(st.st_mode, st.st_dev, st.st_ino, st.st_size, st.st_mtime_ns // 10**9, st.st_ctime_ns // 10**9, None, st.st_uid, st.st_gid, path)


def stat_record(path):
	"""Запись об объекте (без перехода по ссылкам). Время создания через statx, если доступно."""
	global _statx_fn
	if _statx_fn is not None:
		try:
			return _statx_record(path)
		except OSError as e:
			if e.errno not in (errno.ENOSYS, errno.EINVAL, errno.EPERM, errno.EOPNOTSUPP):
				raise
			_statx_fn = None
	return _lstat_record(path)


# ---------------------------------------------------------------- прогресс


ETA_MIN_ELAPSED = 1.0  # раньше оценка оставшегося времени слишком неточна и только мельтешит
ETA_MIN_RATIO = 0.01


def format_duration(seconds):
	s = int(seconds)
	h, rem = divmod(s, 3600)
	m, sec = divmod(rem, 60)
	return f"{h}:{m:02d}:{sec:02d}" if h else f"{m}:{sec:02d}"


def render_progress(done, total, width=30, elapsed=None, label="", finished=False):
	"""Строка прогресса: [####----]  50% 5/10  прошло 0:05, осталось ~0:05"""
	prefix = f"{label}: " if label else ""
	if done is None:  # счётчика нет, этап один длинный вызов
		text = f"{prefix}готово" if finished else f"{label}…"
	elif total is None:
		text = f"{prefix}{done}"
	else:
		ratio = 1.0 if total <= 0 else min(1.0, done / total)
		filled = int(width * ratio)
		text = f"{prefix}[{'#' * filled}{'-' * (width - filled)}] {int(ratio * 100):3d}% {done}/{total}"
	if elapsed is not None:
		text += f"  прошло {format_duration(elapsed)}"
		if total and done and not finished and done < total and elapsed >= ETA_MIN_ELAPSED and done / total >= ETA_MIN_RATIO:
			text += f", осталось ~{format_duration(elapsed * (total - done) / done)}"
	return text


class Progress:
	"""Прогресс одного этапа. counted=False: этап без счётчика (одна долгая операция)."""

	def __init__(self, label="", total=None, stream=None, enabled=None, width=30, interval=0.1, counted=True, clock=time.monotonic):
		self.label = label
		self.total = total
		self.stream = stream if stream is not None else sys.stderr
		self.enabled = (not _progress_disabled and self.stream.isatty()) if enabled is None else enabled
		self.width = width
		self.interval = interval
		self.clock = clock
		self.start = clock()
		self.done = 0 if (counted or total is not None) else None
		self.suspended = False
		self.finished = False
		self._last = None
		self._len = 0
		self._drawn = False

	def __enter__(self):
		if self.enabled:
			self._draw()
		return self

	def __exit__(self, exc_type, exc, tb):
		if exc_type is None:
			self.finish()
		elif self.enabled and self._drawn:
			self.stream.write("\n")
			self.stream.flush()
		return False

	def update(self, done):
		self.done = done
		if not self.enabled or self.suspended:
			return
		now = self.clock()
		if self._last is not None and now - self._last < self.interval:
			return
		self._last = now
		self._draw()

	def _draw(self):
		line = render_progress(self.done, self.total, self.width, self.clock() - self.start, self.label, self.finished)
		self._len = max(self._len, len(line))
		self.stream.write("\r" + line.ljust(self._len))
		self.stream.flush()
		self._drawn = True

	def message(self, text, newline=True):
		"""Печатает строку поверх бара: стирает бар, пишет текст, рисует бар заново."""
		live = self.enabled and not self.suspended
		if live and self._drawn:
			self.stream.write("\r" + " " * self._len + "\r")
			self._drawn = False
		self.stream.write(text + ("\n" if newline else ""))
		self.stream.flush()
		if live and newline:
			self._draw()

	def suspend(self):
		"""Бар больше не рисуется (нужно, когда в текущей строке оставлено незаконченное сообщение)."""
		self.suspended = True

	def finish(self):
		if self.total is not None and self.done is not None:
			self.total = self.done  # оценка могла не совпасть: этап закончен, показываем факт
		self.finished = True
		if self.enabled and not self.suspended:
			self._draw()
			self.stream.write("\n")
			self.stream.flush()


class NullProgress:
	"""Заглушка, когда прогресс не нужен."""

	def update(self, done):
		pass

	def __enter__(self):
		return self

	def __exit__(self, *exc):
		return False


def null_stage(label="", total=None, counted=True):
	return NullProgress()


def make_stage(label="", total=None, counted=True):
	return Progress(label, total, counted=counted)


class WarningPrinter:
	"""Первые LIMIT предупреждений сразу, на следующем «... и ещё », число дописывается в finish()."""

	LIMIT = 10

	def __init__(self, progress):
		self.progress = progress
		self.count = 0

	def __call__(self, text):
		self.count += 1
		if self.count <= self.LIMIT:
			self.progress.message(f"Предупреждение: не прочитано {text}")
		elif self.count == self.LIMIT + 1:
			self.progress.message("... и ещё ", newline=False)
			self.progress.suspend()

	def finish(self):
		if self.count > self.LIMIT:
			self.progress.message(str(self.count - self.LIMIT))


# ---------------------------------------------------------------- скан


def ordered(rec):
	out = {k: rec[k] for k in FIELD_ORDER if k in rec}
	for k in rec:
		if k not in out:
			out[k] = rec[k]
	return out


def scan_fs(root, rules, progress=None, on_warning=None):
	"""Возвращает (flat, warnings): flat = {путь: запись}, корень под ключом '.'.

	on_warning(текст) вызывается сразу, как только предупреждение возникло."""
	warnings = []

	def warn(text):
		warnings.append(text)
		if on_warning is not None:
			on_warning(text)

	try:
		root_rec = stat_record(root)
	except OSError as e:
		raise ChkanError(f"не могу прочитать root {root}: {e.strerror}")
	if root_rec["type"] != "d":
		raise ChkanError(f"root {root} не папка")
	flat = {ROOT_KEY: ordered(root_rec)}
	count = 1
	stack = [(root, ROOT_KEY, rules.root_state())]
	while stack:
		abs_dir, rel_dir, state = stack.pop()
		try:
			with os.scandir(abs_dir) as it:
				names = [e.name for e in it]
		except OSError as e:
			warn(f"{rel_dir}: {e.strerror}")
			continue
		for name in names:
			cstate = rules.child_state(state, name)
			if cstate.excluded:
				continue
			abs_path = os.path.join(abs_dir, name)
			rel = join_path(rel_dir, name)
			try:
				rec = stat_record(abs_path)
			except FileNotFoundError:
				continue
			except OSError as e:
				warn(f"{rel}: {e.strerror}")
				continue
			flat[rel] = ordered(rec)
			count += 1
			if progress is not None:
				progress.update(count)
			if rec["type"] == "d":
				stack.append((abs_path, rel, cstate))
	return flat, warnings


def add_dir_totals(flat, stage=None):
	"""Добавляет папкам поле total: суммарный размер обычных файлов во всём поддереве."""
	stage = stage or null_stage
	dirs = [p for p, r in flat.items() if r["type"] == "d"]
	totals = dict.fromkeys(dirs, 0)
	with stage("размеры папок", total=len(flat) + len(dirs)) as progress:
		done = 0
		for p, r in flat.items():
			if r["type"] == "f":
				totals[p.rpartition("/")[0] or ROOT_KEY] += r["size"]
			done += 1
			progress.update(done)
		for p in sorted(dirs, key=lambda d: -1 if d == ROOT_KEY else d.count("/"), reverse=True):
			if p != ROOT_KEY:
				totals[p.rpartition("/")[0] or ROOT_KEY] += totals[p]
			done += 1
			progress.update(done)
	for p in dirs:
		flat[p] = ordered({**flat[p], "total": totals[p]})
	return flat


# ---------------------------------------------------------------- слепок: плоский вид <-> дерево


def flatten(tree):
	flat = {}
	stack = [(ROOT_KEY, tree, {})]
	while stack:
		path, node, inherited = stack.pop()
		rec = {k: v for k, v in node.items() if k != "ch"}
		for k in INHERITED:
			if k not in rec:
				if k not in inherited:
					raise ChkanError(f"в слепке у {path} нет поля {k}")
				rec[k] = inherited[k]
		flat[path] = ordered(rec)
		inh = {k: rec[k] for k in INHERITED}
		for name, child in node.get("ch", {}).items():
			stack.append((join_path(path, name), child, inh))
	return flat


def build_tree(flat, progress=None):
	if ROOT_KEY not in flat:
		raise ChkanError("в слепке нет корня")
	nodes = {ROOT_KEY: dict(flat[ROOT_KEY])}
	paths = sorted((p for p in flat if p != ROOT_KEY), key=lambda p: (p.count("/"), p))
	for i, path in enumerate(paths, 2):  # корень уже посчитан
		if progress is not None:
			progress.update(i)
		parent, _, name = path.rpartition("/")
		parent = parent or ROOT_KEY
		if parent not in flat:
			raise ChkanError(f"у {path} нет родителя в слепке")
		if flat[parent]["type"] != "d":
			raise ChkanError(f"родитель {parent} объекта {path} не папка")
		pf = flat[parent]
		node = {k: v for k, v in flat[path].items() if not (k in INHERITED and pf.get(k) == v)}
		nodes[path] = node
		nodes[parent].setdefault("ch", {})[name] = node
	return nodes[ROOT_KEY]


def make_head(root, now, rules):
	return {
		"taken_at": now.isoformat(),
		"exclude": list(rules.exclude),
	}


def make_snapshot(root, now, rules, flat, progress=None):
	head = make_head(root, now, rules)
	return {
		"format": FORMAT,
		"root": root,
		"taken_at": head["taken_at"],
		"exclude": head["exclude"],
		"tree": build_tree(flat, progress),
	}


def snapshot_head(snap):
	return {"taken_at": snap["taken_at"], "exclude": list(snap["exclude"])}


def snapshots_equal(a, b):
	return snapshot_head(a) == snapshot_head(b) and a["root"] == b["root"] and flatten(a["tree"]) == flatten(b["tree"])


# ---------------------------------------------------------------- размеры


SIZE_UNITS = "BKMGTPE"


def format_size(n):
	"""1023002146 -> '1_023_002_146=975M': байты с разрядами через _ и размер
	(кратность 1024, отбрасывание лишнего) с 2-3 цифрами и буквой порядка."""
	unit = 0
	while n >= 1024 ** (unit + 1) and unit < len(SIZE_UNITS) - 1:
		unit += 1
	div = 1024**unit
	if unit and n < 10 * div:
		tenths = n * 10 // div
		human = f"{tenths // 10}.{tenths % 10}"
	else:
		human = str(n // div)
	return f"{n:_}={human}{SIZE_UNITS[unit]}"


def parse_size(value):
	"""Обратно: '1_023_002_146=975M' -> 1023002146 (число тоже принимается)."""
	if isinstance(value, int):
		return value
	return int(value.split("=", 1)[0].replace("_", ""))


def format_delta_short(n):
	"""+97G / -1.0K / 0B: только знак и размер для людей."""
	human = format_size(abs(n)).split("=", 1)[1]
	return ("+" if n > 0 else "-" if n < 0 else "") + human


def format_total_pair(pair):
	"""[было, стало] -> [было, стало, +на_сколько] строками; None (поля не было) остаётся null."""
	a, b = pair
	fa = None if a is None else format_size(a)
	fb = None if b is None else format_size(b)
	if a is None or b is None:
		return [fa, fb]
	return [fa, fb, format_delta_short(b - a)]


def parse_total_pair(pair, check_delta=True):
	"""Строки обратно в [было, стало] целыми; третий элемент только для чтения человеком, но сверяется."""
	try:
		if len(pair) not in (2, 3):
			raise ValueError("ожидалось 2 или 3 значения")
		a, b = (None if x is None else parse_size(x) for x in pair[:2])
		if check_delta and len(pair) == 3 and a is not None and b is not None and pair[2] != format_delta_short(b - a):
			raise ValueError(f"третье значение должно быть {format_delta_short(b - a)}")
	except (ValueError, TypeError, AttributeError) as e:
		raise DiffError(f"плохой суммарный размер {pair!r}: {e}")
	return [a, b]


class TimeRef:
	"""Опорные дата и часовой пояс из to.taken_at. В diff время пишется текстом в этом поясе,
	а год, месяц и день, совпадающие с опорными, опускаются: 00:02:04 / 15 00:02:04 / 09-16 00:02:04 / 2024-09-16 00:02:04."""

	def __init__(self, taken_at):
		self.taken_at = taken_at
		self._tz = self._date = None

	def _load(self):
		if self._date is None:
			try:
				dt = datetime.datetime.fromisoformat(self.taken_at)
			except (TypeError, ValueError):
				raise DiffError(f"плохое to.taken_at {self.taken_at!r}")
			if dt.tzinfo is None:
				dt = dt.replace(tzinfo=datetime.timezone.utc)
			self._tz, self._date = dt.tzinfo, dt.date()

	def encode(self, value):
		if isinstance(value, bool) or not isinstance(value, int):
			return value
		self._load()
		try:
			dt = datetime.datetime.fromtimestamp(value, self._tz)
		except (OverflowError, OSError, ValueError):
			return value  # время вне диапазона дат остаётся числом секунд
		clock = f"{dt:%H:%M:%S}"
		ref = self._date
		if dt.date() == ref:
			return clock
		if (dt.year, dt.month) == (ref.year, ref.month):
			return f"{dt.day:02d} {clock}"
		if dt.year == ref.year:
			return f"{dt.month:02d}-{dt.day:02d} {clock}"
		return f"{dt.year:04d}-{dt.month:02d}-{dt.day:02d} {clock}"

	def decode(self, value):
		if not isinstance(value, str):
			return value
		m = TIME_RE.match(value)
		if not m:
			raise DiffError(f"плохое время {value!r}")
		self._load()
		year, month, day, hour, minute, sec = m.groups()
		try:
			dt = datetime.datetime(
				int(year) if year else self._date.year,
				int(month) if month else self._date.month,
				int(day) if day else self._date.day,
				int(hour), int(minute), int(sec),
				tzinfo=self._tz,
			)
		except ValueError:
			raise DiffError(f"плохое время {value!r}")
		return int(dt.timestamp())


def _format_record(rec, ref):
	rec = dict(rec)
	if isinstance(rec.get("total"), int):
		rec["total"] = format_size(rec["total"])
	for k in TIME_FIELDS:
		if k in rec:
			rec[k] = ref.encode(rec[k])
	return rec


def _parse_record(rec, ref):
	rec = dict(rec)
	if isinstance(rec.get("total"), str):
		try:
			rec["total"] = parse_size(rec["total"])
		except ValueError as e:
			raise DiffError(f"плохой суммарный размер {rec['total']!r}: {e}")
	for k in TIME_FIELDS:
		if k in rec:
			rec[k] = ref.decode(rec[k])
	return rec


def _format_set(changes, ref):
	changes = dict(changes)
	if "total" in changes:
		changes["total"] = format_total_pair(changes["total"])
	for k in TIME_FIELDS:
		if k in changes:
			changes[k] = [ref.encode(x) for x in changes[k]]
	return changes


def _parse_set(changes, ref):
	changes = dict(changes)
	if "total" in changes:
		changes["total"] = parse_total_pair(changes["total"])
	for k in TIME_FIELDS:
		if k in changes:
			changes[k] = [ref.decode(x) for x in changes[k]]
	return changes


# ---------------------------------------------------------------- diff: вычисление


def record_changes(old, new):
	out = {}
	keys = list(FIELD_ORDER) + [k for k in list(old) + list(new) if k not in FIELD_ORDER]
	seen = set()
	for k in keys:
		if k in seen:
			continue
		seen.add(k)
		a, b = old.get(k), new.get(k)
		if a != b:
			out[k] = [a, b]
	return out


def same_identity(old, new):
	"""Можно ли считать пару remove/add одним и тем же объектом (перемещением)."""
	if old["type"] != new["type"] or old["dev"] != new["dev"] or old["ino"] != new["ino"]:
		return False
	if old.get("btime") is not None and new.get("btime") is not None:
		return old["btime"] == new["btime"]
	if old["type"] in ("f", "l"):
		return old["size"] == new["size"] and old["mtime"] == new["mtime"]
	return True


def make_ops(old_flat, new_flat, old_under, new_under, stage=None):
	stage = stage or null_stage
	common = sorted(old_flat.keys() & new_flat.keys())
	removed = sorted(old_flat.keys() - new_flat.keys())
	added = sorted(new_flat.keys() - old_flat.keys())
	ops = []
	with stage("diff: сравнение", total=len(common) + len(removed) + 2 * len(added)) as progress:
		done = 0
		for p in common:
			old, new = old_flat[p], new_flat[p]
			ch = record_changes(old, new)
			if ch:
				kind = "change_d" if old["type"] == "d" and new["type"] == "d" else "change"
				ops.append({"op": kind, "path": p, "set": ch})
			done += 1
			progress.update(done)

		plain_removed, plain_added = [], []
		for p in removed:
			if new_under(p):
				ops.append({"op": "remove_scan", "path": p, "old": old_flat[p]})
			else:
				plain_removed.append(p)
			done += 1
			progress.update(done)
		for p in added:
			if old_under(p):
				ops.append({"op": "add_scan", "path": p, "new": new_flat[p]})
			else:
				plain_added.append(p)
			done += 1
			progress.update(done)

		by_id = {}
		for p in plain_removed:
			r = old_flat[p]
			by_id.setdefault((r["type"], r["dev"], r["ino"]), []).append(p)
		used = set()
		moved_to = {}
		for p in plain_added:
			r = new_flat[p]
			cands = [c for c in by_id.get((r["type"], r["dev"], r["ino"]), []) if c not in used and same_identity(old_flat[c], r)]
			if cands:
				base = p.rpartition("/")[2]
				pick = next((c for c in cands if c.rpartition("/")[2] == base), cands[0])
				used.add(pick)
				moved_to[p] = pick
			done += 1
			progress.update(done)

	for p in plain_removed:
		if p not in used:
			ops.append({"op": "remove", "path": p, "old": old_flat[p]})
	for p in plain_added:
		if p in moved_to:
			src = moved_to[p]
			op = {"op": "move", "path": p, "from": src}
			ch = record_changes(old_flat[src], new_flat[p])
			if ch:
				op["set"] = ch
			ops.append(op)
		else:
			ops.append({"op": "add", "path": p, "new": new_flat[p]})
	return ops


def build_ch(nodes):
	"""{путь: узел} -> дерево 'ch' со схлопыванием цепочек контейнеров с одним потомком."""
	trie = {}
	for path, node in nodes.items():
		segs = [ROOT_KEY] if path == ROOT_KEY else path.split("/")
		cur = trie
		for seg in segs[:-1]:
			cur = cur.setdefault(seg, [None, {}])[1]
		cur.setdefault(segs[-1], [None, {}])[0] = node

	def emit(level):
		out = {}
		for name in sorted(level):
			node, kids = level[name]
			while node is None and len(kids) == 1:
				(cname, (cnode, ckids)), = kids.items()
				name = name + "/" + cname
				node, kids = cnode, ckids
			item = dict(node) if node else {}
			if kids:
				item["ch"] = emit(kids)
			out[name] = item
		return out

	return emit(trie)


def op_to_node(op, ref):
	"""Операция -> узел для diff-файла: суммарные размеры строками, время текстом."""
	node = {"op": op["op"]}
	for k in ("new", "old"):
		if k in op:
			node[k] = _format_record(op[k], ref)
	if "from" in op:
		node["from"] = op["from"]
	if "set" in op:
		node["set"] = _format_set(op["set"], ref)
	return node


def build_diff_flat(old_head, old_flat, new_head, new_flat, rules, root, stage=None, comment=None):
	stage = stage or null_stage
	ref = TimeRef(new_head["taken_at"])
	old_under = make_excluder(old_head["exclude"])
	new_under = make_excluder(new_head["exclude"])
	ops = make_ops(old_flat, new_flat, old_under, new_under, stage)
	resolver = rules.resolver()

	groups, scan_nodes = {}, {}
	sizes = {}
	with stage("diff: группы и размеры", total=len(ops) + len(old_flat) + len(new_flat)) as progress:
		done = 0
		for op in ops:
			node = op_to_node(op, ref)
			if op["op"] in ("add_scan", "remove_scan"):
				scan_nodes[op["path"]] = node
			else:
				groups.setdefault(resolver.tags(op["path"]), {})[op["path"]] = node
			done += 1
			progress.update(done)
		for path, rec in old_flat.items():
			if rec["type"] == "f" and not new_under(path):
				sizes.setdefault(resolver.tags(path), [0, 0])[0] += rec["size"]
			done += 1
			progress.update(done)
		for path, rec in new_flat.items():
			if rec["type"] == "f" and not old_under(path):
				sizes.setdefault(resolver.tags(path), [0, 0])[1] += rec["size"]
			done += 1
			progress.update(done)

	keys = set(groups) | {t for t, (a, b) in sizes.items() if a != b}
	entries = []
	for tags in keys:
		group = {"size": format_total_pair(sizes.get(tags, [0, 0]))}
		if tags in groups:
			group["ch"] = build_ch(groups[tags])
		label = "+".join(sorted(tags)) if tags else NO_TAGS_KEY
		entries.append((tags, label, group))
	untagged = [e for e in entries if not e[0]]
	tracked = sorted((e for e in entries if e[0] and not (e[0] & rules.ignore_tags)), key=lambda e: e[1])
	ignored = sorted((e for e in entries if e[0] & rules.ignore_tags), key=lambda e: e[1])

	out_groups = {}
	for _t, label, group in untagged + tracked:
		out_groups[label] = group
	out_groups[SEPARATOR_KEY] = 0
	for _t, label, group in ignored:
		out_groups[label] = group

	diff = {
		"format": FORMAT,
		"root": root,
		"from": dict(old_head),
		"to": dict(new_head),
	}
	if comment:
		diff["c"] = comment
	diff["groups"] = out_groups
	if scan_nodes:
		diff["scan"] = {"ch": build_ch(scan_nodes)}
	return diff


def build_diff(old_snap, new_snap, rules, comment=None):
	return build_diff_flat(
		snapshot_head(old_snap),
		flatten(old_snap["tree"]),
		snapshot_head(new_snap),
		flatten(new_snap["tree"]),
		rules,
		new_snap["root"],
		comment=comment,
	)


# ---------------------------------------------------------------- diff: чтение и применение


def iter_nodes(ch, prefix=""):
	for key, node in ch.items():
		if not isinstance(node, dict):
			continue
		path = prefix + "/" + key if prefix else key
		if "op" in node:
			yield path, node
		if "ch" in node:
			yield from iter_nodes(node["ch"], path)


def node_to_op(path, node, ref):
	op = {"op": node["op"], "path": path}
	for k in ("new", "old"):
		if k in node:
			op[k] = _parse_record(node[k], ref)
	if "from" in node:
		op["from"] = node["from"]
	if "set" in node:
		op["set"] = _parse_set(node["set"], ref)
	return op


def diff_ops(diff):
	ref = TimeRef(diff.get("to", {}).get("taken_at"))
	ops = []
	for key, group in diff.get("groups", {}).items():
		if key == SEPARATOR_KEY or not isinstance(group, dict):
			continue
		ops.extend(node_to_op(p, n, ref) for p, n in iter_nodes(group.get("ch", {})))
	ops.extend(node_to_op(p, n, ref) for p, n in iter_nodes(diff.get("scan", {}).get("ch", {})))
	return ops


COUNT_KEYS = ("add", "remove", "change", "move", "change_d")


def _count_group(group, counts):
	for _path, node in iter_nodes(group.get("ch", {})):
		kind = node["op"]
		if kind in CHANGE_OPS:
			counts["change_d" if kind == "change_d" else "change"] += 1
		elif kind in ("add", "remove", "move"):
			counts[kind] += 1  # add_scan и remove_scan не считаем


def count_ops(diff):
	"""[(метка, счётчики, size)]: без тегов, каждая отслеживаемая комбинация, не отслеживаемые комбинации.
	size: [было, стало, ...] из diff или None."""
	untagged = dict.fromkeys(COUNT_KEYS, 0)
	ignored = dict.fromkeys(COUNT_KEYS, 0)
	untagged_size = None
	rows = []
	after_separator = False
	for key, group in diff.get("groups", {}).items():
		if key == SEPARATOR_KEY:
			after_separator = True
			continue
		if not isinstance(group, dict):
			continue
		if after_separator:
			_count_group(group, ignored)
		elif key == NO_TAGS_KEY:
			_count_group(group, untagged)
			untagged_size = group.get("size")
		else:
			counts = dict.fromkeys(COUNT_KEYS, 0)
			_count_group(group, counts)
			rows.append((key, counts, group.get("size")))
	return [("без тегов", untagged, untagged_size)] + rows + [("с не отслеживаемыми тэгами", ignored, None)]


def _size_suffix(size):
	""" sf:283K st:16M s+16M (было, стало, на сколько изменился)."""
	if not size or len(size) < 2:
		return ""
	a, b = parse_size(size[0]), parse_size(size[1])
	human = lambda n: format_size(n).split("=", 1)[1]
	return f" sf:{human(a)} st:{human(b)} s{format_delta_short(b - a)}"


def format_summary(diff):
	"""+созданные -удалённые ~изменённые(не папки) ->перемещённые d~изменённые папки [sf:было st:стало s±разница]"""
	return [
		f"{label}: +{c['add']} -{c['remove']} ~{c['change']} ->{c['move']} d~{c['change_d']}{_size_suffix(size)}"
		for label, c, size in count_ops(diff)
	]


def invert_op(op):
	kind, path = op["op"], op["path"]
	try:
		if kind in ("add", "add_scan"):
			return {"op": kind.replace("add", "remove"), "path": path, "old": op["new"]}
		if kind in ("remove", "remove_scan"):
			return {"op": kind.replace("remove", "add"), "path": path, "new": op["old"]}
		if kind in CHANGE_OPS:
			return {"op": kind, "path": path, "set": {k: [b, a] for k, (a, b) in op["set"].items()}}
		if kind == "move":
			inv = {"op": "move", "path": op["from"], "from": path}
			if op.get("set"):
				inv["set"] = {k: [b, a] for k, (a, b) in op["set"].items()}
			return inv
	except KeyError as e:
		raise DiffError(f"в записи {path} ({kind}) нет поля {e}")
	raise DiffError(f"неизвестная операция {kind!r} у {path}")


def _apply_set(rec, changes, path, strict):
	out = dict(rec)
	for k, (a, b) in changes.items():
		if strict and rec.get(k) != a:
			raise DiffError(f"{path}: поле {k} равно {rec.get(k)!r}, а в diff ожидалось {a!r}")
		if b is None and k in OPTIONAL_FIELDS:
			out.pop(k, None)
		else:
			out[k] = b
	return ordered(out)


def apply_ops(flat, ops, strict=True, progress=None):
	"""Применяет операции атомарно: сначала все удаления, потом вставки."""
	removals, changes, inserts = [], [], []
	try:
		for i, op in enumerate(ops, 1):
			if progress is not None:
				progress.update(i)
			kind, path = op["op"], op["path"]
			if kind in ("remove", "remove_scan"):
				if path not in flat:
					raise DiffError(f"{path}: нечего удалять")
				if strict and flat[path] != ordered(op["old"]):
					raise DiffError(f"{path}: запись не совпадает с old из diff")
				removals.append(path)
			elif kind in ("add", "add_scan"):
				inserts.append((path, ordered(op["new"])))
			elif kind in CHANGE_OPS:
				if path not in flat:
					raise DiffError(f"{path}: нечего менять")
				changes.append((path, _apply_set(flat[path], op["set"], path, strict)))
			elif kind == "move":
				src = op["from"]
				if src not in flat:
					raise DiffError(f"{src}: нечего перемещать")
				removals.append(src)
				inserts.append((path, _apply_set(flat[src], op.get("set", {}), src, strict)))
			else:
				raise DiffError(f"неизвестная операция {kind!r} у {path}")
	except KeyError as e:
		raise DiffError(f"в записи ({op.get('op')}) нет поля {e}")
	result = dict(flat)
	touched = set()
	for p in removals:
		if p in touched:
			raise DiffError(f"{p}: путь затронут дважды")
		touched.add(p)
		del result[p]
	for p, rec in changes:
		if p in touched:
			raise DiffError(f"{p}: путь затронут дважды")
		touched.add(p)
		result[p] = rec
	for p, rec in inserts:
		if p in result:
			raise DiffError(f"{p}: уже существует")
		result[p] = rec
	return result


class _Offset:
	"""Прогресс со сдвигом: общий бар этапа из нескольких проходов."""

	def __init__(self, progress, offset):
		self.progress, self.offset = progress, offset

	def update(self, done):
		self.progress.update(self.offset + done)


def verify_restore(diff, new_head, new_flat, prev_head, prev_flat, stage=None):
	"""Из нового слепка и diff (в том виде, как он записан на диск) должен получиться предыдущий."""
	stage = stage or null_stage
	if diff["to"] != new_head or diff["from"] != prev_head:
		return False
	ops = [invert_op(o) for o in diff_ops(diff)]
	with stage("проверка: откат", total=len(ops) + len(prev_flat)) as progress:
		restored = apply_ops(new_flat, ops, True, progress)
		if restored != prev_flat:
			return False
		build_tree(restored, _Offset(progress, len(ops)))  # заодно проверяет, что у всех есть родитель
	return True


def apply_diff(snapshot, diff, backward=False, strict=True):
	"""Из слепка 'to' и diff получить слепок 'from' (backward) или наоборот."""
	src_head, dst_head = (diff["to"], diff["from"]) if backward else (diff["from"], diff["to"])
	if strict and snapshot["taken_at"] != src_head["taken_at"]:
		raise DiffError(f"diff относится к слепку {src_head['taken_at']}, а дан {snapshot['taken_at']}")
	ops = diff_ops(diff)
	if backward:
		ops = [invert_op(o) for o in ops]
	flat = apply_ops(flatten(snapshot["tree"]), ops, strict)
	return {
		"format": FORMAT,
		"root": snapshot["root"],
		"taken_at": dst_head["taken_at"],
		"exclude": list(dst_head["exclude"]),
		"tree": build_tree(flat),
	}


# ---------------------------------------------------------------- JSON


def _compact(value):
	return json.dumps(value, ensure_ascii=False, separators=(", ", ": "))


def _has_ch(value):
	if isinstance(value, dict):
		return "ch" in value or any(_has_ch(v) for v in value.values())
	return False


def dumps_pretty(value, level=0):
	"""JSON с табами; листовые узлы в одну строку, узлы с 'ch' раскрыты (удобно править руками)."""
	if isinstance(value, dict):
		if not value:
			return "{}"
		if level > 0 and not _has_ch(value):
			inline = _compact(value)
			if len(inline) <= INLINE_LIMIT:
				return inline
		pad = "\t" * (level + 1)
		items = [f"{pad}{json.dumps(k, ensure_ascii=False)}: {dumps_pretty(v, level + 1)}" for k, v in value.items()]
		return "{\n" + ",\n".join(items) + "\n" + "\t" * level + "}"
	return _compact(value)


def _tabs(n):
	return "\t" * n


def _expanded(node, level):
	items = []
	for k, v in node.items():
		text = _ch_text(v, level + 1) if k == "ch" else _compact(v)
		items.append(f"{_tabs(level + 1)}{json.dumps(k, ensure_ascii=False)}: {text}")
	return "{\n" + ",\n".join(items) + "\n" + _tabs(level) + "}"


def _ch_text(ch, level):
	"""Каждый элемент ch на своей строке; level: уровень самого словаря ch."""
	items = [f"{_tabs(level + 1)}{json.dumps(k, ensure_ascii=False)}: {_node_text(v, level + 1)}" for k, v in ch.items()]
	return "{\n" + ",\n".join(items) + "\n" + _tabs(level) + "}"


def _node_text(node, level):
	"""Узел из одних op и set (изменение) и короткие листья в одну строку; set всегда в одну строку."""
	if not isinstance(node, dict):
		return _compact(node)
	if "ch" not in node and (set(node) == {"op", "set"} or len(_compact(node)) <= INLINE_LIMIT):
		return _compact(node)
	return _expanded(node, level)


def dumps_diff(diff):
	"""Файл diff: format, root, from, to, c, groups, scan. Правится руками (комментарии `c`)."""
	items = []
	for k, v in diff.items():
		if k == "groups":
			inner = [f"{_tabs(2)}{json.dumps(gk, ensure_ascii=False)}: {_node_text(gv, 2)}" for gk, gv in v.items()]
			text = "{\n" + ",\n".join(inner) + "\n\t}"
		elif k == "scan":
			text = _node_text(v, 1)
		elif isinstance(v, dict):
			text = dumps_pretty(v, 1)
		else:
			text = _compact(v)
		items.append(f"\t{json.dumps(k, ensure_ascii=False)}: {text}")
	return "{\n" + ",\n".join(items) + "\n}"


def dumps_snapshot(snapshot):
	return json.dumps(snapshot, ensure_ascii=False, separators=(",", ":"))


def write_text_atomic(path, text):
	tmp = path + ".tmp"
	with open(tmp, "w", encoding="utf-8", errors="surrogateescape", newline="\n") as f:
		f.write(text)
	os.replace(tmp, path)


def read_json(path):
	with open(path, encoding="utf-8", errors="surrogateescape") as f:
		data = json.load(f)
	if data.get("format") != FORMAT:
		hint = "; запустите chkan.py без аргументов, он предложит преобразовать" if data.get("format") == 3 else ""
		raise ChkanError(f"{path}: формат {data.get('format')!r}, ожидался {FORMAT}{hint}")
	return data


# ---------------------------------------------------------------- @run


def run_hooks(root, runs, env_extra):
	"""Выполняет команды @run по очереди, каждую в bash -c; возвращает число неудач."""
	failures = 0
	for rel, command in runs:
		msg(f"{rel} # @run({command})")
		cwd = root if rel == ROOT_KEY else os.path.join(root, *rel.split("/"))
		if not os.path.isdir(cwd):
			msg(f"Ошибка: нет папки {rel}, команда пропущена")
			failures += 1
			continue
		try:
			code = subprocess.run(["bash", "-c", command], cwd=cwd, env={**os.environ, **env_extra}).returncode
		except OSError as e:
			msg(f"Ошибка: не удалось запустить bash: {e}")
			failures += 1
			continue
		if code != 0:
			msg(f"Команда завершилась с кодом {code}")
			failures += 1
	return failures


# ---------------------------------------------------------------- время и файлы


def _now():
	return datetime.datetime.now().astimezone().replace(microsecond=0)


def format_stamp(dt):
	return dt.strftime("%Y%m%d-%H%M%S")


def scan_name(stamp):
	return f"scan_{stamp}.json"


def diff_name(old_stamp, new_stamp):
	return f"diff_{old_stamp}_{new_stamp}.json"


def list_scans(changes_dir):
	stamps = []
	for name in os.listdir(changes_dir):
		m = SCAN_RE.match(name)
		if m:
			stamps.append(m.group(1))
	return sorted(stamps)


def next_moment(last_stamp):
	while True:
		now = _now()
		stamp = format_stamp(now)
		if last_stamp is None or stamp > last_stamp:
			return now, stamp
		if stamp < last_stamp:
			raise ChkanError(f"текущее время {stamp} раньше последнего слепка {last_stamp} (сменили часы или пояс?)")
		time.sleep(0.2)


# ---------------------------------------------------------------- переход с формата 3


def peek_format(path):
	with open(path, "rb") as f:
		head = f.read(300)
	m = re.search(rb'"format"\s*:\s*(\d+)', head)
	return int(m.group(1)) if m else None


def old_format_files(changes):
	"""[(имя, формат)] для scan_*/diff_*, которые не в текущем формате."""
	out = []
	for name in sorted(os.listdir(changes)):
		if SCAN_RE.match(name) or DIFF_RE.match(name):
			fmt = peek_format(os.path.join(changes, name))
			if fmt != FORMAT:
				out.append((name, fmt))
	return out


def _sec(value):
	return value // 10**9 if isinstance(value, int) and not isinstance(value, bool) else value


def migrate_snapshot(data):
	"""Формат 3 -> 4: время в секундах, без tz."""
	data["format"] = FORMAT
	data.pop("tz", None)
	stack = [data["tree"]]
	while stack:
		node = stack.pop()
		for k in TIME_FIELDS:
			if k in node:
				node[k] = _sec(node[k])
		stack.extend(node.get("ch", {}).values())
	return data


def _migrate_node(node, ref):
	"""False, если изменение состояло только из долей секунды и исчезло."""
	for k in ("new", "old"):
		if k in node:
			rec = dict(node[k])
			for f in TIME_FIELDS:
				if f in rec:
					rec[f] = ref.encode(_sec(rec[f]))
			node[k] = rec
	changes = node.get("set")
	if changes is not None:
		for f in TIME_FIELDS:
			if f in changes:
				a, b = (_sec(x) for x in changes[f])
				if a == b:
					del changes[f]
				else:
					changes[f] = [ref.encode(a), ref.encode(b)]
		if "total" in changes:
			changes["total"] = format_total_pair(parse_total_pair(changes["total"], check_delta=False))
		if not changes:
			if node["op"] in CHANGE_OPS:
				return False
			del node["set"]
	return True


def _migrate_ch(ch, ref):
	for key in list(ch):
		node = ch[key]
		if not isinstance(node, dict):
			continue
		if "ch" in node:
			_migrate_ch(node["ch"], ref)
			if not node["ch"]:
				del node["ch"]
		if "op" in node and not _migrate_node(node, ref):
			for k in ("op", "set"):
				node.pop(k, None)
		if not node:
			del ch[key]


def migrate_diff(data):
	"""Формат 3 -> 4: секунды, время текстом, короткие размеры, без tz; комментарии `c` сохраняются."""
	ref = TimeRef(data["to"]["taken_at"])
	data["format"] = FORMAT
	for head in (data["from"], data["to"]):
		head.pop("tz", None)
	for group in data["groups"].values():
		if not isinstance(group, dict):
			continue
		if "size" in group:
			a, b = (parse_size(x) for x in group["size"][:2])
			group["size"] = format_total_pair([a, b])
		if "ch" in group:
			_migrate_ch(group["ch"], ref)
	scan = data.get("scan")
	if scan and "ch" in scan:
		_migrate_ch(scan["ch"], ref)
	return data


def _read_any(path):
	with open(path, encoding="utf-8", errors="surrogateescape") as f:
		return json.load(f)


def check_chain(changes, paths):
	"""Первый скан + все diff подряд должны дать последний скан. paths: имя -> путь к файлу нового формата."""
	scans = list_scans(changes)
	diffs = sorted(n for n in paths if DIFF_RE.match(n))
	if len(scans) < 2 or not diffs:
		return
	first = _read_any(paths[scan_name(scans[0])])
	last = _read_any(paths[scan_name(scans[-1])])
	flat, current = flatten(first["tree"]), first["taken_at"]
	for name in diffs:
		d = _read_any(paths[name])
		if d["from"]["taken_at"] != current:
			raise ChkanError(f"цепочка diff прерывается перед {name}")
		flat = apply_ops(flat, diff_ops(d), True)
		current = d["to"]["taken_at"]
	if current != last["taken_at"] or flat != flatten(last["tree"]):
		raise ChkanError("первый скан и все diff не дают последний скан")


def migrate_if_needed(changes, assume_yes=False):
	"""Переводит файлы формата 3 в формат 4, оригиналы уходят в .changes/format3/. False: отказ."""
	old = old_format_files(changes)
	if not old:
		return True
	unknown = [(n, f) for n, f in old if f != 3]
	if unknown:
		raise ChkanError(f"неизвестный формат у {unknown[0][0]}: {unknown[0][1]!r}")
	backup = os.path.join(changes, BACKUP_DIR)
	if os.path.isdir(backup) and os.listdir(backup):
		raise ChkanError(f"{backup} уже существует и не пуст: уберите его, прежде чем преобразовывать")
	if not assume_yes and not ask(
		f"В {changes} файлов формата 3: {len(old)}. Преобразовать в формат {FORMAT}? "
		f"Оригиналы будут сохранены в {BACKUP_DIR}/ [y/N] "
	):
		msg("Отмена.")
		return False
	converted = {}
	try:
		for name, _fmt in old:
			path = os.path.join(changes, name)
			with make_stage(f"преобразование {name}", counted=False):
				data = _read_any(path)
				if SCAN_RE.match(name):
					text = dumps_snapshot(migrate_snapshot(data))
				else:
					text = dumps_diff(migrate_diff(data)) + "\n"
				with open(path + ".v4", "w", encoding="utf-8", errors="surrogateescape", newline="\n") as f:
					f.write(text)
			converted[name] = path + ".v4"
		paths = {n: converted.get(n, os.path.join(changes, n)) for n in os.listdir(changes) if SCAN_RE.match(n) or DIFF_RE.match(n)}
		with make_stage("проверка: первый скан + все diff = последний скан", counted=False):
			check_chain(changes, paths)
	except BaseException:
		for tmp in converted.values():
			if os.path.exists(tmp):
				os.remove(tmp)
		raise
	os.makedirs(backup, exist_ok=True)
	for name, tmp in converted.items():
		os.replace(os.path.join(changes, name), os.path.join(backup, name))
		os.replace(tmp, os.path.join(changes, name))
	msg(f"Преобразовано в формат {FORMAT}: {len(converted)} файлов, оригиналы в {backup}")
	return True


# ---------------------------------------------------------------- лог и wildcard


def write_log(changes, now, comment, lines):
	"""В .changes/chkan-log.txt: пустая строка, --- дата-время ---, комментарий, статистика."""
	text = "\n--- " + now.strftime("%Y-%m-%d %H:%M:%S") + " ---\n"
	if comment:
		text += comment + "\n"
	text += "".join(line + "\n" for line in lines)
	try:
		with open(os.path.join(changes, LOG_FILE), "a", encoding="utf-8", errors="surrogateescape") as f:
			f.write(text)
	except OSError as e:
		msg(f"Предупреждение: не удалось записать {LOG_FILE}: {e.strerror}")


def cmd_wildcard(root):
	"""chkan.py wildcard: во что раскрываются пути с wildcard-ами из TAGS.txt (в stdout)."""
	tags_path = os.path.join(root, CHANGES_DIR, TAGS_FILE)
	if not os.path.isfile(tags_path):
		raise ChkanError(f"нет файла {tags_path}")
	with open(tags_path, encoding="utf-8", errors="surrogateescape") as f:
		rules = parse_tags(f.read(), root, strict=False)
	for lineno, pattern, paths in rules.wildcards:
		print()
		print(f"{lineno}: {pattern}")
		for path in paths:
			print(path)
	return 0


# ---------------------------------------------------------------- основной сценарий


def run(root, assume_yes=False, warn_yes=False, comment=None, show_progress=True):
	global _progress_disabled
	previous, _progress_disabled = _progress_disabled, not show_progress
	try:
		return _run(root, assume_yes, warn_yes, comment)
	finally:
		_progress_disabled = previous


def _run(root, assume_yes, warn_yes, comment):
	changes = os.path.join(root, CHANGES_DIR)
	if not os.path.isdir(changes):
		if not assume_yes and not ask(f"Папка {changes} не найдена.\nСоздать её и сделать первый скан (может занять время)? [y/N] "):
			msg("Отмена.")
			return 1
		os.makedirs(changes)
		if not os.path.exists(os.path.join(changes, TAGS_FILE)):
			write_text_atomic(os.path.join(changes, TAGS_FILE), TAGS_TEMPLATE)
	tags_path = os.path.join(changes, TAGS_FILE)
	if not os.path.isfile(tags_path):
		raise ChkanError(f"нет файла {tags_path}")
	with open(tags_path, encoding="utf-8", errors="surrogateescape") as f:
		rules = parse_tags(f.read(), root)
	if not migrate_if_needed(changes, assume_yes):
		return 1

	scans = list_scans(changes)
	prev = prev_flat = None
	if scans:
		prev = read_json(os.path.join(changes, scan_name(scans[-1])))
		prev_flat = flatten(prev["tree"])
	now, stamp = next_moment(scans[-1] if scans else None)

	progress = Progress("сканирование", total=len(prev_flat) if prev_flat is not None else None)
	printer = WarningPrinter(progress)
	flat, warnings = scan_fs(root, rules, progress, printer)
	progress.finish()
	printer.finish()

	if warnings and not warn_yes and not ask(
		f"При сканировании были предупреждения ({len(warnings)}). Продолжить и сохранить результат? [y/N] "
	):
		msg("Отмена: ничего не сохранено.")
		return 1

	add_dir_totals(flat, stage=make_stage)
	with make_stage("слепок: дерево", total=len(flat)) as pr:
		snapshot = make_snapshot(root, now, rules, flat, pr)
	scan_path = os.path.join(changes, scan_name(stamp))
	with make_stage("слепок: запись", counted=False):
		write_text_atomic(scan_path, dumps_snapshot(snapshot))

	def finish(diff_path, lines):
		write_log(changes, now, comment, lines)
		env = {
			"CHKAN_TAGS": os.path.abspath(tags_path),
			"CHKAN_DIFF": os.path.abspath(diff_path) if diff_path else "",
			"CHKAN_SCAN": os.path.abspath(scan_path),
			"CHKAN_COMMENT": comment or "",
		}
		return 3 if run_hooks(root, rules.runs, env) else 0

	if prev is None:
		msg(f"первый скан сохранён: {len(flat)} объектов ({scan_name(stamp)})")
		return finish(None, [f"первый скан: {len(flat)} объектов"])

	diff = build_diff_flat(
		snapshot_head(prev), prev_flat, snapshot_head(snapshot), flat, rules, root, stage=make_stage, comment=comment
	)
	diff_path = os.path.join(changes, diff_name(scans[-1], stamp))
	with make_stage("diff: запись", counted=False):
		write_text_atomic(diff_path, dumps_diff(diff) + "\n")

	with make_stage("проверка: чтение diff", counted=False):
		written = read_json(diff_path)
	try:
		restored_ok = verify_restore(written, snapshot_head(snapshot), flat, snapshot_head(prev), prev_flat, stage=make_stage)
	except ChkanError as e:
		raise ChkanError(f"проверка отката не прошла ({e}); предыдущий скан оставлен")
	if not restored_ok:
		raise ChkanError(
			f"предыдущий скан не восстанавливается из нового слепка и {os.path.basename(diff_path)}; "
			f"предыдущий скан оставлен"
		)
	if scans[-1] != scans[0]:
		os.remove(os.path.join(changes, scan_name(scans[-1])))

	lines = format_summary(diff)
	for line in lines:
		print(line)
	return finish(diff_path, lines)


def main(argv=None):
	parser = argparse.ArgumentParser(prog="chkan.py", description="Скан ФС и обратимый diff в ./.changes/")
	parser.add_argument(
		"args", nargs="*", metavar="[wildcard] [папка]",
		help="папка, в которой лежит .changes (по умолчанию текущая); слово wildcard вместо скана "
		"показывает, во что раскрываются wildcard-ы из TAGS.txt (папку с таким именем задавайте как ./wildcard)",
	)
	parser.add_argument("-y", "--yes", action="store_true", help="не спрашивать при создании .changes и переходе на новый формат")
	parser.add_argument(
		"-w", "--warn-yes", action="store_true",
		help="если при сканировании были предупреждения, не спрашивать, а продолжать",
	)
	parser.add_argument("-c", "--comment", metavar="КОММЕНТАРИЙ", help="комментарий к этому скану: в diff (ключ c), в лог и в $CHKAN_COMMENT")
	parser.add_argument("-P", "--no-progress", action="store_true", help="не показывать прогресс-бары")
	args = parser.parse_args(argv)
	rest = list(args.args)
	command = rest.pop(0) if rest and rest[0] == "wildcard" else None
	if len(rest) > 1:
		parser.error("лишние аргументы: " + " ".join(rest[1:]))
	root = os.path.abspath(rest[0] if rest else ".")
	try:
		if command == "wildcard":
			return cmd_wildcard(root)
		return run(root, assume_yes=args.yes, warn_yes=args.warn_yes, comment=args.comment, show_progress=not args.no_progress)
	except ChkanError as e:
		msg(f"Ошибка: {e}")
		return 2


if __name__ == "__main__":
	sys.exit(main())
