#!/usr/bin/env python3
"""chkan.py: снимки файловой системы и обратимые diff-ы между ними.

Запуск из папки, в которой лежит (или будет создана) ./.changes/:

	python3 chkan.py [-y] [-w] [папка]

root = папка, в которой лежит .changes/TAGS.txt (по умолчанию текущая).
Файлы в .changes/:
	TAGS.txt                           теги и исключения
	scan_YYYYMMDD-HHMMSS.json          слепки (остаются первый и последний)
	diff_<от>_<до>.json                diff-ы между соседними слепками
"""

import argparse
import ctypes
import datetime
import errno
import json
import os
import re
import stat
import sys
import time
from collections import namedtuple

CHANGES_DIR = ".changes"
TAGS_FILE = "TAGS.txt"
YES_ANSWERS = ("y", "yes", "д", "да")
FORMAT = 3
ROOT_KEY = "."
NO_TAGS_KEY = "-"
SEPARATOR_KEY = "-" * 10
INLINE_LIMIT = 200

BASE_FIELDS = ("type", "dev", "ino", "size", "mtime", "ctime", "btime", "uid", "gid", "mode")
FIELD_ORDER = BASE_FIELDS + ("target",)
INHERITED = ("dev", "uid", "gid")

TAG_RE = re.compile(r"^[\w.]+$")
TOKEN_RE = re.compile(r"^(?P<name>[^()\s]+)(?P<ignore>\(ignore\))?$")
HASH_RE = re.compile(r"(^|\s)#")
SCAN_RE = re.compile(r"^scan_(\d{8}-\d{6})\.json$")

TAGS_TEMPLATE = """\
// root = папка, в которой лежит .changes/
// Строка: путь<TAB># тег тег(ignore) // комментарий. Вложенность задаётся табами.
// Зарезервировано: exclude (не сканировать), префикс no_ (снимает тег), and, or.
// Пример:
//   .cache/	# exclude
//   projects/	# work
//   	old/	# no_work
// # noisy(ignore)	// строка без пути только помечает тег игнорируемым
"""


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
	def __init__(self, trie, ignore_tags, exclude):
		self.trie = trie
		self.ignore_tags = frozenset(ignore_tags)
		self.exclude = list(exclude)

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
	m = TOKEN_RE.match(token)
	if not m:
		raise ChkanError(f"{where}: непонятный тег {token!r}")
	name = m.group("name")
	ignore = bool(m.group("ignore"))
	if name == "no_exclude":
		raise ChkanError(f"{where}: тег no_exclude запрещён (exclude ничем не снимается)")
	cancel = name.startswith("no_")
	base = name[3:] if cancel else name
	if not base or not TAG_RE.match(base):
		raise ChkanError(f"{where}: недопустимое имя тега {token!r} (буквы, цифры, _ и .)")
	if base.startswith("no_"):
		raise ChkanError(f"{where}: имя тега не может начинаться с no_: {token!r}")
	if base in ("and", "or"):
		raise ChkanError(f"{where}: тег {base!r} зарезервирован")
	if ignore and (cancel or base == "exclude"):
		raise ChkanError(f"{where}: (ignore) нельзя ставить на {name!r}")
	kind = "cancel" if cancel else ("exclude" if base == "exclude" else "add")
	return kind, base, ignore


def parse_tags(text):
	trie = TagNode()
	ignore_tags = set()
	exclude = []
	stack = []
	for lineno, raw in enumerate(text.splitlines(), 1):
		line = raw.split("//", 1)[0].rstrip()
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
		parsed = [parse_tag_token(t, where) for t in tags_part.split()]
		for _kind, base, ignore in parsed:
			if ignore:
				ignore_tags.add(base)
		if not path_part:
			continue
		if path_part.startswith("/"):
			raise ChkanError(f"{where}: пути задаются относительно root, без начального /")
		segs = [s for s in path_part.split("/") if s not in ("", ".")]
		if ".." in segs:
			raise ChkanError(f"{where}: .. в путях запрещено")
		if indent > len(stack):
			raise ChkanError(f"{where}: слишком глубокий отступ")
		del stack[indent:]
		full = (stack[-1] if stack else ()) + tuple(segs)
		stack.append(full)
		node = trie
		for seg in full:
			node = node.children.setdefault(seg, TagNode())
		for kind, base, _ignore in parsed:
			node.ops.append((kind, base))
			if kind == "exclude":
				if not full:
					raise ChkanError(f"{where}: нельзя исключить root")
				exclude.append("/".join(full))
	return Rules(trie, ignore_tags, canonical_exclude(exclude))


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
		btime = buf.stx_btime.tv_sec * 10**9 + buf.stx_btime.tv_nsec
	return _make_record(
		buf.stx_mode,
		os.makedev(buf.stx_dev_major, buf.stx_dev_minor),
		buf.stx_ino,
		buf.stx_size,
		buf.stx_mtime.tv_sec * 10**9 + buf.stx_mtime.tv_nsec,
		buf.stx_ctime.tv_sec * 10**9 + buf.stx_ctime.tv_nsec,
		btime,
		buf.stx_uid,
		buf.stx_gid,
		path,
	)


def _lstat_record(path):
	st = os.lstat(path)
	return _make_record(st.st_mode, st.st_dev, st.st_ino, st.st_size, st.st_mtime_ns, st.st_ctime_ns, None, st.st_uid, st.st_gid, path)


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


def render_progress(done, total, width=30):
	if total is None:
		return f"сканирую: {done}"
	ratio = 1.0 if total <= 0 else min(1.0, done / total)
	filled = int(width * ratio)
	return f"[{'#' * filled}{'-' * (width - filled)}] {int(ratio * 100):3d}% {done}/{total}"


class Progress:
	def __init__(self, total=None, stream=None, enabled=None, width=30, interval=0.1):
		self.total = total
		self.stream = stream if stream is not None else sys.stderr
		self.enabled = self.stream.isatty() if enabled is None else enabled
		self.width = width
		self.interval = interval
		self.done = 0
		self.suspended = False
		self._last = None
		self._len = 0
		self._drawn = False

	def update(self, done):
		self.done = done
		if not self.enabled or self.suspended:
			return
		now = time.monotonic()
		if self._last is not None and now - self._last < self.interval:
			return
		self._last = now
		self._draw()

	def _draw(self):
		line = render_progress(self.done, self.total, self.width)
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
		if self.enabled and not self.suspended:
			self._draw()
			self.stream.write("\n")
			self.stream.flush()


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
			if rel_dir == ROOT_KEY and name == CHANGES_DIR:
				continue
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


def build_tree(flat):
	if ROOT_KEY not in flat:
		raise ChkanError("в слепке нет корня")
	nodes = {ROOT_KEY: dict(flat[ROOT_KEY])}
	paths = sorted((p for p in flat if p != ROOT_KEY), key=lambda p: (p.count("/"), p))
	for path in paths:
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
		"tz": tz_string(now),
		"exclude": list(rules.exclude),
	}


def make_snapshot(root, now, rules, flat):
	head = make_head(root, now, rules)
	return {
		"format": FORMAT,
		"root": root,
		"taken_at": head["taken_at"],
		"tz": head["tz"],
		"exclude": head["exclude"],
		"tree": build_tree(flat),
	}


def snapshot_head(snap):
	return {"taken_at": snap["taken_at"], "tz": snap["tz"], "exclude": list(snap["exclude"])}


def snapshots_equal(a, b):
	return snapshot_head(a) == snapshot_head(b) and a["root"] == b["root"] and flatten(a["tree"]) == flatten(b["tree"])


def tz_string(dt):
	off = dt.utcoffset() or datetime.timedelta(0)
	total = int(off.total_seconds())
	sign = "-" if total < 0 else "+"
	minutes = abs(total) // 60
	return f"{sign}{minutes // 60:02d}:{minutes % 60:02d}"


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


def make_ops(old_flat, new_flat, old_under, new_under):
	ops = []
	for p in sorted(old_flat.keys() & new_flat.keys()):
		ch = record_changes(old_flat[p], new_flat[p])
		if ch:
			ops.append({"op": "change", "path": p, "set": ch})

	plain_removed, plain_added = [], []
	for p in sorted(old_flat.keys() - new_flat.keys()):
		if new_under(p):
			ops.append({"op": "remove_scan", "path": p, "old": old_flat[p]})
		else:
			plain_removed.append(p)
	for p in sorted(new_flat.keys() - old_flat.keys()):
		if old_under(p):
			ops.append({"op": "add_scan", "path": p, "new": new_flat[p]})
		else:
			plain_added.append(p)

	by_id = {}
	for p in plain_removed:
		r = old_flat[p]
		by_id.setdefault((r["type"], r["dev"], r["ino"]), []).append(p)
	used = set()
	moved_to = {}
	for p in plain_added:
		r = new_flat[p]
		cands = [c for c in by_id.get((r["type"], r["dev"], r["ino"]), []) if c not in used and same_identity(old_flat[c], r)]
		if not cands:
			continue
		base = p.rpartition("/")[2]
		pick = next((c for c in cands if c.rpartition("/")[2] == base), cands[0])
		used.add(pick)
		moved_to[p] = pick

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


def op_to_node(op):
	node = {"op": op["op"]}
	for k in ("new", "old", "from", "set"):
		if k in op:
			node[k] = op[k]
	return node


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


def build_diff_flat(old_head, old_flat, new_head, new_flat, rules, root):
	old_under = make_excluder(old_head["exclude"])
	new_under = make_excluder(new_head["exclude"])
	ops = make_ops(old_flat, new_flat, old_under, new_under)
	resolver = rules.resolver()

	groups, scan_nodes = {}, {}
	for op in ops:
		node = op_to_node(op)
		if op["op"] in ("add_scan", "remove_scan"):
			scan_nodes[op["path"]] = node
		else:
			groups.setdefault(resolver.tags(op["path"]), {})[op["path"]] = node

	sizes = {}
	for path, rec in old_flat.items():
		if rec["type"] == "f" and not new_under(path):
			sizes.setdefault(resolver.tags(path), [0, 0])[0] += rec["size"]
	for path, rec in new_flat.items():
		if rec["type"] == "f" and not old_under(path):
			sizes.setdefault(resolver.tags(path), [0, 0])[1] += rec["size"]

	keys = set(groups) | {t for t, (a, b) in sizes.items() if a != b}
	entries = []
	for tags in keys:
		group = {"size": list(sizes.get(tags, [0, 0]))}
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
		"groups": out_groups,
	}
	if scan_nodes:
		diff["scan"] = {"ch": build_ch(scan_nodes)}
	return diff


def build_diff(old_snap, new_snap, rules):
	return build_diff_flat(
		snapshot_head(old_snap),
		flatten(old_snap["tree"]),
		snapshot_head(new_snap),
		flatten(new_snap["tree"]),
		rules,
		new_snap["root"],
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


def node_to_op(path, node):
	op = {"op": node["op"], "path": path}
	for k in ("new", "old", "from", "set"):
		if k in node:
			op[k] = node[k]
	return op


def diff_ops(diff):
	ops = []
	for key, group in diff.get("groups", {}).items():
		if key == SEPARATOR_KEY or not isinstance(group, dict):
			continue
		ops.extend(node_to_op(p, n) for p, n in iter_nodes(group.get("ch", {})))
	ops.extend(node_to_op(p, n) for p, n in iter_nodes(diff.get("scan", {}).get("ch", {})))
	return ops


def count_objects(diff):
	"""(без тегов, с отслеживаемыми тегами): узлы с op до разделителя."""
	untagged = tracked = 0
	for key, group in diff.get("groups", {}).items():
		if key == SEPARATOR_KEY:
			break
		n = sum(1 for _ in iter_nodes(group.get("ch", {})))
		if key == NO_TAGS_KEY:
			untagged += n
		else:
			tracked += n
	return untagged, tracked


def invert_op(op):
	kind, path = op["op"], op["path"]
	try:
		if kind in ("add", "add_scan"):
			return {"op": kind.replace("add", "remove"), "path": path, "old": op["new"]}
		if kind in ("remove", "remove_scan"):
			return {"op": kind.replace("remove", "add"), "path": path, "new": op["old"]}
		if kind == "change":
			return {"op": "change", "path": path, "set": {k: [b, a] for k, (a, b) in op["set"].items()}}
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
		if b is None and k == "target":
			out.pop(k, None)
		else:
			out[k] = b
	return ordered(out)


def apply_ops(flat, ops, strict=True):
	"""Применяет операции атомарно: сначала все удаления, потом вставки."""
	removals, changes, inserts = [], [], []
	try:
		for op in ops:
			kind, path = op["op"], op["path"]
			if kind in ("remove", "remove_scan"):
				if path not in flat:
					raise DiffError(f"{path}: нечего удалять")
				if strict and flat[path] != ordered(op["old"]):
					raise DiffError(f"{path}: запись не совпадает с old из diff")
				removals.append(path)
			elif kind in ("add", "add_scan"):
				inserts.append((path, ordered(op["new"])))
			elif kind == "change":
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
		"tz": dst_head["tz"],
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
		raise ChkanError(f"{path}: формат {data.get('format')!r}, ожидался {FORMAT}")
	return data


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


# ---------------------------------------------------------------- основной сценарий


def run(root, assume_yes=False, warn_yes=False):
	changes = os.path.join(root, CHANGES_DIR)
	if not os.path.isdir(changes):
		if not assume_yes:
			try:
				answer = input(f"Папка {changes} не найдена.\nСоздать её и сделать первый скан (может занять время)? [y/N] ")
			except EOFError:
				answer = ""
			if answer.strip().lower() not in YES_ANSWERS:
				print("Отмена.")
				return 1
		os.makedirs(changes)
		tags_path = os.path.join(changes, TAGS_FILE)
		if not os.path.exists(tags_path):
			write_text_atomic(tags_path, TAGS_TEMPLATE)
	tags_path = os.path.join(changes, TAGS_FILE)
	if not os.path.isfile(tags_path):
		raise ChkanError(f"нет файла {tags_path}")
	with open(tags_path, encoding="utf-8", errors="surrogateescape") as f:
		rules = parse_tags(f.read())

	scans = list_scans(changes)
	prev = prev_flat = None
	if scans:
		prev = read_json(os.path.join(changes, scan_name(scans[-1])))
		prev_flat = flatten(prev["tree"])
	now, stamp = next_moment(scans[-1] if scans else None)

	progress = Progress(total=len(prev_flat) if prev_flat is not None else None)
	printer = WarningPrinter(progress)
	flat, warnings = scan_fs(root, rules, progress, printer)
	progress.finish()
	printer.finish()

	if warnings and not warn_yes:
		try:
			answer = input(f"При сканировании были предупреждения ({len(warnings)}). Продолжить и сохранить результат? [y/N] ")
		except EOFError:
			answer = ""
		if answer.strip().lower() not in YES_ANSWERS:
			print("Отмена: ничего не сохранено.")
			return 1

	snapshot = make_snapshot(root, now, rules, flat)
	write_text_atomic(os.path.join(changes, scan_name(stamp)), dumps_snapshot(snapshot))

	if prev is None:
		print(f"первый скан сохранён: {len(flat)} объектов ({scan_name(stamp)})")
		return 0

	diff = build_diff_flat(snapshot_head(prev), prev_flat, snapshot_head(snapshot), flat, rules, root)
	diff_path = os.path.join(changes, diff_name(scans[-1], stamp))
	write_text_atomic(diff_path, dumps_pretty(diff) + "\n")

	restored = apply_diff(snapshot, read_json(diff_path), backward=True)
	if not snapshots_equal(restored, prev):
		raise ChkanError(
			f"предыдущий скан не восстанавливается из нового слепка и {os.path.basename(diff_path)}; "
			f"предыдущий скан оставлен"
		)
	if scans[-1] != scans[0]:
		os.remove(os.path.join(changes, scan_name(scans[-1])))

	untagged, tracked = count_objects(diff)
	print(f"без тегов: {untagged}")
	print(f"с отслеживаемыми тегами: {tracked}")
	return 0


def main(argv=None):
	parser = argparse.ArgumentParser(prog="chkan.py", description="Скан ФС и обратимый diff в ./.changes/")
	parser.add_argument("dir", nargs="?", default=".", help="папка, в которой лежит .changes (по умолчанию текущая)")
	parser.add_argument("-y", "--yes", action="store_true", help="не спрашивать подтверждение при создании .changes")
	parser.add_argument(
		"-w", "--warn-yes", action="store_true",
		help="если при сканировании были предупреждения, не спрашивать, а продолжать",
	)
	args = parser.parse_args(argv)
	try:
		return run(os.path.abspath(args.dir), assume_yes=args.yes, warn_yes=args.warn_yes)
	except ChkanError as e:
		print(f"Ошибка: {e}", file=sys.stderr)
		return 2


if __name__ == "__main__":
	sys.exit(main())
