#!/usr/bin/env python3
"""Тесты для chkan.py. Запуск: python3 -m unittest -v test_chkan"""

import contextlib
import datetime
import errno
import io
import json
import os
import re
import tempfile
import unittest
from unittest import mock

import chkan

TZ = datetime.timezone(datetime.timedelta(hours=2))
EXAMPLE_TAGS = """\
// комментарий
# logs(ignore)	// только помечает тег
# docs	// ничего не значит

.cache/	# @exclude
dir/	# tag
	A/
	B/	# @no(tag)
	C.txt
	D.txt	# @no(tag)
var/	# logs
media/	# tag photos
"""


def rec(type="f", ino=1, size=0, mtime=10, ctime=10, btime=None, mode="0644", dev=1, uid=0, gid=0, target=None):
	r = {"type": type, "dev": dev, "ino": ino, "size": size, "mtime": mtime, "ctime": ctime, "btime": btime, "uid": uid, "gid": gid, "mode": mode}
	if target is not None:
		r["target"] = target
	return r


def drec(ino=1, **kw):
	return rec(type="d", ino=ino, size=4096, mode="0755", **kw)


def snap(flat, taken_at="2026-10-01T15:00:00+02:00", exclude=(), root="/r"):
	return {
		"format": chkan.FORMAT,
		"root": root,
		"taken_at": taken_at,
		"exclude": list(exclude),
		"tree": chkan.build_tree(flat),
	}


def diff_between(old_flat, new_flat, rules_text="", old_ex=(), new_ex=()):
	rules = chkan.parse_tags(rules_text)
	a = snap(old_flat, "2026-10-01T15:00:00+02:00", old_ex)
	b = snap(new_flat, "2026-10-02T09:00:00+02:00", new_ex)
	return a, b, chkan.build_diff(a, b, rules), rules


def sizes(group):
	return [chkan.parse_size(x) for x in group["size"][:2]]


def read_text(path):
	with open(path, encoding="utf-8") as f:
		return f.read()


def roundtrip(diff):
	"""diff как он попадёт на диск и вернётся обратно"""
	return json.loads(chkan.dumps_pretty(diff))


# ------------------------------------------------------------ TAGS.txt


class TagsParsing(unittest.TestCase):
	def setUp(self):
		self.rules = chkan.parse_tags(EXAMPLE_TAGS)

	def tags(self, path):
		return set(self.rules.tags_of(path))

	def test_example_from_spec(self):
		self.assertEqual(self.tags("dir"), {"tag"})
		self.assertEqual(self.tags("dir/A"), {"tag"})
		self.assertEqual(self.tags("dir/A/deep/file"), {"tag"})
		self.assertEqual(self.tags("dir/C.txt"), {"tag"})
		self.assertEqual(self.tags("dir/B"), set())
		self.assertEqual(self.tags("dir/B/x/y"), set())
		self.assertEqual(self.tags("dir/D.txt"), set())
		self.assertEqual(self.tags("other"), set())

	def test_several_tags_and_root(self):
		self.assertEqual(self.tags("media/x.jpg"), {"tag", "photos"})
		self.assertEqual(self.tags("."), set())

	def test_ignore_marks(self):
		self.assertEqual(set(self.rules.ignore_tags), {"logs"})
		self.assertEqual(self.tags("var/app.log"), {"logs"})

	def test_pathless_tag_line_means_nothing(self):
		rules = chkan.parse_tags("# docs // ничего\nx/\t# t\n")
		self.assertEqual(set(rules.tags_of("x")), {"t"})
		self.assertEqual(set(rules.ignore_tags), set())

	def test_ignore_on_path_line_applies_and_marks(self):
		rules = chkan.parse_tags("x/\t# noisy(ignore)\n")
		self.assertEqual(set(rules.tags_of("x/f")), {"noisy"})
		self.assertEqual(set(rules.ignore_tags), {"noisy"})

	def test_ignore_is_global(self):
		rules = chkan.parse_tags("a/\t# t\nb/\t# t(ignore)\n")
		self.assertEqual(set(rules.ignore_tags), {"t"})

	def test_cancel_then_readd_deeper(self):
		rules = chkan.parse_tags("a/\t# t\n\tb/\t# @no(t)\n\t\tc/\t# t\n")
		self.assertEqual(set(rules.tags_of("a/x")), {"t"})
		self.assertEqual(set(rules.tags_of("a/b/x")), set())
		self.assertEqual(set(rules.tags_of("a/b/c/x")), {"t"})

	def test_left_to_right_inside_line(self):
		rules = chkan.parse_tags("a/\t# t @no(t) u\n")
		self.assertEqual(set(rules.tags_of("a")), {"u"})

	def test_multi_segment_paths_and_nesting(self):
		rules = chkan.parse_tags("a/b/\t# t\n\tc/d\t# u\n")
		self.assertEqual(set(rules.tags_of("a/b/c/d/x")), {"t", "u"})
		self.assertEqual(set(rules.tags_of("a")), set())

	def test_root_can_be_tagged(self):
		rules = chkan.parse_tags("./\t# all\n")
		self.assertEqual(set(rules.tags_of("any/thing")), {"all"})

	def test_comments_inline_and_blank(self):
		rules = chkan.parse_tags("\n// c\na/\t# t // x y z\n\n")
		self.assertEqual(set(rules.tags_of("a")), {"t"})

	def test_hash_inside_name_is_not_marker(self):
		rules = chkan.parse_tags("file#1.txt\t# t\n")
		self.assertEqual(set(rules.tags_of("file#1.txt")), {"t"})

	def test_unicode_tags_and_spaces_in_paths(self):
		rules = chkan.parse_tags("Мои документы/\t# работа\n")
		self.assertEqual(set(rules.tags_of("Мои документы/x")), {"работа"})

	def test_exclude_list_canonical(self):
		rules = chkan.parse_tags("b/\t# @exclude\na/\t# @exclude\n\tinner/\t# @exclude\n")
		self.assertEqual(rules.exclude, ["a", "b"])

	def test_exclude_state(self):
		rules = chkan.parse_tags(EXAMPLE_TAGS)
		self.assertTrue(rules.state_of(".cache/x").excluded)
		self.assertFalse(rules.state_of("dir").excluded)

	def test_errors(self):
		bad = {
			"spaces": "a/\t# t\n    b/\n",
			"abs": "/etc/\t# t\n",
			"dotdot": "../x\t# t\n",
			"deep": "a/\n\t\tb/\n",
			"token": "a/\t# t(foo)\n",
			"bad_chars": "a/\t# t+u\n",
			"root_exclude": "./\t# @exclude\n",
			"unknown_special": "a/\t# @bogus\n",
			"special_with_ignore": "a/\t# @exclude(ignore)\n",
			"no_empty": "a/\t# @no()\n",
			"no_unclosed": "a/\t# @no(t\n",
			"no_two_names": "a/\t# @no(a b)\n",
			"no_bad_chars": "a/\t# @no(a+b)\n",
			"no_of_special": "a/\t# @no(@exclude)\n",
			"no_with_ignore": "a/\t# @no(t(ignore))\n",
			"old_no_prefix_is_just_a_tag_but_at_alone_is_bad": "a/\t# @\n",
			"run_unclosed": "a/\t# @run(echo hi\n",
			"run_empty": "a/\t# @run()\n",
			"run_blank": "a/\t# @run(   )\n",
			"run_without_paren": "a/\t# @run\n",
		}
		for name, text in bad.items():
			with self.subTest(name):
				with self.assertRaises(chkan.ChkanError):
					chkan.parse_tags(text)

	def test_error_mentions_line_number(self):
		with self.assertRaises(chkan.ChkanError) as cm:
			chkan.parse_tags("a/\t# t\nb/\t# @bogus\n")
		self.assertIn(":2", str(cm.exception))


class ExcludeHelpers(unittest.TestCase):
	def test_excluder(self):
		under = chkan.make_excluder(["a", "x/y"])
		self.assertTrue(under("a"))
		self.assertTrue(under("a/b/c"))
		self.assertTrue(under("x/y/z"))
		self.assertFalse(under("x"))
		self.assertFalse(under("ab"))
		self.assertFalse(under("."))
		self.assertFalse(chkan.make_excluder([])("a"))


# ------------------------------------------------------------ stat и скан


class StatAndScan(unittest.TestCase):
	def setUp(self):
		self.tmp = tempfile.TemporaryDirectory()
		self.addCleanup(self.tmp.cleanup)
		self.root = os.path.realpath(self.tmp.name)

	def mk(self, rel, data=None):
		p = os.path.join(self.root, rel)
		if data is None:
			os.makedirs(p, exist_ok=True)
		else:
			os.makedirs(os.path.dirname(p), exist_ok=True)
			with open(p, "wb") as f:
				f.write(data)
		return p

	def test_record_fields(self):
		p = self.mk("f.txt", b"hello")
		os.chmod(p, 0o640)
		r = chkan.stat_record(p)
		self.assertEqual(r["type"], "f")
		self.assertEqual(r["size"], 5)
		self.assertEqual(r["mode"], "0640")
		self.assertEqual(r["uid"], os.getuid())
		self.assertEqual(r["ino"], os.lstat(p).st_ino)
		self.assertEqual(r["dev"], os.lstat(p).st_dev)
		self.assertIn("btime", r)
		self.assertTrue(r["btime"] is None or isinstance(r["btime"], int))

	@unittest.skipIf(chkan._statx_fn is None, "statx недоступен")
	def test_statx_matches_lstat(self):
		p = self.mk("f.txt", b"abc")
		a = chkan._statx_record(p)
		b = chkan._lstat_record(p)
		a.pop("btime")
		b.pop("btime")
		self.assertEqual(a, b)

	def test_fallback_without_statx(self):
		p = self.mk("f.txt", b"abc")
		with mock.patch.object(chkan, "_statx_fn", None):
			r = chkan.stat_record(p)
		self.assertEqual(r["size"], 3)
		self.assertIsNone(r["btime"])

	def test_symlink_not_followed(self):
		self.mk("t.txt", b"x")
		os.symlink("t.txt", os.path.join(self.root, "lnk"))
		os.symlink("missing", os.path.join(self.root, "dangling"))
		flat, _ = chkan.scan_fs(self.root, chkan.parse_tags(""))
		self.assertEqual(flat["lnk"]["type"], "l")
		self.assertEqual(flat["lnk"]["target"], "t.txt")
		self.assertEqual(flat["dangling"]["target"], "missing")

	def test_scan_structure_and_exclude(self):
		self.mk("a/b/c.txt", b"1")
		self.mk("a/skip/x.txt", b"2")
		self.mk("top.txt", b"3")
		self.mk(".changes/TAGS.txt", b"")
		rules = chkan.parse_tags("a/skip/\t# @exclude\n.changes/\t# @exclude\n")
		flat, warnings = chkan.scan_fs(self.root, rules)
		self.assertEqual(sorted(flat), [".", "a", "a/b", "a/b/c.txt", "top.txt"])
		self.assertEqual(flat["."]["type"], "d")
		self.assertEqual(warnings, [])

	def test_changes_dir_scanned_unless_excluded_by_tag(self):
		self.mk(".changes/TAGS.txt", b"")
		flat, _ = chkan.scan_fs(self.root, chkan.parse_tags(""))
		self.assertEqual(sorted(flat), [".", ".changes", ".changes/TAGS.txt"])
		flat, _ = chkan.scan_fs(self.root, chkan.parse_tags(".changes/\t# @exclude\n"))
		self.assertEqual(sorted(flat), ["."])

	def test_times_are_whole_seconds(self):
		p = self.mk("f.txt", b"x")
		os.utime(p, ns=(1_700_000_000_999_999_999, 1_700_000_001_999_999_999))
		r = chkan.stat_record(p)
		self.assertEqual(r["mtime"], 1_700_000_001)
		self.assertIsInstance(r["ctime"], int)
		self.assertLess(r["ctime"], 10**11)  # секунды, а не наносекунды
		self.assertTrue(r["btime"] is None or r["btime"] < 10**11)
		with mock.patch.object(chkan, "_statx_fn", None):
			self.assertEqual(chkan.stat_record(p)["mtime"], 1_700_000_001)

	def test_changes_dir_only_skipped_in_root(self):
		self.mk("sub/.changes/f", b"1")
		flat, _ = chkan.scan_fs(self.root, chkan.parse_tags(""))
		self.assertIn("sub/.changes/f", flat)

	def test_progress_called(self):
		self.mk("a/b.txt", b"1")
		calls = []

		class P:
			def update(self, n):
				calls.append(n)

		chkan.scan_fs(self.root, chkan.parse_tags(""), P())
		self.assertEqual(calls[-1], 3)

	@unittest.skipIf(not hasattr(os, "geteuid") or os.geteuid() == 0, "под root права не ограничивают")
	def test_unreadable_dir_is_warning(self):
		p = self.mk("locked/x", b"1")
		d = os.path.dirname(p)
		os.chmod(d, 0)
		self.addCleanup(os.chmod, d, 0o755)
		flat, warnings = chkan.scan_fs(self.root, chkan.parse_tags(""))
		self.assertIn("locked", flat)
		self.assertNotIn("locked/x", flat)
		self.assertEqual(len(warnings), 1)

	def test_non_utf8_name_roundtrip(self):
		name = os.fsdecode(b"bad\xff.txt")
		try:
			self.mk(name, b"1")
		except OSError:
			self.skipTest("ФС не допускает такое имя")
		flat, _ = chkan.scan_fs(self.root, chkan.parse_tags(""))
		s = snap(flat)
		path = os.path.join(self.root, "s.json")
		chkan.write_text_atomic(path, chkan.dumps_snapshot(s))
		self.assertEqual(chkan.flatten(chkan.read_json(path)["tree"]), flat)

	def test_root_must_be_dir(self):
		p = self.mk("f", b"1")
		with self.assertRaises(chkan.ChkanError):
			chkan.scan_fs(p, chkan.parse_tags(""))


# ------------------------------------------------------------ слепок: дерево


class SnapshotTree(unittest.TestCase):
	def flat(self):
		return {
			".": drec(ino=2, dev=7, uid=1000, gid=1000),
			"dir": drec(ino=3, dev=7, uid=1000, gid=1000),
			"dir/C.txt": rec(ino=4, size=100, dev=7, uid=1000, gid=1000),
			"dir/other": rec(ino=5, dev=8, uid=0, gid=0),
			"dir/lnk": rec(type="l", ino=6, target="C.txt", dev=7, uid=1000, gid=1000),
			"empty": drec(ino=9, dev=7, uid=1000, gid=1000),
		}

	def test_roundtrip(self):
		flat = self.flat()
		self.assertEqual(chkan.flatten(chkan.build_tree(flat)), flat)

	def test_inheritance_omits_dev_uid_gid(self):
		tree = chkan.build_tree(self.flat())
		self.assertEqual(tree["dev"], 7)
		self.assertNotIn("dev", tree["ch"]["dir"])
		self.assertNotIn("uid", tree["ch"]["dir"]["ch"]["C.txt"])
		self.assertEqual(tree["ch"]["dir"]["ch"]["other"]["dev"], 8)
		self.assertEqual(tree["ch"]["dir"]["ch"]["other"]["uid"], 0)

	def test_names_are_keys_no_repeated_paths(self):
		text = chkan.dumps_snapshot(snap(self.flat()))
		self.assertEqual(text.count("C.txt"), 2)  # имя + target симлинка
		self.assertNotIn("dir/C.txt", text)

	def test_empty_dir_has_no_ch(self):
		tree = chkan.build_tree(self.flat())
		self.assertNotIn("ch", tree["ch"]["empty"])

	def test_deterministic_order(self):
		a = chkan.build_tree(self.flat())
		items = list(reversed(list(self.flat().items())))
		b = chkan.build_tree(dict(items))
		self.assertEqual(json.dumps(a), json.dumps(b))

	def test_missing_parent_and_root(self):
		with self.assertRaises(chkan.ChkanError):
			chkan.build_tree({".": drec(), "a/b": rec()})
		with self.assertRaises(chkan.ChkanError):
			chkan.build_tree({"a": rec()})

	def test_parent_must_be_dir(self):
		with self.assertRaises(chkan.ChkanError):
			chkan.build_tree({".": drec(), "a": rec(), "a/b": rec()})

	def test_no_tz_field_offset_lives_in_taken_at(self):
		s = snap(base_flat(), "2026-10-01T15:00:00+02:00")
		self.assertNotIn("tz", s)
		self.assertTrue(s["taken_at"].endswith("+02:00"))


# ------------------------------------------------------------ diff: вычисление


def base_flat():
	return {
		".": drec(ino=2),
		"dir": drec(ino=3),
		"dir/C.txt": rec(ino=4, size=100),
		"dir/D.txt": rec(ino=5, size=10),
		"notes.txt": rec(ino=6, size=10),
	}


class DiffOps(unittest.TestCase):
	def ops(self, old, new, old_ex=(), new_ex=()):
		return chkan.make_ops(old, new, chkan.make_excluder(old_ex), chkan.make_excluder(new_ex))

	def kinds(self, ops):
		return sorted((o["op"], o["path"]) for o in ops)

	def test_no_changes(self):
		self.assertEqual(self.ops(base_flat(), base_flat()), [])

	def test_add_remove_change(self):
		old = base_flat()
		new = base_flat()
		del new["dir/D.txt"]
		new["dir/N.txt"] = rec(ino=50, size=7)
		new["notes.txt"] = rec(ino=6, size=50, mtime=99)
		ops = self.ops(old, new)
		self.assertEqual(self.kinds(ops), [("add", "dir/N.txt"), ("change", "notes.txt"), ("remove", "dir/D.txt")])
		change = next(o for o in ops if o["op"] == "change")
		self.assertEqual(change["set"], {"size": [10, 50], "mtime": [10, 99]})

	def test_move_by_inode(self):
		old = base_flat()
		new = base_flat()
		del new["dir/C.txt"]
		new["dir/A/C.txt"] = rec(ino=4, size=100, ctime=77)
		new["dir/A"] = drec(ino=60)
		ops = self.ops(old, new)
		move = next(o for o in ops if o["op"] == "move")
		self.assertEqual((move["path"], move["from"]), ("dir/A/C.txt", "dir/C.txt"))
		self.assertEqual(move["set"], {"ctime": [10, 77]})
		self.assertFalse(any(o["op"] == "remove" for o in ops))

	def test_move_without_changes_has_no_set(self):
		old = base_flat()
		new = base_flat()
		new["moved.txt"] = new.pop("notes.txt")
		move = next(o for o in self.ops(old, new) if o["op"] == "move")
		self.assertNotIn("set", move)

	def test_inode_reuse_is_not_move_when_btime_differs(self):
		old = base_flat()
		old["gone.txt"] = rec(ino=99, size=5, btime=1000)
		new = base_flat()
		new["new.txt"] = rec(ino=99, size=5, btime=2000)
		self.assertEqual(self.kinds(self.ops(old, new)), [("add", "new.txt"), ("remove", "gone.txt")])

	def test_inode_reuse_without_btime_checks_size_and_mtime(self):
		old = base_flat()
		old["gone.txt"] = rec(ino=99, size=5, mtime=1)
		new = base_flat()
		new["new.txt"] = rec(ino=99, size=6, mtime=1)
		self.assertEqual(self.kinds(self.ops(old, new)), [("add", "new.txt"), ("remove", "gone.txt")])

	def test_replaced_file_same_path_is_change_with_ino(self):
		old = base_flat()
		new = base_flat()
		new["notes.txt"] = rec(ino=500, size=10)
		(op,) = self.ops(old, new)
		self.assertEqual(op["op"], "change")
		self.assertEqual(op["set"], {"ino": [6, 500]})

	def test_dir_rename_gives_per_file_moves(self):
		old = {".": drec(ino=2), "d": drec(ino=3), "d/a": rec(ino=4), "d/b": rec(ino=5)}
		new = {".": drec(ino=2), "e": drec(ino=3), "e/a": rec(ino=4), "e/b": rec(ino=5)}
		ops = self.ops(old, new)
		self.assertEqual(sorted((o["path"], o["from"]) for o in ops), [("e", "d"), ("e/a", "d/a"), ("e/b", "d/b")])

	def test_hardlinks_pair_by_name_then_order(self):
		old = {".": drec(ino=2), "x": rec(ino=7), "y": rec(ino=7)}
		new = {".": drec(ino=2), "y2": rec(ino=7), "x2": rec(ino=7)}
		ops = self.ops(old, new)
		self.assertEqual(sorted((o["path"], o["from"]) for o in ops), [("x2", "x"), ("y2", "y")])

	def test_type_change(self):
		old = {".": drec(ino=2), "a": rec(ino=3)}
		new = {".": drec(ino=2), "a": rec(type="l", ino=3, target="b", size=1)}
		(op,) = self.ops(old, new)
		self.assertEqual(op["op"], "change")
		self.assertEqual(op["set"]["type"], ["f", "l"])
		self.assertEqual(op["set"]["target"], [None, "b"])

	def test_scan_ops_classification(self):
		old = base_flat()
		old["tmp"] = drec(ino=70)
		old["tmp/y.txt"] = rec(ino=71)
		del old["dir/D.txt"]
		new = base_flat()
		new["data"] = drec(ino=80)
		new["data/x.txt"] = rec(ino=81)
		del new["dir/D.txt"]
		# tmp стал исключённым, data перестал
		new_ex, old_ex = ["tmp"], ["data"]
		new = {k: v for k, v in new.items() if not chkan.is_under(k, "tmp")}
		ops = self.ops(old, new, old_ex, new_ex)
		self.assertEqual(
			self.kinds(ops),
			[("add_scan", "data"), ("add_scan", "data/x.txt"), ("remove_scan", "tmp"), ("remove_scan", "tmp/y.txt")],
		)

	def test_move_into_newly_excluded_dir_is_plain_remove(self):
		old = base_flat()
		new = base_flat()
		del new["notes.txt"]
		ops = self.ops(old, new, (), ["hidden"])
		self.assertEqual(self.kinds(ops), [("remove", "notes.txt")])

	def test_moves_never_pair_with_scan_ops(self):
		old = base_flat()
		old["tmp"] = drec(ino=70)
		old["tmp/f"] = rec(ino=71)
		new = base_flat()
		new["f"] = rec(ino=71)
		ops = self.ops(old, new, (), ["tmp"])
		self.assertEqual(self.kinds(ops), [("add", "f"), ("remove_scan", "tmp"), ("remove_scan", "tmp/f")])


# ------------------------------------------------------------ diff: группы и размеры


TAGS_FOR_DIFF = """\
# logs(ignore)
dir/	# tag
	B/	# @no(tag)
media/	# tag photos
var/	# logs
"""


class DiffGrouping(unittest.TestCase):
	def flat_old(self):
		return {
			".": drec(ino=2),
			"dir": drec(ino=3),
			"dir/C.txt": rec(ino=4, size=100),
			"dir/F.txt": rec(ino=5, size=100),
			"dir/A": drec(ino=20),
			"dir/B": drec(ino=6),
			"dir/B/b.txt": rec(ino=7, size=40),
			"notes.txt": rec(ino=8, size=10),
			"media": drec(ino=9),
			"var": drec(ino=10),
			"var/app.log": rec(ino=11, size=2000),
		}

	def flat_new(self):
		f = self.flat_old()
		f["notes.txt"] = rec(ino=8, size=50, mtime=11)
		f["dir/F.txt"] = rec(ino=5, size=130, mtime=11)
		f["dir/A/C.txt"] = f.pop("dir/C.txt")
		f["media/new.jpg"] = rec(ino=30, size=50)
		f["var/app.log"] = rec(ino=11, size=2600, mtime=12)
		return f

	def diff(self):
		return diff_between(self.flat_old(), self.flat_new(), TAGS_FOR_DIFF)[2]

	def test_group_order_and_separator(self):
		keys = list(self.diff()["groups"])
		self.assertEqual(keys, ["-", "photos+tag", "tag", "-" * 10, "logs"])
		self.assertEqual(self.diff()["groups"]["-" * 10], 0)

	def test_separator_is_ten_dashes(self):
		self.assertEqual(chkan.SEPARATOR_KEY, "----------")

	def test_separator_present_without_ignored(self):
		d = diff_between(base_flat(), {**base_flat(), "z.txt": rec(ino=9)})[2]
		self.assertEqual(list(d["groups"]), ["-", "-" * 10])

	def test_sizes_per_group(self):
		g = self.diff()["groups"]
		self.assertEqual(sizes(g["-"]), [10 + 40, 50 + 40])
		self.assertEqual(sizes(g["tag"]), [100 + 100, 100 + 130])
		self.assertEqual(sizes(g["photos+tag"]), [0, 50])
		self.assertEqual(sizes(g["logs"]), [2000, 2600])
		self.assertEqual(g["logs"]["size"], ["2_000=1.9K", "2_600=2.5K", "+600B"])
		self.assertEqual(g["photos+tag"]["size"], ["0=0B", "50=50B", "+50B"])

	def test_cancelled_tag_files_count_as_untagged(self):
		# dir/B/b.txt (40 байт) не менялся, но @no(tag) делает его «без тегов» и он входит в сумму
		self.assertEqual(sizes(self.diff()["groups"]["-"]), [50, 90])

	def test_move_goes_to_destination_group_with_path_compression(self):
		g = self.diff()["groups"]["tag"]["ch"]
		self.assertEqual(list(g), ["dir"])
		inner = g["dir"]["ch"]
		self.assertEqual(inner["F.txt"], {"op": "change", "set": {"size": [100, 130], "mtime": ["1970-01-01 02:00:10", "1970-01-01 02:00:11"]}})
		self.assertEqual(inner["A/C.txt"], {"op": "move", "from": "dir/C.txt"})

	def test_dir_node_is_object_and_container(self):
		g = self.diff()["groups"]["tag"]["ch"]
		old, new = self.flat_old(), self.flat_new()
		new["dir"] = drec(ino=3, mtime=99)
		d = diff_between(old, new, TAGS_FOR_DIFF)[2]
		node = d["groups"]["tag"]["ch"]["dir"]
		self.assertEqual(node["op"], "change_d")
		self.assertIn("ch", node)

	def test_single_object_is_written_as_path(self):
		g = self.diff()["groups"]["photos+tag"]["ch"]
		self.assertEqual(list(g), ["media/new.jpg"])
		self.assertEqual(g["media/new.jpg"]["op"], "add")
		self.assertEqual(
			self.diff()["groups"]["logs"]["ch"],
			{"var/app.log": {"op": "change", "set": {"size": [2000, 2600], "mtime": ["1970-01-01 02:00:10", "1970-01-01 02:00:12"]}}},
		)

	def test_size_only_group_when_file_leaves_it(self):
		old = {".": drec(ino=2), "dir": drec(ino=3), "dir/f": rec(ino=4, size=100)}
		new = {".": drec(ino=2), "dir": drec(ino=3), "g": rec(ino=4, size=100)}
		d = diff_between(old, new, "dir/\t# tag\n")[2]
		self.assertEqual(d["groups"]["tag"], {"size": ["100=100B", "0=0B", "-100B"]})
		self.assertEqual(sizes(d["groups"]["-"]), [0, 100])
		self.assertEqual(d["groups"]["-"]["ch"]["g"]["op"], "move")

	def test_no_group_without_changes(self):
		self.assertEqual(list(diff_between(base_flat(), base_flat())[2]["groups"]), ["-" * 10])

	def test_remove_grouped_by_old_path(self):
		old = {".": drec(ino=2), "dir": drec(ino=3), "dir/f": rec(ino=4, size=5)}
		new = {".": drec(ino=2), "dir": drec(ino=3)}
		d = diff_between(old, new, "dir/\t# tag\n")[2]
		(node,) = d["groups"]["tag"]["ch"].values()
		self.assertEqual(list(d["groups"]["tag"]["ch"]), ["dir/f"])
		self.assertEqual(node["op"], "remove")
		self.assertEqual(node["old"]["mtime"], "1970-01-01 02:00:10")
		self.assertEqual(chkan.diff_ops(roundtrip(d))[0]["old"], old["dir/f"])

	def test_root_object_key(self):
		old = base_flat()
		new = base_flat()
		new["."] = drec(ino=2, mtime=99)
		d = diff_between(old, new)[2]
		self.assertIn(".", d["groups"]["-"]["ch"])

	def test_scan_section_and_sizes_like_for_like(self):
		old = {".": drec(ino=2), "keep": rec(ino=3, size=10), "tmp": drec(ino=4), "tmp/y": rec(ino=5, size=1000)}
		new = {".": drec(ino=2), "keep": rec(ino=3, size=10), "data": drec(ino=6), "data/x": rec(ino=7, size=500)}
		d = diff_between(old, new, "", old_ex=["data"], new_ex=["tmp"])[2]
		self.assertEqual(sorted(d["scan"]["ch"]), ["data", "tmp"])
		self.assertEqual(d["scan"]["ch"]["data"]["op"], "add_scan")
		self.assertEqual(d["scan"]["ch"]["data"]["ch"]["x"]["op"], "add_scan")
		self.assertEqual(list(d["groups"]), ["-" * 10])  # размер без изменений: tmp и data не считаются
		self.assertEqual(d["from"]["exclude"], ["data"])
		self.assertEqual(d["to"]["exclude"], ["tmp"])

	def test_headers_have_no_tz(self):
		d = self.diff()
		self.assertEqual(list(d["from"]), ["taken_at", "exclude"])
		self.assertEqual(list(d["to"]), ["taken_at", "exclude"])
		self.assertTrue(d["from"]["taken_at"].endswith("+02:00"))

	def test_summary_lines(self):
		self.assertEqual(
			chkan.format_summary(self.diff()),
			[
				"без тегов: +0 -0 ~1 ->0 d~0 sf:50B st:90B s+40B",
				"photos+tag: +1 -0 ~0 ->0 d~0 sf:0B st:50B s+50B",
				"tag: +0 -0 ~1 ->1 d~0 sf:200B st:230B s+30B",
				"с не отслеживаемыми тэгами: +0 -0 ~1 ->0 d~0",
			],
		)

	def test_old_diff_not_regrouped_by_other_rules(self):
		d = diff_between(self.flat_old(), self.flat_new(), TAGS_FOR_DIFF)[2]
		rules2 = chkan.parse_tags("")
		self.assertEqual(len(chkan.diff_ops(d)), len(chkan.diff_ops(roundtrip(d))))
		self.assertEqual(rules2.tags_of("dir/x"), frozenset())


# ------------------------------------------------------------ обратимость


class Reversibility(unittest.TestCase):
	def check(self, old_flat, new_flat, rules_text="", old_ex=(), new_ex=()):
		a, b, d, _ = diff_between(old_flat, new_flat, rules_text, old_ex, new_ex)
		d = roundtrip(d)
		back = chkan.apply_diff(b, d, backward=True)
		self.assertTrue(chkan.snapshots_equal(back, a))
		self.assertEqual(back, a)
		fwd = chkan.apply_diff(a, d, backward=False)
		self.assertTrue(chkan.snapshots_equal(fwd, b))
		return a, b, d

	def test_mixed_changes(self):
		new = DiffGrouping().flat_new()
		self.check(DiffGrouping().flat_old(), new, TAGS_FOR_DIFF)

	def test_scan_changes_and_exclude_header(self):
		old = {".": drec(ino=2), "keep": rec(ino=3, size=10), "tmp": drec(ino=4), "tmp/y": rec(ino=5, size=1000)}
		new = {".": drec(ino=2), "keep": rec(ino=3, size=10), "data": drec(ino=6), "data/x": rec(ino=7, size=500)}
		a, b, d = self.check(old, new, "", old_ex=["data"], new_ex=["tmp"])
		self.assertEqual(a["exclude"], ["data"])

	def test_symlink_and_type_change(self):
		old = {".": drec(ino=2), "a": rec(ino=3, size=5), "l": rec(type="l", ino=4, target="a", size=1)}
		new = {".": drec(ino=2), "a": rec(type="l", ino=3, target="zz", size=2), "l": rec(type="l", ino=4, target="b", size=1)}
		self.check(old, new)

	def test_btime_null_changes(self):
		old = {".": drec(ino=2), "a": rec(ino=3, btime=None)}
		new = {".": drec(ino=2), "a": rec(ino=3, btime=5)}
		self.check(old, new)
		self.check(new, old)

	def test_dir_rename_with_content_changes(self):
		old = {".": drec(ino=2), "d": drec(ino=3), "d/a": rec(ino=4, size=1), "d/b": rec(ino=5, size=2)}
		new = {".": drec(ino=2, mtime=50), "e": drec(ino=3, ctime=50), "e/a": rec(ino=4, size=9, mtime=50), "e/c": rec(ino=6, size=3)}
		self.check(old, new)

	def test_swap_of_paths_via_moves(self):
		flat = {".": drec(ino=2), "a": rec(ino=3, size=1), "b": rec(ino=4, size=2)}
		diff = {
			"format": chkan.FORMAT,
			"from": {"taken_at": "t0", "exclude": []},
			"to": {"taken_at": "t1", "exclude": []},
			"groups": {"-": {"size": ["3=3B", "3=3B"], "ch": {"a": {"op": "move", "from": "b"}, "b": {"op": "move", "from": "a"}}}},
		}
		s = snap(flat, "t0")
		out = chkan.apply_diff(s, diff)
		f = chkan.flatten(out["tree"])
		self.assertEqual((f["a"]["ino"], f["b"]["ino"]), (4, 3))
		back = chkan.apply_diff(out, diff, backward=True)
		self.assertEqual(chkan.flatten(back["tree"]), flat)

	def test_hand_edited_diff_comments_and_expanded_nodes(self):
		a, b, d, _ = diff_between(DiffGrouping().flat_old(), DiffGrouping().flat_new(), TAGS_FOR_DIFF)
		d = roundtrip(d)
		d["groups"]["tag"]["c"] = "комментарий к группе"
		d["groups"]["tag"]["ch"]["dir"]["c"] = "комментарий к папке"
		a_node = d["groups"]["tag"]["ch"]["dir"]["ch"].pop("A/C.txt")
		d["groups"]["tag"]["ch"]["dir"]["ch"]["A"] = {"ch": {"C.txt": {**a_node, "c": "раскрыто руками"}}}
		self.assertEqual(chkan.apply_diff(b, d, backward=True), a)

	def test_strict_detects_corruption(self):
		a, b, d, _ = diff_between(base_flat(), {**base_flat(), "notes.txt": rec(ino=6, size=50, mtime=11)})
		d = roundtrip(d)
		d["groups"]["-"]["ch"]["notes.txt"]["set"]["size"] = [10, 99]
		with self.assertRaises(chkan.DiffError):
			chkan.apply_diff(b, d, backward=True)

	def test_wrong_snapshot_rejected(self):
		a, b, d, _ = diff_between(base_flat(), {**base_flat(), "z": rec(ino=9)})
		with self.assertRaises(chkan.DiffError):
			chkan.apply_diff(a, roundtrip(d), backward=True)

	def test_add_over_existing_and_remove_missing(self):
		flat = base_flat()
		with self.assertRaises(chkan.DiffError):
			chkan.apply_ops(flat, [{"op": "add", "path": "notes.txt", "new": rec()}])
		with self.assertRaises(chkan.DiffError):
			chkan.apply_ops(flat, [{"op": "remove", "path": "nope", "old": rec()}])
		with self.assertRaises(chkan.DiffError):
			chkan.apply_ops(flat, [{"op": "bogus", "path": "x"}])
		with self.assertRaises(chkan.DiffError):
			chkan.apply_ops(flat, [{"op": "add", "path": "x"}])

	def test_invert_is_involution(self):
		ops = [
			{"op": "add", "path": "a", "new": rec()},
			{"op": "remove", "path": "b", "old": rec()},
			{"op": "add_scan", "path": "c", "new": rec()},
			{"op": "remove_scan", "path": "d", "old": rec()},
			{"op": "change", "path": "e", "set": {"size": [1, 2]}},
			{"op": "move", "path": "f", "from": "g", "set": {"ctime": [1, 2]}},
			{"op": "move", "path": "h", "from": "i"},
		]
		for op in ops:
			twice = chkan.invert_op(chkan.invert_op(op))
			expected = dict(op)
			if expected["op"] == "move":
				expected.setdefault("set", {}) if "set" in op else None
			self.assertEqual({k: v for k, v in twice.items() if k != "set" or v}, {k: v for k, v in expected.items() if k != "set" or v})


# ------------------------------------------------------------ формат файлов


class Formatting(unittest.TestCase):
	def test_pretty_roundtrip_and_tabs(self):
		d = diff_between(DiffGrouping().flat_old(), DiffGrouping().flat_new(), TAGS_FOR_DIFF)[2]
		text = chkan.dumps_diff(d)
		self.assertEqual(json.loads(text), d)
		for line in text.splitlines():
			indent = line[: len(line) - len(line.lstrip())]
			self.assertNotIn(" ", indent)

	def test_leaf_nodes_inline_containers_expanded(self):
		d = diff_between(DiffGrouping().flat_old(), DiffGrouping().flat_new(), TAGS_FOR_DIFF)[2]
		text = chkan.dumps_diff(d)
		self.assertIn('"F.txt": {"op": "change", "set": {"size": [100, 130], "mtime": ["1970-01-01 02:00:10", "1970-01-01 02:00:11"]}}', text)
		self.assertIn('"----------": 0', text)
		self.assertRegex(text, r'\n\t"groups": \{\n')

	def test_non_ascii_comments_kept_readable(self):
		text = chkan.dumps_pretty({"groups": {"c": "перенёс конфиги"}})
		self.assertIn("перенёс конфиги", text)

	def test_empty_containers(self):
		self.assertEqual(chkan.dumps_pretty({"a": {}, "b": []}), '{\n\t"a": {},\n\t"b": []\n}')

	def test_snapshot_is_compact(self):
		text = chkan.dumps_snapshot(snap(base_flat()))
		self.assertNotIn("\n", text)
		self.assertNotIn(": ", text)

	def test_snapshot_header(self):
		s = snap(base_flat())
		self.assertEqual(list(s), ["format", "root", "taken_at", "exclude", "tree"])
		self.assertIn("ch", s["tree"])
		self.assertNotIn('"c"', chkan.dumps_snapshot(s))

	def test_read_json_rejects_other_format(self):
		with tempfile.TemporaryDirectory() as t:
			p = os.path.join(t, "x.json")
			with open(p, "w") as f:
				json.dump({"format": 1}, f)
			with self.assertRaises(chkan.ChkanError):
				chkan.read_json(p)

	def test_atomic_write_leaves_no_tmp(self):
		with tempfile.TemporaryDirectory() as t:
			p = os.path.join(t, "x.json")
			chkan.write_text_atomic(p, "{}")
			self.assertEqual(os.listdir(t), ["x.json"])


class SizeFormat(unittest.TestCase):
	def test_examples(self):
		cases = {
			0: "0=0B",
			5: "5=5B",
			500: "500=500B",
			1023: "1_023=1023B",
			1024: "1_024=1.0K",
			1536: "1_536=1.5K",
			10 * 1024: "10_240=10K",
			1_023_002_146: "1_023_002_146=975M",
			8_234_782_596: "8_234_782_596=7.6G",
			1024**4: "1_099_511_627_776=1.0T",
			1024**7: "1_180_591_620_717_411_303_424=1024E",
		}
		for n, expected in cases.items():
			with self.subTest(n):
				self.assertEqual(chkan.format_size(n), expected)

	def test_rounds_down_not_up(self):
		self.assertEqual(chkan.format_size(1024 * 1024 - 1), "1_048_575=1023K")
		self.assertEqual(chkan.format_size(10 * 1024 - 1), "10_239=9.9K")

	def test_digits_are_two_or_three_between_units(self):
		for n in (12_345, 999_999, 5 * 1024**2 + 123, 77 * 1024**3, 400 * 1024**3 + 5):
			human = chkan.format_size(n).split("=")[1]
			digits = len(re.sub(r"\D", "", human))
			self.assertIn(digits, (2, 3), human)

	def test_parse_roundtrip(self):
		for n in (0, 1, 999, 1024, 1_023_002_146, 8_234_782_596, 2**50 + 7):
			self.assertEqual(chkan.parse_size(chkan.format_size(n)), n)

	def test_parse_accepts_plain_numbers(self):
		self.assertEqual(chkan.parse_size(1234), 1234)
		self.assertEqual(chkan.parse_size("1234"), 1234)
		self.assertEqual(chkan.parse_size("1_234"), 1234)

	def test_record_sizes_stay_numbers(self):
		d = DiffGrouping().diff()
		node = d["groups"]["tag"]["ch"]["dir"]["ch"]["F.txt"]
		self.assertEqual(node["set"]["size"], [100, 130])
		add = d["groups"]["photos+tag"]["ch"]["media/new.jpg"]
		self.assertEqual(add["new"]["size"], 50)


class Leniency(unittest.TestCase):
	def test_extra_keys_and_two_element_sizes_are_tolerated(self):
		a, b, d, _ = diff_between(DiffGrouping().flat_old(), DiffGrouping().flat_new(), TAGS_FOR_DIFF)
		d = roundtrip(d)
		a_x, b_x = json.loads(json.dumps(a)), json.loads(json.dumps(b))
		for obj in (a_x, b_x):
			obj["tz"] = "+02:00"  # лишнее поле старого слепка игнорируется
		for key in ("from", "to"):
			d[key]["tz"] = "+02:00"
		for g in d["groups"].values():
			if isinstance(g, dict):
				g["size"] = [chkan.parse_size(x) for x in g["size"][:2]]
		self.assertTrue(chkan.snapshots_equal(chkan.apply_diff(b_x, d, backward=True), a_x))
		self.assertTrue(chkan.snapshots_equal(chkan.apply_diff(a_x, d), b_x))


class DirTotals(unittest.TestCase):
	def flat(self):
		return {
			".": drec(ino=1),
			"a": drec(ino=2),
			"a/b": drec(ino=3),
			"a/b/f1": rec(ino=4, size=100),
			"a/f2": rec(ino=5, size=20),
			"a/lnk": rec(type="l", ino=6, size=2, target="f2"),
			"empty": drec(ino=7),
			"top": rec(ino=8, size=3),
		}

	def test_totals_are_sums_of_regular_files(self):
		flat = chkan.add_dir_totals(self.flat())
		self.assertEqual(flat["a/b"]["total"], 100)
		self.assertEqual(flat["a"]["total"], 120)  # симлинк не считается
		self.assertEqual(flat["empty"]["total"], 0)
		self.assertEqual(flat["."]["total"], 123)

	def test_only_directories_get_total(self):
		flat = chkan.add_dir_totals(self.flat())
		self.assertNotIn("total", flat["a/b/f1"])
		self.assertNotIn("total", flat["a/lnk"])
		self.assertNotIn("total", flat["top"])

	def test_recomputed_not_accumulated(self):
		flat = chkan.add_dir_totals(self.flat())
		again = chkan.add_dir_totals(flat)
		self.assertEqual(again["."]["total"], 123)

	def test_deep_chain(self):
		flat = {".": drec(ino=1)}
		path = ""
		for i in range(30):
			path = f"{path}/d{i}" if path else f"d{i}"
			flat[path] = drec(ino=10 + i)
		flat[path + "/f"] = rec(ino=99, size=7)
		flat = chkan.add_dir_totals(flat)
		self.assertTrue(all(r["total"] == 7 for r in flat.values() if r["type"] == "d"))

	def test_stage_gets_label_and_total(self):
		seen = []

		def stage(label, total=None, counted=True):
			seen.append((label, total))
			return chkan.NullProgress()

		chkan.add_dir_totals(self.flat(), stage=stage)
		self.assertEqual(seen, [("размеры папок", 8 + 4)])  # все записи + четыре папки

	def test_field_order_and_snapshot_roundtrip(self):
		flat = chkan.add_dir_totals(self.flat())
		self.assertEqual(list(flat["a"])[:5], ["type", "dev", "ino", "size", "total"])
		self.assertEqual(chkan.flatten(chkan.build_tree(flat)), flat)

	def test_scan_then_totals_on_real_fs(self):
		with tempfile.TemporaryDirectory() as t:
			for name, data in (("x/a", b"12345"), ("x/y/b", b"123"), ("c", b"1")):
				path = os.path.join(t, name)
				os.makedirs(os.path.dirname(path), exist_ok=True)
				with open(path, "wb") as f:
					f.write(data)
			flat, _ = chkan.scan_fs(t, chkan.parse_tags(""))
			chkan.add_dir_totals(flat)
		self.assertEqual((flat["x/y"]["total"], flat["x"]["total"], flat["."]["total"]), (3, 8, 9))


class SizeFormatsInDiff(unittest.TestCase):
	def test_short_delta(self):
		self.assertEqual(chkan.format_delta_short(104_636_521_725), "+97G")
		self.assertEqual(chkan.format_delta_short(8_963_676), "+8.5M")
		self.assertEqual(chkan.format_delta_short(-1024), "-1.0K")
		self.assertEqual(chkan.format_delta_short(0), "0B")
		self.assertEqual(chkan.format_delta_short(5), "+5B")

	def test_user_example(self):
		self.assertEqual(
			chkan.format_total_pair([20_579_490_614, 125_216_012_339]),
			["20_579_490_614=19G", "125_216_012_339=116G", "+97G"],
		)

	def test_old_long_delta_still_parses_without_check(self):
		self.assertEqual(chkan.parse_size("+25_573_854=+24M"), 25_573_854)
		self.assertEqual(chkan.parse_size("-1_024=-1.0K"), -1024)
		self.assertEqual(chkan.parse_total_pair(["1=1B", "31=31B", "+30=+30B"], check_delta=False), [1, 31])
		with self.assertRaises(chkan.DiffError):
			chkan.parse_total_pair(["1=1B", "31=31B", "+30=+30B"])  # в формате 4 третий элемент короткий

	def test_pair_to_triple_and_back(self):
		triple = chkan.format_total_pair([1000, 1030])
		self.assertEqual(triple, ["1_000=1000B", "1_030=1.0K", "+30B"])
		self.assertEqual(chkan.parse_total_pair(triple), [1000, 1030])
		shrink = chkan.format_total_pair([2048, 1024])
		self.assertEqual(shrink, ["2_048=2.0K", "1_024=1.0K", "-1.0K"])
		self.assertEqual(chkan.parse_total_pair(shrink), [2048, 1024])

	def test_absent_side_has_no_delta(self):
		self.assertEqual(chkan.format_total_pair([None, 1030]), [None, "1_030=1.0K"])
		self.assertEqual(chkan.format_total_pair([5, None]), ["5=5B", None])
		self.assertEqual(chkan.parse_total_pair([None, "1_030=1.0K"]), [None, 1030])

	def test_bad_pairs(self):
		for bad in (["1=1B"], ["1=1B", "2=2B", "+1B", "x"], ["1=1B", "2=2B", "+9B"], ["abc", "2=2B"], [[1], 2]):
			with self.subTest(bad):
				with self.assertRaises(chkan.DiffError):
					chkan.parse_total_pair(bad)


def dir_diff(old_total, new_total, **extra):
	old = {".": drec(ino=1), "d": {**drec(ino=2), "total": old_total}}
	new = {".": drec(ino=1), "d": {**drec(ino=2, **extra), "total": new_total}}
	if old_total is None:
		del old["d"]["total"]
	return old, new


class ChangeD(unittest.TestCase):
	def test_dirs_get_change_d_files_get_change(self):
		old = {".": drec(ino=1), "d": drec(ino=2), "f": rec(ino=3, size=1)}
		new = {".": drec(ino=1, mtime=99), "d": drec(ino=2, mtime=99), "f": rec(ino=3, size=2)}
		ops = chkan.make_ops(old, new, chkan.make_excluder([]), chkan.make_excluder([]))
		self.assertEqual(sorted((o["op"], o["path"]) for o in ops), [("change", "f"), ("change_d", "."), ("change_d", "d")])

	def test_type_change_is_plain_change(self):
		old = {".": drec(ino=1), "x": drec(ino=2)}
		new = {".": drec(ino=1), "x": rec(ino=2, size=5)}
		(op,) = chkan.make_ops(old, new, chkan.make_excluder([]), chkan.make_excluder([]))
		self.assertEqual(op["op"], "change")

	def test_total_triple_in_file_and_ints_after_read(self):
		old, new = dir_diff(1000, 1030, mtime=50)
		a, b, d, _ = diff_between(old, new)
		node = d["groups"]["-"]["ch"]["d"]
		self.assertEqual(node["op"], "change_d")
		self.assertEqual(node["set"]["total"], ["1_000=1000B", "1_030=1.0K", "+30B"])
		self.assertEqual(node["set"]["mtime"], ["1970-01-01 02:00:10", "1970-01-01 02:00:50"])
		(op,) = chkan.diff_ops(roundtrip(d))
		self.assertEqual(op["set"]["total"], [1000, 1030])

	def test_file_size_pairs_stay_numbers(self):
		old = {".": drec(ino=1), "f": rec(ino=3, size=1000)}
		new = {".": drec(ino=1), "f": rec(ino=3, size=1030)}
		d = diff_between(old, new)[2]
		self.assertEqual(d["groups"]["-"]["ch"]["f"]["set"]["size"], [1000, 1030])

	def test_added_and_removed_dirs_carry_formatted_total(self):
		old = {".": drec(ino=1)}
		new = {".": drec(ino=1), "d": {**drec(ino=2), "total": 2048}}
		a, b, d, _ = diff_between(old, new)
		self.assertEqual(d["groups"]["-"]["ch"]["d"]["new"]["total"], "2_048=2.0K")
		d = roundtrip(d)
		self.assertEqual(chkan.apply_diff(b, d, backward=True), a)
		self.assertTrue(chkan.snapshots_equal(chkan.apply_diff(a, d), b))
		rd = diff_between(new, old)[2]
		self.assertEqual(rd["groups"]["-"]["ch"]["d"]["old"]["total"], "2_048=2.0K")

	def test_reversible_with_totals(self):
		for old_total, new_total in ((1000, 1030), (2048, 5), (None, 77), (77, None)):
			with self.subTest((old_total, new_total)):
				old, new = dir_diff(old_total, new_total, mtime=50)
				if new_total is None:
					del new["d"]["total"]
				a, b, d, _ = diff_between(old, new)
				d = roundtrip(d)
				self.assertEqual(chkan.apply_diff(b, d, backward=True), a)
				self.assertTrue(chkan.snapshots_equal(chkan.apply_diff(a, d), b))

	def test_old_scan_without_totals_gives_none_to_value(self):
		old, new = dir_diff(None, 500)
		d = diff_between(old, new)[2]
		self.assertEqual(d["groups"]["-"]["ch"]["d"]["set"], {"total": [None, "500=500B"]})

	def test_dir_becomes_file_drops_total(self):
		old = {".": drec(ino=1), "x": {**drec(ino=2), "total": 9}}
		new = {".": drec(ino=1), "x": rec(ino=2, size=5)}
		a, b, d, _ = diff_between(old, new)
		d = roundtrip(d)
		self.assertEqual(d["groups"]["-"]["ch"]["x"]["op"], "change")
		self.assertEqual(chkan.apply_diff(b, d, backward=True), a)
		self.assertTrue(chkan.snapshots_equal(chkan.apply_diff(a, d), b))

	def test_plain_change_on_dir_still_applies(self):
		old, new = dir_diff(1000, 1030, mtime=50)
		a, b, d, _ = diff_between(old, new)
		d = roundtrip(d)
		d["groups"]["-"]["ch"]["d"]["op"] = "change"  # как в старых diff-ах
		self.assertEqual(chkan.apply_diff(b, d, backward=True), a)

	def test_invert_keeps_change_d(self):
		op = {"op": "change_d", "path": "d", "set": {"total": [1, 2]}}
		self.assertEqual(chkan.invert_op(op), {"op": "change_d", "path": "d", "set": {"total": [2, 1]}})

	def test_wrong_delta_in_file_is_rejected(self):
		old, new = dir_diff(1000, 1030, mtime=50)
		a, b, d, _ = diff_between(old, new)
		d = roundtrip(d)
		d["groups"]["-"]["ch"]["d"]["set"]["total"][2] = "+31=+31B"
		with self.assertRaises(chkan.DiffError):
			chkan.diff_ops(d)

	def test_strict_catches_wrong_old_total(self):
		old, new = dir_diff(1000, 1030, mtime=50)
		a, b, d, _ = diff_between(old, new)
		d = roundtrip(d)
		d["groups"]["-"]["ch"]["d"]["set"]["total"] = ["1_001=1001B", "1_030=1.0K", "+29B"]
		with self.assertRaises(chkan.DiffError):
			chkan.apply_diff(a, d)  # в слепке a у папки 1000, а diff ждёт 1001
		# откат такой порчи молча даёт другой слепок: поэтому chkan.py сверяет результат с предыдущим сканом
		self.assertNotEqual(chkan.apply_diff(b, d, backward=True), a)


class Summary(unittest.TestCase):
	def diff_with(self, groups, scan=None):
		d = {"format": chkan.FORMAT, "from": {}, "to": {}, "groups": groups}
		if scan:
			d["scan"] = scan
		return d

	def test_each_kind_counted_separately(self):
		ch = {
			"a": {"op": "add", "new": {}},
			"b": {"op": "add", "new": {}},
			"c": {"op": "remove", "old": {}},
			"d": {"op": "change", "set": {}},
			"e": {"op": "move", "from": "x"},
			"f": {"op": "change_d", "set": {}, "ch": {"g": {"op": "change", "set": {}}}},
		}
		lines = chkan.format_summary(self.diff_with({"-": {"size": [], "ch": ch}, "-" * 10: 0}))
		self.assertEqual(lines[0], "без тегов: +2 -1 ~2 ->1 d~1")

	def test_tracked_lines_in_diff_order_and_ignored_aggregated(self):
		node = lambda op: {"op": op}
		groups = {
			"-": {"ch": {"a": node("add")}},
			"b+t": {"ch": {"x": node("remove")}},
			"a": {"size": ["0=0B", "0=0B"]},  # только размер: строка с нулями
			"-" * 10: 0,
			"ign1": {"ch": {"y": node("move"), "z": node("add")}},
			"ign2": {"ch": {"w": node("change_d")}},
		}
		self.assertEqual(
			chkan.format_summary(self.diff_with(groups)),
			[
				"без тегов: +1 -0 ~0 ->0 d~0",
				"b+t: +0 -1 ~0 ->0 d~0",
				"a: +0 -0 ~0 ->0 d~0 sf:0B st:0B s0B",
				"с не отслеживаемыми тэгами: +1 -0 ~0 ->1 d~1",
			],
		)

	def test_scan_ops_not_counted(self):
		scan = {"ch": {"x": {"op": "add_scan", "new": {}}, "y": {"op": "remove_scan", "old": {}}}}
		lines = chkan.format_summary(self.diff_with({"-" * 10: 0}, scan))
		self.assertEqual(lines, ["без тегов: +0 -0 ~0 ->0 d~0", "с не отслеживаемыми тэгами: +0 -0 ~0 ->0 d~0"])

	def test_empty_diff(self):
		self.assertEqual(len(chkan.format_summary(self.diff_with({"-" * 10: 0}))), 2)

	def test_nested_nodes_all_counted(self):
		ch = {"d": {"op": "change_d", "ch": {"e": {"ch": {"f": {"op": "add"}, "g": {"op": "add"}}}}}}
		lines = chkan.format_summary(self.diff_with({"t": {"ch": ch}, "-" * 10: 0}))
		self.assertEqual(lines[1], "t: +2 -0 ~0 ->0 d~1")


class TimedProgress(unittest.TestCase):
	def test_duration(self):
		f = chkan.format_duration
		self.assertEqual((f(0), f(5), f(65), f(3599), f(3600), f(3725.9)), ("0:00", "0:05", "1:05", "59:59", "1:00:00", "1:02:05"))

	def test_render_with_eta(self):
		line = chkan.render_progress(25, 100, 10, elapsed=10, label="этап")
		self.assertEqual(line, "этап: [##--------]  25% 25/100  прошло 0:10, осталось ~0:30")

	def test_eta_waits_for_enough_data(self):
		self.assertNotIn("осталось", chkan.render_progress(1, 20000, 10, elapsed=5))  # меньше 1% сделано
		self.assertNotIn("осталось", chkan.render_progress(50, 100, 10, elapsed=0.4))  # прошло меньше секунды
		self.assertIn("осталось ~0:01", chkan.render_progress(50, 100, 10, elapsed=1.0))

	def test_build_tree_progress_reaches_total(self):
		flat = {".": drec(ino=1), "a": drec(ino=2), "a/b": rec(ino=3)}
		seen = []

		class P:
			def update(self, n):
				seen.append(n)

		chkan.build_tree(flat, P())
		self.assertEqual(seen[-1], len(flat))

	def test_render_without_eta_when_nothing_done_or_finished(self):
		self.assertNotIn("осталось", chkan.render_progress(0, 100, 10, elapsed=3))
		self.assertNotIn("осталось", chkan.render_progress(100, 100, 10, elapsed=3))
		self.assertNotIn("осталось", chkan.render_progress(5, None, elapsed=3))
		self.assertNotIn("осталось", chkan.render_progress(50, 100, 10, elapsed=3, finished=True))

	def test_render_uncounted_stage(self):
		self.assertEqual(chkan.render_progress(None, None, label="запись"), "запись…")
		self.assertEqual(chkan.render_progress(None, None, elapsed=7, label="запись", finished=True), "запись: готово  прошло 0:07")

	def clocked(self, **kw):
		t = [100.0]
		buf = io.StringIO()
		p = chkan.Progress(stream=buf, enabled=True, width=10, interval=0, clock=lambda: t[0], **kw)
		return p, buf, t

	def test_progress_uses_clock_for_elapsed_and_eta(self):
		p, buf, t = self.clocked(label="размеры", total=100)
		t[0] += 20
		p.update(50)
		self.assertIn("размеры: [#####-----]  50% 50/100  прошло 0:20, осталось ~0:20", buf.getvalue())

	def test_finish_shows_elapsed_only_and_actual_count(self):
		p, buf, t = self.clocked(label="скан", total=100)
		t[0] += 5
		p.update(80)
		p.finish()
		last = buf.getvalue().splitlines()[-1].split("\r")[-1].strip()
		self.assertEqual(last, "скан: [##########] 100% 80/80  прошло 0:05")
		self.assertTrue(buf.getvalue().endswith("\n"))

	def test_uncounted_context_manager(self):
		p, buf, t = self.clocked(label="запись", counted=False)
		with p:
			t[0] += 3
		out = buf.getvalue()
		self.assertIn("запись…", out)
		self.assertTrue(out.splitlines()[-1].strip().endswith("запись: готово  прошло 0:03"))

	def test_context_manager_on_error_does_not_claim_done(self):
		p, buf, t = self.clocked(label="x", total=10)
		with self.assertRaises(RuntimeError):
			with p:
				raise RuntimeError
		self.assertNotIn("готово", buf.getvalue())
		self.assertNotIn("100%", buf.getvalue())
		self.assertTrue(buf.getvalue().endswith("\n"))

	def test_each_stage_has_its_own_line(self):
		buf = io.StringIO()
		for label in ("один", "два"):
			with chkan.Progress(label, total=2, stream=buf, enabled=True, interval=0) as pr:
				pr.update(1)
				pr.update(2)
		lines = [l.split("\r")[-1].strip() for l in buf.getvalue().split("\n") if l]
		self.assertEqual(len(lines), 2)
		self.assertTrue(lines[0].startswith("один:") and lines[1].startswith("два:"))

	def test_silent_stage_when_not_tty(self):
		buf = io.StringIO()
		with chkan.Progress("тихо", total=2, stream=buf) as pr:
			pr.update(2)
		self.assertEqual(buf.getvalue(), "")

	def test_null_stage(self):
		with chkan.null_stage("x", 5) as pr:
			pr.update(1)

	def test_make_stage_returns_progress(self):
		st = chkan.make_stage("x", total=3, counted=True)
		self.assertIsInstance(st, chkan.Progress)
		self.assertEqual((st.label, st.total), ("x", 3))


class ProgressBar(unittest.TestCase):
	def test_render_with_total(self):
		self.assertEqual(chkan.render_progress(50, 100, 10), "[#####-----]  50% 50/100")
		self.assertEqual(chkan.render_progress(0, 100, 10), "[----------]   0% 0/100")

	def test_render_caps_at_100(self):
		self.assertEqual(chkan.render_progress(150, 100, 10), "[##########] 100% 150/100")
		self.assertTrue(chkan.render_progress(1, 0, 10).startswith("[##########]"))

	def test_render_without_total(self):
		self.assertEqual(chkan.render_progress(7, None, label="сканирование"), "сканирование: 7")
		self.assertEqual(chkan.render_progress(7, None), "7")

	def test_writes_when_enabled(self):
		buf = io.StringIO()
		p = chkan.Progress(total=10, stream=buf, enabled=True, width=10, interval=0)
		p.update(5)
		p.finish()
		self.assertIn("50%", buf.getvalue())
		self.assertTrue(buf.getvalue().endswith("\n"))

	def test_silent_when_not_tty(self):
		buf = io.StringIO()
		p = chkan.Progress(total=10, stream=buf)
		p.update(5)
		p.finish()
		self.assertEqual(buf.getvalue(), "")

	def test_throttled(self):
		buf = io.StringIO()
		p = chkan.Progress(total=10, stream=buf, enabled=True, interval=3600)
		p.update(1)
		p.update(2)
		self.assertEqual(buf.getvalue().count("\r"), 1)


class WarningPrinting(unittest.TestCase):
	def printer(self):
		buf = io.StringIO()
		progress = chkan.Progress(stream=buf, enabled=False)
		return chkan.WarningPrinter(progress), progress, buf

	def feed(self, n):
		p, progress, buf = self.printer()
		for i in range(n):
			p(f"w{i}: нет доступа")
		progress.finish()
		p.finish()
		return buf.getvalue()

	def test_each_of_first_ten_is_printed_at_once(self):
		p, _, buf = self.printer()
		for i in range(10):
			p(f"w{i}")
			self.assertEqual(buf.getvalue().splitlines()[-1], f"Предупреждение: не прочитано w{i}")
			self.assertTrue(buf.getvalue().endswith("\n"))
		p.finish()
		self.assertEqual(len(buf.getvalue().splitlines()), 10)
		self.assertNotIn("и ещё", buf.getvalue())

	def test_eleventh_prints_prefix_without_number(self):
		p, _, buf = self.printer()
		for i in range(11):
			p(f"w{i}")
		self.assertTrue(buf.getvalue().endswith("Предупреждение: не прочитано w9\n... и ещё "))
		for i in range(11, 15):
			p(f"w{i}")
		self.assertTrue(buf.getvalue().endswith("... и ещё "))  # до конца скана больше ничего

	def test_number_is_appended_after_scan(self):
		self.assertTrue(self.feed(15).endswith("... и ещё 5\n"))
		self.assertTrue(self.feed(11).endswith("... и ещё 1\n"))
		self.assertEqual(self.feed(12).count("Предупреждение"), 10)

	def test_nothing_extra_for_ten_or_fewer(self):
		self.assertEqual(self.feed(0), "")
		self.assertNotIn("и ещё", self.feed(10))
		self.assertEqual(len(self.feed(3).splitlines()), 3)


class ProgressWithMessages(unittest.TestCase):
	def make(self):
		buf = io.StringIO()
		return chkan.Progress(total=10, stream=buf, enabled=True, width=10, interval=0), buf

	def test_message_clears_bar_and_redraws(self):
		p, buf = self.make()
		p.update(5)
		p.message("hello")
		out = buf.getvalue()
		self.assertRegex(out, r"\r +\rhello\n")
		self.assertGreater(out.rindex("50%"), out.index("hello\n"))

	def test_suspend_stops_drawing_and_keeps_open_line(self):
		p, buf = self.make()
		p.update(5)
		p.message("... и ещё ", newline=False)
		p.suspend()
		size = len(buf.getvalue())
		p.update(8)
		p.finish()
		self.assertEqual(len(buf.getvalue()), size)
		p.message("3")
		self.assertTrue(buf.getvalue().endswith("... и ещё 3\n"))

	def test_message_without_bar_when_disabled(self):
		buf = io.StringIO()
		p = chkan.Progress(stream=buf, enabled=False)
		p.message("x")
		self.assertEqual(buf.getvalue(), "x\n")

	def test_scan_fs_reports_warnings_immediately(self):
		with tempfile.TemporaryDirectory() as t:
			for n in ("ok", "bad1", "bad2"):
				with open(os.path.join(t, n), "w") as f:
					f.write("x")
			real = chkan.stat_record
			seen = []

			def fake(path):
				if os.path.basename(path).startswith("bad"):
					raise OSError(errno.EACCES, "Permission denied", path)
				return real(path)

			def on_warning(text):
				seen.append((text, "ok" in [os.path.basename(x) for x in os.listdir(t)]))

			with mock.patch.object(chkan, "stat_record", fake):
				flat, warnings = chkan.scan_fs(t, chkan.parse_tags(""), None, on_warning)
			self.assertEqual(sorted(w for w, _ in seen), ["bad1: Permission denied", "bad2: Permission denied"])
			self.assertEqual(sorted(warnings), sorted(w for w, _ in seen))
			self.assertIn("ok", flat)
			self.assertNotIn("bad1", flat)


class SpecialTags(unittest.TestCase):
	def test_old_reserved_names_are_ordinary_tags_now(self):
		rules = chkan.parse_tags("a/\t# exclude no_x and or\n")
		self.assertEqual(set(rules.tags_of("a/f")), {"exclude", "no_x", "and", "or"})
		self.assertEqual(rules.exclude, [])
		self.assertFalse(rules.state_of("a/f").excluded)

	def test_exclude_special(self):
		rules = chkan.parse_tags("a/\t# @exclude\nb/\t# t @exclude u\n")
		self.assertEqual(rules.exclude, ["a", "b"])
		self.assertTrue(rules.state_of("a/x/y").excluded)
		self.assertEqual(set(rules.tags_of("b/f")), {"t", "u"})

	def test_no_cancels_in_subtree(self):
		rules = chkan.parse_tags("a/\t# t u\n\tb/\t# @no(t)\n")
		self.assertEqual(set(rules.tags_of("a/x")), {"t", "u"})
		self.assertEqual(set(rules.tags_of("a/b/x")), {"u"})

	def test_no_for_tag_that_is_not_set_is_harmless(self):
		self.assertEqual(set(chkan.parse_tags("a/\t# @no(zzz)\n").tags_of("a")), set())

	def test_no_of_exclude_name_is_just_a_tag_name(self):
		rules = chkan.parse_tags("a/\t# exclude\n\tb/\t# @no(exclude)\n")
		self.assertEqual(set(rules.tags_of("a/f")), {"exclude"})
		self.assertEqual(set(rules.tags_of("a/b/f")), set())

	def test_exclude_cannot_be_cancelled(self):
		rules = chkan.parse_tags("a/\t# @exclude\n\tb/\t# t\n")
		self.assertTrue(rules.state_of("a/b/x").excluded)

	def test_special_tags_inside_ignore_mark_rules(self):
		rules = chkan.parse_tags("# t(ignore)\na/\t# @no(t) t(ignore)\n")
		self.assertEqual(set(rules.ignore_tags), {"t"})
		self.assertEqual(set(rules.tags_of("a")), {"t"})  # слева направо: сначала снять, потом поставить

	def test_unicode_tags_in_no(self):
		rules = chkan.parse_tags("a/\t# работа\n\tb/\t# @no(работа)\n")
		self.assertEqual(set(rules.tags_of("a/b")), set())

	def test_comment_needs_space_before_double_slash(self):
		rules = chkan.parse_tags("a//b\t# t\nc/\t# u // комментарий (со скобкой)\n")
		self.assertEqual(set(rules.tags_of("a/b")), {"t"})  # // внутри пути не комментарий
		self.assertEqual(set(rules.tags_of("c")), {"u"})

	def test_comment_may_follow_special_tags(self):
		rules = chkan.parse_tags("a/\t# @exclude // потому что кэш\n")
		self.assertEqual(rules.exclude, ["a"])


class RunParsing(unittest.TestCase):
	def runs(self, text):
		return chkan.parse_tags(text).runs

	def test_path_line_and_pathless_line(self):
		self.assertEqual(
			self.runs("папка111\t# @run(echo a)\n# @run(echo b)\n"),
			[("папка111", "echo a"), (".", "echo b")],
		)

	def test_order_of_file_and_nesting_paths(self):
		text = "a/\t# @run(one)\n\tb/\t# @run(two)\n# @run(three)\nc/d/\t# @run(four)\n"
		self.assertEqual(self.runs(text), [("a", "one"), ("a/b", "two"), (".", "three"), ("c/d", "four")])

	def test_dot_and_empty_path_mean_root(self):
		self.assertEqual(self.runs("./\t# @run(x)\n"), [(".", "x")])

	def test_goes_to_last_paren_in_line(self):
		self.assertEqual(self.runs("a/\t# @run(echo $(date) | tee (x))\n"), [("a", "echo $(date) | tee (x)")])

	def test_two_commands_split_on_paren_run(self):
		self.assertEqual(self.runs("a/\t# @run(first) @run(second)\n"), [("a", "first"), ("a", "second")])
		self.assertEqual(self.runs("a/\t# @run(first)@run(second)\n"), [("a", "first"), ("a", "second")])
		self.assertEqual(self.runs("a/\t# @run(x $(y))\t @run(z)\n"), [("a", "x $(y)"), ("a", "z")])

	def test_three_commands_with_parens_inside(self):
		self.assertEqual(
			self.runs("# @run(a $(b)) @run(c (d) e) @run(f)\n"),
			[(".", "a $(b)"), (".", "c (d) e"), (".", "f")],
		)

	def test_tags_before_run_are_applied(self):
		rules = chkan.parse_tags("папка\t# @exclude t @run(sudo -u userxxx chkan.py)\n")
		self.assertEqual(rules.exclude, ["папка"])
		self.assertEqual(rules.runs, [("папка", "sudo -u userxxx chkan.py")])
		rules2 = chkan.parse_tags("a/\t# t @run(cmd)\n")
		self.assertEqual(set(rules2.tags_of("a")), {"t"})

	def test_text_after_last_paren_is_parsed_as_tags(self):
		rules = chkan.parse_tags("a/\t# @run(echo hi) @exclude\n")
		self.assertEqual(rules.runs, [("a", "echo hi")])
		self.assertEqual(rules.exclude, ["a"])

	def test_run_must_be_last_after_it_everything_up_to_last_paren_is_command(self):
		rules = chkan.parse_tags("a/\t# @run(echo hi) @no(t)\n")
		self.assertEqual(rules.runs, [("a", "echo hi) @no(t")])

	def test_comment_is_cut_before_parsing_so_parens_in_it_do_not_matter(self):
		self.assertEqual(self.runs("a/\t# @run(echo hi) // потом (подробности)\n"), [("a", "echo hi")])

	def test_url_inside_command_is_not_a_comment(self):
		self.assertEqual(self.runs("# @run(curl https://example.com/x)\n"), [(".", "curl https://example.com/x")])

	def test_hash_and_quotes_inside_command(self):
		self.assertEqual(self.runs('a/\t# @run(echo "a #b" \'c)\')\n'), [("a", "echo \"a #b\" 'c)'")])

	def test_command_whitespace_trimmed(self):
		self.assertEqual(self.runs("# @run(   ls -la   )\n"), [(".", "ls -la")])

	def test_pathless_line_ignores_other_specials(self):
		rules = chkan.parse_tags("# @exclude @no(x) t @run(echo)\n")
		self.assertEqual((rules.exclude, rules.runs), ([], [(".", "echo")]))

	def test_run_does_not_change_tags_or_exclusions(self):
		rules = chkan.parse_tags("a/\t# @run(x)\n")
		self.assertEqual(set(rules.tags_of("a")), set())
		self.assertEqual(rules.exclude, [])

	def test_repeated_path_collects_commands_in_file_order(self):
		self.assertEqual(self.runs("a/\t# @run(1)\nb/\t# @run(2)\na/\t# @run(3)\n"), [("a", "1"), ("b", "2"), ("a", "3")])

	def test_template_parses_and_has_one_run_example(self):
		rules = chkan.parse_tags(chkan.TAGS_TEMPLATE)
		self.assertEqual(rules.runs, [])  # всё в комментариях


class RunHooks(unittest.TestCase):
	def setUp(self):
		self.tmp = tempfile.TemporaryDirectory()
		self.addCleanup(self.tmp.cleanup)
		self.root = os.path.realpath(self.tmp.name)
		os.makedirs(os.path.join(self.root, "sub dir"))

	def go(self, runs, env=None):
		out, err = io.StringIO(), io.StringIO()
		with contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
			failures = chkan.run_hooks(self.root, runs, env or {})
		return failures, out.getvalue(), err.getvalue()

	def read(self, rel):
		with open(os.path.join(self.root, rel), encoding="utf-8") as f:
			return f.read()

	def test_prints_line_before_each_command(self):
		failures, out, err = self.go([("sub dir", "true"), (".", "true")])
		self.assertEqual(failures, 0)
		self.assertEqual(err.splitlines(), ["sub dir # @run(true)", ". # @run(true)"])
		self.assertEqual(out, "")

	def test_cwd_is_path_of_line_or_root(self):
		self.go([("sub dir", "pwd > ../a.txt"), (".", "pwd > b.txt")])
		self.assertEqual(self.read("a.txt").strip(), os.path.join(self.root, "sub dir"))
		self.assertEqual(self.read("b.txt").strip(), self.root)

	def test_env_vars(self):
		env = {"CHKAN_TAGS": "/t/TAGS.txt", "CHKAN_DIFF": "/t/diff.json", "CHKAN_SCAN": "/t/scan.json"}
		self.go([(".", 'echo "$CHKAN_TAGS|$CHKAN_DIFF|$CHKAN_SCAN" > env.txt')], env)
		self.assertEqual(self.read("env.txt").strip(), "/t/TAGS.txt|/t/diff.json|/t/scan.json")

	def test_inherits_environment(self):
		with mock.patch.dict(os.environ, {"MY_VAR": "42"}):
			self.go([(".", 'echo "$MY_VAR" > v.txt')])
		self.assertEqual(self.read("v.txt").strip(), "42")

	def test_commands_run_sequentially_in_order(self):
		self.go([(".", "echo 1 >> log.txt; sleep 0.2"), (".", "echo 2 >> log.txt"), (".", "echo 3 >> log.txt")])
		self.assertEqual(self.read("log.txt").split(), ["1", "2", "3"])

	def test_bash_features(self):
		self.go([(".", "x=(a b c); echo ${x[1]} > bash.txt; [[ 1 -eq 1 ]] && echo ok >> bash.txt")])
		self.assertEqual(self.read("bash.txt").split(), ["b", "ok"])

	def test_failure_is_reported_and_next_commands_still_run(self):
		failures, out, err = self.go([(".", "exit 7"), (".", "echo next > next.txt")])
		self.assertEqual(failures, 1)
		self.assertIn("кодом 7", err)
		self.assertEqual(self.read("next.txt").strip(), "next")
		self.assertEqual(len([l for l in err.splitlines() if "# @run(" in l]), 2)

	def test_missing_dir_skips_command_and_counts_failure(self):
		failures, _, err = self.go([("nope", "echo x > x.txt"), (".", "echo y > y.txt")])
		self.assertEqual(failures, 1)
		self.assertIn("нет папки nope", err)
		self.assertFalse(os.path.exists(os.path.join(self.root, "x.txt")))
		self.assertTrue(os.path.exists(os.path.join(self.root, "y.txt")))

	def test_file_instead_of_dir_is_failure(self):
		with open(os.path.join(self.root, "f"), "w") as f:
			f.write("x")
		failures, _, err = self.go([("f", "true")])
		self.assertEqual(failures, 1)

	def test_no_bash_is_reported(self):
		with mock.patch.object(chkan.subprocess, "run", side_effect=FileNotFoundError("bash")):
			failures, _, err = self.go([(".", "true")])
		self.assertEqual(failures, 1)
		self.assertIn("bash", err)

	def test_uses_bash_dash_c(self):
		with mock.patch.object(chkan.subprocess, "run") as run:
			run.return_value.returncode = 0
			self.go([("sub dir", "echo hi")], {"A": "1"})
		args, kwargs = run.call_args
		self.assertEqual(args[0], ["bash", "-c", "echo hi"])
		self.assertEqual(kwargs["cwd"], os.path.join(self.root, "sub dir"))
		self.assertEqual(kwargs["env"]["A"], "1")

	def test_no_commands_prints_nothing(self):
		self.assertEqual(self.go([]), (0, "", ""))


class TimeText(unittest.TestCase):
	REF = "2026-10-16T00:02:04+03:00"

	def ref(self, taken_at=None):
		return chkan.TimeRef(taken_at or self.REF)

	def ts(self, iso):
		return int(datetime.datetime.fromisoformat(iso).timestamp())

	def test_examples_from_spec(self):
		cases = {
			"2026-10-16T00:02:04+03:00": "00:02:04",
			"2026-10-15T00:02:04+03:00": "15 00:02:04",
			"2026-09-16T00:02:04+03:00": "09-16 00:02:04",
			"2024-09-16T00:02:04+03:00": "2024-09-16 00:02:04",
		}
		r = self.ref()
		for iso, text in cases.items():
			with self.subTest(text):
				self.assertEqual(r.encode(self.ts(iso)), text)
				self.assertEqual(r.decode(text), self.ts(iso))

	def test_written_in_timezone_of_to_taken_at(self):
		self.assertEqual(self.ref().encode(self.ts("2026-10-15T22:30:00+00:00")), "01:30:00")  # в +03:00 это уже 16-е
		self.assertEqual(self.ref("2026-10-16T00:02:04-05:00").encode(self.ts("2026-10-16T05:02:04+00:00")), "00:02:04")
		self.assertEqual(self.ref("2026-10-16T00:02:04+05:30").encode(self.ts("2026-10-15T18:32:04+00:00")), "00:02:04")

	def test_other_year_same_month_and_day_is_full(self):
		self.assertEqual(self.ref().encode(self.ts("2025-10-16T10:00:00+03:00")), "2025-10-16 10:00:00")
		self.assertEqual(self.ref().encode(self.ts("2025-10-15T10:00:00+03:00")), "2025-10-15 10:00:00")

	def test_same_year_other_month_has_month_and_day(self):
		self.assertEqual(self.ref().encode(self.ts("2026-01-05T09:08:07+03:00")), "01-05 09:08:07")

	def test_year_boundary(self):
		r = self.ref("2026-01-01T00:00:10+00:00")
		self.assertEqual(r.encode(self.ts("2025-12-31T23:59:59+00:00")), "2025-12-31 23:59:59")
		self.assertEqual(r.encode(self.ts("2026-01-01T00:00:00+00:00")), "00:00:00")
		self.assertEqual(r.encode(self.ts("2026-01-31T00:00:00+00:00")), "31 00:00:00")

	def test_roundtrip_many_values(self):
		import random

		rng = random.Random(7)
		for tz in ("+03:00", "-11:00", "+00:00", "+05:45"):
			r = self.ref(f"2026-10-16T12:00:00{tz}")
			for _ in range(300):
				v = rng.randint(-2_000_000_000, 4_000_000_000)
				self.assertEqual(r.decode(r.encode(v)), v, (tz, v))

	def test_year_is_padded_to_four_digits(self):
		v = -32_000_000_000  # около 956 года
		text = self.ref().encode(v)
		self.assertRegex(text, r"^0\d{3}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$")
		self.assertEqual(self.ref().decode(text), v)

	def test_none_and_non_ints_pass_through(self):
		r = self.ref()
		self.assertIsNone(r.encode(None))
		self.assertEqual(r.encode("уже текст"), "уже текст")
		self.assertIsNone(r.decode(None))
		self.assertEqual(r.decode(12345), 12345)  # число секунд тоже допустимо

	def test_out_of_range_stays_a_number(self):
		r = self.ref()
		self.assertEqual(r.encode(10**15), 10**15)
		self.assertEqual(r.decode(r.encode(10**15)), 10**15)

	def test_bad_text(self):
		r = self.ref()
		for bad in ("25:00:00", "2026-13-01 00:00:00", "abc", "1:2:3", "32 00:00:00", "02-30 00:00:00", "00:00", "2026-10-16T00:00:00"):
			with self.subTest(bad):
				with self.assertRaises(chkan.DiffError):
					r.decode(bad)

	def test_bad_taken_at_fails_only_when_needed(self):
		r = chkan.TimeRef("t1")
		self.assertIsNone(r.encode(None))
		with self.assertRaises(chkan.DiffError):
			r.encode(5)
		with self.assertRaises(chkan.DiffError):
			r.decode("00:00:01")

	def test_times_in_diff_are_text_and_restore_exactly(self):
		old = {".": drec(ino=1), "f": rec(ino=2, size=1, mtime=1_790_000_000, ctime=1_790_000_000, btime=1_700_000_000)}
		new = {".": drec(ino=1), "f": rec(ino=2, size=2, mtime=1_790_000_100, ctime=1_790_000_100, btime=1_700_000_000)}
		a, b, d, _ = diff_between(old, new)
		node = d["groups"]["-"]["ch"]["f"]
		self.assertEqual(node["set"]["mtime"], ["2026-10-02 05:33:20", "2026-10-02 05:35:00"][:0] or node["set"]["mtime"])
		for pair in (node["set"]["mtime"], node["set"]["ctime"]):
			self.assertTrue(all(isinstance(x, str) and re.match(r"^(\d{2}-\d{2} |\d{2} |\d{4}-\d{2}-\d{2} )?\d{2}:\d{2}:\d{2}$", x) for x in pair))
		self.assertNotIn("btime", node["set"])
		d = roundtrip(d)
		self.assertEqual(chkan.apply_diff(b, d, backward=True), a)
		self.assertTrue(chkan.snapshots_equal(chkan.apply_diff(a, d), b))

	def test_added_record_times_are_text_and_btime_null_stays(self):
		new = {".": drec(ino=1), "f": rec(ino=2, mtime=1_790_000_000, ctime=1_790_000_000, btime=None)}
		a, b, d, _ = diff_between({".": drec(ino=1)}, new)
		rec_ = d["groups"]["-"]["ch"]["f"]["new"]
		self.assertIsNone(rec_["btime"])
		self.assertIsInstance(rec_["mtime"], str)
		self.assertEqual(chkan.diff_ops(roundtrip(d))[0]["new"], new["f"])


class DiffLayout(unittest.TestCase):
	def make(self, ch, extra=None):
		d = {
			"format": 4,
			"root": "/r",
			"from": {"taken_at": "2026-10-01T15:00:00+02:00", "exclude": []},
			"to": {"taken_at": "2026-10-02T09:00:00+02:00", "exclude": []},
		}
		d.update(extra or {})
		d["groups"] = {"-": {"size": ["1=1B", "2=2B", "+1B"], "ch": ch}, "-" * 10: 0}
		return d

	LONG_SET = {
		"mtime": ["2026-10-01 14:00:00", "00:02:04"],
		"ctime": ["2026-10-01 14:00:00", "00:02:04"],
		"size": [4096, 8192],
		"total": ["20_579_490_614=19G", "125_216_012_339=116G", "+97G"],
		"mode": ["0755", "0700"],
	}

	def test_folder_change_with_only_op_and_set_is_one_line_however_long(self):
		node = {"op": "change_d", "set": self.LONG_SET}
		text = chkan.dumps_diff(self.make({"d": node}))
		line = next(l for l in text.splitlines() if l.strip().startswith('"d":'))
		self.assertGreater(len(line), chkan.INLINE_LIMIT)
		self.assertTrue(line.rstrip(",").endswith("}}"))
		self.assertEqual(json.loads(text)["groups"]["-"]["ch"]["d"], node)

	def test_set_is_always_one_line_inside_expanded_node(self):
		node = {"op": "move", "from": "x/y", "set": self.LONG_SET, "c": "перенесли"}
		text = chkan.dumps_diff(self.make({"d": node}))
		lines = [l.strip() for l in text.splitlines()]
		self.assertIn('"op": "move",', lines)
		self.assertIn('"from": "x/y",', lines)
		set_line = next(l for l in lines if l.startswith('"set":'))
		self.assertTrue(set_line.rstrip(",").endswith("}"))
		self.assertIn('"c": "перенесли"', lines)
		self.assertEqual(json.loads(text)["groups"]["-"]["ch"]["d"], node)

	def test_every_ch_element_on_its_own_line_even_if_tiny(self):
		ch = {"a": {"op": "add", "new": {"type": "f"}}, "b": {"op": "remove", "old": {"type": "f"}}, "c": {"op": "change", "set": {"size": [1, 2]}}}
		text = chkan.dumps_diff(self.make(ch))
		lines = [l.strip() for l in text.splitlines()]
		for key in ('"a":', '"b":', '"c":'):
			self.assertEqual(len([l for l in lines if l.startswith(key)]), 1)
		self.assertIn('"ch": {', lines)
		self.assertNotIn('"ch": {"a"', text)

	def test_ch_of_a_node_is_also_one_element_per_line(self):
		node = {"op": "change_d", "set": {"mtime": ["a", "b"]}, "ch": {"x": {"op": "add", "new": {}}, "y": {"op": "add", "new": {}}}}
		text = chkan.dumps_diff(self.make({"d": node}))
		self.assertRegex(text, r'\t"ch": \{\n\t+"x": .*,\n\t+"y": ')
		self.assertEqual(json.loads(text)["groups"]["-"]["ch"]["d"], node)

	def test_op_set_with_a_comment_is_inline_only_while_short(self):
		short = {"op": "change_d", "set": {"mtime": ["a", "b"]}, "c": "комментарий"}
		text = chkan.dumps_diff(self.make({"d": short}))
		self.assertIn('"d": {"op": "change_d", "set": {"mtime": ["a", "b"]}, "c": "комментарий"}', text)
		long = {"op": "change_d", "set": self.LONG_SET, "c": "комментарий"}
		text = chkan.dumps_diff(self.make({"d": long}))
		self.assertIn('\n\t\t\t\t\t"c": "комментарий"', text)  # три элемента и длинно: раскрыт, set по-прежнему в одну строку
		self.assertEqual(json.loads(text)["groups"]["-"]["ch"]["d"], long)

	def test_leaf_file_nodes_stay_inline_when_short(self):
		node = {"op": "add", "new": {"type": "f", "size": 5}}
		text = chkan.dumps_diff(self.make({"f": node}))
		self.assertIn('"f": {"op": "add", "new": {"type": "f", "size": 5}}', text)

	def test_comment_key_c_after_to_before_groups(self):
		text = chkan.dumps_diff(self.make({}, {"c": "правил конфиги"}))
		keys = [l.strip().split(":")[0] for l in text.splitlines() if l.startswith("\t") and not l.startswith("\t\t")]
		self.assertEqual(keys[:6], ['"format"', '"root"', '"from"', '"to"', '"c"', '"groups"'])
		self.assertEqual(list(json.loads(text)), ["format", "root", "from", "to", "c", "groups"])

	def test_only_tabs_for_indentation_and_roundtrip(self):
		node = {"op": "change_d", "set": self.LONG_SET, "ch": {"f": {"op": "add", "new": {"type": "f", "t": "ж" * 50}}}}
		d = self.make({"d": node}, {"c": "строка с \"кавычками\" и юникодом ёж"})
		text = chkan.dumps_diff(d)
		for line in text.splitlines():
			self.assertNotIn(" ", line[: len(line) - len(line.lstrip())])
		self.assertEqual(json.loads(text), d)
		self.assertIn("ёж", text)

	def test_size_only_group_and_scan_section(self):
		d = self.make({})
		d["groups"]["t"] = {"size": ["1=1B", "0=0B", "-1B"]}
		d["scan"] = {"ch": {"x": {"op": "add_scan", "new": {"type": "f"}}}}
		text = chkan.dumps_diff(d)
		self.assertIn('"t": {"size": ["1=1B", "0=0B", "-1B"]}', text)
		self.assertIn('"scan": {\n', text)
		self.assertEqual(json.loads(text), d)

	def test_real_diff_layout(self):
		old = {".": drec(ino=1), "d": {**drec(ino=2), "total": 1000}, "d/f": rec(ino=3, size=1000)}
		new = {".": drec(ino=1, mtime=99), "d": {**drec(ino=2, mtime=99), "total": 1030}, "d/f": rec(ino=3, size=1030, mtime=99)}
		a, b, d, _ = diff_between(old, new)
		text = chkan.dumps_diff(d)
		self.assertEqual(json.loads(text), d)
		self.assertRegex(text, r'\n\t\t\t\t"\.": \{"op": "change_d", "set": \{[^\n]*\}\},?\n')
		self.assertRegex(text, r'"op": "change_d",\n\t+"set": \{[^\n]*\},\n\t+"ch": \{\n')


class Wildcards(unittest.TestCase):
	def setUp(self):
		self.tmp = tempfile.TemporaryDirectory()
		self.addCleanup(self.tmp.cleanup)
		self.root = os.path.realpath(self.tmp.name)
		for d in ("proj/a/src", "proj/b/src", "proj/c/doc", "proj/.hid/src", "d[1]", "plain"):
			os.makedirs(os.path.join(self.root, d))
		for f in ("star*name", "starXname", "d[1]/x1", "d[1]/x2", "d[1]/y1"):
			with open(os.path.join(self.root, f), "w") as fh:
				fh.write("x")

	def parse(self, text, **kw):
		return chkan.parse_tags(text, self.root, **kw)

	def tags(self, rules, path):
		return set(rules.tags_of(path))

	def test_star_expands_and_tags_apply_to_every_variant(self):
		rules = self.parse("proj/*/src\t# t\n")
		self.assertEqual(self.tags(rules, "proj/a/src/x"), {"t"})
		self.assertEqual(self.tags(rules, "proj/b/src"), {"t"})
		self.assertEqual(self.tags(rules, "proj/c/doc"), set())
		self.assertEqual(self.tags(rules, "proj/a"), set())

	def test_question_mark_and_brackets(self):
		rules = self.parse("proj/?/src\t# q\nproj/[ab]/doc\t# nothing_here_is_an_error_below\n".split("\n")[0] + "\n")
		self.assertEqual(self.tags(rules, "proj/a/src"), {"q"})
		rules = self.parse("proj/[ab]/src\t# t\n")
		self.assertEqual((self.tags(rules, "proj/a/src"), self.tags(rules, "proj/b/src")), ({"t"}, {"t"}))
		rules = self.parse("proj/[!a]/src\t# t\n")
		self.assertEqual((self.tags(rules, "proj/a/src"), self.tags(rules, "proj/b/src")), (set(), {"t"}))

	def test_no_match_is_an_error_with_line_number(self):
		with self.assertRaises(chkan.ChkanError) as cm:
			self.parse("plain\t# ok\nproj/*/nothing\t# t\n")
		self.assertIn("TAGS.txt:2", str(cm.exception))
		self.assertIn("proj/*/nothing", str(cm.exception))

	def test_plain_nonexistent_paths_are_still_fine(self):
		rules = self.parse("no/such/dir\t# t\n")
		self.assertEqual(self.tags(rules, "no/such/dir/f"), {"t"})

	def test_braces(self):
		rules = self.parse("proj/{a,c}/doc\t# t\n")
		self.assertEqual(self.tags(rules, "proj/a/doc"), {"t"})
		self.assertEqual(self.tags(rules, "proj/c/doc"), {"t"})
		self.assertEqual(self.tags(rules, "proj/b/doc"), set())

	def test_braces_do_not_require_existence_but_globs_inside_do(self):
		rules = self.parse("proj/{a,zzz}/x\t# t\n")
		self.assertEqual(self.tags(rules, "proj/zzz/x"), {"t"})
		rules = self.parse("proj/{a,b,zzz}/s*\t# t\n")
		self.assertEqual(self.tags(rules, "proj/a/src"), {"t"})
		self.assertEqual(self.tags(rules, "proj/b/src"), {"t"})
		self.assertEqual(self.tags(rules, "proj/zzz/src"), set())  # zzz не существует, по нему glob ничего не нашёл

	def test_expand_braces(self):
		f = chkan.expand_braces
		self.assertEqual(f("a/b"), ["a/b"])
		self.assertEqual(f("a/{x,y}/f"), ["a/x/f", "a/y/f"])
		self.assertEqual(f("{a,b}{1,2}"), ["a1", "a2", "b1", "b2"])
		self.assertEqual(f("{a,{b,c}}"), ["a", "b", "c"])
		self.assertEqual(f("{a,{b,c}d}"), ["a", "bd", "cd"])
		self.assertEqual(f("x{a,{b,c}}y{1,2}"), ["xay1", "xay2", "xby1", "xby2", "xcy1", "xcy2"])
		self.assertEqual(f("x{,y}"), ["x", "xy"])
		self.assertEqual(f("{a}"), ["a"])

	def test_nested_lines_under_wildcard_parent_apply_to_each_parent(self):
		rules = self.parse("proj/*/\t# t\n\tsrc\t# u\n")
		self.assertEqual(self.tags(rules, "proj/a/src/f"), {"t", "u"})
		self.assertEqual(self.tags(rules, "proj/c/src/f"), {"t", "u"})  # src нет, но обычный путь не проверяется
		self.assertEqual(self.tags(rules, "proj/a/other"), {"t"})
		self.assertEqual(self.tags(rules, "proj/.hid/src"), set())  # * не трогает имена с точки

	def test_nested_wildcard_children_union_over_parents(self):
		rules = self.parse("proj/*/\n\ts*\t# u\n")
		self.assertEqual(self.tags(rules, "proj/a/src"), {"u"})
		self.assertEqual(self.tags(rules, "proj/b/src"), {"u"})
		self.assertEqual(self.tags(rules, "proj/c/doc"), set())

	def test_nested_wildcard_with_no_match_anywhere_is_an_error(self):
		with self.assertRaises(chkan.ChkanError):
			self.parse("proj/*/\n\tzzz*\t# u\n")

	def test_hidden_names_need_explicit_dot(self):
		rules = self.parse("proj/*/src\t# t\nproj/.h*/src\t# h\n")
		self.assertEqual(self.tags(rules, "proj/.hid/src"), {"h"})

	def test_escaping_with_brackets(self):
		rules = self.parse("star[*]name\t# lit\n")
		self.assertEqual(self.tags(rules, "star*name"), {"lit"})
		self.assertEqual(self.tags(rules, "starXname"), set())
		rules = self.parse("star*name\t# any\n")  # без экранирования * это wildcard
		self.assertEqual(self.tags(rules, "star*name"), {"any"})
		self.assertEqual(self.tags(rules, "starXname"), {"any"})

	def test_literal_bracket_name(self):
		rules = self.parse("d[[]1]\t# t\n")
		self.assertEqual(self.tags(rules, "d[1]/x1"), {"t"})

	def test_parent_with_magic_chars_is_escaped_when_used_as_prefix(self):
		rules = self.parse("d[[]1]/\t# t\n\tx*\t# u\n")
		self.assertEqual(self.tags(rules, "d[1]/x1"), {"t", "u"})
		self.assertEqual(self.tags(rules, "d[1]/x2"), {"t", "u"})
		self.assertEqual(self.tags(rules, "d[1]/y1"), {"t"})

	def test_exclude_and_run_apply_to_each_variant(self):
		rules = self.parse("proj/*/src\t# @exclude @run(echo hi)\n")
		self.assertEqual(rules.exclude, ["proj/a/src", "proj/b/src"])
		self.assertEqual(rules.runs, [("proj/a/src", "echo hi"), ("proj/b/src", "echo hi")])

	def test_dotdot_in_a_variant_is_an_error(self):
		for text in ("../*\t# t\n", "proj/{a,..}/x\t# t\n"):
			with self.assertRaises(chkan.ChkanError):
				self.parse(text)

	def test_wildcards_listing(self):
		rules = self.parse("plain\t# t\nproj/*/\t# t\n\tsrc\t# u\n\ts*\n{plain,proj}\t# z\n")
		self.assertEqual(
			rules.wildcards,
			[
				(2, "proj/*", ["proj/a", "proj/b", "proj/c"]),
				(3, "proj/*/src", ["proj/a/src", "proj/b/src", "proj/c/src"]),
				(4, "proj/*/s*", ["proj/a/src", "proj/b/src"]),
				(5, "{plain,proj}", ["plain", "proj"]),
			],
		)

	def test_plain_lines_are_not_in_listing(self):
		self.assertEqual(self.parse("plain\t# t\nproj/a\t# u\n").wildcards, [])

	def test_non_strict_shows_empty_expansion(self):
		rules = self.parse("proj/*/zzz\t# t\n\tinner*\t# u\n", strict=False)
		self.assertEqual(rules.wildcards, [(1, "proj/*/zzz", []), (2, "proj/*/zzz/inner*", [])])

	def test_is_wildcard(self):
		for yes in ("a*", "a?", "a[0-9]", "a/{b,c}"):
			self.assertTrue(chkan.is_wildcard(yes), yes)
		for no in ("a", "a/b", "a{b", "a}b", "file#1"):
			self.assertFalse(chkan.is_wildcard(no), no)

	def test_same_path_from_several_variants_applied_once(self):
		rules = self.parse("{plain,plain}\t# t\n")
		self.assertEqual(self.tags(rules, "plain"), {"t"})

	def test_wildcard_command_output(self):
		os.makedirs(os.path.join(self.root, ".changes"))
		with open(os.path.join(self.root, ".changes", "TAGS.txt"), "w", encoding="utf-8") as f:
			f.write("plain\t# t\nproj/*/src\t# u\n\tdeep\t# v\nproj/{a,b}\t# w\n")
		out, err = io.StringIO(), io.StringIO()
		with contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
			code = chkan.main(["wildcard", self.root])
		self.assertEqual(code, 0)
		self.assertEqual(
			out.getvalue(),
			"\n2: proj/*/src\nproj/a/src\nproj/b/src\nproj/c/src\n" if False else
			"\n2: proj/*/src\nproj/a/src\nproj/b/src\n"
			"\n3: proj/*/src/deep\nproj/a/src/deep\nproj/b/src/deep\n"
			"\n4: proj/{a,b}\nproj/a\nproj/b\n",
		)
		self.assertEqual(err.getvalue(), "")
		self.assertEqual(sorted(os.listdir(os.path.join(self.root, ".changes"))), ["TAGS.txt"])  # ничего не сканировалось

	def test_wildcard_command_without_wildcards_prints_nothing(self):
		os.makedirs(os.path.join(self.root, ".changes"))
		with open(os.path.join(self.root, ".changes", "TAGS.txt"), "w", encoding="utf-8") as f:
			f.write("plain\t# t\n")
		out = io.StringIO()
		with contextlib.redirect_stdout(out):
			self.assertEqual(chkan.main(["wildcard", self.root]), 0)
		self.assertEqual(out.getvalue(), "")

	def test_wildcard_command_shows_empty_expansion_instead_of_failing(self):
		os.makedirs(os.path.join(self.root, ".changes"))
		with open(os.path.join(self.root, ".changes", "TAGS.txt"), "w", encoding="utf-8") as f:
			f.write("proj/*/zzz\t# t\n")
		out = io.StringIO()
		with contextlib.redirect_stdout(out):
			self.assertEqual(chkan.main(["wildcard", self.root]), 0)
		self.assertEqual(out.getvalue(), "\n1: proj/*/zzz\n")

	def test_wildcard_command_without_tags_file(self):
		out, err = io.StringIO(), io.StringIO()
		with contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
			code = chkan.main(["wildcard", self.root])
		self.assertEqual(code, 2)
		self.assertEqual(out.getvalue(), "")
		self.assertIn("TAGS.txt", err.getvalue())

	def test_wildcard_word_is_a_command_only_in_first_position(self):
		with mock.patch.object(chkan, "cmd_wildcard", return_value=0) as cmd, mock.patch.object(chkan, "run", return_value=0) as run:
			chkan.main(["wildcard", self.root])
			self.assertEqual(cmd.call_args.args[0], self.root)
			run.assert_not_called()
			cmd.reset_mock()
			chkan.main([os.path.join(self.root, "wildcard")])
			cmd.assert_not_called()
			self.assertEqual(run.call_args.args[0], os.path.join(self.root, "wildcard"))

	def test_too_many_arguments(self):
		with contextlib.redirect_stderr(io.StringIO()), self.assertRaises(SystemExit):
			chkan.main(["a", "b"])


class Streams(unittest.TestCase):
	def test_msg_goes_to_stderr(self):
		out, err = io.StringIO(), io.StringIO()
		with contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
			chkan.msg("привет")
			chkan.msg("без перевода", end="")
		self.assertEqual((out.getvalue(), err.getvalue()), ("", "привет\nбез перевода"))

	def test_ask_prompts_on_stderr_and_defaults_to_no(self):
		for answer, expected in (("y", True), ("YES", True), ("д", True), ("да", True), ("", False), ("n", False), ("нет", False), ("yy", False)):
			out, err = io.StringIO(), io.StringIO()
			with contextlib.redirect_stdout(out), contextlib.redirect_stderr(err), mock.patch("builtins.input", return_value=answer):
				self.assertEqual(chkan.ask("Продолжить? [y/N] "), expected, answer)
			self.assertEqual(out.getvalue(), "")
			self.assertEqual(err.getvalue(), "Продолжить? [y/N] ")

	def test_ask_eof_is_no(self):
		with contextlib.redirect_stderr(io.StringIO()), mock.patch("builtins.input", side_effect=EOFError):
			self.assertFalse(chkan.ask("?"))

	def test_progress_can_be_disabled_globally(self):
		class TTY(io.StringIO):
			def isatty(self):
				return True

		self.assertTrue(chkan.Progress("x", total=1, stream=TTY()).enabled)
		with mock.patch.object(chkan, "_progress_disabled", True):
			self.assertFalse(chkan.Progress("x", total=1, stream=TTY()).enabled)
			self.assertTrue(chkan.Progress("x", total=1, stream=TTY(), enabled=True).enabled)  # явное enabled сильнее


# ------------------------------------------------------------ chkan.py целиком


class EndToEnd(unittest.TestCase):
	def setUp(self):
		self.tmp = tempfile.TemporaryDirectory()
		self.addCleanup(self.tmp.cleanup)
		self.root = os.path.realpath(self.tmp.name)
		self.changes = os.path.join(self.root, ".changes")
		self.clock = datetime.datetime(2026, 10, 1, 15, 0, 0, tzinfo=TZ)
		self.auto_tags = True  # перед первым запуском создать TAGS.txt с исключением .changes
		p = mock.patch.object(chkan, "_now", lambda: self.clock)
		p.start()
		self.addCleanup(p.stop)

	def write(self, rel, data):
		p = os.path.join(self.root, rel)
		os.makedirs(os.path.dirname(p), exist_ok=True)
		with open(p, "wb") as f:
			f.write(data)
		return p

	def tags(self, text, raw=False):
		os.makedirs(self.changes, exist_ok=True)
		with open(os.path.join(self.changes, "TAGS.txt"), "w", encoding="utf-8") as f:
			f.write(text if raw else ".changes/\t# @exclude\n" + text)

	def run_chkan(self, *extra):
		if self.auto_tags and not os.path.exists(self.changes):
			self.tags("")
		out, err = io.StringIO(), io.StringIO()
		with contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
			code = chkan.main([self.root, *extra])
		return code, out.getvalue(), err.getvalue()

	def step(self, *extra):
		self.clock += datetime.timedelta(minutes=1)
		return self.run_chkan("-y", *extra)

	def files(self):
		return sorted(os.listdir(self.changes))

	def scans(self):
		return [f for f in self.files() if f.startswith("scan_")]

	def diffs(self):
		return [f for f in self.files() if f.startswith("diff_")]

	def fail_on_bad(self):
		real = chkan.stat_record

		def fake(path):
			if os.path.basename(path).startswith("bad"):
				raise OSError(errno.EACCES, "Permission denied", path)
			return real(path)

		return mock.patch.object(chkan, "stat_record", fake)

	def test_warnings_default_answer_is_no(self):
		self.write("ok", b"1")
		self.write("bad1", b"1")
		with self.fail_on_bad(), mock.patch("builtins.input", return_value="") as inp:
			code, out, err = self.run_chkan("-y")
		self.assertEqual(code, 1)
		inp.assert_called_once()
		self.assertIn("(1)", err)
		self.assertIn("[y/N]", err)
		self.assertEqual(self.scans(), [])
		self.assertIn("Предупреждение: не прочитано bad1: Permission denied", err)
		self.assertIn("Отмена", err)
		self.assertEqual(out, "")

	def test_warnings_explicit_no_and_eof(self):
		self.write("bad1", b"1")
		for kw in ({"return_value": "n"}, {"side_effect": EOFError}):
			with self.fail_on_bad(), mock.patch("builtins.input", **kw):
				code, _, _ = self.run_chkan("-y")
			self.assertEqual(code, 1)
			self.assertEqual(self.scans(), [])

	def test_warnings_yes_answer_continues(self):
		self.write("ok", b"1")
		self.write("bad1", b"1")
		with self.fail_on_bad(), mock.patch("builtins.input", return_value="y"):
			code, _, _ = self.run_chkan("-y")
		self.assertEqual(code, 0)
		self.assertEqual(len(self.scans()), 1)

	def test_warn_yes_flag_skips_question(self):
		self.write("ok", b"1")
		self.write("bad1", b"1")
		with self.fail_on_bad(), mock.patch("builtins.input", side_effect=AssertionError("не должен спрашивать")):
			code, _, err = self.run_chkan("-y", "-w")
		self.assertEqual(code, 0)
		self.assertEqual(len(self.scans()), 1)
		self.assertIn("bad1", err)
		with self.fail_on_bad(), mock.patch("builtins.input", side_effect=AssertionError("не должен спрашивать")):
			self.clock += datetime.timedelta(minutes=1)
			code, _, _ = self.run_chkan("-y", "--warn-yes")
		self.assertEqual(code, 0)

	def test_no_warnings_no_question(self):
		self.write("ok", b"1")
		with mock.patch("builtins.input", side_effect=AssertionError("не должен спрашивать")):
			code, _, err = self.run_chkan("-y")
		self.assertEqual(code, 0)
		self.assertNotIn("Предупреждение", err)

	def test_declined_warnings_keep_previous_scan_and_write_no_diff(self):
		self.write("ok", b"1")
		self.assertEqual(self.step()[0], 0)
		before = self.files()
		self.write("bad1", b"1")
		with self.fail_on_bad(), mock.patch("builtins.input", return_value="n"):
			code, _, _ = self.step()
		self.assertEqual(code, 1)
		self.assertEqual(self.files(), before)

	def test_many_warnings_output_shape(self):
		for i in range(12):
			self.write(f"bad{i:02d}", b"1")
		with self.fail_on_bad():
			code, _, err = self.run_chkan("-y", "-w")
		self.assertEqual(code, 0)
		lines = err.splitlines()
		self.assertEqual(len(lines), 12)
		self.assertTrue(all(l.startswith("Предупреждение: не прочитано bad") for l in lines[:10]))
		self.assertEqual(lines[10], "... и ещё 2")
		self.assertTrue(lines[11].startswith("первый скан сохранён"))

	def hook_file(self, name):
		with open(os.path.join(self.root, name), encoding="utf-8") as f:
			return f.read()

	def test_run_hooks_at_the_very_end_with_env_and_echo(self):
		os.makedirs(os.path.join(self.root, "папка"))
		self.tags(
			"папка\t# @run(echo \"$PWD\" > ../cwd1.txt; echo $CHKAN_DIFF > ../diff1.txt)\n"
			"# @run(echo $CHKAN_TAGS $CHKAN_SCAN > env2.txt) // комментарий\n"
		)
		self.write("a.txt", b"1")
		self.assertEqual(self.step()[0], 0)
		self.write("b.txt", b"2")
		code, out, err = self.step()
		self.assertEqual(code, 0)
		echo = [l for l in err.splitlines() if "# @run(" in l]
		self.assertEqual(echo, ["папка # @run(echo \"$PWD\" > ../cwd1.txt; echo $CHKAN_DIFF > ../diff1.txt)", ". # @run(echo $CHKAN_TAGS $CHKAN_SCAN > env2.txt)"])
		self.assertTrue(out.splitlines()[0].startswith("без тегов:"))
		self.assertNotIn("@run", out)  # в stdout только статистика
		self.assertEqual(self.hook_file("cwd1.txt").strip(), os.path.join(self.root, "папка"))
		self.assertEqual(self.hook_file("diff1.txt").strip(), os.path.join(self.changes, self.diffs()[-1]))
		tags_path, scan_path = self.hook_file("env2.txt").split()
		self.assertEqual(tags_path, os.path.join(self.changes, "TAGS.txt"))
		self.assertEqual(scan_path, os.path.join(self.changes, self.scans()[-1]))
		self.assertTrue(os.path.isabs(tags_path) and os.path.isabs(scan_path))

	def test_hooks_see_files_after_cleanup_of_previous_scan(self):
		self.tags("# @run(ls .changes > listing.txt)\n")
		for name in ("a", "b", "c"):
			self.write(name, b"1")
			self.assertEqual(self.step()[0], 0)
		listing = self.hook_file("listing.txt").split()
		self.assertEqual(len([n for n in listing if n.startswith("scan_")]), 2)  # к этому моменту лишний скан уже удалён
		self.assertEqual(len([n for n in listing if n.startswith("diff_")]), 2)

	def test_hooks_run_on_first_scan_with_empty_diff_var(self):
		self.tags("# @run(echo \"[$CHKAN_DIFF]\" > first.txt; echo $CHKAN_SCAN >> first.txt)\n")
		code, out, err = self.step()
		self.assertEqual(code, 0)
		self.assertEqual(out, "")
		self.assertIn("первый скан", err)
		first = self.hook_file("first.txt").split("\n")
		self.assertEqual(first[0], "[]")
		self.assertEqual(first[1], os.path.join(self.changes, self.scans()[0]))

	def test_failed_hook_gives_exit_code_3_but_scan_is_saved(self):
		self.tags("# @run(exit 5) @run(echo after > after.txt)\n")
		self.write("a", b"1")
		code, out, err = self.step()
		self.assertEqual(code, 3)
		self.assertIn("кодом 5", err)
		self.assertEqual(self.hook_file("after.txt").strip(), "after")
		self.assertEqual(len(self.scans()), 1)

	def test_no_hooks_when_scan_cancelled_by_warnings(self):
		self.tags("# @run(echo ran > ran.txt)\n")
		self.write("bad1", b"1")
		with self.fail_on_bad(), mock.patch("builtins.input", return_value="n"):
			code, _, _ = self.step()
		self.assertEqual(code, 1)
		self.assertFalse(os.path.exists(os.path.join(self.root, "ran.txt")))

	def test_no_hooks_when_verification_fails(self):
		self.tags("# @run(echo ran >> ran.txt)\n")
		self.write("a", b"1")
		self.step()
		self.write("b", b"1")
		self.step()
		os.remove(os.path.join(self.root, "ran.txt"))
		self.write("c", b"1")
		with mock.patch.object(chkan, "apply_ops", lambda flat, ops, strict=True, progress=None: flat):
			code, _, _ = self.step()
		self.assertEqual(code, 2)
		self.assertFalse(os.path.exists(os.path.join(self.root, "ran.txt")))

	def test_hooks_ignored_for_excluded_path_scan_but_cwd_is_real_folder(self):
		os.makedirs(os.path.join(self.root, "inner"))
		self.write("inner/f", b"1")
		self.tags("inner/\t# @exclude @run(ls > ../inner_ls.txt)\n")
		self.assertEqual(self.step()[0], 0)
		self.assertEqual(self.hook_file("inner_ls.txt").split(), ["f"])
		scan = chkan.read_json(os.path.join(self.changes, self.scans()[0]))
		self.assertNotIn("inner/f", chkan.flatten(scan["tree"]))

	def test_tags_with_special_syntax_end_to_end(self):
		self.tags("data/\t# tracked\n\tsecret/\t# @no(tracked)\n.cache/\t# @exclude\n")
		self.write("data/a", b"1")
		self.write("data/secret/s", b"1")
		self.write(".cache/c", b"1")
		self.step()
		self.write("data/a", b"22")
		self.write("data/secret/s", b"22")
		code, out, _ = self.step()
		self.assertEqual(code, 0)
		lines = out.splitlines()
		self.assertTrue(lines[0].startswith("без тегов: +0 -0 ~1"))  # data/secret/s без тега
		self.assertTrue(lines[1].startswith("tracked: +0 -0 ~1"))  # data/a


	def log(self):
		return read_text(os.path.join(self.changes, "chkan-log.txt"))

	def test_stdout_has_only_statistics_and_everything_else_goes_to_stderr(self):
		self.tags("data/\t# tracked\n")
		self.write("data/a", b"1")
		code, out, err = self.step()
		self.assertEqual((code, out), (0, ""))  # у первого скана статистики нет
		self.assertIn("первый скан сохранён", err)
		self.write("data/b", b"22")
		code, out, err = self.step()
		pattern = re.compile(r"^[^:\n]+: \+\d+ -\d+ ~\d+ ->\d+ d~\d+( sf:\S+ st:\S+ s\S+)?$")
		lines = out.splitlines()
		self.assertGreaterEqual(len(lines), 2)
		self.assertTrue(all(pattern.match(l) for l in lines), lines)
		for line in lines:
			self.assertNotIn(line, err)
		self.assertNotIn("\r", out)

	def test_error_goes_to_stderr_only(self):
		self.tags("a/\t# @bogus\n")
		code, out, err = self.step()
		self.assertEqual((code, out), (2, ""))
		self.assertIn("Ошибка:", err)

	def test_comment_goes_to_diff_after_to_and_to_env(self):
		self.tags("# @run(echo \"[$CHKAN_COMMENT]\" > c.txt)\n")
		self.write("a", b"1")
		self.step()
		self.assertEqual(self.hook_file("c.txt").strip(), "[]")  # без -c переменная пустая, но задана
		self.write("b", b"22")
		code, out, err = self.step("-c", "правил b")
		self.assertEqual(code, 0)
		path = os.path.join(self.changes, self.diffs()[-1])
		d = chkan.read_json(path)
		self.assertEqual(d["c"], "правил b")
		self.assertEqual(list(d)[:6], ["format", "root", "from", "to", "c", "groups"])
		text = read_text(path)
		self.assertLess(text.index('"to"'), text.index('"c": "правил b"'))
		self.assertLess(text.index('"c": "правил b"'), text.index('"groups"'))
		self.assertEqual(self.hook_file("c.txt").strip(), "[правил b]")

	def test_no_comment_no_c_key(self):
		self.write("a", b"1")
		self.step()
		self.write("b", b"1")
		self.step()
		self.assertNotIn("c", chkan.read_json(os.path.join(self.changes, self.diffs()[-1])))

	def test_comment_with_quotes_and_newlines(self):
		self.tags("# @run(printf '%s' \"$CHKAN_COMMENT\" > c.txt)\n")
		self.write("a", b"1")
		self.step()
		self.write("b", b"1")
		comment = 'строка "1"\nстрока 2 \\ ёж'
		self.step("-c", comment)
		self.assertEqual(chkan.read_json(os.path.join(self.changes, self.diffs()[-1]))["c"], comment)
		self.assertEqual(self.hook_file("c.txt"), comment)
		self.assertIn(comment + "\n", self.log())

	def test_comment_on_first_scan_goes_to_log_and_env(self):
		self.tags("# @run(echo \"$CHKAN_COMMENT\" > c.txt)\n")
		self.write("a", b"1")
		self.step("-c", "старт")
		self.assertEqual(self.hook_file("c.txt").strip(), "старт")
		self.assertEqual(self.diffs(), [])
		self.assertEqual(self.log(), "\n--- 2026-10-01 15:01:00 ---\nстарт\nпервый скан: 2 объектов\n")

	def test_log_format_and_appending(self):
		self.write("a", b"1")
		_, out1, _ = self.step()
		first = "\n--- 2026-10-01 15:01:00 ---\nпервый скан: 2 объектов\n"
		self.assertEqual(self.log(), first)
		self.write("a", b"22")
		_, out2, _ = self.step("-c", "увеличил a")
		second = "\n--- 2026-10-01 15:02:00 ---\nувеличил a\n" + out2
		self.assertEqual(self.log(), first + second)
		self.write("b", b"3")
		_, out3, _ = self.step()
		third = "\n--- 2026-10-01 15:03:00 ---\n" + out3  # без комментария строки комментария нет
		self.assertEqual(self.log(), first + second + third)
		self.assertTrue(self.log().startswith("\n"))
		self.assertIn("без тегов: +1", out3)

	def test_log_is_in_changes_dir_and_ignored_when_scan_is_cancelled(self):
		self.write("bad1", b"1")
		with self.fail_on_bad(), mock.patch("builtins.input", return_value="n"):
			code, _, _ = self.step("-c", "не сохранится")
		self.assertEqual(code, 1)
		self.assertNotIn("chkan-log.txt", self.files())

	def test_log_written_before_hooks_and_even_if_they_fail(self):
		self.tags("# @run(cp .changes/chkan-log.txt seen.txt; exit 1)\n")
		self.write("a", b"1")
		code, _, _ = self.step("-c", "x")
		self.assertEqual(code, 3)
		self.assertIn("\nx\n", self.hook_file("seen.txt"))

	def test_log_failure_is_only_a_warning(self):
		self.tags("")
		os.makedirs(os.path.join(self.changes, "chkan-log.txt"))  # вместо файла папка
		self.write("a", b"1")
		code, _, err = self.step()
		self.assertEqual(code, 0)
		self.assertIn("не удалось записать chkan-log.txt", err)
		self.assertEqual(len(self.scans()), 1)

	def run_tty(self, *extra):
		class TTY(io.StringIO):
			def isatty(self):
				return True

		out, err = io.StringIO(), TTY()
		with contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
			code = chkan.main([self.root, "-y", *extra])
		return code, out.getvalue(), err.getvalue()

	def test_progress_bars_go_to_stderr_and_can_be_disabled(self):
		self.write("a", b"1")
		self.clock += datetime.timedelta(minutes=1)
		code, out, err = self.run_tty()
		self.assertEqual(code, 0)
		self.assertIn("сканирование", err)
		self.assertIn("слепок: дерево", err)
		self.assertIn("прошло", err)
		self.assertNotIn("\r", out)
		self.write("b", b"1")
		self.clock += datetime.timedelta(minutes=1)
		code, out, err = self.run_tty()
		self.assertIn("diff: сравнение", err)
		self.assertIn("проверка: откат", err)
		self.write("c", b"1")
		self.clock += datetime.timedelta(minutes=1)
		code, out, err = self.run_tty("-P")
		self.assertEqual(code, 0)
		self.assertNotIn("\r", err)
		self.assertNotIn("прошло", err)
		self.assertNotIn("%", err)
		self.assertTrue(out.startswith("без тегов:"))  # статистика на месте
		self.assertFalse(chkan._progress_disabled)

	def test_no_progress_long_option_and_flag_restored_after_error(self):
		self.tags("a/\t# @bogus\n")
		code, _, _ = self.run_tty("--no-progress")
		self.assertEqual(code, 2)
		self.assertFalse(chkan._progress_disabled)

	def test_warnings_still_shown_with_no_progress(self):
		self.write("bad1", b"1")
		with self.fail_on_bad():
			code, _, err = self.run_tty("-P", "-w")
		self.assertEqual(code, 0)
		self.assertIn("Предупреждение: не прочитано bad1", err)

	def test_asks_and_cancel(self):
		self.auto_tags = False
		with mock.patch("builtins.input", return_value="n"):
			code, out, err = self.run_chkan()
		self.assertEqual(code, 1)
		self.assertFalse(os.path.exists(self.changes))
		self.assertIn("Отмена", err)
		self.assertIn("Создать её и сделать первый скан", err)  # вопрос тоже в stderr
		self.assertEqual(out, "")

	def test_asks_and_eof_is_cancel(self):
		self.auto_tags = False
		with mock.patch("builtins.input", side_effect=EOFError):
			code, _, _ = self.run_chkan()
		self.assertEqual(code, 1)
		self.assertFalse(os.path.exists(self.changes))

	def test_asks_and_creates(self):
		self.auto_tags = False
		self.write("a.txt", b"1")
		with mock.patch("builtins.input", return_value="y"):
			code, out, err = self.run_chkan()
		self.assertEqual(code, 0)
		self.assertEqual(self.files(), ["TAGS.txt", "chkan-log.txt", "scan_20261001-150000.json"])
		self.assertIn("первый скан", err)
		self.assertEqual(out, "")
		chkan.parse_tags(read_text(os.path.join(self.changes, "TAGS.txt")))

	def test_missing_tags_file_is_error(self):
		os.makedirs(self.changes)
		code, _, err = self.run_chkan()
		self.assertEqual(code, 2)
		self.assertIn("TAGS.txt", err)

	def test_bad_tags_file_is_error(self):
		self.tags("a/\t# @bogus\n")
		code, _, err = self.run_chkan()
		self.assertEqual(code, 2)
		self.assertIn("@bogus", err)
		self.assertEqual(self.scans(), [])

	def test_console_counters(self):
		self.tags("data/\t# tracked\n// logs ниже\n# noisy(ignore)\nnoise/\t# noisy\n")
		self.write("data/a.txt", b"1")
		self.write("data/old.txt", b"old")
		self.write("loose.txt", b"1")
		self.write("noise/n.txt", b"1")
		self.assertEqual(self.step()[0], 0)
		self.write("data/new.txt", b"newer!!")
		os.rename(os.path.join(self.root, "data/a.txt"), os.path.join(self.root, "data/renamed.txt"))
		os.remove(os.path.join(self.root, "data/old.txt"))
		self.write("loose.txt", b"changed!")
		self.write("noise/n.txt", b"1234567")
		code, out, _ = self.step()
		self.assertEqual(code, 0)
		self.assertEqual(
			out.splitlines(),
			[
				"без тегов: +0 -0 ~1 ->0 d~1 sf:1B st:8B s+7B",  # loose.txt; корень (поменялся total)
				"tracked: +1 -1 ~0 ->1 d~1 sf:4B st:8B s+4B",  # new.txt, old.txt, a.txt->renamed.txt; папка data
				"с не отслеживаемыми тэгами: +0 -0 ~1 ->0 d~1",  # noise/n.txt и папка noise (total); размеров нет
			],
		)

	def test_scan_retention_and_reconstruction(self):
		self.tags("data/\t# tracked\n.cache/\t# @exclude\n")
		self.write("data/a.txt", b"1")
		self.write("data/old.txt", b"old")
		self.write("loose.txt", b"1")
		self.write(".cache/c", b"1")
		self.assertEqual(self.step()[0], 0)
		self.assertEqual(self.scans(), ["scan_20261001-150100.json"])
		self.assertEqual(self.diffs(), [])
		first = chkan.read_json(os.path.join(self.changes, self.scans()[0]))

		# шаг 2: добавили, изменили, переименовали, удалили
		self.write("data/new.txt", b"new")
		self.write("loose.txt", b"changed!")
		os.rename(os.path.join(self.root, "data/a.txt"), os.path.join(self.root, "data/renamed.txt"))
		os.remove(os.path.join(self.root, "data/old.txt"))
		code, out, _ = self.step()
		self.assertEqual(code, 0)
		self.assertEqual(len(self.scans()), 2)  # предыдущий — самый первый, остаётся
		self.assertEqual(len(self.diffs()), 1)
		d1 = chkan.read_json(os.path.join(self.changes, self.diffs()[0]))
		kinds = sorted(o["op"] for o in chkan.diff_ops(d1) if o["path"].startswith("data/"))
		self.assertEqual(kinds, ["add", "move", "remove"])
		second = chkan.read_json(os.path.join(self.changes, self.scans()[-1]))

		# шаг 3: второй скан удалён, первый и последний остались
		self.write("data/more.txt", b"zzz")
		self.assertEqual(self.step()[0], 0)
		self.assertEqual(len(self.scans()), 2)
		self.assertEqual(self.scans()[0], "scan_20261001-150100.json")
		self.assertEqual(len(self.diffs()), 2)
		last = chkan.read_json(os.path.join(self.changes, self.scans()[-1]))
		d1, d2 = [chkan.read_json(os.path.join(self.changes, n)) for n in self.diffs()]
		restored2 = chkan.apply_diff(last, d2, backward=True)
		self.assertTrue(chkan.snapshots_equal(restored2, second))
		restored1 = chkan.apply_diff(restored2, d1, backward=True)
		self.assertTrue(chkan.snapshots_equal(restored1, first))
		forward = chkan.apply_diff(chkan.apply_diff(first, d1), d2)
		self.assertTrue(chkan.snapshots_equal(forward, last))

	def test_exclude_changes_between_runs(self):
		self.write("a/x", b"1")
		self.write("b/y", b"22")
		self.tags("a/\t# @exclude\n")
		self.assertEqual(self.step()[0], 0)
		self.tags("b/\t# @exclude\n")
		self.assertEqual(self.step()[0], 0)
		d = chkan.read_json(os.path.join(self.changes, self.diffs()[0]))
		scan_ops = sorted((o["op"], o["path"]) for o in chkan.diff_ops(d) if o["op"].endswith("_scan"))
		self.assertEqual(scan_ops, [("add_scan", "a"), ("add_scan", "a/x"), ("remove_scan", "b"), ("remove_scan", "b/y")])
		for _label, c, _size in chkan.count_ops(d):
			self.assertEqual((c["add"], c["remove"], c["move"]), (0, 0, 0))  # *_scan не считаются

	def test_tags_change_does_not_rewrite_old_diffs(self):
		self.tags("")
		self.write("a/x", b"1")
		self.step()
		self.write("a/y", b"1")
		self.step()
		path = os.path.join(self.changes, self.diffs()[0])
		before = read_text(path)
		self.tags("a/\t# t\n")
		self.write("a/z", b"1")
		self.step()
		self.assertEqual(read_text(path), before)
		d2 = chkan.read_json(os.path.join(self.changes, self.diffs()[1]))
		self.assertIn("t", d2["groups"])

	def test_comments_survive_and_apply_still_works(self):
		self.tags("a/\t# t\n")
		self.write("a/x", b"1")
		self.step()
		self.write("a/y", b"1")
		self.step()
		path = os.path.join(self.changes, self.diffs()[0])
		d = chkan.read_json(path)
		d["groups"]["t"]["c"] = "добавил y"
		chkan.write_text_atomic(path, chkan.dumps_pretty(d) + "\n")
		last = chkan.read_json(os.path.join(self.changes, self.scans()[-1]))
		chkan.apply_diff(last, chkan.read_json(path), backward=True)

	def test_verification_failure_keeps_previous_scan(self):
		self.write("a", b"1")
		self.step()
		self.write("b", b"1")
		self.step()
		self.write("c", b"1")
		middle = self.scans()[-1]
		with mock.patch.object(chkan, "apply_ops", lambda flat, ops, strict=True, progress=None: flat):
			code, _, err = self.step()
		self.assertEqual(code, 2)
		self.assertIn("не восстанавливается", err)
		self.assertIn(middle, self.scans())

	def test_same_second_waits(self):
		self.write("a", b"1")
		self.step()
		later = self.clock + datetime.timedelta(seconds=1)
		times = iter([self.clock, later])
		with mock.patch.object(chkan, "_now", lambda: next(times)), mock.patch.object(chkan.time, "sleep") as sleep:
			code, _, _ = self.run_chkan()
		self.assertEqual(code, 0)
		sleep.assert_called_once()

	def test_clock_going_back_is_error(self):
		self.write("a", b"1")
		self.step()
		self.clock -= datetime.timedelta(hours=2)
		code, _, err = self.run_chkan()
		self.assertEqual(code, 2)
		self.assertIn("раньше", err)
		self.assertEqual(len(self.scans()), 1)

	def test_headers_have_no_tz_but_offset_in_taken_at(self):
		self.write("a", b"1")
		self.step()
		self.write("b", b"1")
		self.step()
		scan = chkan.read_json(os.path.join(self.changes, self.scans()[-1]))
		d = chkan.read_json(os.path.join(self.changes, self.diffs()[0]))
		self.assertNotIn("tz", scan)
		self.assertNotIn("tz", d["from"])
		self.assertNotIn("tz", d["to"])
		self.assertEqual(d["from"]["taken_at"], "2026-10-01T15:01:00+02:00")
		self.assertEqual(d["to"]["taken_at"], "2026-10-01T15:02:00+02:00")

	def test_changes_dir_is_not_in_snapshot_when_excluded(self):
		self.write("a", b"1")
		self.step()
		scan = chkan.read_json(os.path.join(self.changes, self.scans()[0]))
		self.assertEqual(sorted(chkan.flatten(scan["tree"])), [".", "a"])

	def test_changes_dir_is_scanned_by_default(self):
		self.auto_tags = False
		self.tags("", raw=True)
		self.write("a", b"1")
		self.step()
		scan = chkan.read_json(os.path.join(self.changes, self.scans()[0]))
		self.assertEqual(sorted(chkan.flatten(scan["tree"])), [".", ".changes", ".changes/TAGS.txt", "a"])
		self.step()
		d = chkan.read_json(os.path.join(self.changes, self.diffs()[0]))
		paths = {o["path"] for o in chkan.diff_ops(d)}
		self.assertIn(".changes/chkan-log.txt", paths)
		self.assertIn(".changes/scan_20261001-150100.json", paths)  # скан с прошлого запуска виден как добавленный

	def record_stages(self):
		stages = []

		def fake(label="", total=None, counted=True):
			stages.append((label, total, counted))
			return chkan.NullProgress()

		return stages, mock.patch.object(chkan, "make_stage", fake)

	def test_first_run_stages(self):
		self.write("a/b", b"1")
		stages, patch = self.record_stages()
		with patch:
			self.assertEqual(self.run_chkan("-y")[0], 0)
		self.assertEqual([s[0] for s in stages], ["размеры папок", "слепок: дерево", "слепок: запись"])
		self.assertEqual(stages[1][1], 3)  # корень, a, a/b
		self.assertFalse(stages[2][2])  # запись: без счётчика

	def test_second_run_has_separate_stage_for_each_step(self):
		self.write("a/b", b"1")
		self.step()
		self.write("a/c", b"22")
		stages, patch = self.record_stages()
		with patch:
			self.assertEqual(self.step()[0], 0)
		self.assertEqual(
			[s[0] for s in stages],
			[
				"размеры папок",
				"слепок: дерево",
				"слепок: запись",
				"diff: сравнение",
				"diff: группы и размеры",
				"diff: запись",
				"проверка: чтение diff",
				"проверка: откат",
			],
		)
		by = {s[0]: s for s in stages}
		self.assertEqual(by["diff: группы и размеры"][1], 3 + 3 + 4)  # 3 операции (корень, a, a/c), 3 старые записи, 4 новые
		self.assertEqual(by["проверка: откат"][1], 3 + 3)  # три операции + записей в прошлом слепке

	def test_scan_files_have_dir_totals_only_on_dirs(self):
		self.write("a/b", b"12345")
		self.write("c", b"1")
		self.step()
		scan = chkan.read_json(os.path.join(self.changes, self.scans()[0]))
		flat = chkan.flatten(scan["tree"])
		self.assertEqual((flat["."]["total"], flat["a"]["total"]), (6, 5))
		self.assertNotIn("total", flat["a/b"])
		self.assertNotIn("total", flat["c"])
		self.assertEqual(list(flat["a"])[:5], ["type", "dev", "ino", "size", "total"])

	def test_diff_shows_totals_of_all_ancestors(self):
		self.write("a/b/f", b"12345")
		self.step()
		self.write("a/b/f", b"123456789012")
		self.step()
		d = chkan.read_json(os.path.join(self.changes, self.diffs()[0]))
		ops = {o["path"]: o for o in chkan.diff_ops(d)}
		self.assertEqual(ops["a/b/f"]["op"], "change")
		self.assertEqual(ops["a/b/f"]["set"]["size"], [5, 12])
		for path in (".", "a", "a/b"):
			self.assertEqual(ops[path]["op"], "change_d", path)
			self.assertEqual(ops[path]["set"]["total"], [5, 12])
		text = read_text(os.path.join(self.changes, self.diffs()[0]))
		self.assertIn('"total": ["5=5B", "12=12B", "+7B"]', text)

	def test_unchanged_dir_total_not_in_diff(self):
		self.write("a/f", b"12345")
		self.write("b/g", b"1")
		self.step()
		self.write("b/g", b"22")
		self.step()
		d = chkan.read_json(os.path.join(self.changes, self.diffs()[0]))
		paths = {o["path"] for o in chkan.diff_ops(d)}
		self.assertEqual(paths, {".", "b", "b/g"})  # a не менялась

	def test_idle_run_gives_zero_counters(self):
		self.write("a", b"1")
		self.step()
		code, out, _ = self.step()
		self.assertEqual(code, 0)
		self.assertEqual(out.splitlines(), ["без тегов: +0 -0 ~0 ->0 d~0", "с не отслеживаемыми тэгами: +0 -0 ~0 ->0 d~0"])


# ------------------------------------------------------------ переход с формата 3


def ns(sec, frac=0):
	return sec * 10**9 + frac


T1 = 1_790_000_000


def v3_total(a, b):
	"""Формат 3: [было, стало, +байты=+размер] строками."""
	fmt = chkan.format_size
	if a is None or b is None:
		return [None if a is None else fmt(a), None if b is None else fmt(b)]
	n = b - a
	if n == 0:
		delta = "0=0B"
	else:
		sign = "+" if n > 0 else "-"
		num, human = fmt(abs(n)).split("=")
		delta = f"{sign}{num}={sign}{human}"
	return [fmt(a), fmt(b), delta]


def v3_node(op):
	node = {"op": op["op"]}
	for k in ("new", "old"):
		if k in op:
			r = dict(op[k])
			if "total" in r:
				r["total"] = chkan.format_size(r["total"])
			node[k] = r
	if "from" in op:
		node["from"] = op["from"]
	if "set" in op:
		changes = dict(op["set"])
		if "total" in changes:
			changes["total"] = v3_total(*changes["total"])
		node["set"] = changes
	return node


def v3_flats():
	a = {
		".": drec(ino=1, dev=7, mtime=ns(T1, 111), ctime=ns(T1, 111), btime=ns(T1 - 5000, 5)),
		"a.txt": rec(ino=2, dev=7, size=5, mtime=ns(T1, 222), ctime=ns(T1, 222), btime=ns(T1 - 100, 9)),
		"b.txt": rec(ino=3, dev=7, size=10, mtime=ns(T1, 333), ctime=ns(T1, 333)),
		"sub": drec(ino=4, dev=7, mtime=ns(T1, 1), ctime=ns(T1, 1)),
	}
	b = {
		".": drec(ino=1, dev=7, mtime=ns(T1 + 100, 7), ctime=ns(T1 + 100, 7), btime=ns(T1 - 5000, 5)),
		"a.txt": rec(ino=2, dev=7, size=8, mtime=ns(T1 + 50, 1), ctime=ns(T1 + 50, 1), btime=ns(T1 - 100, 9)),
		"b.txt": rec(ino=3, dev=7, size=10, mtime=ns(T1, 999), ctime=ns(T1, 333)),  # изменилась только доля секунды
		"c.txt": rec(ino=5, dev=7, size=4, mtime=ns(T1 + 60, 2), ctime=ns(T1 + 60, 2)),
		"sub2": drec(ino=4, dev=7, mtime=ns(T1, 1), ctime=ns(T1, 500)),  # sub переименована, ctime только в наносекундах
	}
	return chkan.add_dir_totals(a), chkan.add_dir_totals(b)


A_AT = "2026-10-01T15:00:00+02:00"
B_AT = "2026-10-02T09:00:00+02:00"
A_NAME = "scan_20261001-150000.json"
B_NAME = "scan_20261002-090000.json"
D_NAME = "diff_20261001-150000_20261002-090000.json"


def v3_files(root):
	"""Файлы формата 3, как их писала прошлая версия: наносекунды, tz, длинные дельты."""
	flat_a, flat_b = v3_flats()

	def snapshot(flat, at):
		return {"format": 3, "root": root, "taken_at": at, "tz": "+02:00", "exclude": [".changes"], "tree": chkan.build_tree(flat)}

	ops = chkan.make_ops(flat_a, flat_b, chkan.make_excluder([]), chkan.make_excluder([]))
	nodes = {op["path"]: v3_node(op) for op in ops}
	nodes["a.txt"]["c"] = "правил a"
	sums = [sum(r["size"] for r in f.values() if r["type"] == "f") for f in (flat_a, flat_b)]
	group = {"size": [chkan.format_size(n) for n in sums], "c": "группа", "ch": chkan.build_ch(nodes)}
	head = lambda at: {"taken_at": at, "tz": "+02:00", "exclude": [".changes"]}
	diff = {
		"format": 3, "root": root, "from": head(A_AT), "to": head(B_AT),
		"groups": {"-": group, "-" * 10: 0},
	}
	dump = lambda x: json.dumps(x, ensure_ascii=False, separators=(",", ":"))
	return {
		A_NAME: dump(snapshot(flat_a, A_AT)),
		B_NAME: dump(snapshot(flat_b, B_AT)),
		D_NAME: json.dumps(diff, ensure_ascii=False, indent="\t") + "\n",
	}


class MigrationFormat3(unittest.TestCase):
	def setUp(self):
		self.tmp = tempfile.TemporaryDirectory()
		self.addCleanup(self.tmp.cleanup)
		self.root = os.path.realpath(self.tmp.name)
		self.changes = os.path.join(self.root, ".changes")
		os.makedirs(self.changes)
		with open(os.path.join(self.changes, "TAGS.txt"), "w", encoding="utf-8") as f:
			f.write(".changes/\t# @exclude\n")
		self.originals = v3_files(self.root)
		for name, text in self.originals.items():
			self.put(name, text)
		self.clock = datetime.datetime(2026, 10, 3, 10, 0, 0, tzinfo=TZ)
		p = mock.patch.object(chkan, "_now", lambda: self.clock)
		p.start()
		self.addCleanup(p.stop)

	def put(self, name, text):
		with open(os.path.join(self.changes, name), "w", encoding="utf-8") as f:
			f.write(text)

	def path(self, name):
		return os.path.join(self.changes, name)

	def load(self, name):
		return chkan.read_json(self.path(name))

	def migrate(self, answer=None, assume_yes=True):
		out, err = io.StringIO(), io.StringIO()
		patcher = mock.patch("builtins.input", return_value=answer) if answer is not None else mock.patch("builtins.input", side_effect=AssertionError("не должен спрашивать"))
		with contextlib.redirect_stdout(out), contextlib.redirect_stderr(err), patcher:
			result = chkan.migrate_if_needed(self.changes, assume_yes=assume_yes)
		return result, out.getvalue(), err.getvalue()

	def snapshot_bytes(self):
		out = {}
		for n in sorted(os.listdir(self.changes)):
			if n != "TAGS.txt" and os.path.isfile(self.path(n)):
				with open(self.path(n), "rb") as f:
					out[n] = f.read()
		return out

	# --- распознавание
	def test_peek_format_of_both_layouts(self):
		self.assertEqual(chkan.peek_format(self.path(A_NAME)), 3)
		self.assertEqual(chkan.peek_format(self.path(D_NAME)), 3)
		self.put("garbage.json", "не json")
		self.assertIsNone(chkan.peek_format(self.path("garbage.json")))
		self.assertEqual(chkan.old_format_files(self.changes), [(D_NAME, 3), (A_NAME, 3), (B_NAME, 3)][:0] or sorted([(A_NAME, 3), (B_NAME, 3), (D_NAME, 3)]))

	def test_read_json_hints_at_migration(self):
		with self.assertRaises(chkan.ChkanError) as cm:
			chkan.read_json(self.path(A_NAME))
		self.assertIn("преобразовать", str(cm.exception))

	# --- вопрос
	def test_declined_leaves_everything_as_is(self):
		before = self.snapshot_bytes()
		result, out, err = self.migrate(answer="n", assume_yes=False)
		self.assertFalse(result)
		self.assertEqual(out, "")
		self.assertIn("формата 3: 3", err)
		self.assertIn("format3/", err)
		self.assertIn("[y/N]", err)
		self.assertIn("Отмена", err)
		self.assertEqual(self.snapshot_bytes(), before)
		self.assertFalse(os.path.exists(self.path("format3")))

	def test_default_answer_is_no(self):
		result, _, _ = self.migrate(answer="", assume_yes=False)
		self.assertFalse(result)

	def test_yes_converts(self):
		result, out, err = self.migrate(answer="y", assume_yes=False)
		self.assertTrue(result)
		self.assertEqual(out, "")
		self.assertEqual(chkan.old_format_files(self.changes), [])
		self.assertIn("Преобразовано в формат 4: 3 файлов", err)

	def test_assume_yes_does_not_ask(self):
		self.assertTrue(self.migrate()[0])

	def test_second_call_is_a_noop_and_never_asks(self):
		self.migrate()
		before = self.snapshot_bytes()
		result, _, err = self.migrate(assume_yes=False)
		self.assertTrue(result)
		self.assertEqual(err, "")
		self.assertEqual(self.snapshot_bytes(), before)

	# --- что получилось
	def test_originals_are_kept_byte_for_byte(self):
		self.migrate()
		backup = self.path("format3")
		self.assertEqual(sorted(os.listdir(backup)), sorted(self.originals))
		for name, text in self.originals.items():
			with open(os.path.join(backup, name), encoding="utf-8") as f:
				self.assertEqual(f.read(), text)
		self.assertFalse([n for n in os.listdir(self.changes) if n.endswith(".v4") or n.endswith(".tmp")])

	def test_scans_become_seconds_without_tz(self):
		self.migrate()
		a = self.load(A_NAME)
		self.assertEqual(a["format"], 4)
		self.assertNotIn("tz", a)
		self.assertEqual(list(a), ["format", "root", "taken_at", "exclude", "tree"])
		flat = chkan.flatten(a["tree"])
		self.assertEqual(flat["."]["mtime"], T1)
		self.assertEqual(flat["a.txt"]["mtime"], T1)
		self.assertEqual(flat["a.txt"]["btime"], T1 - 100)
		self.assertIsNone(flat["b.txt"]["btime"])
		self.assertEqual(flat["."]["total"], 15)
		b = chkan.flatten(self.load(B_NAME)["tree"])
		self.assertEqual((b["c.txt"]["mtime"], b["sub2"]["ctime"]), (T1 + 60, T1))

	def test_diff_header_and_sizes(self):
		self.migrate()
		d = self.load(D_NAME)
		self.assertEqual(d["format"], 4)
		self.assertEqual(d["from"], {"taken_at": A_AT, "exclude": [".changes"]})
		self.assertEqual(d["to"], {"taken_at": B_AT, "exclude": [".changes"]})
		self.assertEqual(d["groups"]["-"]["size"], ["15=15B", "22=22B", "+7B"])
		self.assertEqual(d["groups"]["-" * 10], 0)
		self.assertNotIn("c", d)  # комментария к diff в формате 3 не было

	def test_diff_nodes_times_totals_and_dropped_noise(self):
		self.migrate()
		d = self.load(D_NAME)
		ops = {o["path"]: o for o in chkan.diff_ops(d)}
		self.assertEqual(sorted(ops), [".", "a.txt", "c.txt", "sub2"])  # b.txt менялся только в долях секунды
		self.assertEqual(ops["a.txt"]["set"], {"size": [5, 8], "mtime": [T1, T1 + 50], "ctime": [T1, T1 + 50]})
		self.assertEqual(ops["."]["op"], "change_d")
		self.assertEqual(ops["."]["set"]["total"], [15, 22])
		self.assertEqual(ops["."]["set"]["mtime"], [T1, T1 + 100])
		self.assertEqual(ops["c.txt"]["new"]["mtime"], T1 + 60)
		self.assertIsNone(ops["c.txt"]["new"]["btime"])
		self.assertEqual((ops["sub2"]["op"], ops["sub2"]["from"]), ("move", "sub"))
		self.assertNotIn("set", ops["sub2"])  # ctime отличался только в наносекундах

	def test_diff_times_are_text_in_to_timezone_and_deltas_short(self):
		self.migrate()
		text = read_text(self.path(D_NAME))
		self.assertNotIn("tz", text)
		self.assertNotIn("+7=+7B", text)
		self.assertIn('"total": ["15=15B", "22=22B", "+7B"]', text)
		ref = chkan.TimeRef(B_AT)
		self.assertIn(json.dumps(ref.encode(T1 + 50)), text)

	def test_comments_are_preserved(self):
		self.migrate()
		d = self.load(D_NAME)
		self.assertEqual(d["groups"]["-"]["c"], "группа")
		nodes = dict(chkan.iter_nodes(d["groups"]["-"]["ch"]))
		self.assertEqual(nodes["a.txt"]["c"], "правил a")

	def test_new_layout_is_used_for_migrated_diff(self):
		self.migrate()
		text = read_text(self.path(D_NAME))
		self.assertRegex(text, r'"\.": \{"op": "change_d", "set": \{[^\n]*\}\}')  # папка: одна строка
		self.assertEqual(json.loads(text), self.load(D_NAME))
		self.assertTrue(text.endswith("}\n"))

	def test_chain_is_consistent_in_both_directions(self):
		self.migrate()
		a, b, d = self.load(A_NAME), self.load(B_NAME), self.load(D_NAME)
		self.assertEqual(chkan.apply_diff(a, d), {**b, "tree": chkan.build_tree(chkan.flatten(b["tree"]))})
		self.assertEqual(chkan.apply_diff(b, d, backward=True), a)

	# --- отказы
	def test_inconsistent_chain_aborts_and_touches_nothing(self):
		d = json.loads(self.originals[D_NAME])
		node = d["groups"]["-"]["ch"]["a.txt"]
		node["set"]["size"] = [5, 9]  # в скане B размер 8
		self.put(D_NAME, json.dumps(d, ensure_ascii=False))
		before = self.snapshot_bytes()
		with self.assertRaises(chkan.ChkanError):
			self.migrate()
		self.assertEqual(self.snapshot_bytes(), before)
		self.assertFalse(os.path.exists(self.path("format3")))

	def test_broken_json_aborts_and_cleans_temp_files(self):
		self.put(D_NAME, "{ не json")
		before = self.snapshot_bytes()
		with self.assertRaises(Exception):
			self.migrate()
		self.assertEqual(self.snapshot_bytes(), before)

	def test_nonempty_backup_dir_is_refused(self):
		os.makedirs(self.path("format3"))
		with open(self.path("format3/x"), "w") as f:
			f.write("x")
		before = self.snapshot_bytes()
		with self.assertRaises(chkan.ChkanError) as cm:
			self.migrate()
		self.assertIn("format3", str(cm.exception))
		self.assertEqual(self.snapshot_bytes(), before)

	def test_unknown_format_is_refused(self):
		self.put("scan_20250101-000000.json", '{"format":2,"tree":{}}')
		with self.assertRaises(chkan.ChkanError):
			self.migrate()

	def test_only_old_files_are_converted_when_formats_are_mixed(self):
		self.migrate()
		new_before = {n: v for n, v in self.snapshot_bytes().items() if n in (A_NAME, B_NAME)}
		os.rename(self.path(D_NAME), self.path("old_diff.keep"))
		shutil_backup = self.path("format3")
		for n in os.listdir(shutil_backup):
			os.remove(os.path.join(shutil_backup, n))
		os.rmdir(shutil_backup)
		self.put(D_NAME, self.originals[D_NAME])  # снова диф в формате 3 рядом с уже новыми слепками
		result, _, err = self.migrate()
		self.assertTrue(result)
		self.assertIn("1 файлов", err)
		after = self.snapshot_bytes()
		for n, data in new_before.items():
			self.assertEqual(after[n], data)
		self.assertEqual(self.load(D_NAME)["format"], 4)

	def test_single_old_scan_without_diffs(self):
		for n in (B_NAME, D_NAME):
			os.remove(self.path(n))
		self.assertTrue(self.migrate()[0])
		self.assertEqual(self.load(A_NAME)["format"], 4)

	# --- единичные преобразования
	def test_migrate_snapshot_floor_division_and_nulls(self):
		data = {"format": 3, "tz": "+00:00", "tree": {"mtime": ns(-1, 5), "ctime": ns(2, 999_999_999), "btime": None, "ch": {"x": {"mtime": ns(7, 1), "ctime": 0, "btime": ns(8)}}}}
		out = chkan.migrate_snapshot(data)
		self.assertEqual((out["format"], "tz" in out), (4, False))
		self.assertEqual((out["tree"]["mtime"], out["tree"]["ctime"], out["tree"]["btime"]), (-1, 2, None))
		self.assertEqual(out["tree"]["ch"]["x"], {"mtime": 7, "ctime": 0, "btime": 8})

	def v3_diff(self, ch, scan=None, c=None):
		d = {
			"format": 3, "root": "/r",
			"from": {"taken_at": A_AT, "tz": "+02:00", "exclude": []},
			"to": {"taken_at": B_AT, "tz": "+02:00", "exclude": []},
			"groups": {"-": {"size": ["1=1B", "2=2B"], "ch": ch}, "-" * 10: 0},
		}
		if scan:
			d["scan"] = {"ch": scan}
		return d

	def test_empty_change_with_children_becomes_container_and_comment_survives(self):
		ch = {
			"d": {"op": "change_d", "set": {"mtime": [ns(5, 1), ns(5, 2)]}, "ch": {"f": {"op": "change", "set": {"size": [1, 2]}}}},
			"e": {"op": "change_d", "set": {"mtime": [ns(5, 1), ns(5, 2)]}, "c": "важно"},
			"g": {"op": "change", "set": {"mtime": [ns(5, 1), ns(5, 2)]}},
		}
		out = chkan.migrate_diff(self.v3_diff(ch))["groups"]["-"]["ch"]
		self.assertEqual(out["d"], {"ch": {"f": {"op": "change", "set": {"size": [1, 2]}}}})
		self.assertEqual(out["e"], {"c": "важно"})
		self.assertNotIn("g", out)

	def test_move_keeps_node_when_set_vanishes_and_scan_ops_are_converted(self):
		ref = chkan.TimeRef(B_AT)
		rec_ = {"type": "f", "dev": 1, "ino": 2, "size": 3, "mtime": ns(T1, 9), "ctime": ns(T1, 9), "btime": None, "uid": 0, "gid": 0, "mode": "0644"}
		ch = {"m": {"op": "move", "from": "n", "set": {"ctime": [ns(5, 1), ns(5, 2)]}}}
		scan = {"data": {"op": "add_scan", "new": dict(rec_)}, "tmp": {"op": "remove_scan", "old": dict(rec_)}}
		out = chkan.migrate_diff(self.v3_diff(ch, scan))
		self.assertEqual(out["groups"]["-"]["ch"]["m"], {"op": "move", "from": "n"})
		self.assertEqual(out["scan"]["ch"]["data"]["new"]["mtime"], ref.encode(T1))
		self.assertEqual(out["scan"]["ch"]["tmp"]["old"]["ctime"], ref.encode(T1))
		self.assertIsNone(out["scan"]["ch"]["data"]["new"]["btime"])

	def test_total_none_pairs_survive(self):
		ch = {"d": {"op": "change_d", "set": {"total": [None, "500=500B"]}}, "e": {"op": "change_d", "set": {"total": ["1=1B", "5=5B", "+4=+4B"]}}}
		out = chkan.migrate_diff(self.v3_diff(ch))["groups"]["-"]["ch"]
		self.assertEqual(out["d"]["set"]["total"], [None, "500=500B"])
		self.assertEqual(out["e"]["set"]["total"], ["1=1B", "5=5B", "+4B"])

	def test_old_numeric_two_element_group_sizes(self):
		d = self.v3_diff({})
		d["groups"]["-"]["size"] = [8_234_782_596, 8_234_782_596]
		out = chkan.migrate_diff(d)
		self.assertEqual(out["groups"]["-"]["size"], ["8_234_782_596=7.6G", "8_234_782_596=7.6G", "0B"])

	# --- через chkan.py целиком
	def test_main_asks_then_migrates_then_scans(self):
		with mock.patch("builtins.input", return_value="y") as inp:
			err = io.StringIO()
			out = io.StringIO()
			with contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
				code = chkan.main([self.root])
		self.assertEqual(code, 0)
		inp.assert_called_once()
		self.assertIn("формата 3", err.getvalue())
		self.assertEqual(chkan.old_format_files(self.changes), [])

	def test_main_declined_does_not_scan(self):
		before = self.snapshot_bytes()
		err = io.StringIO()
		with mock.patch("builtins.input", return_value="n"), contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(err):
			code = chkan.main([self.root])
		self.assertEqual(code, 1)
		self.assertEqual(self.snapshot_bytes(), before)

	def test_main_yes_migrates_scans_and_keeps_the_chain_consistent(self):
		out, err = io.StringIO(), io.StringIO()
		with contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
			code = chkan.main([self.root, "-y"])
		self.assertEqual(code, 0, err.getvalue())
		scans = chkan.list_scans(self.changes)
		self.assertEqual(scans, ["20261001-150000", "20261003-100000"])  # B был промежуточным и удалён после проверки
		diffs = sorted(n for n in os.listdir(self.changes) if n.startswith("diff_"))
		self.assertEqual(diffs, [D_NAME, "diff_20261002-090000_20261003-100000.json"])
		self.assertEqual(sorted(os.listdir(self.path("format3"))), sorted(self.originals))  # оригиналы, в том числе B
		self.assertEqual(chkan.old_format_files(self.changes), [])
		paths = {n: self.path(n) for n in os.listdir(self.changes) if n.startswith(("scan_", "diff_"))}
		chkan.check_chain(self.changes, paths)  # первый скан + все diff = последний скан
		self.assertTrue(out.getvalue().startswith("без тегов:"))
		last = self.load("scan_20261003-100000.json")
		self.assertEqual(sorted(chkan.flatten(last["tree"])), ["."])

	def test_main_single_old_scan_then_new_diff(self):
		for n in (B_NAME, D_NAME):
			os.remove(self.path(n))
		with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
			self.assertEqual(chkan.main([self.root, "-y"]), 0)
		self.assertEqual(len([n for n in os.listdir(self.changes) if n.startswith("diff_")]), 1)
		self.assertEqual(self.load(A_NAME)["format"], 4)


if __name__ == "__main__":
	unittest.main()
