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
	return [chkan.parse_size(x) for x in group["size"]]


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
		rules = chkan.parse_tags("a/skip/\t# @exclude\n")
		flat, warnings = chkan.scan_fs(self.root, rules)
		self.assertEqual(sorted(flat), [".", "a", "a/b", "a/b/c.txt", "top.txt"])
		self.assertEqual(flat["."]["type"], "d")
		self.assertEqual(warnings, [])

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
		self.assertEqual(g["logs"]["size"], ["2_000=1.9K", "2_600=2.5K"])
		self.assertEqual(g["photos+tag"]["size"], ["0=0B", "50=50B"])

	def test_cancelled_tag_files_count_as_untagged(self):
		# dir/B/b.txt (40 байт) не менялся, но @no(tag) делает его «без тегов» и он входит в сумму
		self.assertEqual(sizes(self.diff()["groups"]["-"]), [50, 90])

	def test_move_goes_to_destination_group_with_path_compression(self):
		g = self.diff()["groups"]["tag"]["ch"]
		self.assertEqual(list(g), ["dir"])
		inner = g["dir"]["ch"]
		self.assertEqual(inner["F.txt"], {"op": "change", "set": {"size": [100, 130], "mtime": [10, 11]}})
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
		self.assertEqual(self.diff()["groups"]["logs"]["ch"], {"var/app.log": {"op": "change", "set": {"size": [2000, 2600], "mtime": [10, 12]}}})

	def test_size_only_group_when_file_leaves_it(self):
		old = {".": drec(ino=2), "dir": drec(ino=3), "dir/f": rec(ino=4, size=100)}
		new = {".": drec(ino=2), "dir": drec(ino=3), "g": rec(ino=4, size=100)}
		d = diff_between(old, new, "dir/\t# tag\n")[2]
		self.assertEqual(d["groups"]["tag"], {"size": ["100=100B", "0=0B"]})
		self.assertEqual(sizes(d["groups"]["-"]), [0, 100])
		self.assertEqual(d["groups"]["-"]["ch"]["g"]["op"], "move")

	def test_no_group_without_changes(self):
		self.assertEqual(list(diff_between(base_flat(), base_flat())[2]["groups"]), ["-" * 10])

	def test_remove_grouped_by_old_path(self):
		old = {".": drec(ino=2), "dir": drec(ino=3), "dir/f": rec(ino=4, size=5)}
		new = {".": drec(ino=2), "dir": drec(ino=3)}
		d = diff_between(old, new, "dir/\t# tag\n")[2]
		self.assertEqual(d["groups"]["tag"]["ch"], {"dir/f": {"op": "remove", "old": old["dir/f"]}})

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
				"без тегов: +0 -0 ~1 ->0 d~0",
				"photos+tag: +1 -0 ~0 ->0 d~0",
				"tag: +0 -0 ~1 ->1 d~0",
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
		text = chkan.dumps_pretty(d)
		self.assertEqual(json.loads(text), d)
		for line in text.splitlines():
			indent = line[: len(line) - len(line.lstrip())]
			self.assertNotIn(" ", indent)

	def test_leaf_nodes_inline_containers_expanded(self):
		d = diff_between(DiffGrouping().flat_old(), DiffGrouping().flat_new(), TAGS_FOR_DIFF)[2]
		text = chkan.dumps_pretty(d)
		self.assertIn('"F.txt": {"op": "change", "set": {"size": [100, 130], "mtime": [10, 11]}}', text)
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


class OldFilesStillRead(unittest.TestCase):
	def test_old_files_with_tz_and_numeric_sizes_still_apply(self):
		a, b, d, _ = diff_between(DiffGrouping().flat_old(), DiffGrouping().flat_new(), TAGS_FOR_DIFF)
		d = roundtrip(d)
		a_old = json.loads(json.dumps(a))
		b_old = json.loads(json.dumps(b))
		for obj in (a_old, b_old):
			obj["tz"] = "+02:00"
		for key in ("from", "to"):
			d[key]["tz"] = "+02:00"
		for g in d["groups"].values():
			if isinstance(g, dict):
				g["size"] = [chkan.parse_size(x) for x in g["size"]]
		back = chkan.apply_diff(b_old, d, backward=True)
		self.assertTrue(chkan.snapshots_equal(back, a_old))
		self.assertTrue(chkan.snapshots_equal(chkan.apply_diff(a_old, d), b_old))


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
	def test_delta(self):
		self.assertEqual(chkan.format_delta(25_573_854), "+25_573_854=+24M")
		self.assertEqual(chkan.format_delta(-1024), "-1_024=-1.0K")
		self.assertEqual(chkan.format_delta(0), "0=0B")
		self.assertEqual(chkan.parse_size("+25_573_854=+24M"), 25_573_854)
		self.assertEqual(chkan.parse_size("-1_024=-1.0K"), -1024)

	def test_pair_to_triple_and_back(self):
		triple = chkan.format_total_pair([1000, 1030])
		self.assertEqual(triple, ["1_000=1000B", "1_030=1.0K", "+30=+30B"])
		self.assertEqual(chkan.parse_total_pair(triple), [1000, 1030])
		shrink = chkan.format_total_pair([2048, 1024])
		self.assertEqual(shrink, ["2_048=2.0K", "1_024=1.0K", "-1_024=-1.0K"])
		self.assertEqual(chkan.parse_total_pair(shrink), [2048, 1024])

	def test_absent_side_has_no_delta(self):
		self.assertEqual(chkan.format_total_pair([None, 1030]), [None, "1_030=1.0K"])
		self.assertEqual(chkan.format_total_pair([5, None]), ["5=5B", None])
		self.assertEqual(chkan.parse_total_pair([None, "1_030=1.0K"]), [None, 1030])

	def test_bad_pairs(self):
		for bad in (["1=1B"], ["1=1B", "2=2B", "+5=+5B", "x"], ["1=1B", "2=2B", "+9=+9B"], ["abc", "2=2B"], [[1], 2]):
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
		self.assertEqual(node["set"]["total"], ["1_000=1000B", "1_030=1.0K", "+30=+30B"])
		self.assertEqual(node["set"]["mtime"], [10, 50])
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
		d["groups"]["-"]["ch"]["d"]["set"]["total"] = ["1_001=1001B", "1_030=1.0K", "+29=+29B"]
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
				"a: +0 -0 ~0 ->0 d~0",
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
		failures, out, _ = self.go([("sub dir", "true"), (".", "true")])
		self.assertEqual(failures, 0)
		self.assertEqual(out.splitlines(), ["sub dir # @run(true)", ". # @run(true)"])

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
		self.assertEqual(len(out.splitlines()), 2)

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


# ------------------------------------------------------------ chkan.py целиком


class EndToEnd(unittest.TestCase):
	def setUp(self):
		self.tmp = tempfile.TemporaryDirectory()
		self.addCleanup(self.tmp.cleanup)
		self.root = os.path.realpath(self.tmp.name)
		self.changes = os.path.join(self.root, ".changes")
		self.clock = datetime.datetime(2026, 10, 1, 15, 0, 0, tzinfo=TZ)
		p = mock.patch.object(chkan, "_now", lambda: self.clock)
		p.start()
		self.addCleanup(p.stop)

	def write(self, rel, data):
		p = os.path.join(self.root, rel)
		os.makedirs(os.path.dirname(p), exist_ok=True)
		with open(p, "wb") as f:
			f.write(data)
		return p

	def tags(self, text):
		os.makedirs(self.changes, exist_ok=True)
		with open(os.path.join(self.changes, "TAGS.txt"), "w", encoding="utf-8") as f:
			f.write(text)

	def run_chkan(self, *extra):
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
		self.assertIn("(1)", inp.call_args.args[0])
		self.assertIn("[y/N]", inp.call_args.args[0])
		self.assertEqual(self.scans(), [])
		self.assertIn("Предупреждение: не прочитано bad1: Permission denied", err)
		self.assertIn("Отмена", out)

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
		self.assertEqual(len(lines), 11)
		self.assertTrue(all(l.startswith("Предупреждение: не прочитано bad") for l in lines[:10]))
		self.assertEqual(lines[10], "... и ещё 2")

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
		lines = out.splitlines()
		self.assertEqual(lines[-2:], ["папка # @run(echo \"$PWD\" > ../cwd1.txt; echo $CHKAN_DIFF > ../diff1.txt)", ". # @run(echo $CHKAN_TAGS $CHKAN_SCAN > env2.txt)"])
		self.assertTrue(lines[0].startswith("без тегов:"))  # счётчики раньше команд
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
		code, out, _ = self.step()
		self.assertEqual(code, 0)
		self.assertIn("первый скан", out.splitlines()[0])
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


	def test_asks_and_cancel(self):
		with mock.patch("builtins.input", return_value="n"):
			code, out, _ = self.run_chkan()
		self.assertEqual(code, 1)
		self.assertFalse(os.path.exists(self.changes))
		self.assertIn("Отмена", out)

	def test_asks_and_eof_is_cancel(self):
		with mock.patch("builtins.input", side_effect=EOFError):
			code, _, _ = self.run_chkan()
		self.assertEqual(code, 1)
		self.assertFalse(os.path.exists(self.changes))

	def test_asks_and_creates(self):
		self.write("a.txt", b"1")
		with mock.patch("builtins.input", return_value="y"):
			code, out, _ = self.run_chkan()
		self.assertEqual(code, 0)
		self.assertEqual(self.files(), ["TAGS.txt", "scan_20261001-150000.json"])
		self.assertIn("первый скан", out)
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
				"без тегов: +0 -0 ~1 ->0 d~1",  # loose.txt; корень (поменялся total)
				"tracked: +1 -1 ~0 ->1 d~1",  # new.txt, old.txt, a.txt->renamed.txt; папка data
				"с не отслеживаемыми тэгами: +0 -0 ~1 ->0 d~1",  # noise/n.txt и папка noise (total)
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
		for _label, c in chkan.count_ops(d):
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

	def test_changes_dir_is_not_in_snapshot(self):
		self.write("a", b"1")
		self.step()
		scan = chkan.read_json(os.path.join(self.changes, self.scans()[0]))
		self.assertEqual(sorted(chkan.flatten(scan["tree"])), [".", "a"])

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
		self.assertIn('"total": ["5=5B", "12=12B", "+7=+7B"]', text)

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


if __name__ == "__main__":
	unittest.main()
