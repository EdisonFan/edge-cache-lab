#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""collect_by_school.collect_school 的本地 mock 端到端自测（不联网）。

覆盖：正常多页、空片、页残缺补采、school_id 串片、重复 id、
numFound 封顶、超 200 页、探测失败。
"""
import json
import os
import sys
import tempfile

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from collect_by_school import collect_school, SIZE

PASS = []


def mkfetch(nf, sid=678, size=SIZE, bad_pages=(), impure_pages=(), dup=False):
    """构造确定性假接口：第 p 页返回该页应有的记录。"""
    pages = (nf + size - 1) // size
    gid = [0]

    def make(p):
        n = size if p < pages else nf - (pages - 1) * size
        rows = []
        for i in range(n):
            gid[0] += 1
            rid = (gid[0] // 2) if dup else gid[0]
            s = 999 if p in impure_pages else sid
            rows.append({'id': rid, 'school_id': s, 'name': 'x'})
        return rows

    def fetch(page):
        if page in bad_pages:
            return nf, []            # 页残缺
        return nf, make(page)

    return fetch


def check(name, cond, extra=''):
    PASS.append(cond)
    print('%-28s %s %s' % (name, 'PASS' if cond else 'FAIL', extra), flush=True)


tmp = tempfile.mkdtemp()

# T1: 正常 4 页（63 条，size=20 -> 20/20/20/3）
o = collect_school(678, mkfetch(63), out_dir=tmp)
check('T1 正常多页达标', o['gate_pass'] and o['count'] == 63
      and o['pages'] == 4 and o['pages_ok'] == 4
      and o['unique_ids'] == 63, 'count=%d pages=%d' % (o['count'], o['pages']))
check('T1 落盘文件可解析',
      json.load(open(os.path.join(tmp, 'school_37_2024_678.json')))['count'] == 63)

# T2: 合法空片
o = collect_school(1165, mkfetch(0, sid=1165), out_dir=tmp)
check('T2 空片合法达标', o['gate_pass'] and o['count'] == 0
      and o['pages'] == 0 and o['gate']['school_pure'])

# T3: 页残缺由补采路径覆盖（第一次第 2 页残缺，之后正常）
def _real(nf, page):
    pages = (nf + SIZE - 1) // SIZE
    n = SIZE if page < pages else nf - (pages - 1) * SIZE
    start = (page - 1) * SIZE
    return nf, [{'id': start + i, 'school_id': 678} for i in range(n)]

def fetch_flaky2(page):
    if page == 2 and not getattr(fetch_flaky2, 'hit', False):
        fetch_flaky2.hit = True
        return 63, []
    return _real(63, page)

o = collect_school(678, fetch_flaky2, out_dir=tmp)
check('T3 残缺页补采后达标', o['gate_pass'] and o['count'] == 63
      and o['pages_failed'] == 0, 'failed=%d' % o['pages_failed'])

# T4: school_id 串片 -> school_pure=False
o = collect_school(678, mkfetch(63, impure_pages={1}), out_dir=tmp)
check('T4 串片被拦截', not o['gate_pass'] and not o['gate']['school_pure'])

# T5: 重复 id -> unique_ids=False
o = collect_school(678, mkfetch(63, dup=True), out_dir=tmp)
check('T5 重复 id 被拦截', not o['gate_pass']
      and not o['gate']['unique_ids'] and o['dup_ids'] > 0,
      'dup=%d' % o['dup_ids'])

# T6: numFound 封顶 -> 判失败
o = collect_school(678, mkfetch(10000), out_dir=tmp)
check('T6 封顶片判失败', not o['gate_pass'] and '封顶' in o.get('error', ''))

# T7: 超过 200 页 -> 判失败
o = collect_school(678, mkfetch(4001), out_dir=tmp)
check('T7 超页上限判失败', not o['gate_pass'] and o.get('pages') == 201)

# T8: 探测失败（代理耗尽）
o = collect_school(678, lambda p: (None, None), out_dir=tmp)
check('T8 探测失败判失败', not o['gate_pass'] and 'error' in o)

# T9: payload 里 school_id 是 int（防止写成 str 导致筛选失效）
import inspect
src = inspect.getsource(__import__('collect_by_school').make_real_fetch)
check('T9 payload school_id 原样传 sid', "'school_id': sid" in src)

print('\n=== %d/%d PASS ===' % (sum(PASS), len(PASS)))
sys.exit(0 if all(PASS) else 1)
