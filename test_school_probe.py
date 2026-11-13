#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""school_probe_all.py 的 mock 自测（不依赖网络/代理）。"""
import json
import os
import shutil
import sys
import tempfile

sys.path.insert(0, os.path.dirname(__file__))
import school_probe_all as m

PASS = FAIL = 0


def check(name, cond, extra=''):
    global PASS, FAIL
    if cond:
        PASS += 1
        print('  PASS', name, extra)
    else:
        FAIL += 1
        print('  FAIL', name, extra)


tmp = tempfile.mkdtemp()
try:
    # T1 分片取模：不重不漏
    sids = list(range(30, 40))
    sh = [m.shard_of(sids, i, 3) for i in range(3)]
    check('T1 分片取模不重不漏',
          sorted(sum(sh, [])) == sids and all(len(x) > 0 for x in sh),
          'shards=%s' % [len(x) for x in sh])

    nf_map = {30: 0, 31: 63, 32: 41, 33: 2, 34: 15}

    # T2 正常探测统计
    d = tempfile.mkdtemp(dir=tmp)

    def pf(sid):
        return nf_map[sid], None

    r = m.probe_shard(list(nf_map), pf, out_dir=d, idx=0, total=1)
    check('T2 统计正确',
          r['schools'] == 5 and r['ok'] == 5 and r['failed'] == 0
          and r['sum_nf'] == 121 and r['nonempty'] == 4,
          'sum=%d nonempty=%d' % (r['sum_nf'], r['nonempty']))

    # T3 jsonl 断点格式
    ck = os.path.join(d, 'probe_shard_0.jsonl')
    lines = [json.loads(x) for x in open(ck, encoding='utf-8')]
    check('T3 断点每行合法且 sid 唯一',
          len(lines) == 5 and len({x['sid'] for x in lines}) == 5)

    # T4 断点复用零重探
    d2 = tempfile.mkdtemp(dir=tmp)
    shutil.copy(ck, os.path.join(d2, 'probe_shard_0.jsonl'))
    calls = []

    def pf2(sid):
        calls.append(sid)
        return nf_map[sid], None

    r2 = m.probe_shard(list(nf_map), pf2, out_dir=d2, idx=0, total=1)
    check('T4 断点复用零重探',
          calls == [] and r2['schools'] == 5 and r2['sum_nf'] == 121,
          'calls=%d' % len(calls))

    # T5 部分失败不计入 Σ
    d3 = tempfile.mkdtemp(dir=tmp)

    def pf3(sid):
        if sid == 33:
            return None, '代理耗尽'
        return nf_map[sid], None

    r3 = m.probe_shard(list(nf_map), pf3, out_dir=d3, idx=0, total=1)
    check('T5 失败校不计入 Σ',
          r3['failed'] == 1 and r3['ok'] == 4 and r3['sum_nf'] == 119
          and r3['failed_sids'] == [33], 'sum=%d' % r3['sum_nf'])

    # T6 守恒 PASS
    s = m.summarize([r], 121, list(nf_map))
    check('T6 守恒 PASS', s['conserve'] and s['delta'] == 0 and s['sigma'] == 121,
          'sigma=%d delta=%d' % (s['sigma'], s['delta']))

    # T7 Σ 偏小判不守恒（漏号）
    s2 = m.summarize([r], 130, list(nf_map))
    check('T7 Σ 偏小判不守恒', not s2['conserve'] and s2['delta'] == -9)

    # T8 有失败校判不守恒
    s3 = m.summarize([r3], 121, list(nf_map))
    check('T8 有失败校判不守恒', not s3['conserve'] and s3['failed'] == 1)

    # T9 全集漏号被列出
    s4 = m.summarize([r], 121, list(nf_map) + [999])
    check('T9 漏号被列出', not s4['conserve'] and s4['missing_sids'] == [999])

    # T10 非空院校清单正确
    s5 = m.summarize([r], 121, list(nf_map))
    check('T10 非空院校清单正确',
          s5['school_ids_nonempty'] == [31, 32, 33, 34]
          and s5['empty_schools'] == 1,
          'nonempty=%s' % s5['school_ids_nonempty'])
finally:
    shutil.rmtree(tmp, ignore_errors=True)

print('\n=== %d PASS / %d FAIL ===' % (PASS, FAIL))
sys.exit(1 if FAIL else 0)
