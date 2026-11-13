#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""school_id 全集探测（分片执行，供 Σ 守恒验收与空片跳过）。

背景（2026-11-13）：POC 已用 4 校证明 school_id 筛选生效、单校片能采全、
数据不串片。但要用它收口整个 37-2024，还欠一个决定性证明：

    Σ(院校库 2970 所全集各片 numFound) == 34省Σ真值(37-2024 = 32723)

相等才说明全集没漏号（确定性完备划分），不相等必须补候选再切。
同时筛出 numFound=0 的空校，全量阶段只采有数据的学校，不起 2970 个空 job。

分片：SHARD_TOTAL 个 job 各探 i % TOTAL == INDEX 的学校；每片独立刷池，
max-parallel: 2 低并发（实测 4 并发会把探测请求打穿，见 bench 并发批失败）。

请求只取 numFound，故 size=1：响应体最小、避开 1090(>30000 字符)。
numFound 是接口给的总数，与 size 无关（POC 已验证 678 -> 63 条 / 4 页）。

结果只落工作区 out/ 供 artifact 取走，不推任何 special-* 分支。
"""
import json
import os
import random
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

U = 'https://api.zjzw.cn/web/api/'
API = 'apidata/api/gk/score/special'
H = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
                  'AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36',
    'Origin': 'https://gkcx.eol.cn',
    'Referer': 'https://gkcx.eol.cn/',
    'Accept': 'application/json,text/plain,*/*',
    'Content-Type': 'application/json',
}
PROV = os.environ.get('PROV', '37')
YEAR = int(os.environ.get('YEAR', '2024'))
IDX = int(os.environ.get('SHARD_INDEX', '0'))
TOTAL = int(os.environ.get('SHARD_TOTAL', '10'))
WORKERS = int(os.environ.get('PROBE_WORKERS', '6'))
ROUNDS = int(os.environ.get('PROBE_ROUNDS', '8'))
OUT = 'out'


def load_sids(prov=PROV, year=YEAR):
    plan = json.load(open('school_plan_%s_%s.json' % (prov, year), encoding='utf-8'))
    return sorted(int(x) for x in plan['school_ids'])


def shard_of(sids, idx=IDX, total=TOTAL):
    """按位置取模分片，保证不重不漏且各片均衡。"""
    return [s for i, s in enumerate(sids) if i % total == idx]


def load_proxies(path='proxies_cnb.txt'):
    return [x.strip() for x in open(path, encoding='utf-8')
            if x.strip() and not x.strip().startswith('#')]


def make_probe_fn(proxies, prov=PROV, year=YEAR, rounds=ROUNDS):
    """构造 probe(sid) -> (numFound|None, err)。代理轮换，绝不直连。"""
    import requests
    pool = list(proxies)
    random.shuffle(pool)

    def _once(px, sid):
        q = {'uri': API, 'local_province_id': prov, 'year': year,
             'page': 1, 'size': 1, 'school_id': sid}
        r = requests.post(U, json=q, headers=H,
                          proxies={'http': px, 'https': px}, timeout=(6, 14))
        return r.json()

    def probe(sid):
        last = ''
        for _ in range(rounds):
            for px in pool:
                try:
                    d = _once(px, sid)
                except Exception as e:
                    last = type(e).__name__
                    continue
                if str(d.get('code')) != '0000':
                    last = 'code=' + str(d.get('code'))
                    continue
                dat = d.get('data')
                if not isinstance(dat, dict):
                    last = 'data 非 dict'
                    continue
                nf = dat.get('numFound')
                if nf is None:
                    last = '缺 numFound'
                    continue
                return int(nf), None
        return None, last or '代理耗尽'

    return probe


def probe_shard(sids, probe_fn, out_dir=OUT, idx=IDX, total=TOTAL):
    """纯逻辑：并发探测 + jsonl 断点，返回该片结果 dict。可注入 probe_fn 测试。"""
    os.makedirs(out_dir, exist_ok=True)
    ckpt = os.path.join(out_dir, 'probe_shard_%s.jsonl' % idx)
    done = {}
    if os.path.exists(ckpt):
        for line in open(ckpt, encoding='utf-8'):
            try:
                r = json.loads(line)
                if 'sid' in r:
                    done[int(r['sid'])] = r
            except Exception:
                continue
    todo = [s for s in sids if s not in done]
    if done:
        print('[断点] 复用已探测 %d 校，本片待探 %d 校' % (len(done), len(todo)), flush=True)

    lock = threading.Lock()
    fh = open(ckpt, 'a', encoding='utf-8')

    def work(sid):
        nf, err = probe_fn(sid)
        row = ({'sid': sid, 'nf': None, 'ok': False, 'err': err}
               if nf is None else {'sid': sid, 'nf': nf, 'ok': True})
        with lock:
            done[sid] = row
            fh.write(json.dumps(row, ensure_ascii=False) + '\n')
            fh.flush()
        return row

    t0 = time.time()
    with ThreadPoolExecutor(max_workers=WORKERS) as ex:
        list(as_completed([ex.submit(work, s) for s in todo]))
    fh.close()

    rows = [done[s] for s in sids if s in done]
    ok = [r for r in rows if r.get('ok')]
    failed = [r for r in rows if not r.get('ok')]
    result = {
        'shard': idx, 'shard_total': total, 'prov': PROV, 'year': YEAR,
        'schools': len(rows), 'ok': len(ok), 'failed': len(failed),
        'sum_nf': sum(r['nf'] for r in ok),
        'nonempty': sum(1 for r in ok if r['nf'] > 0),
        'failed_sids': [r['sid'] for r in failed],
        'elapsed': round(time.time() - t0, 1),
        'rows': rows,
    }
    path = os.path.join(out_dir, 'probe_shard_%s.json' % idx)
    with open(path, 'w', encoding='utf-8') as f:
        json.dump(result, f, ensure_ascii=False)
    print('SHARD_JSON ' + json.dumps({k: result[k] for k in
          ('shard', 'schools', 'ok', 'failed', 'sum_nf', 'nonempty', 'elapsed')},
          ensure_ascii=False), flush=True)
    return result


def summarize(shards, true_nf, expected_sids):
    """汇总所有分片，做 Σ 守恒判定。纯函数，便于 mock 测试。"""
    rows = {}
    for sh in shards:
        for r in sh['rows']:
            rows[int(r['sid'])] = r
    ok = [r for r in rows.values() if r.get('ok')]
    failed = [r for r in rows.values() if not r.get('ok')]
    sigma = sum(r['nf'] for r in ok)
    nonempty = sorted(s for s, r in rows.items() if r.get('ok') and r['nf'] > 0)
    exp = set(int(x) for x in expected_sids)
    missing = sorted(exp - set(rows))
    extra = sorted(set(rows) - exp)
    return {
        'prov': PROV, 'year': YEAR,
        'expected_schools': len(exp), 'probed_schools': len(rows),
        'ok': len(ok), 'failed': len(failed),
        'failed_sids': [r['sid'] for r in failed],
        'missing_sids': missing, 'extra_sids': extra,
        'sigma': sigma, 'true_nf': int(true_nf), 'delta': sigma - int(true_nf),
        'nonempty_schools': len(nonempty), 'empty_schools': len(rows) - len(nonempty),
        'school_ids_nonempty': nonempty,
        'conserve': (not failed and not missing and not extra
                     and sigma == int(true_nf)),
    }


def main():
    sids = load_sids()
    mine = shard_of(sids)
    proxies = load_proxies()
    if not proxies:
        print('::error::proxies_cnb.txt 为空，拒绝直连', flush=True)
        sys.exit(1)
    print('=== 全集探测 PROV=%s YEAR=%s 分片 %d/%d 本校 %d 代理 %d ===' %
          (PROV, YEAR, IDX, TOTAL, len(mine), len(proxies)), flush=True)
    res = probe_shard(mine, make_probe_fn(proxies))
    if res['failed']:
        print('::warning::本片 %d 校探测失败（汇总会判不守恒）' % res['failed'], flush=True)


if __name__ == '__main__':
    main()
