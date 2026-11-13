#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""按 school_id 单校分片采集（37 省 POC）。

切分维度：school_id。每所学校一个分片，一次进程只采一所（matrix 一 job 一校）。

    payload = {uri, local_province_id: PROV, year: YEAR, page, size: 20,
               school_id: <int>}

为什么这条切法成立（2026-11-13 实测）：
  * 接口真实认 school_id 筛选：school_id=678 -> numFound=63，且返回记录的
    school_id 全为 678；不存在的 ID -> numFound=0（不是忽略参数）。
  * 单校在一个考生省一年的专业录取记录只有几十~几百条，天然远低于
    200 页 * size=20 = 4000 的单片硬上限，无需任何二级切分。
  * school_id 全集来自 院校库_2970.csv 的 school_code（2970 所），
    与接口返回的 school_id 同源（抽样 21/21 命中），属确定性完备划分，
    Sigma over 全集 = 组合真值；不像 level2_name 需要翻页猜取值而漏值。

本脚本不推任何 special-* 分支（主仓回收只认该前缀，避免污染），
结果只写工作区 out/ 供 artifact 取走。
"""
import json
import os
import random
import sys
import time

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
SIZE = 20
PAGE_CAP = 200
NUMFOUND_CAP = 10000
QUERY_ROUNDS = 12
RETRY_ROUNDS = 8


def make_real_fetch(proxies, sid):
    """构造真实 fetch(page) -> (numFound|None, items|None)，带代理轮换。

    轮换语义：每轮把整池扫一遍，遇到 code=0000 且 data 是 dict 即返回；
    页残缺/连接失败/限流都换下一个号，绝不直连（runner 共享出口会被限流）。
    """
    import requests

    pool = list(proxies)
    random.shuffle(pool)

    def _once(px, page):
        q = {'uri': API, 'local_province_id': PROV, 'year': YEAR,
             'page': page, 'size': SIZE, 'school_id': sid}
        r = requests.post(U, json=q, headers=H,
                          proxies={'http': px, 'https': px}, timeout=(6, 14))
        return r.json()

    def fetch(page):
        for _ in range(QUERY_ROUNDS):
            for px in pool:
                try:
                    d = _once(px, page)
                except Exception:
                    continue
                if str(d.get('code')) != '0000':
                    continue
                dat = d.get('data')
                if not isinstance(dat, dict):
                    continue
                return dat.get('numFound'), (dat.get('item') or [])
        return None, None

    return fetch


def collect_school(sid, fetch, prov=PROV, year=YEAR, size=SIZE, out_dir='out'):
    """纯逻辑：给定 fetch(page) 采一个学校片，校验四道闸门并落盘。

    fetch 可注入，便于本地 mock 端到端自测（不依赖网络/代理）。
    """
    t0 = time.time()
    nf, first = fetch(1)
    if nf is None:
        out = {'prov': prov, 'year': year, 'school_id': sid,
               'error': 'numFound 探测失败（代理耗尽）', 'gate_pass': False}
        _emit(out, out_dir)
        return out

    nf = int(nf)
    if nf >= NUMFOUND_CAP:
        out = {'prov': prov, 'year': year, 'school_id': sid, 'num_found': nf,
               'error': '单校片 numFound 封顶，需再切分', 'gate_pass': False}
        _emit(out, out_dir)
        return out

    pages = (nf + size - 1) // size
    if pages > PAGE_CAP:
        out = {'prov': prov, 'year': year, 'school_id': sid, 'num_found': nf,
               'pages': pages, 'error': '超过 200 页翻页上限', 'gate_pass': False}
        _emit(out, out_dir)
        return out

    def expected(p):
        return size if p < pages else nf - (pages - 1) * size

    res, failed = {}, []
    for p in range(1, pages + 1):
        if p == 1 and first is not None:
            items = first
        else:
            _, items = fetch(p)
            items = items or []
        if len(items) < expected(p):
            failed.append(p)
        else:
            res[p] = items

    # 失败页补采：换代理重试到 0 失败
    for p in list(failed):
        for _ in range(RETRY_ROUNDS):
            _, items = fetch(p)
            if items and len(items) >= expected(p):
                res[p] = items
                failed.remove(p)
                break

    all_items = []
    for p in sorted(res):
        all_items += res[p]
    ids = [x.get('id') for x in all_items]
    pure = all(str(x.get('school_id')) == str(sid) for x in all_items)
    uniq = len(set(ids))

    gate = {
        'pages_zero_fail': len(failed) == 0,
        'count_match': len(all_items) == nf,
        'school_pure': pure,
        'unique_ids': uniq == len(all_items),
    }
    out = {
        'prov': prov, 'year': year, 'school_id': sid,
        'dimension': 'school_id',
        'num_found': nf, 'count': len(all_items),
        'pages': pages, 'pages_ok': len(res), 'pages_failed': len(failed),
        'unique_ids': uniq, 'dup_ids': len(all_items) - uniq,
        'school_pure': pure,
        'gate': gate,
        'gate_pass': all(gate.values()),
        'elapsed': round(time.time() - t0, 1),
        'items': all_items,
    }
    _emit(out, out_dir)
    return out


def _emit(out, out_dir):
    os.makedirs(out_dir, exist_ok=True)
    path = os.path.join(out_dir, 'school_%s_%s_%s.json' %
                        (out['prov'], out['year'], out['school_id']))
    with open(path, 'w', encoding='utf-8') as f:
        json.dump(out, f, ensure_ascii=False)
    summary = {k: out[k] for k in
               ('prov', 'year', 'school_id', 'num_found', 'count', 'pages',
                'pages_ok', 'pages_failed', 'unique_ids', 'school_pure',
                'gate_pass', 'gate', 'error') if k in out}
    print('RESULT_JSON ' + json.dumps(summary, ensure_ascii=False), flush=True)
    print('结果文件 %s (%.1f MB)' % (path, os.path.getsize(path) / 1e6), flush=True)


def main():
    sid = int(os.environ['SCHOOL'])
    proxies = [x.strip() for x in open('proxies_cnb.txt', encoding='utf-8')
               if x.strip() and not x.strip().startswith('#')]
    if not proxies:
        print('::error::proxies_cnb.txt 为空，拒绝直连', flush=True)
        sys.exit(1)
    print('=== school_id 分片采集 PROV=%s YEAR=%s SCHOOL=%d 代理=%d ===' %
          (PROV, YEAR, sid, len(proxies)), flush=True)
    out = collect_school(sid, make_real_fetch(proxies, sid))
    if not out.get('gate_pass'):
        print('::error::学校 %d 未达标：%s' %
              (sid, out.get('error') or out.get('gate')), flush=True)
        sys.exit(1)
    print('学校 %d 达标：%d 条 / %d 页 / 唯一 id %d' %
          (sid, out['count'], out['pages'], out['unique_ids']), flush=True)


if __name__ == '__main__':
    main()
