#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""启动前刷新代理池（供 GitHub Actions 每个 job 独立运行）。

背景（2026-11-12 实测）：免费代理衰减极快，新仓库 177 个出口 1 小时后
只剩 15 个存活（8%）。而采集器在 main() 里一次性读入 proxies_cnb.txt
（第 725 行），之后不再重读。池子里死出口占 92% 时，采集器每页要轮换
10 次才可能碰到一个活出口，等于 280 分钟预算全耗在无效请求上，
这就是 run 43153887986 跑 4 小时零产出的真实原因。

动作：
1. 稳健探活现有 proxies_cnb.txt：每个出口最多试 2 次，避免单次抖动误杀；
2. 存活数低于 MIN_ALIVE 时，从 candidate_pool.txt（历史大池，实测存活率
   2.7%）与公开源补候选，逐个稳健探活直到够数；
3. 就地重写 proxies_cnb.txt（原子替换），采集器随后读到的是高存活率池。

候选池说明：历史大池 1654 个实测活 45 个（2.7%），公开源 3704 个实测活
9 个（0.24%）。历史池是首选补货来源，公开源只作补充。

只改本 job 工作区里的 proxies_cnb.txt，不改仓库、不落盘到远端。
"""
import json
import os
import random
import sys
import time
import urllib.request
from concurrent.futures import ThreadPoolExecutor

import requests

POOL = 'proxies_cnb.txt'
CAND = 'candidate_pool.txt'
MIN_ALIVE = int(os.environ.get('MIN_ALIVE', '40'))
PROBE_TIMEOUT = (4, 10)
WORKERS = int(os.environ.get('PROBE_WORKERS', '16'))
MAX_FETCH = int(os.environ.get('MAX_FETCH', '6000'))
DEADLINE = time.time() + int(os.environ.get('POOL_DEADLINE', '300'))
ATTEMPTS = 2

U = 'https://api.zjzw.cn/web/api/'
H = {'User-Agent': 'Mozilla/5.0', 'Origin': 'https://gkcx.eol.cn',
     'Referer': 'https://gkcx.eol.cn/',
     'Accept': 'application/json,text/plain,*/*',
     'Content-Type': 'application/json'}
Q = {'uri': 'apidata/api/gk/score/special', 'local_province_id': '44',
     'year': 2023, 'page': 3, 'size': 20}

SOURCES = [
    'https://raw.githubusercontent.com/TheSpeedX/PROXY-List/master/http.txt',
    'https://raw.githubusercontent.com/clarketm/proxy-list/master/proxy-list-raw.txt',
    'https://raw.githubusercontent.com/proxifly/free-proxy-list/main/proxies/all/data.txt',
    'https://proxylist.geonode.com/api/proxy-list?limit=500&page=1&sort_by=lastChecked&sort_type=desc',
]


def norm(x):
    x = (x or '').strip()
    if not x or x.startswith('#'):
        return None
    if not x.startswith('http'):
        x = 'http://' + x
    tail = x.split('://', 1)[-1].split('/')[0]
    if ':' not in tail:
        return None
    host, port = tail.rsplit(':', 1)
    if not port.isdigit() or not host or '@' in host:
        return None
    return 'http://' + host + ':' + port


def _once(px):
    try:
        r = requests.post(U, json=Q, headers=H,
                          proxies={'http': px, 'https': px}, timeout=PROBE_TIMEOUT)
        d = r.json()
        if str(d.get('code')) == '0000' and isinstance(d.get('data'), dict):
            return True
    except Exception:
        pass
    return False


def probe(px):
    """稳健探活：最多试 ATTEMPTS 次，任意一次成功即算活。

    避免单次网络抖动把好出口误判为死——刷新只能让池子变好，不能变差。
    """
    for _ in range(ATTEMPTS):
        if _once(px):
            return px
    return None


def candidates():
    """补货候选：历史大池优先（存活率 2.7%），公开源补充（0.24%）。"""
    out = []
    # 1) 历史大池：实测存活率远高于公开源
    try:
        for l in open(CAND, encoding='utf-8'):
            p = norm(l)
            if p:
                out.append(p)
    except Exception:
        pass
    print('历史候选池 %d 个' % len(out), flush=True)
    # 2) 公开源补充
    for url in SOURCES:
        try:
            req = urllib.request.Request(url, headers={'User-Agent': 'Mozilla/5.0'})
            with urllib.request.urlopen(req, timeout=25) as r:
                txt = r.read().decode('utf-8', 'ignore')
            if 'geonode' in url:
                for it in json.loads(txt).get('data', []):
                    out.append('%s:%s' % (it.get('ip'), it.get('port')))
            else:
                out += txt.splitlines()
        except Exception as e:
            print('源失败 %s: %s' % (url.split('/')[2], str(e)[:60]), flush=True)
    seen, res = set(), []
    for x in out:
        p = norm(x)
        if p and p not in seen:
            seen.add(p)
            res.append(p)
    return res[:MAX_FETCH]


def read_pool():
    try:
        return [l.strip() for l in open(POOL, encoding='utf-8') if l.strip()]
    except Exception:
        return []


def write_pool(items):
    tmp = POOL + '.tmp'
    with open(tmp, 'w', encoding='utf-8') as f:
        f.write('\n'.join(items) + ('\n' if items else ''))
    os.replace(tmp, POOL)


def main():
    old = read_pool()
    print('池中原有 %d 个，开始稳健探活（每出口最多试 %d 次）' % (len(old), ATTEMPTS), flush=True)
    alive = []
    with ThreadPoolExecutor(WORKERS) as ex:
        for r in ex.map(probe, old):
            if r:
                alive.append(r)
    print('探活完成：%d / %d 存活 (%.0f%%)'
          % (len(alive), len(old), 100.0 * len(alive) / max(1, len(old))), flush=True)

    if len(alive) < MIN_ALIVE:
        print('存活不足 %d，从公开源补货' % MIN_ALIVE, flush=True)
        cands = [c for c in candidates() if c not in set(alive)]
        print('候选 %d 个' % len(cands), flush=True)
        for i in range(0, len(cands), 200):
            if len(alive) >= MIN_ALIVE or time.time() >= DEADLINE:
                break
            batch = cands[i:i + 200]
            with ThreadPoolExecutor(WORKERS) as ex:
                for r in ex.map(probe, batch):
                    if r and r not in alive:
                        alive.append(r)
            print('  补货后存活 %d' % len(alive), flush=True)

    random.shuffle(alive)
    if len(alive) < MIN_ALIVE:
        print('::error::探活后仅 %d 个可用代理（要求 %d），拒绝用低存活率池采集，'
              '以免 280 分钟预算全耗在无效请求上'
              % (len(alive), MIN_ALIVE), flush=True)
        sys.exit(2)
    write_pool(alive)
    print('已重写 %s：%d 个存活代理' % (POOL, len(alive)), flush=True)


if __name__ == '__main__':
    main()
