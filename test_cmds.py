#!/usr/bin/env python3

BIN = './zig-out/bin/client'

CASES = f'''
$ {BIN} zscore asdf n1
(nil)
$ {BIN} zquery xxx 1 asdf 1 10
(arr) len=0
(arr) end
$ {BIN} zadd zset 1 n1
(int) 1
$ {BIN} zadd zset 2 n2
(int) 1
$ {BIN} zadd zset 1.1 n1
(int) 0
$ {BIN} zscore zset n1
(float) 1.1
$ {BIN} zquery zset 1 "" 0 10
(arr) len=4
(str) n1
(float) 1.1
(str) n2
(float) 2
(arr) end
$ {BIN} zquery zset 1.1 "" 1 10
(arr) len=2
(str) n2
(float) 2
(arr) end
$ {BIN} zquery zset 1.1 "" 2 10
(arr) len=0
(arr) end
$ {BIN} zrem zset adsf
(int) 0
$ {BIN} zrem zset n1
(int) 1
$ {BIN} zquery zset 1 "" 0 10
(arr) len=2
(str) n2
(float) 2
(arr) end
'''


import shlex
import subprocess

cmds = []
outputs = []
lines = CASES.splitlines()
for x in lines:
    x = x.strip()
    if not x:
        continue
    if x.startswith('$ '):
        cmds.append(x[2:])
        outputs.append('')
    else:
        outputs[-1] = outputs[-1] + x + '\n'

assert len(cmds) == len(outputs)
for cmd, expect in zip(cmds, outputs):
    out = subprocess.check_output(shlex.split(cmd)).decode('utf-8')
    assert out == expect, f'cmd:{cmd} out:{out} expect:{expect}'

print(f'{len(cmds)} cases passed')
