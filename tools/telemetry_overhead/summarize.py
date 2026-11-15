"""
Medians and differences from ab.sh's output: python3 summarize.py LOG OFF_PORT[,OFF_PORT...]

LOG is what ab.sh printed (sections may be headed '== name'). For each section
and server: the median over the rounds of p50 and p95, and of the CPU ticks per
round (the server and its run processes; every server gets the same requests,
so ticks compare). "Added" is a server's median minus the baseline: the mean of
the medians of the servers with telemetry off. The spread of those "off"
servers' medians is the noise floor: an added figure within it is not a cost.
"""

import re
import statistics
import sys

LINE = re.compile(r'round (\d+) port (\d+) \{.*"p50_ms": ([\d.]+), "p95_ms": ([\d.]+)\} cpu_ticks=(-?\d+)')


def main() -> None:
    path, off = sys.argv[1], sys.argv[2].split(',')
    sections: dict = {}
    section = ''
    failed = 0
    for line in open(path):
        if line.startswith('=='):
            section = line.strip('= \n')
        elif match := LINE.match(line):
            _, port, p50, p95, ticks = match.groups()
            sections.setdefault(section, {}).setdefault(port, []).append((float(p50), float(p95), int(ticks)))
        elif ' FAILED' in line:
            failed += 1
    for name, servers in sections.items():
        medians = {
            port: tuple(statistics.median(row[i] for row in rows) for i in range(3)) for port, rows in servers.items()
        }
        base = [statistics.mean(medians[port][i] for port in off if port in medians) for i in range(3)]
        rounds = min(len(rows) for rows in servers.values())
        print(f'== {name or "(all)"}: {rounds} rounds; baseline = mean of off servers {",".join(off)}')
        print(f'{"server":>8} {"p50 ms":>8} {"added":>13} {"p95 ms":>8} {"added":>13} {"cpu ticks":>10} {"added":>13}')
        for port, (p50, p95, ticks) in medians.items():
            tag = ' (off)' if port in off else ''

            def added(value, baseline):
                return f'{value - baseline:+.1f} {100 * (value - baseline) / baseline:+.0f}%'

            print(
                f'{port:>8} {p50:8.1f} {added(p50, base[0]):>13} {p95:8.1f} {added(p95, base[1]):>13}'
                f' {ticks:10.0f} {added(ticks, base[2]):>13}{tag}'
            )
        spread = [max(medians[p][i] for p in off) - min(medians[p][i] for p in off) for i in range(3)]
        print(
            f'   noise floor (off servers apart): p50 {spread[0]:.1f} ms, p95 {spread[1]:.1f} ms, cpu {spread[2]:.0f} ticks'
        )
    if failed:
        print(f'{failed} run(s) FAILED: see the log')


main()
