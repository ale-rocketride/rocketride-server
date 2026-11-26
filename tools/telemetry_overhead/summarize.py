"""
Medians and differences from ab.sh's output: python3 summarize.py LOG OFF_PORT[,OFF_PORT...]

LOG is what ab.sh printed (sections may be headed '== name'). For each section
and server: the median over the rounds of p50 and p95, and of the CPU ticks per
round (the server and its run processes; every server gets the same requests,
so ticks compare), over the rounds every server completed (the others are listed
as excluded). "Added" is a server's median minus the baseline: the mean of
the medians of the servers with telemetry off. The spread of those "off"
servers' medians is the noise floor: an added figure within it is not a cost.
"""

import re
import statistics
import sys

LINE = re.compile(r'round (\d+) port (\d+) \{.*"p50_ms": ([\d.]+), "p95_ms": ([\d.]+)\} cpu_ticks=(-?\d+)')


def main() -> None:
    path, off = sys.argv[1], sys.argv[2].split(',')
    sections: dict = {}  # section -> port -> round -> (p50, p95, ticks)
    section = ''
    failed = 0
    for line in open(path):
        if line.startswith('=='):
            section = line.strip('= \n')
        elif match := LINE.match(line):
            round_id, port, p50, p95, ticks = match.groups()
            sections.setdefault(section, {}).setdefault(port, {})[int(round_id)] = (float(p50), float(p95), int(ticks))
        elif ' FAILED' in line:
            failed += 1
    for name, servers in sections.items():
        title = name or '(all)'
        # Only the rounds every server completed: a failed load leaves the others' rounds unmatched
        every = set.union(*(set(rows) for rows in servers.values()))
        common = set.intersection(*(set(rows) for rows in servers.values()))
        excluded = sorted(every - common)
        present = [port for port in off if port in servers]
        if not common or not present:
            missing = 'no round completed by every server' if not common else 'no rows from the off servers'
            print(f'== {title}: skipped, {missing}')
            continue
        medians = {
            port: tuple(statistics.median(rows[r][i] for r in common) for i in range(3))
            for port, rows in servers.items()
        }
        base = [statistics.mean(medians[port][i] for port in present) for i in range(3)]
        print(f'== {title}: {len(common)} rounds; baseline = mean of off servers {",".join(present)}')
        if excluded:
            print(f'   excluded rounds (not completed by every server): {", ".join(map(str, excluded))}')
        print(f'{"server":>8} {"p50 ms":>8} {"added":>13} {"p95 ms":>8} {"added":>13} {"cpu ticks":>10} {"added":>13}')
        for port, (p50, p95, ticks) in medians.items():
            tag = ' (off)' if port in off else ''

            def added(value, baseline):
                percent = f'{100 * (value - baseline) / baseline:+.0f}%' if baseline else 'n/a'
                return f'{value - baseline:+.1f} {percent}'

            print(
                f'{port:>8} {p50:8.1f} {added(p50, base[0]):>13} {p95:8.1f} {added(p95, base[1]):>13}'
                f' {ticks:10.0f} {added(ticks, base[2]):>13}{tag}'
            )
        spread = [max(medians[p][i] for p in present) - min(medians[p][i] for p in present) for i in range(3)]
        print(
            f'   noise floor (off servers apart): p50 {spread[0]:.1f} ms, p95 {spread[1]:.1f} ms, cpu {spread[2]:.0f} ticks'
        )
    if failed:
        print(f'{failed} run(s) FAILED: see the log')


main()
