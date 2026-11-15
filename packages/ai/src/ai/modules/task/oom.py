# =============================================================================
# MIT License
# Copyright (c) 2026 Aparavi Software AG
# =============================================================================

"""
Whether the kernel's OOM killer acted in this container: its cgroup's oom_kill count.

A task process killed by SIGKILL says nothing more; the count rising while it ran says
memory was the likely cause. The count is the container's (every task in it), so a
rise while a task died by SIGKILL is a suspicion, not a proof.
"""

from typing import Optional

# cgroup v2 (memory.events), then v1 (memory.oom_control): 'oom_kill <count>'
_SOURCES = ('/sys/fs/cgroup/memory.events', '/sys/fs/cgroup/memory/memory.oom_control')


def oom_kills(sources=_SOURCES) -> Optional[int]:
    """The container's OOM kills so far, or None where the count cannot be read (not Linux, no cgroup)."""
    for path in sources:
        try:
            with open(path, encoding='ascii') as handle:
                for line in handle:
                    key, _, value = line.partition(' ')
                    if key == 'oom_kill' and value.strip().isdigit():
                        return int(value)
        except (OSError, ValueError):
            continue
    return None


def suspected(before: Optional[int], signal_name: Optional[str], sources=_SOURCES) -> bool:
    """A process killed by SIGKILL while the container's OOM kill count rose (since `before`)."""
    if signal_name != 'SIGKILL' or before is None:
        return False
    after = oom_kills(sources)
    return after is not None and after > before
