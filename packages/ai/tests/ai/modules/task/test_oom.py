"""The container's OOM kill count: a run killed by SIGKILL while it rose is a suspected OOM kill."""

from ai.modules.task import oom


def _events(tmp_path, count):
    path = tmp_path / 'memory.events'
    path.write_text(f'low 0\nhigh 0\nmax 3\noom 1\noom_kill {count}\n', encoding='ascii')
    return (str(path),)


def test_the_count_is_read_from_cgroup_v2(tmp_path):
    assert oom.oom_kills(_events(tmp_path, 4)) == 4


def test_no_cgroup_is_no_count(tmp_path):
    assert oom.oom_kills((str(tmp_path / 'missing'),)) is None


def test_a_sigkill_while_the_count_rose_is_suspected(tmp_path):
    sources = _events(tmp_path, 5)
    assert oom.suspected(4, 'SIGKILL', sources) is True
    assert oom.suspected(5, 'SIGKILL', sources) is False  # no new OOM kill
    assert oom.suspected(4, 'SIGSEGV', sources) is False  # not the OOM killer's signal
    assert oom.suspected(None, 'SIGKILL', sources) is False  # no count at start
