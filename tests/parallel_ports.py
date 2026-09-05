"""Collision-free port allocation for parallel integration runs.

Lives in its own module rather than in ``conftest.py`` because three importable
``conftest`` modules sit on ``sys.path`` during a run (this suite's, and one in each
copy of the vendored framework), so ``from conftest import ...`` does not reliably
name this one.

See docs/plans/parallel-integration-tests-plan.md.
"""

import fcntl
import os
import socket
import tempfile


class SafePortTracker(object):
    """Hand out ports that no other worker or process will take.

    Replaces the valkey-test-framework ``PortTracker``, which walks a hash chain
    over one shared range and *unlinks* its lock files on release. Two weaknesses
    matter once tests run concurrently:

      * unlinking means two processes can hold ``flock`` on two different inodes
        that share one path, so the lock stops excluding anything;
      * the bind probe closes its socket before valkey-server binds, leaving a
        window in which another worker can win the same port.

    Both are addressed here. Each xdist worker owns a disjoint band, so the common
    case never contends at all; the file locks remain as a guard against processes
    outside this suite, and are held (never unlinked) for the life of the fixture.

    Port layout, kept below the Linux ephemeral floor of 32768:

        client ports   10000 + 100 * worker_idx, 100 per worker  -> [10000, 19999]
        cluster bus    client port + 10000                       -> [20000, 29999]

    A serial run is not under xdist and owns the entire client band rather than a
    100-port slice, so it has as much room as it ever had.
    """

    CLUSTER_BUS_PORT_OFFSET = 10000

    BASE_PORT = 10000
    PORTS_PER_WORKER = 100
    MAX_WORKERS = 100

    LOCKS_DIR = os.path.join(
        tempfile.gettempdir(),
        "valkey_ts_port_locks_%d" % (os.getuid() if hasattr(os, "getuid") else 0),
    )

    # Monotonic, per-process. Cycling rather than restarting at the band's base
    # keeps consecutive tests on one worker off a port whose previous socket is
    # still in TIME_WAIT.
    _port_offset = 0

    def __init__(self, node_id=None):
        self.node_id = node_id
        os.makedirs(self.LOCKS_DIR, exist_ok=True)
        self.locked_fds = {}

        worker = os.environ.get("PYTEST_XDIST_WORKER", "master")
        if worker.startswith("gw"):
            try:
                self.worker_idx = int(worker[2:])
            except ValueError:
                self.worker_idx = 0
            if self.worker_idx >= self.MAX_WORKERS:
                raise RuntimeError(
                    "xdist worker index %d exceeds the %d supported by the port "
                    "bands in tests/conftest.py" % (self.worker_idx, self.MAX_WORKERS)
                )
            self.band_start = self.BASE_PORT + self.worker_idx * self.PORTS_PER_WORKER
            self.band_size = self.PORTS_PER_WORKER
        else:
            # Not under xdist: no one to collide with, so take the whole band.
            self.worker_idx = 0
            self.band_start = self.BASE_PORT
            self.band_size = self.PORTS_PER_WORKER * self.MAX_WORKERS

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        for fd in list(self.locked_fds.values()):
            self._unlock_fd(fd)
        self.locked_fds.clear()

    @staticmethod
    def _unlock_fd(fd):
        try:
            fcntl.flock(fd, fcntl.LOCK_UN)
        except OSError:
            pass
        try:
            os.close(fd)
        except OSError:
            pass

    def _try_lock_port(self, port):
        """Return an open, flocked fd for `port`, or None if it is unavailable.

        The lock file is deliberately never removed: an unlinked lock file lets a
        second process create a fresh inode at the same path and lock that
        instead, which excludes nobody.
        """
        lock_path = os.path.join(self.LOCKS_DIR, "port_%d.lock" % port)
        fd = None
        try:
            fd = os.open(lock_path, os.O_CREAT | os.O_RDWR, 0o666)
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except (OSError, BlockingIOError):
            if fd is not None:
                try:
                    os.close(fd)
                except OSError:
                    pass
            return None

        # The lock only excludes other users of this tracker. Something outside the
        # suite may hold the port, so confirm it can actually be bound.
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            try:
                sock.bind(("0.0.0.0", port))
            except OSError:
                self._unlock_fd(fd)
                return None

        return fd

    def get_unused_port(self):
        """Reserve a client port together with its cluster-bus port."""
        for _ in range(self.band_size):
            candidate = self.band_start + (SafePortTracker._port_offset % self.band_size)
            SafePortTracker._port_offset += 1

            wanted = (candidate, candidate + self.CLUSTER_BUS_PORT_OFFSET)
            acquired = {}
            for port in wanted:
                fd = self._try_lock_port(port)
                if fd is None:
                    break
                acquired[port] = fd
            else:
                self.locked_fds.update(acquired)
                return candidate

            # Partial acquisition: release before trying the next candidate, or the
            # band leaks a lock per failed attempt.
            for fd in acquired.values():
                self._unlock_fd(fd)

        raise RuntimeError(
            "no free port in band [%d, %d) for worker %d after %d attempts"
            % (
                self.band_start,
                self.band_start + self.band_size,
                self.worker_idx,
                self.band_size,
            )
        )
