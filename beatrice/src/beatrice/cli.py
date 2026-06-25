"""Beatrice CLI — the Susan -> Beatrice matching stage.

Subcommands:
  beatrice run  group-rerank Susan guidance against Judit law (batch)

The subcommand has its own ``--help``.
"""

from __future__ import annotations

import sys

_USAGE = "usage: beatrice run ... (use `beatrice run --help` for options)\n"


def main() -> None:
    if len(sys.argv) < 2:
        sys.stderr.write(_USAGE)
        raise SystemExit(2)

    cmd = sys.argv.pop(1)  # drop the subcommand so batch_match's argparse sees the rest
    if cmd in ("-h", "--help"):
        sys.stdout.write(_USAGE)
        raise SystemExit(0)
    if cmd != "run":
        sys.stderr.write(f"beatrice: unknown command {cmd!r}\n{_USAGE}")
        raise SystemExit(2)
    from beatrice.pipeline.batch_match import main as run
    run()


if __name__ == "__main__":
    main()
