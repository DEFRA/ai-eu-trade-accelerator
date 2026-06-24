"""Beatrice CLI — the Susan -> Beatrice matching stage.

Subcommands:
  beatrice run         group-rerank Susan guidance against Judit law (batch)
  beatrice tag-topics  tag guidance/law propositions with a closed-vocab topic
  beatrice type-law    classify law propositions by drafting function

Each subcommand has its own ``--help``.
"""

from __future__ import annotations

import sys

_USAGE = "usage: beatrice {run|tag-topics|type-law} ... (use <cmd> --help for options)\n"


def main() -> None:
    if len(sys.argv) < 2:
        sys.stderr.write(_USAGE)
        raise SystemExit(2)

    cmd = sys.argv.pop(1)  # drop the subcommand so each main()'s argparse sees the rest
    if cmd in ("-h", "--help"):
        sys.stdout.write(_USAGE)
        raise SystemExit(0)
    if cmd == "run":
        from beatrice.pipeline.batch_match import main as run
    elif cmd == "tag-topics":
        from beatrice.pipeline.tag_topics import main as run
    elif cmd == "type-law":
        from beatrice.pipeline.type_law import main as run
    else:
        sys.stderr.write(f"beatrice: unknown command {cmd!r}\n{_USAGE}")
        raise SystemExit(2)
    run()


if __name__ == "__main__":
    main()
