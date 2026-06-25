"""Shared Anthropic batch mechanics for Radia's two passes.

Both the pass-1 classifier and the pass-2 adjudicator submit a batch of requests,
poll it to completion, and collect the first text block of every succeeded reply.
That submit/poll/collect loop lives here so neither pass owns a private copy.
"""
from __future__ import annotations

import sys
import time

from anthropic import Anthropic
from anthropic.types import Message, TextBlock
from anthropic.types.messages import MessageBatch
from anthropic.types.messages.batch_create_params import Request

POLL_SECONDS = 10


def text_of(message: Message) -> str:
    """First text block of a model reply, or '' (skips thinking / tool blocks)."""
    for block in message.content:
        if isinstance(block, TextBlock):
            return block.text
    return ""


def _log_progress(batch: MessageBatch, label: str) -> None:
    """Print one batch-poll line to stderr so a long run isn't a silent wait."""
    c = batch.request_counts
    print(
        f"  {label} {batch.id}: {c.succeeded} succeeded, {c.processing} processing, "
        f"{c.errored} errored",
        file=sys.stderr,
    )


def submit_and_collect(
    client: Anthropic, requests: list[Request], *, label: str
) -> tuple[dict[str, str], dict[str, int]]:
    """Create a batch, poll to completion, return ``({custom_id: text}, usage)``.

    ``usage`` totals the ``input``/``output`` tokens over the succeeded rows so the
    caller can price the pass. Rows that errored or were cancelled are simply absent
    from the result (and unbilled); the caller decides how to handle a missing
    custom_id (pass 1 rescues, pass 2 keeps).
    """
    batch = client.messages.batches.create(requests=requests)
    while batch.processing_status == "in_progress":
        _log_progress(batch, label)
        time.sleep(POLL_SECONDS)
        batch = client.messages.batches.retrieve(batch.id)

    raw: dict[str, str] = {}
    usage = {"input": 0, "output": 0}
    for r in client.messages.batches.results(batch.id):
        if r.result.type == "succeeded":
            raw[r.custom_id] = text_of(r.result.message)
            u = r.result.message.usage
            usage["input"] += u.input_tokens
            usage["output"] += u.output_tokens
    return raw, usage
