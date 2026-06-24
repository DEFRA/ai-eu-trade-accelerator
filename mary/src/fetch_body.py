"""
Fetch full gov.uk page body text for pipeline-format items.

Stage 2 of Mary: body fetch. Reads pipeline JSON (list of {url, content_id,
meta_data}) and fetches the gov.uk content API for each item, adding body_text
to meta_data. Restartable: URLs already present in the output file with a
non-null body_text are skipped — that output file doubles as the run cache.

A failed fetch is recorded as body_text=None (not dropped) so a re-run retries
it; the final summary counts and reports those failures loudly.

Usage:
    python fetch_body.py [input.json] [output.json]

Optional:
    --concurrency 3       Max concurrent requests (default: from config)
    --rate 10.0           Max requests per second (default: from config)
"""

import asyncio
import html
import json
import re
import sys
import time
from html.parser import HTMLParser
from pathlib import Path
from urllib.parse import urlparse

import httpx
import typer

# Resolve sibling modules explicitly from this file's directory, rather than
# relying on the implicit current-working-directory entry on sys.path.
sys.path.insert(0, str(Path(__file__).resolve().parent))

from config import (  # noqa: E402
    CONTENT_API_BASE,
    DEFAULT_BODY_OUTPUT,
    DEFAULT_CONCURRENCY,
    DEFAULT_RATE,
    DEFAULT_SEARCH_OUTPUT,
    INITIAL_BACKOFF,
    MAX_BACKOFF,
    MAX_RETRIES,
    PROGRESS_EVERY,
    REQUEST_TIMEOUT,
)
from page import PageItem  # noqa: E402


class _TextExtractor(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.chunks: list[str] = []
        self._skip = False

    def handle_starttag(self, tag: str, attrs: list) -> None:
        if tag in ("script", "style"):
            self._skip = True

    def handle_endtag(self, tag: str) -> None:
        if tag in ("script", "style"):
            self._skip = False
        if tag in ("p", "li", "h1", "h2", "h3", "h4", "td", "th"):
            self.chunks.append("\n")

    def handle_data(self, data: str) -> None:
        if not self._skip:
            self.chunks.append(data)


def _html_to_text(raw_html: str) -> str:
    parser = _TextExtractor()
    parser.feed(html.unescape(raw_html))
    text = "".join(parser.chunks)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def _extract_body_text(content: dict) -> str:
    details = content.get("details", {})
    body = details.get("body", "")
    if body:
        return _html_to_text(body)
    parts = details.get("parts", [])
    if parts:
        segments = [_html_to_text(p.get("body", "")) for p in parts if p.get("body")]
        return "\n\n".join(segments)
    return ""


def _url_to_api_url(url: str) -> str:
    path = urlparse(url).path
    return f"{CONTENT_API_BASE}/api/content{path}"


class RateLimiter:
    def __init__(self, rate: float) -> None:
        self.rate = rate
        self.tokens = rate
        self.last_refill = time.monotonic()
        self._lock = asyncio.Lock()

    async def acquire(self) -> None:
        async with self._lock:
            now = time.monotonic()
            self.tokens = min(self.rate, self.tokens + (now - self.last_refill) * self.rate)
            self.last_refill = now
            if self.tokens < 1:
                wait = (1 - self.tokens) / self.rate
                await asyncio.sleep(wait)
                self.tokens = 0
            else:
                self.tokens -= 1


async def _fetch_one(
    client: httpx.AsyncClient,
    item: dict,
    rate_limiter: RateLimiter,
    counters: dict,
) -> dict:
    await rate_limiter.acquire()
    api_url = _url_to_api_url(item["url"])
    result = {**item, "meta_data": dict(item.get("meta_data", {}))}
    backoff = INITIAL_BACKOFF
    for attempt in range(MAX_RETRIES):
        try:
            response = await client.get(api_url)
            if response.status_code == 200:
                content = response.json()
                result["meta_data"]["body_text"] = _extract_body_text(content)
                counters["ok"] += 1
                break
            elif response.status_code == 429:
                retry_after = float(response.headers.get("Retry-After", backoff))
                print(f"  429 rate limited: {item['url']} — waiting {retry_after:.0f}s (attempt {attempt + 1}/{MAX_RETRIES})")
                await asyncio.sleep(retry_after)
                backoff = min(backoff * 2, MAX_BACKOFF)
            else:
                result["meta_data"]["body_text"] = None
                counters["errors"] += 1
                print(f"  HTTP {response.status_code}: {item['url']}")
                break
        except httpx.HTTPError as e:
            result["meta_data"]["body_text"] = None
            counters["errors"] += 1
            print(f"  Network error: {item['url']} — {e}")
            break
    else:
        result["meta_data"]["body_text"] = None
        counters["errors"] += 1
        print(f"  Gave up after {MAX_RETRIES} retries: {item['url']}")

    counters["done"] += 1
    if counters["done"] % PROGRESS_EVERY == 0:
        print(f"  Progress: {counters['done']}/{counters['total']} "
              f"(ok={counters['ok']}, errors={counters['errors']})")
    return result


async def _fetch_all(
    items: list[dict],
    concurrency: int,
    rate: float,
) -> list[dict]:
    rate_limiter = RateLimiter(rate)
    counters = {"done": 0, "ok": 0, "errors": 0, "total": len(items)}
    # Result order is irrelevant — run() re-sorts by the input's URL order — so
    # workers just append as they finish (append doesn't yield in asyncio).
    results: list[dict] = []

    queue: asyncio.Queue = asyncio.Queue()
    for item in items:
        queue.put_nowait(item)

    async with httpx.AsyncClient(timeout=REQUEST_TIMEOUT, follow_redirects=True) as client:
        async def worker() -> None:
            while True:
                try:
                    item = queue.get_nowait()
                except asyncio.QueueEmpty:
                    break
                results.append(await _fetch_one(client, item, rate_limiter, counters))
                queue.task_done()

        workers = [asyncio.create_task(worker()) for _ in range(concurrency)]
        await asyncio.gather(*workers)

    return results


def _report(output: list[PageItem], output_path: str) -> None:
    """Final summary — loudly counts pages that have no usable body text."""
    failed = sum(1 for i in output if i["meta_data"].get("body_text") is None)
    empty = sum(1 for i in output if i["meta_data"].get("body_text") == "")
    present = len(output) - failed - empty

    print(f"\nDone. {len(output)} items → {output_path}")
    print(f"  body_text present: {present}")
    if empty:
        print(f"  body_text empty (page has no body): {empty}")
    if failed:
        print(
            f"  ⚠ body_text MISSING ({failed}) — these fetches failed and are "
            f"invisible to Radia. Re-run to retry them."
        )


def run(input_path: str, output_path: str, concurrency: int, rate: float) -> None:
    all_items = json.loads(Path(input_path).read_text())
    print(f"Input items: {len(all_items)}")

    out_path = Path(output_path)
    existing: dict[str, dict] = {}
    if out_path.exists():
        try:
            for item in json.loads(out_path.read_text()):
                if item.get("meta_data", {}).get("body_text") is not None:
                    existing[item["url"]] = item
        except (json.JSONDecodeError, KeyError):
            pass
    print(f"Already fetched: {len(existing)}")

    to_fetch = [item for item in all_items if item["url"] not in existing]
    print(f"To fetch:        {len(to_fetch)}")

    if to_fetch:
        print(f"Concurrency: {concurrency}, Rate limit: {rate} req/sec")
        fetched = asyncio.run(_fetch_all(to_fetch, concurrency, rate))
        for item in fetched:
            existing[item["url"]] = item

    url_order = [item["url"] for item in all_items]
    output = [existing[url] for url in url_order if url in existing]

    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(output, indent=2))
    _report(output, output_path)


def main(
    input_path: str = typer.Argument(
        DEFAULT_SEARCH_OUTPUT, help="Path to pipeline-format input JSON (stage 1 output)"
    ),
    output_path: str = typer.Argument(
        DEFAULT_BODY_OUTPUT, help="Path to output JSON (Radia input)"
    ),
    concurrency: int = typer.Option(DEFAULT_CONCURRENCY, help="Max concurrent requests"),
    rate: float = typer.Option(DEFAULT_RATE, help="Max requests per second"),
) -> None:
    run(input_path, output_path, concurrency, rate)


if __name__ == "__main__":
    typer.run(main)
