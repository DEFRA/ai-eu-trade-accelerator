import hashlib
import json
import os
import threading
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from urllib.error import URLError
from urllib.request import Request, urlopen

from fastapi import FastAPI, HTTPException
from fastapi.responses import HTMLResponse
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

from beatrice_domain import Proposition
from beatrice_guidance import extract_propositions, parse_content_api_response
from beatrice_guidance.models import GuidanceProposition
from beatrice_llm import BeatriceLLMClient
from beatrice_matching.bert_score import compute_bert_scores
from beatrice_matching.classify import classify_match
from beatrice_matching.embeddings import embed_texts, find_candidates


@asynccontextmanager
async def lifespan(app: FastAPI):
    _load_cache()
    _load_extract_cache()
    _load_summarise_cache()
    yield


app = FastAPI(title="Beatrice Guidance API", version="0.1.0", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:3001",
        "http://127.0.0.1:3001",
        "http://localhost:3000",
        "http://127.0.0.1:3000",
    ],
    allow_methods=["*"],
    allow_headers=["*"],
)


def _llm_client() -> BeatriceLLMClient:
    return BeatriceLLMClient()


# ── Law embedding store ────────────────────────────────────────────────────────

@dataclass
class LawEmbeddingCache:
    file_hash: str
    filename: str
    proposition_count: int
    created_at: datetime
    propositions_by_id: dict[str, dict] = field(default_factory=dict)
    law_index: list[tuple[str, list[float]]] = field(default_factory=list)


_law_store: dict[str, LawEmbeddingCache] = {}

_CACHE_PATH = Path(os.getenv("EMBEDDINGS_CACHE_PATH", "/tmp/beatrice/law-embeddings-cache.json"))


def _save_cache() -> None:
    _CACHE_PATH.parent.mkdir(parents=True, exist_ok=True)
    serialisable = {
        h: {
            "file_hash": e.file_hash,
            "filename": e.filename,
            "proposition_count": e.proposition_count,
            "created_at": e.created_at.isoformat(),
            "propositions_by_id": e.propositions_by_id,
            "law_index": [[id_, emb] for id_, emb in e.law_index],
        }
        for h, e in _law_store.items()
    }
    _CACHE_PATH.write_text(json.dumps(serialisable))


def _load_cache() -> None:
    if not _CACHE_PATH.exists():
        return
    try:
        data = json.loads(_CACHE_PATH.read_text())
        for h, raw in data.items():
            _law_store[h] = LawEmbeddingCache(
                file_hash=raw["file_hash"],
                filename=raw["filename"],
                proposition_count=raw["proposition_count"],
                created_at=datetime.fromisoformat(raw["created_at"]),
                propositions_by_id=raw["propositions_by_id"],
                law_index=[(id_, emb) for id_, emb in raw["law_index"]],
            )
    except Exception:
        pass  # corrupt cache — start fresh


# ── Extraction cache ──────────────────────────────────────────────────────────

_EXTRACT_CACHE_PATH = Path(os.getenv("EXTRACT_CACHE_PATH", "/tmp/beatrice/extract-cache.json"))
_extract_cache: dict[str, list[dict]] = {}


def _load_extract_cache() -> None:
    if not _EXTRACT_CACHE_PATH.exists():
        return
    try:
        _extract_cache.update(json.loads(_EXTRACT_CACHE_PATH.read_text()))
    except Exception:
        pass


def _save_extract_cache() -> None:
    _EXTRACT_CACHE_PATH.parent.mkdir(parents=True, exist_ok=True)
    _EXTRACT_CACHE_PATH.write_text(json.dumps(_extract_cache))


# ── Summarise cache ───────────────────────────────────────────────────────────

_SUMMARISE_CACHE_PATH = Path(os.getenv("SUMMARISE_CACHE_PATH", "/tmp/beatrice/summarise-cache.json"))
_summarise_cache: dict[str, str] = {}
_summarise_cache_lock = threading.Lock()


def _load_summarise_cache() -> None:
    if not _SUMMARISE_CACHE_PATH.exists():
        return
    try:
        _summarise_cache.update(json.loads(_SUMMARISE_CACHE_PATH.read_text()))
    except Exception:
        pass


def _save_summarise_cache() -> None:
    _SUMMARISE_CACHE_PATH.parent.mkdir(parents=True, exist_ok=True)
    _SUMMARISE_CACHE_PATH.write_text(json.dumps(_summarise_cache))


def _content_api_url(guidance_url: str) -> str:
    """Derive the GOV.UK Content API URL from a guidance page URL."""
    path = guidance_url.replace("https://www.gov.uk", "").replace("http://www.gov.uk", "")
    path = path.split("#")[0].rstrip("/")
    return f"https://www.gov.uk/api/content{path}"


def _fetch_content_api(url: str) -> dict[str, Any]:
    req = Request(url, headers={"Accept": "application/json"})
    try:
        with urlopen(req, timeout=20) as resp:
            return json.loads(resp.read().decode("utf-8"))
    except URLError as e:
        raise HTTPException(status_code=502, detail=f"Failed to fetch GOV.UK Content API: {e}")


_BEATRICE_SCRIPT = """
<script>
(function () {
  var _markers = {};
  var _activeId = null;

  window.addEventListener('message', function (e) {
    if (!e.data || e.data.type !== 'BEATRICE_PROPOSITIONS') return;
    inject(e.data.propositions);
  });

  document.addEventListener('click', function () {
    window.parent.postMessage({ type: 'BEATRICE_PAGE_CLICK' }, '*');
  });

  var _proxyOrigin = window.location.origin;
  document.addEventListener('click', function (e) {
    var a = e.target.closest('a');
    if (!a) return;
    var href = a.href;
    if (!href) return;
    if ((href.startsWith('https://www.gov.uk') || href.startsWith('http://www.gov.uk')) &&
        href !== window.location.href.split('#')[0]) {
      e.preventDefault();
      window.location.href = _proxyOrigin + '/proxy?url=' + encodeURIComponent(href);
    }
  });

  window.addEventListener('scroll', function () {
    if (!_activeId || !_markers[_activeId]) return;
    var r = _markers[_activeId].getBoundingClientRect();
    window.parent.postMessage(
      { type: 'BEATRICE_MARKER_MOVE', gpId: _activeId,
        rect: { top: r.top, left: r.left, bottom: r.bottom, right: r.right } },
      '*'
    );
  }, { passive: true });

  function highlightColour(status) {
    if (status === 'green')  return 'rgba(34,197,94,0.30)';
    if (status === 'red')    return 'rgba(239,68,68,0.25)';
    if (status === 'amber')  return 'rgba(251,191,36,0.30)';
    return 'rgba(156,163,175,0.20)';
  }

  function highlightText(target, gp, searchText) {
    var colour = highlightColour(gp.status);
    var normSearch = (searchText || '').replace(/\s+/g, ' ').trim();
    var key = normSearch.substring(0, 60).toLowerCase();

    // Collect all text nodes and build a combined string with their offsets
    var walker = document.createTreeWalker(target, NodeFilter.SHOW_TEXT, null, false);
    var textNodes = [];
    var node;
    while ((node = walker.nextNode())) { textNodes.push(node); }

    var combined = '';
    var offsets = [];
    for (var i = 0; i < textNodes.length; i++) {
      var t = (textNodes[i].textContent || '').replace(/\s+/g, ' ');
      offsets.push({ node: textNodes[i], start: combined.length, end: combined.length + textNodes[i].textContent.length });
      combined += t;
    }

    var startIdx = combined.toLowerCase().indexOf(key);
    if (startIdx === -1) {
      target.style.backgroundColor = colour;
      target.setAttribute('data-beatrice-highlight', gp.id);
      return;
    }
    var endIdx = Math.min(startIdx + searchText.length, combined.length);

    // Wrap each text node segment that falls within [startIdx, endIdx]
    var highlighted = false;
    for (var j = 0; j < offsets.length; j++) {
      var o = offsets[j];
      if (o.end <= startIdx || o.start >= endIdx) continue;
      var nodeStart = Math.max(0, startIdx - o.start);
      var nodeEnd = Math.min(o.node.textContent.length, endIdx - o.start);
      try {
        var range = document.createRange();
        range.setStart(o.node, nodeStart);
        range.setEnd(o.node, nodeEnd);
        var span = document.createElement('span');
        span.setAttribute('data-beatrice-highlight', gp.id);
        span.style.cssText = 'background:' + colour + ';border-radius:2px;';
        range.surroundContents(span);
        highlighted = true;
      } catch (_) { /* node already wrapped or other issue — skip segment */ }
    }
    if (!highlighted) {
      target.style.backgroundColor = colour;
      target.setAttribute('data-beatrice-highlight', gp.id);
    }
  }

  function extractSectionId(locator) {
    if (!locator) return null;
    var parts = locator.split(':section:');
    return parts.length > 1 ? parts[parts.length - 1] : null;
  }

  function getSectionCandidates(sectionId) {
    if (!sectionId) return Array.from(document.querySelectorAll('p, li, td'));
    var h2 = document.getElementById(sectionId);
    if (!h2) return Array.from(document.querySelectorAll('p, li, td'));
    var candidates = [];
    var el = h2.nextElementSibling;
    while (el && el.tagName !== 'H2') {
      var inner = Array.from(el.querySelectorAll('p, li, td'));
      if (inner.length) {
        candidates = candidates.concat(inner);
      } else if (/^(P|LI|TD)$/.test(el.tagName)) {
        candidates.push(el);
      }
      el = el.nextElementSibling;
    }
    return candidates.length ? candidates : Array.from(document.querySelectorAll('p, li, td'));
  }

  function inject(propositions) {
    // Remove previous highlights and markers
    document.querySelectorAll('[data-beatrice-highlight]').forEach(function (el) {
      if (el.tagName === 'SPAN') {
        var parent = el.parentNode;
        while (el.firstChild) parent.insertBefore(el.firstChild, el);
        parent.removeChild(el);
      } else {
        el.style.backgroundColor = '';
        el.removeAttribute('data-beatrice-highlight');
      }
    });
    Object.values(_markers).forEach(function (b) { b.remove(); });
    _markers = {};

    function normTxt(s) { return (s || '').replace(/\\s+/g, ' ').trim().toLowerCase(); }

    propositions.forEach(function (gp) {
      var sectionId = extractSectionId(gp.section_locator);
      var candidates = getSectionCandidates(sectionId);

      var paras = (gp.source_paragraphs && gp.source_paragraphs.length)
        ? gp.source_paragraphs
        : [gp.text];

      console.log('[BEATRICE]', gp.id, 'source_paragraphs:', gp.source_paragraphs, 'candidates:', candidates.length);

      var targets = [];
      paras.forEach(function (para) {
        var key = normTxt(para).substring(0, 60);
        for (var i = 0; i < candidates.length; i++) {
          if (normTxt(candidates[i].textContent).indexOf(key) !== -1) {
            targets.push({ el: candidates[i], text: para }); break;
          }
        }
      });
      console.log('[BEATRICE]', gp.id, 'matched targets:', targets.length);
      if (!targets.length) return;

      targets.forEach(function (t) { highlightText(t.el, gp, t.text); });
      var target = targets[0].el;

      var btnColour = gp.status === 'green'
        ? 'background:#dcfce7;color:#166534;border-color:#86efac;'
        : gp.status === 'red'
          ? 'background:#fee2e2;color:#991b1b;border-color:#fca5a5;'
          : gp.status === 'amber'
            ? 'background:#fef3c7;color:#92400e;border-color:#fcd34d;'
          : 'background:#f3f4f6;color:#6b7280;border-color:#d1d5db;';
      var btn = document.createElement('button');
      btn.textContent = gp.count > 0 ? String(gp.count) : '?';
      btn.style.cssText = 'display:inline-flex;align-items:center;justify-content:center;'
        + 'width:20px;height:20px;border-radius:50%;font-size:10px;font-weight:700;'
        + 'cursor:pointer;margin-left:4px;vertical-align:middle;border:1.5px solid;' + btnColour;
      btn.title = gp.text;
      btn.setAttribute('data-beatrice-marker', gp.id);

      btn.addEventListener('click', function (e) {
        e.stopPropagation();
        _activeId = gp.id;
        var r = btn.getBoundingClientRect();
        window.parent.postMessage(
          { type: 'BEATRICE_MARKER_CLICK', gpId: gp.id,
            rect: { top: r.top, left: r.left, bottom: r.bottom, right: r.right } },
          '*'
        );
      });

      target.appendChild(btn);
      _markers[gp.id] = btn;
    });
  }
})();
</script>
"""


@app.get("/proxy")
def proxy_page(url: str) -> HTMLResponse:
    """Fetch the real GOV.UK page and serve it without X-Frame-Options so it can be embedded.
    Injects a <base> tag for relative URLs and a postMessage bridge for proposition markers."""
    req = Request(
        url,
        headers={"User-Agent": "Mozilla/5.0", "Accept": "text/html,application/xhtml+xml"},
    )
    try:
        with urlopen(req, timeout=20) as resp:
            html = resp.read().decode("utf-8", errors="replace")
    except URLError as e:
        raise HTTPException(status_code=502, detail=f"Failed to fetch page: {e}")

    base_tag = '<base href="https://www.gov.uk/">'
    if "<head>" in html:
        html = html.replace("<head>", f"<head>{base_tag}", 1)
    else:
        html = base_tag + html

    if "</body>" in html:
        html = html.replace("</body>", f"{_BEATRICE_SCRIPT}</body>", 1)
    else:
        html += _BEATRICE_SCRIPT

    return HTMLResponse(content=html)


@app.get("/content")
def get_content(url: str) -> dict[str, str]:
    """Return the raw HTML body from the GOV.UK Content API for rendering in the overlay."""
    api_url = _content_api_url(url)
    data = _fetch_content_api(api_url)
    html = data.get("details", {}).get("body", "")
    title = data.get("title", "")
    return {"html": html, "title": title}


# ── Request / Response models ─────────────────────────────────────────────────

class ExtractRequest(BaseModel):
    url: str
    section: str | None = None
    method: str = "heuristic"  # "heuristic" | "llm"


class EmbedLawRequest(BaseModel):
    propositions: list[dict]
    filename: str


class MatchRequest(BaseModel):
    guidance_propositions: list[dict]
    law_file_hash: str
    top_k: int = 3


class MatchEntry(BaseModel):
    law_proposition: dict
    similarity_score: float
    bert_score_f1: float = 0.0


class ClassifyRequest(BaseModel):
    guidance_proposition: dict
    matches: list[MatchEntry]


class SummariseRequest(BaseModel):
    guidance_proposition: dict
    classified_matches: list[dict]


# ── Endpoints ─────────────────────────────────────────────────────────────────

@app.get("/health")
def health() -> dict:
    return {"status": "ok"}


@app.post("/embed-law")
def embed_law(req: EmbedLawRequest) -> dict:
    """Embed a law propositions file and cache by content hash. Returns immediately if already cached."""
    file_hash = hashlib.sha256(
        json.dumps(req.propositions, sort_keys=True).encode()
    ).hexdigest()[:16]

    if file_hash in _law_store:
        entry = _law_store[file_hash]
        return {"file_hash": file_hash, "filename": entry.filename, "proposition_count": entry.proposition_count, "cached": True}

    try:
        law_propositions = [Proposition.model_validate(p) for p in req.propositions]
    except Exception as e:
        raise HTTPException(status_code=422, detail=f"Invalid proposition data: {e}")

    client = _llm_client()
    embeddings = embed_texts([lp.proposition_text for lp in law_propositions], client)

    _law_store[file_hash] = LawEmbeddingCache(
        file_hash=file_hash,
        filename=req.filename,
        proposition_count=len(law_propositions),
        created_at=datetime.now(UTC),
        propositions_by_id={lp.id: lp.model_dump() for lp in law_propositions},
        law_index=[(lp.id, emb) for lp, emb in zip(law_propositions, embeddings, strict=False)],
    )
    _save_cache()
    return {"file_hash": file_hash, "filename": req.filename, "proposition_count": len(law_propositions), "cached": False}


@app.get("/embedded-laws")
def embedded_laws() -> list[dict]:
    """List all cached law proposition sets."""
    return [
        {
            "file_hash": e.file_hash,
            "filename": e.filename,
            "proposition_count": e.proposition_count,
            "created_at": e.created_at.isoformat(),
        }
        for e in _law_store.values()
    ]


@app.delete("/embedded-laws/{file_hash}")
def delete_embedded_law(file_hash: str) -> dict:
    """Remove a cached law proposition set and update the on-disk cache."""
    if file_hash not in _law_store:
        raise HTTPException(status_code=404, detail=f"No cached embeddings found for hash '{file_hash}'.")
    del _law_store[file_hash]
    _save_cache()
    return {"deleted": file_hash}


@app.post("/extract")
def extract(req: ExtractRequest) -> dict:
    """Fetch GOV.UK Content API JSON and extract guidance propositions. Results are cached by URL+section+method."""
    cache_key = hashlib.sha256(
        f"{req.url}:{req.section or ''}:{req.method}".encode()
    ).hexdigest()

    if cache_key in _extract_cache:
        print(
            f"[extract] cache hit for {req.url} (section={req.section}, method={req.method})",
            file=__import__("sys").stderr,
        )
        return {"propositions": _extract_cache[cache_key], "cached": True}

    print(
        f"[extract] extracting fresh for {req.url} (section={req.section}, method={req.method})",
        file=__import__("sys").stderr,
    )

    api_url = _content_api_url(req.url)
    data = _fetch_content_api(api_url)

    fragments = parse_content_api_response(data, section_filter=req.section)
    if not fragments:
        raise HTTPException(status_code=404, detail="No sections found for the given URL/section.")

    propositions = extract_propositions(
        fragments=fragments,
        topic="GOV.UK guidance",
        source_url=req.url,
        limit=100,
        llm_client=_llm_client() if req.method == "llm" else None,
    )
    result = [p.model_dump() for p in propositions]
    _extract_cache[cache_key] = result
    _save_extract_cache()
    return {"propositions": result, "cached": False}


@app.post("/match")
def match(req: MatchRequest) -> list[dict]:
    """Embed guidance propositions and return top-k nearest law matches using cached law embeddings."""
    cached = _law_store.get(req.law_file_hash)
    if not cached:
        raise HTTPException(
            status_code=404,
            detail=f"Law embeddings not found for hash '{req.law_file_hash}'. Re-upload the file.",
        )

    try:
        guidance_propositions = [GuidanceProposition.model_validate(p) for p in req.guidance_propositions]
    except Exception as e:
        raise HTTPException(status_code=422, detail=f"Invalid guidance proposition data: {e}")

    if not guidance_propositions:
        return []

    client = _llm_client()
    guidance_embeddings = embed_texts([gp.proposition_text for gp in guidance_propositions], client)

    results = []
    for gp, g_emb in zip(guidance_propositions, guidance_embeddings, strict=False):
        candidates = find_candidates(g_emb, cached.law_index, top_k=req.top_k, threshold=0.0)
        law_texts = [cached.propositions_by_id[law_id]["proposition_text"] for law_id, _ in candidates]
        bert_scores = compute_bert_scores([(gp.proposition_text, lt) for lt in law_texts])
        results.append({
            "guidance_proposition_id": gp.id,
            "matches": [
                {
                    "law_proposition": cached.propositions_by_id[law_id],
                    "similarity_score": score,
                    "bert_score_f1": bert_scores[i],
                }
                for i, (law_id, score) in enumerate(candidates)
            ],
        })
    return results


@app.post("/summarise")
def summarise(req: SummariseRequest) -> dict:
    """LLM-generated summary of how a guidance proposition relates to its classified law matches."""
    try:
        gp = GuidanceProposition.model_validate(req.guidance_proposition)
    except Exception as e:
        raise HTTPException(status_code=422, detail=f"Invalid guidance proposition: {e}")

    matches_text = "\n\n".join(
        f"- Relationship: {m.get('relationship')}\n"
        f"  Law: {m.get('law_proposition', {}).get('proposition_text', '')}\n"
        f"  Citation: {m.get('law_proposition', {}).get('article_reference', '—')}\n"
        f"  Explanation: {m.get('explanation', '')}"
        for m in req.classified_matches
    )

    prompt = f"""You are summarising how a piece of GOV.UK guidance relates to relevant law propositions.

GUIDANCE PROPOSITION
Source: {gp.source_url}
Section: {gp.section_locator}
Text: {gp.proposition_text}

RELEVANT LAW MATCHES
{matches_text if matches_text else "No relevant law matches found."}

Write a concise 2-3 sentence summary (maximum 100 words) of how the guidance aligns with (or diverges from) the relevant law. Focus on practical implications for compliance."""

    cache_key = hashlib.sha256(prompt.encode()).hexdigest()

    with _summarise_cache_lock:
        cached_summary = _summarise_cache.get(cache_key)

    if cached_summary is not None:
        print(f"[summarise] cache hit for {gp.id}", file=__import__("sys").stderr)
        return {"summary": cached_summary, "cached": True}

    print(f"[summarise] LLM call for {gp.id}", file=__import__("sys").stderr)
    summary = _llm_client().guidance_summarise_text(prompt)

    with _summarise_cache_lock:
        _summarise_cache[cache_key] = summary
        _save_summarise_cache()

    return {"summary": summary, "cached": False}


@app.post("/classify")
def classify(req: ClassifyRequest) -> list[dict]:
    """LLM-classify the relationship between a guidance proposition and its matched law propositions."""
    try:
        gp = GuidanceProposition.model_validate(req.guidance_proposition)
    except Exception as e:
        raise HTTPException(status_code=422, detail=f"Invalid guidance proposition: {e}")

    try:
        law_propositions = [Proposition.model_validate(entry.law_proposition) for entry in req.matches]
    except Exception as e:
        raise HTTPException(status_code=422, detail=f"Invalid law proposition: {e}")

    client = _llm_client()
    results = []
    for entry, lp in zip(req.matches, law_propositions, strict=False):
        result = classify_match(
            guidance_prop=gp,
            law_prop=lp,
            similarity_score=entry.similarity_score,
            llm_client=client,
            bert_score_f1=entry.bert_score_f1,
        )
        results.append({
            "law_proposition": lp.model_dump(),
            "similarity_score": entry.similarity_score,
            "bert_score_f1": entry.bert_score_f1,
            "relationship": result.relationship,
            "confidence": result.confidence,
            "explanation": result.explanation,
            "correctness_score": result.correctness_score,
            "classify_cached": result.classify_cached,
        })
    return results
