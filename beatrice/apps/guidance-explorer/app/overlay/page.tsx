"use client";

import { Suspense, useCallback, useEffect, useRef, useState, Fragment } from "react";
import { useSearchParams } from "next/navigation";

const API_BASE = process.env.NEXT_PUBLIC_GUIDANCE_API_BASE ?? "http://127.0.0.1:8011";

// ── Types ─────────────────────────────────────────────────────────────────────

interface GuidanceProposition {
  id: string;
  proposition_text: string;
  section_locator: string;
  source_paragraphs?: string[];
}

interface LawProposition {
  id: string;
  proposition_text: string;
  article_reference?: string;
}

interface MatchEntry {
  law_proposition: LawProposition;
  similarity_score: number;
}

interface ClassifiedEntry extends MatchEntry {
  relationship: string;
  confidence: string;
  explanation: string;
}

interface StoredData {
  url: string;
  propositions: GuidanceProposition[];
  matches: Record<string, MatchEntry[]>;
  classifications: Record<string, ClassifiedEntry[]>;
  summaries: Record<string, string>;
}

interface MarkerRect {
  top: number;
  left: number;
  bottom: number;
  right: number;
}

interface TooltipState {
  gpId: string;
  rect: MarkerRect;
  entries: ClassifiedEntry[];
  gpText: string;
  summary: string | null;
}

// ── Relationship colours ──────────────────────────────────────────────────────

const RELATIONSHIP_COLOURS: Record<string, string> = {
  confirmed: "bg-green-100 text-green-800 border-green-200",
  outdated: "bg-yellow-100 text-yellow-800 border-yellow-200",
  "guidance omits detail": "bg-blue-100 text-blue-800 border-blue-200",
  "guidance contains additional detail": "bg-purple-100 text-purple-800 border-purple-200",
  contradicts: "bg-red-100 text-red-800 border-red-200",
  "does not match": "bg-gray-100 text-gray-700 border-gray-200",
};

// ── Tooltip ───────────────────────────────────────────────────────────────────

function Tooltip({ state, onClose }: { state: TooltipState; onClose: () => void }): JSX.Element {
  const ref = useRef<HTMLDivElement>(null);
  const [pos, setPos] = useState<React.CSSProperties>({});
  const [expanded, setExpanded] = useState(false);

  // Position relative to the marker rect (viewport coords from the iframe)
  function computePos(rect: MarkerRect): React.CSSProperties {
    const tipHeight = ref.current?.offsetHeight ?? 320;
    const showAbove = window.innerHeight - rect.bottom < tipHeight + 12;
    const left = Math.min(rect.left, window.innerWidth - 408);
    return showAbove
      ? { top: rect.top - tipHeight - 6, left }
      : { top: rect.bottom + 6, left };
  }

  useEffect(() => {
    setPos(computePos(state.rect));
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [state.rect]);

  useEffect(() => {
    function handler(e: MouseEvent) {
      if (ref.current && !ref.current.contains(e.target as Node)) onClose();
    }
    document.addEventListener("click", handler);
    return () => document.removeEventListener("click", handler);
  }, [onClose]);

  // Reset expanded state when tooltip switches to a different proposition
  useEffect(() => { setExpanded(false); }, [state.gpId]);

  return (
    <div
      ref={ref}
      className="fixed z-50 w-96 rounded-lg border border-border bg-card p-3 shadow-xl"
      style={pos}
      onClick={(e) => e.stopPropagation()}
    >
      {state.summary ? (
        <Fragment>
          <p className="text-[11px] leading-relaxed text-foreground">{state.summary}</p>
          {state.entries.length > 0 && (
            <Fragment>
              <button
                type="button"
                onClick={() => setExpanded((v) => !v)}
                className="mt-2 text-[11px] text-blue-600 hover:underline"
              >
                {expanded ? "Hide law matches" : `Show law matches (${state.entries.length})`}
              </button>
              {expanded && (
                <div className="mt-2 space-y-2">
                  {state.entries.map((e, i) => (
                    <div key={i} className="rounded border border-border bg-muted/40 p-2">
                      <div className="mb-1 flex items-center gap-2">
                        <span
                          className={`rounded border px-1.5 py-0.5 text-[10px] font-semibold uppercase tracking-wide ${RELATIONSHIP_COLOURS[e.relationship] ?? "bg-gray-100 text-gray-700 border-gray-200"}`}
                        >
                          {e.relationship}
                        </span>
                      </div>
                      <p className="text-[11px] leading-relaxed text-foreground line-clamp-3">
                        {e.law_proposition.proposition_text}
                      </p>
                      {e.law_proposition.article_reference && (
                        <p className="mt-0.5 text-[10px] text-muted-foreground">
                          {e.law_proposition.article_reference}
                        </p>
                      )}
                      {e.explanation && (
                        <p className="mt-1 text-[10px] italic text-muted-foreground">{e.explanation}</p>
                      )}
                    </div>
                  ))}
                </div>
              )}
            </Fragment>
          )}
        </Fragment>
      ) : state.entries.length === 0 ? (
        <p className="text-xs text-muted-foreground">No relevant law matches.</p>
      ) : (
        <div className="space-y-2">
          {state.entries.map((e, i) => (
            <div key={i} className="rounded border border-border bg-muted/40 p-2">
              <div className="mb-1 flex items-center gap-2">
                <span
                  className={`rounded border px-1.5 py-0.5 text-[10px] font-semibold uppercase tracking-wide ${RELATIONSHIP_COLOURS[e.relationship] ?? "bg-gray-100 text-gray-700 border-gray-200"}`}
                >
                  {e.relationship}
                </span>
              </div>
              <p className="text-[11px] leading-relaxed text-foreground line-clamp-3">
                {e.law_proposition.proposition_text}
              </p>
              {e.law_proposition.article_reference && (
                <p className="mt-0.5 text-[10px] text-muted-foreground">
                  {e.law_proposition.article_reference}
                </p>
              )}
              {e.explanation && (
                <p className="mt-1 text-[10px] italic text-muted-foreground">{e.explanation}</p>
              )}
            </div>
          ))}
        </div>
      )}
    </div>
  );
}

// ── Inner component ───────────────────────────────────────────────────────────

function OverlayInner(): JSX.Element {
  const searchParams = useSearchParams();
  const pageUrl = searchParams.get("url") ?? "";

  const [data, setData] = useState<StoredData | null>(null);
  const [tooltip, setTooltip] = useState<TooltipState | null>(null);
  const iframeRef = useRef<HTMLIFrameElement>(null);
  const dataRef = useRef<StoredData | null>(null);

  useEffect(() => {
    const raw = sessionStorage.getItem("guidance-explorer-data");
    if (raw) {
      try {
        const parsed = JSON.parse(raw) as StoredData;
        setData(parsed);
        dataRef.current = parsed;
      } catch {
        // ignore
      }
    }
  }, []);

  // Build the proposition list to send to the iframe
  function buildPayload(d: StoredData) {
    return d.propositions.map((gp) => {
      const gpMatches = d.matches[gp.id] ?? [];
      const classified = d.classifications[gp.id] ?? [];
      const nonUngrounded = classified.filter((c) => c.relationship !== "does not match");
      const allConfirmed = nonUngrounded.length > 0 && nonUngrounded.every((c) => c.relationship === "confirmed");
      const allContradicts = nonUngrounded.length > 0 && nonUngrounded.every((c) => c.relationship === "contradicts");
      const status =
        nonUngrounded.length === 0 ? "none"
        : allConfirmed ? "green"
        : allContradicts ? "red"
        : "amber";
      return { id: gp.id, text: gp.proposition_text, source_paragraphs: gp.source_paragraphs ?? [], section_locator: gp.section_locator, count: nonUngrounded.length, status };
    });
  }

  // Send propositions to iframe once it has loaded
  function handleIframeLoad() {
    const d = dataRef.current;
    if (!d || !iframeRef.current?.contentWindow) return;
    iframeRef.current.contentWindow.postMessage(
      { type: "BEATRICE_PROPOSITIONS", propositions: buildPayload(d) },
      "*",
    );
  }

  // Also re-send if data arrives after iframe already loaded
  useEffect(() => {
    if (!data || !iframeRef.current?.contentWindow) return;
    iframeRef.current.contentWindow.postMessage(
      { type: "BEATRICE_PROPOSITIONS", propositions: buildPayload(data) },
      "*",
    );
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [data]);

  const closeTooltip = useCallback(() => setTooltip(null), []);

  // Listen for messages from the iframe
  useEffect(() => {
    function onMessage(e: MessageEvent) {
      const d = dataRef.current;
      if (!d) return;

      if (e.data?.type === "BEATRICE_MARKER_CLICK") {
        const { gpId, rect } = e.data as { gpId: string; rect: MarkerRect };
        // Toggle off if same marker
        if (tooltip?.gpId === gpId) { setTooltip(null); return; }
        const gp = d.propositions.find((p) => p.id === gpId);
        if (!gp) return;
        const classified = d.classifications[gpId] ?? [];
        const entries = classified.filter((c) => c.relationship !== "does not match");
        const summary = d.summaries?.[gpId] ?? null;
        setTooltip({ gpId, rect, entries, gpText: gp.proposition_text, summary });
      }

      if (e.data?.type === "BEATRICE_PAGE_CLICK") {
        setTooltip(null);
        return;
      }

      if (e.data?.type === "BEATRICE_MARKER_MOVE") {
        const { gpId, rect } = e.data as { gpId: string; rect: MarkerRect };
        setTooltip((prev) => (prev?.gpId === gpId ? { ...prev, rect } : prev));
      }
    }

    window.addEventListener("message", onMessage);
    return () => window.removeEventListener("message", onMessage);
  }, [tooltip?.gpId]);

  return (
    <div className="relative h-screen w-full overflow-hidden">
      <iframe
        ref={iframeRef}
        src={pageUrl ? `${API_BASE}/proxy?url=${encodeURIComponent(pageUrl)}` : undefined}
        className="h-full w-full border-0"
        title="GOV.UK guidance page"
        onLoad={handleIframeLoad}
      />
      {tooltip && <Tooltip state={tooltip} onClose={closeTooltip} />}
    </div>
  );
}

// ── Page export ───────────────────────────────────────────────────────────────

export default function OverlayPage(): JSX.Element {
  return (
    <Suspense fallback={<div className="p-8 text-sm text-muted-foreground">Loading…</div>}>
      <OverlayInner />
    </Suspense>
  );
}
