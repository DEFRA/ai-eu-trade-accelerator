"use client";

import { useEffect, useRef, useState } from "react";

const API_BASE = process.env.NEXT_PUBLIC_GUIDANCE_API_BASE ?? "http://127.0.0.1:8011";

// ── Types ─────────────────────────────────────────────────────────────────────

interface GuidanceProposition {
  id: string;
  section_locator: string;
  proposition_text: string;
  legal_subject: string;
  action: string;
  conditions: string[];
  required_documents: string[];
  source_url: string;
  extraction_method: string;
}

interface LawProposition {
  id: string;
  proposition_text: string;
  article_reference?: string;
  jurisdiction?: string;
  label?: string;
}

interface MatchEntry {
  law_proposition: LawProposition;
  similarity_score: number;
  bert_score_f1: number;
}

interface ClassifiedEntry extends MatchEntry {
  relationship: string;
  confidence: string;
  explanation: string;
  correctness_score: number;
  classify_cached: boolean;
}

interface EmbeddedFileInfo {
  file_hash: string;
  filename: string;
  proposition_count: number;
  created_at: string;
}

// ── Helpers ───────────────────────────────────────────────────────────────────

const RELATIONSHIP_COLOURS: Record<string, string> = {
  confirmed: "bg-green-100 text-green-800 border-green-200",
  outdated: "bg-yellow-100 text-yellow-800 border-yellow-200",
  "guidance omits detail": "bg-blue-100 text-blue-800 border-blue-200",
  "guidance contains additional detail": "bg-purple-100 text-purple-800 border-purple-200",
  contradicts: "bg-red-100 text-red-800 border-red-200",
  "does not match": "bg-gray-100 text-gray-700 border-gray-200",
};

function scoreBg(score: number): string {
  // Interpolate hue: 0 = red (0°), 0.5 = amber (45°), 1 = green (120°)
  const hue = score <= 0.5
    ? score * 2 * 45
    : 45 + (score - 0.5) * 2 * 75;
  return `hsl(${hue.toFixed(0)}, 90%, 72%)`;
}

function RelationshipBadge({ value }: { value: string }): JSX.Element {
  const cls = RELATIONSHIP_COLOURS[value] ?? RELATIONSHIP_COLOURS["does not match"];
  return (
    <span className={`inline-block rounded border px-2 py-0.5 text-[11px] font-semibold uppercase tracking-wide ${cls}`}>
      {value}
    </span>
  );
}

function ConfidenceDot({ value }: { value: string }): JSX.Element {
  const colours: Record<string, string> = {
    high: "text-green-600",
    medium: "text-yellow-600",
    low: "text-red-500",
  };
  return (
    <span className={`text-xs font-medium ${colours[value] ?? "text-gray-500"}`}>
      {value} confidence
    </span>
  );
}

// ── Main page ─────────────────────────────────────────────────────────────────

export default function GuidanceExplorerPage(): JSX.Element {
  const [url, setUrl] = useState("");
  const [section, setSection] = useState("");
  const [topK, setTopK] = useState(3);
  const [extractMethod, setExtractMethod] = useState<"heuristic" | "llm">("heuristic");
  const [lawFileHash, setLawFileHash] = useState<string | null>(null);
  const [embeddedFiles, setEmbeddedFiles] = useState<EmbeddedFileInfo[]>([]);
  const [embedding, setEmbedding] = useState(false);
  const [guidancePropositions, setGuidancePropositions] = useState<GuidanceProposition[]>([]);
  const [extractedFromCache, setExtractedFromCache] = useState<boolean | null>(null);
  const [extracting, setExtracting] = useState(false);
  const [extractError, setExtractError] = useState<string | null>(null);

  const [matches, setMatches] = useState<Record<string, MatchEntry[]>>({});
  const [classifications, setClassifications] = useState<Record<string, ClassifiedEntry[]>>({});
  const [summaries, setSummaries] = useState<Record<string, string>>({});
  const [summaryCached, setSummaryCached] = useState<Record<string, boolean>>({});
  const [loading, setLoading] = useState<Record<string, "matching" | "classifying" | "summarising" | null>>({});
  const [runningAll, setRunningAll] = useState(false);
  const [sortBy, setSortBy] = useState<"similarity" | "bert">("bert");
  const [classifyTopK, setClassifyTopK] = useState(3);

  const fileInputRef = useRef<HTMLInputElement>(null);

  // ── Load existing embedded files on mount ─────────────────────────────────

  useEffect(() => {
    fetch(`${API_BASE}/embedded-laws`)
      .then((r) => r.json())
      .then((data: EmbeddedFileInfo[]) => {
        setEmbeddedFiles(data);
        if (data.length > 0 && !lawFileHash) {
          setLawFileHash(data[data.length - 1].file_hash);
        }
      })
      .catch(() => {/* server not yet running */});
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  function handleViewOverlay(): void {
    sessionStorage.setItem(
      "guidance-explorer-data",
      JSON.stringify({ url, propositions: guidancePropositions, matches, classifications, summaries }),
    );
    window.open(`/overlay?url=${encodeURIComponent(url)}`, "_blank");
  }

  // ── File upload → embed ───────────────────────────────────────────────────

  function handleFileChange(e: React.ChangeEvent<HTMLInputElement>): void {
    const file = e.target.files?.[0];
    if (!file) return;
    const reader = new FileReader();
    reader.onload = async (ev) => {
      try {
        const raw = JSON.parse(ev.target?.result as string);
        const propositions: LawProposition[] = Array.isArray(raw)
          ? raw
          : Array.isArray(raw?.propositions)
            ? raw.propositions
            : [];
        if (!propositions.length) {
          alert("No propositions found in file.");
          return;
        }
        setEmbedding(true);
        const res = await fetch(`${API_BASE}/embed-law`, {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ propositions, filename: file.name }),
        });
        if (!res.ok) {
          const detail = await res.json().catch(() => ({ detail: res.statusText }));
          throw new Error(detail?.detail ?? res.statusText);
        }
        const result: { file_hash: string; filename: string; proposition_count: number; cached: boolean } =
          await res.json();
        setLawFileHash(result.file_hash);
        // Refresh list
        const listRes = await fetch(`${API_BASE}/embedded-laws`);
        if (listRes.ok) setEmbeddedFiles(await listRes.json());
      } catch (err) {
        alert(`Embed failed: ${err instanceof Error ? err.message : String(err)}`);
      } finally {
        setEmbedding(false);
        // Reset input so same file can be re-uploaded
        if (fileInputRef.current) fileInputRef.current.value = "";
      }
    };
    reader.readAsText(file);
  }

  // ── Extract ──────────────────────────────────────────────────────────────────

  async function handleExtract(): Promise<void> {
    if (!url.trim()) return;
    setExtracting(true);
    setExtractError(null);
    setGuidancePropositions([]);
    setExtractedFromCache(null);
    setMatches({});
    setClassifications({});
    setSummaries({});
    setSummaryCached({});
    try {
      const res = await fetch(`${API_BASE}/extract`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ url: url.trim(), section: section.trim() || null, method: extractMethod }),
      });
      if (!res.ok) {
        const detail = await res.json().catch(() => ({ detail: res.statusText }));
        throw new Error(detail?.detail ?? res.statusText);
      }
      const data: { propositions: GuidanceProposition[]; cached: boolean } = await res.json();
      setGuidancePropositions(data.propositions);
      setExtractedFromCache(data.cached);
      if (data.propositions.length === 0) {
        setExtractError("No propositions found. Try removing the section filter or check the URL.");
      }
    } catch (err) {
      setExtractError(err instanceof Error ? err.message : String(err));
    } finally {
      setExtracting(false);
    }
  }

  // ── Match ────────────────────────────────────────────────────────────────────

  async function handleDeleteEmbedding(fileHash: string): Promise<void> {
    const res = await fetch(`${API_BASE}/embedded-laws/${fileHash}`, { method: "DELETE" });
    if (!res.ok) return;
    if (lawFileHash === fileHash) setLawFileHash(null);
    setEmbeddedFiles((prev) => prev.filter((f) => f.file_hash !== fileHash));
  }

  async function handleMatch(gp: GuidanceProposition): Promise<void> {
    if (!lawFileHash) {
      alert("Upload a law propositions file first.");
      return;
    }
    setLoading((prev) => ({ ...prev, [gp.id]: "matching" }));
    try {
      const res = await fetch(`${API_BASE}/match`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          guidance_propositions: [gp],
          law_file_hash: lawFileHash,
          top_k: topK,
        }),
      });
      if (!res.ok) {
        const detail = await res.json().catch(() => ({ detail: res.statusText }));
        throw new Error(detail?.detail ?? res.statusText);
      }
      const data: Array<{ guidance_proposition_id: string; matches: MatchEntry[] }> =
        await res.json();
      const entry = data.find((d) => d.guidance_proposition_id === gp.id);
      setMatches((prev) => ({ ...prev, [gp.id]: entry?.matches ?? [] }));
      // Clear stale classifications
      setClassifications((prev) => {
        const next = { ...prev };
        delete next[gp.id];
        return next;
      });
    } catch (err) {
      alert(`Match failed: ${err instanceof Error ? err.message : String(err)}`);
    } finally {
      setLoading((prev) => ({ ...prev, [gp.id]: null }));
    }
  }

  // ── Classify ─────────────────────────────────────────────────────────────────

  function topMatchesForClassify(gpMatches: MatchEntry[]): MatchEntry[] {
    return [...gpMatches]
      .sort((a, b) =>
        sortBy === "bert"
          ? b.bert_score_f1 - a.bert_score_f1
          : b.similarity_score - a.similarity_score
      )
      .slice(0, classifyTopK);
  }

  async function handleClassify(gp: GuidanceProposition): Promise<void> {
    const gpMatches = matches[gp.id];
    if (!gpMatches?.length) return;
    setLoading((prev) => ({ ...prev, [gp.id]: "classifying" }));
    try {
      const res = await fetch(`${API_BASE}/classify`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          guidance_proposition: gp,
          matches: topMatchesForClassify(gpMatches),
        }),
      });
      if (!res.ok) {
        const detail = await res.json().catch(() => ({ detail: res.statusText }));
        throw new Error(detail?.detail ?? res.statusText);
      }
      const data: ClassifiedEntry[] = await res.json();
      setClassifications((prev) => ({ ...prev, [gp.id]: data }));
    } catch (err) {
      alert(`Classify failed: ${err instanceof Error ? err.message : String(err)}`);
    } finally {
      setLoading((prev) => ({ ...prev, [gp.id]: null }));
    }
  }

  // ── Summarise ────────────────────────────────────────────────────────────────

  async function handleSummarise(gp: GuidanceProposition): Promise<void> {
    const gpClassified = classifications[gp.id];
    const relevant = gpClassified?.filter((c) => c.relationship !== "does not match") ?? [];
    if (!relevant.length) return;
    setLoading((prev) => ({ ...prev, [gp.id]: "summarising" }));
    try {
      const res = await fetch(`${API_BASE}/summarise`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ guidance_proposition: gp, classified_matches: relevant }),
      });
      if (!res.ok) {
        const detail = await res.json().catch(() => ({ detail: res.statusText }));
        throw new Error(detail?.detail ?? res.statusText);
      }
      const data: { summary: string; cached: boolean } = await res.json();
      setSummaries((prev) => ({ ...prev, [gp.id]: data.summary }));
      setSummaryCached((prev) => ({ ...prev, [gp.id]: data.cached }));
    } catch (err) {
      alert(`Summarise failed: ${err instanceof Error ? err.message : String(err)}`);
    } finally {
      setLoading((prev) => ({ ...prev, [gp.id]: null }));
    }
  }

  // ── Run all ───────────────────────────────────────────────────────────────────

  async function handleRunAll(): Promise<void> {
    if (!lawFileHash || !guidancePropositions.length) return;
    setRunningAll(true);
    for (const gp of guidancePropositions) {
      // Match
      setLoading((prev) => ({ ...prev, [gp.id]: "matching" }));
      let gpMatches: MatchEntry[] = [];
      try {
        const res = await fetch(`${API_BASE}/match`, {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ guidance_propositions: [gp], law_file_hash: lawFileHash, top_k: topK }),
        });
        if (res.ok) {
          const data: Array<{ guidance_proposition_id: string; matches: MatchEntry[] }> = await res.json();
          gpMatches = data.find((d) => d.guidance_proposition_id === gp.id)?.matches ?? [];
          setMatches((prev) => ({ ...prev, [gp.id]: gpMatches }));
          setClassifications((prev) => { const next = { ...prev }; delete next[gp.id]; return next; });
        }
      } catch { /* continue */ }

      // Classify
      let gpClassified: ClassifiedEntry[] = [];
      if (gpMatches.length) {
        setLoading((prev) => ({ ...prev, [gp.id]: "classifying" }));
        try {
          const res = await fetch(`${API_BASE}/classify`, {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ guidance_proposition: gp, matches: topMatchesForClassify(gpMatches) }),
          });
          if (res.ok) {
            gpClassified = await res.json();
            setClassifications((prev) => ({ ...prev, [gp.id]: gpClassified }));
          }
        } catch { /* continue */ }
      }

      // Summarise
      const relevant = gpClassified.filter((c) => c.relationship !== "does not match");
      if (relevant.length) {
        setLoading((prev) => ({ ...prev, [gp.id]: "summarising" }));
        try {
          const res = await fetch(`${API_BASE}/summarise`, {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ guidance_proposition: gp, classified_matches: relevant }),
          });
          if (res.ok) {
            const data: { summary: string; cached: boolean } = await res.json();
            setSummaries((prev) => ({ ...prev, [gp.id]: data.summary }));
            setSummaryCached((prev) => ({ ...prev, [gp.id]: data.cached }));
          }
        } catch { /* continue */ }
      }

      setLoading((prev) => ({ ...prev, [gp.id]: null }));
    }
    setRunningAll(false);
  }

  // ── Export CSV ────────────────────────────────────────────────────────────────

  function handleExportCsv(): void {
    const escape = (v: string | number | null | undefined) => {
      const s = String(v ?? "");
      return s.includes(",") || s.includes('"') || s.includes("\n")
        ? `"${s.replace(/"/g, '""')}"` : s;
    };

    const headers = [
      "Guidance Proposition", "Summary",
      "Law Proposition", "Law Citation",
      "BERT Score", "Relationship", "Explanation", "Confidence", "Correctness Score",
    ];

    const rows: string[][] = [];
    for (const gp of guidancePropositions) {
      const summary = summaries[gp.id] ?? "";
      const classified = classifications[gp.id];
      if (!classified?.length) {
        rows.push([gp.proposition_text, summary, "", "", "", "", "", "", ""]);
      } else {
        for (const c of classified) {
          rows.push([
            gp.proposition_text,
            summary,
            c.law_proposition.proposition_text,
            c.law_proposition.article_reference ?? "",
            c.bert_score_f1.toFixed(3),
            c.relationship,
            c.explanation,
            c.confidence,
            c.correctness_score.toFixed(2),
          ]);
        }
      }
    }

    const csv = [headers, ...rows].map((r) => r.map(escape).join(",")).join("\n");
    const blob = new Blob([csv], { type: "text/csv" });
    const url = URL.createObjectURL(blob);
    const a = document.createElement("a");
    a.href = url;
    a.download = "guidance-classifications.csv";
    a.click();
    URL.revokeObjectURL(url);
  }

  // ── Render ───────────────────────────────────────────────────────────────────

  return (
    <main className="mx-auto min-h-screen w-full max-w-5xl px-4 py-8 sm:px-6 lg:px-8">
      {/* Header */}
      <header className="mb-8">
        <h1 className="text-3xl font-semibold tracking-tight">Guidance Explorer</h1>
        <p className="mt-1 text-sm text-muted-foreground">
          Extract propositions from a GOV.UK guidance page and match them against law propositions.
        </p>
      </header>

      {/* Inputs */}
      <section className="mb-8 rounded-lg border border-border bg-card p-5 shadow-sm">
        <div className="space-y-4">
          <div>
            <label className="mb-1 block text-sm font-medium">GOV.UK URL</label>
            <input
              type="url"
              value={url}
              onChange={(e) => setUrl(e.target.value)}
              placeholder="https://www.gov.uk/guidance/…"
              className="w-full rounded-md border border-input bg-background px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-ring"
            />
          </div>
          <div>
            <label className="mb-1 block text-sm font-medium">
              Section <span className="text-muted-foreground">(optional — section anchor, e.g. notify-apha-about-imports)</span>
            </label>
            <input
              type="text"
              value={section}
              onChange={(e) => setSection(e.target.value)}
              placeholder="section-anchor-id"
              className="w-full rounded-md border border-input bg-background px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-ring"
            />
          </div>
          <div>
            <label className="mb-1 block text-sm font-medium">
              Matches per proposition
            </label>
            <input
              type="number"
              min={1}
              max={20}
              value={topK}
              onChange={(e) => setTopK(Math.max(1, parseInt(e.target.value) || 1))}
              className="w-24 rounded-md border border-input bg-background px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-ring"
            />
          </div>
          <div>
            <label className="mb-1 block text-sm font-medium">
              Matches to classify <span className="text-muted-foreground">(top by selected sort)</span>
            </label>
            <input
              type="number"
              min={1}
              max={20}
              value={classifyTopK}
              onChange={(e) => setClassifyTopK(Math.max(1, parseInt(e.target.value) || 1))}
              className="w-24 rounded-md border border-input bg-background px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-ring"
            />
          </div>
          <div>
            <label className="mb-2 block text-sm font-medium">Law propositions</label>
            {embeddedFiles.length > 0 && (
              <div className="mb-2 space-y-1">
                {embeddedFiles.map((f) => (
                  <div
                    key={f.file_hash}
                    className={`flex items-center rounded-md border text-sm transition-colors ${
                      lawFileHash === f.file_hash
                        ? "border-ring bg-accent font-medium"
                        : "border-border bg-background"
                    }`}
                  >
                    <button
                      type="button"
                      onClick={() => setLawFileHash(f.file_hash)}
                      className="flex min-w-0 flex-1 items-center justify-between px-3 py-2 text-left"
                    >
                      <span className="truncate">{f.filename}</span>
                      <span className="ml-3 shrink-0 text-xs text-muted-foreground">
                        {f.proposition_count} propositions
                      </span>
                    </button>
                    <button
                      type="button"
                      onClick={() => handleDeleteEmbedding(f.file_hash)}
                      className="shrink-0 px-2 py-2 text-muted-foreground hover:text-destructive"
                      title="Delete embeddings"
                    >
                      ✕
                    </button>
                  </div>
                ))}
              </div>
            )}
            <button
              type="button"
              onClick={() => fileInputRef.current?.click()}
              disabled={embedding}
              className="rounded-md border border-border bg-muted px-3 py-2 text-sm font-medium hover:bg-accent disabled:cursor-not-allowed disabled:opacity-50"
            >
              {embedding ? "Embedding…" : "Upload new file"}
            </button>
            <input
              ref={fileInputRef}
              type="file"
              accept=".json"
              className="hidden"
              onChange={handleFileChange}
            />
          </div>
          <div className="flex items-center gap-1 rounded-md border border-border bg-muted p-1 w-fit">
            {(["heuristic", "llm"] as const).map((m) => (
              <button
                key={m}
                type="button"
                onClick={() => setExtractMethod(m)}
                className={`rounded px-3 py-1 text-sm font-medium transition-colors ${
                  extractMethod === m
                    ? "bg-background shadow-sm"
                    : "text-muted-foreground hover:text-foreground"
                }`}
              >
                {m === "heuristic" ? "Heuristic" : "LLM"}
              </button>
            ))}
          </div>
          <button
            type="button"
            onClick={handleExtract}
            disabled={extracting || !url.trim()}
            className="rounded-md bg-foreground px-4 py-2 text-sm font-medium text-background hover:opacity-90 disabled:cursor-not-allowed disabled:opacity-50"
          >
            {extracting ? "Extracting…" : "Extract Propositions"}
          </button>
          {extractError && (
            <p className="text-sm text-destructive">{extractError}</p>
          )}
        </div>
      </section>

      {/* Proposition cards */}
      {guidancePropositions.length > 0 && (
        <section className="space-y-4">
          {(() => {
            const scores = guidancePropositions
              .map((gp) => {
                const entries = classifications[gp.id];
                if (!entries?.length) return null;
                return Math.max(...entries.map((c) => c.correctness_score));
              })
              .filter((s): s is number => s !== null);
            const avgScore = scores.length
              ? scores.reduce((a, b) => a + b, 0) / scores.length
              : null;
            return avgScore !== null ? (
              <div className="rounded-lg border border-border bg-card p-4 shadow-sm">
                <div className="mb-1 flex items-center justify-between text-sm">
                  <span className="font-medium">Page correctness score</span>
                  <span className="font-mono text-sm">{avgScore.toFixed(2)}</span>
                </div>
                <div className="h-2 w-full overflow-hidden rounded-full bg-muted">
                  <div
                    className={`h-full rounded-full transition-all ${
                      avgScore >= 0.8 ? "bg-green-500" : avgScore >= 0.5 ? "bg-yellow-500" : "bg-red-500"
                    }`}
                    style={{ width: `${avgScore * 100}%` }}
                  />
                </div>
                <p className="mt-1 text-xs text-muted-foreground">
                  Average of best match scores across {scores.length} of {guidancePropositions.length} classified proposition{guidancePropositions.length !== 1 ? "s" : ""}
                </p>
              </div>
            ) : null;
          })()}
          <div className="flex items-center justify-between">
            <div className="flex items-center gap-2">
              <p className="text-sm text-muted-foreground">
                {guidancePropositions.length} proposition{guidancePropositions.length !== 1 ? "s" : ""} extracted
              </p>
              {extractedFromCache !== null && (
                <span className={`rounded border px-1.5 py-0.5 text-[10px] font-semibold uppercase tracking-wide ${extractedFromCache ? "border-amber-200 bg-amber-100 text-amber-800" : "border-green-200 bg-green-100 text-green-800"}`}>
                  {extractedFromCache ? "cached" : "fresh"}
                </span>
              )}
            </div>
            <div className="flex gap-2">
              <div className="flex items-center gap-1 rounded-md border border-border bg-muted p-1">
                {(["similarity", "bert"] as const).map((s) => (
                  <button
                    key={s}
                    type="button"
                    onClick={() => setSortBy(s)}
                    className={`rounded px-2 py-0.5 text-xs font-medium transition-colors ${
                      sortBy === s ? "bg-background shadow-sm" : "text-muted-foreground hover:text-foreground"
                    }`}
                  >
                    {s === "similarity" ? "Sort: sim" : "Sort: bert"}
                  </button>
                ))}
              </div>
              <button
                type="button"
                onClick={handleRunAll}
                disabled={runningAll || !lawFileHash}
                className="rounded-md bg-foreground px-3 py-1.5 text-xs font-medium text-background hover:opacity-90 disabled:cursor-not-allowed disabled:opacity-50"
              >
                {runningAll ? "Running…" : "Match & Classify All"}
              </button>
              <button
                type="button"
                onClick={handleViewOverlay}
                className="rounded-md border border-border bg-background px-3 py-1.5 text-xs font-medium hover:bg-accent"
              >
                View Overlay
              </button>
              <button
                type="button"
                onClick={handleExportCsv}
                disabled={Object.keys(classifications).length === 0}
                className="rounded-md border border-border bg-background px-3 py-1.5 text-xs font-medium hover:bg-accent disabled:cursor-not-allowed disabled:opacity-50"
              >
                Export CSV
              </button>
            </div>
          </div>
          {guidancePropositions.map((gp) => {
            const rawMatches = matches[gp.id];
            const gpMatches = rawMatches
              ? [...rawMatches].sort((a, b) =>
                  sortBy === "bert"
                    ? b.bert_score_f1 - a.bert_score_f1
                    : b.similarity_score - a.similarity_score
                )
              : rawMatches;
            const gpClassified = classifications[gp.id];
            const gpLoading = loading[gp.id];

            return (
              <div
                key={gp.id}
                className="rounded-lg border border-border bg-card p-5 shadow-sm"
              >
                {/* Proposition text */}
                <p className="mb-3 text-sm font-medium leading-relaxed">{gp.proposition_text}</p>

                {/* Metadata row */}
                <div className="mb-4 flex flex-wrap gap-x-4 gap-y-1 text-xs text-muted-foreground">
                  <span>
                    <span className="font-medium text-foreground">Section:</span> {gp.section_locator}
                  </span>
                  {gp.legal_subject && (
                    <span>
                      <span className="font-medium text-foreground">Subject:</span> {gp.legal_subject}
                    </span>
                  )}
                  {gp.action && (
                    <span>
                      <span className="font-medium text-foreground">Action:</span> {gp.action}
                    </span>
                  )}
                  <span className="ml-auto rounded border border-border bg-muted px-1.5 py-0.5">
                    {gp.extraction_method}
                  </span>
                </div>

                {/* Action buttons */}
                <div className="mb-4 flex flex-wrap items-center gap-2">
                  <button
                    type="button"
                    onClick={() => handleMatch(gp)}
                    disabled={gpLoading !== null && gpLoading !== undefined}
                    className="rounded-md border border-border bg-background px-3 py-1.5 text-xs font-medium hover:bg-accent disabled:cursor-not-allowed disabled:opacity-50"
                  >
                    {gpLoading === "matching" ? "Finding…" : "Find Matches"}
                  </button>
                  <button
                    type="button"
                    onClick={() => handleClassify(gp)}
                    disabled={
                      !gpMatches?.length ||
                      (gpLoading !== null && gpLoading !== undefined)
                    }
                    className="rounded-md border border-border bg-background px-3 py-1.5 text-xs font-medium hover:bg-accent disabled:cursor-not-allowed disabled:opacity-50"
                  >
                    {gpLoading === "classifying" ? "Classifying…" : "Classify Matches"}
                  </button>
                  <button
                    type="button"
                    onClick={() => handleSummarise(gp)}
                    disabled={
                      !gpClassified?.some((c) => c.relationship !== "does not match") ||
                      (gpLoading !== null && gpLoading !== undefined)
                    }
                    className="rounded-md border border-border bg-background px-3 py-1.5 text-xs font-medium hover:bg-accent disabled:cursor-not-allowed disabled:opacity-50"
                  >
                    {gpLoading === "summarising" ? "Summarising…" : "Summarise"}
                  </button>
                </div>

                {/* Summary */}
                {summaries[gp.id] && (
                  <div className="mb-4 rounded-md border border-blue-200 bg-blue-50 px-3 py-2 text-xs leading-relaxed text-blue-900">
                    <div className="mb-1 flex items-center gap-2">
                      <span className="font-semibold">Summary</span>
                      {gp.id in summaryCached && (
                        <span className={`rounded border px-1.5 py-0.5 text-[10px] font-semibold uppercase tracking-wide ${summaryCached[gp.id] ? "border-amber-200 bg-amber-100 text-amber-800" : "border-green-200 bg-green-100 text-green-800"}`}>
                          {summaryCached[gp.id] ? "cached" : "fresh"}
                        </span>
                      )}
                    </div>
                    {summaries[gp.id]}
                  </div>
                )}

                {/* Match results */}
                {gpMatches && gpMatches.length === 0 && (
                  <p className="text-xs text-muted-foreground">No matches found.</p>
                )}
                {gpMatches && gpMatches.length > 0 && (
                  <div className="space-y-2">
                    {gpMatches.map((m, i) => {
                      const classified = gpClassified?.find(
                        (c) => c.law_proposition.id === m.law_proposition.id,
                      );
                      return (
                        <div
                          key={m.law_proposition.id ?? i}
                          className="rounded-md border border-border bg-muted/40 p-3"
                        >
                          <div className="mb-1 flex items-start justify-between gap-2">
                            <p className="text-xs leading-relaxed text-foreground">
                              {m.law_proposition.proposition_text}
                            </p>
                            <div className="flex shrink-0 flex-col items-end gap-1">
                              <span className="rounded border border-border px-1.5 py-0.5 text-[11px] font-mono text-foreground" title="Embedding cosine similarity" style={{ backgroundColor: scoreBg(m.similarity_score) }}>
                                sim {m.similarity_score.toFixed(3)}
                              </span>
                              <span className="rounded border border-border px-1.5 py-0.5 text-[11px] font-mono text-foreground" title="BERTScore F1 (legal-bert)" style={{ backgroundColor: scoreBg(m.bert_score_f1) }}>
                                bert {m.bert_score_f1.toFixed(3)}
                              </span>
                            </div>
                          </div>
                          {m.law_proposition.article_reference && (
                            <p className="mb-2 text-[11px] text-muted-foreground">
                              {m.law_proposition.article_reference}
                              {m.law_proposition.jurisdiction
                                ? ` · ${m.law_proposition.jurisdiction}`
                                : ""}
                            </p>
                          )}
                          {classified && (
                            <div className="mt-2 space-y-1">
                              <div className="flex items-center gap-2">
                                <RelationshipBadge value={classified.relationship} />
                                <ConfidenceDot value={classified.confidence} />
                                <span className={`rounded border px-1.5 py-0.5 text-[10px] font-semibold uppercase tracking-wide ${classified.classify_cached ? "border-amber-200 bg-amber-100 text-amber-800" : "border-green-200 bg-green-100 text-green-800"}`}>
                                  {classified.classify_cached ? "cached" : "fresh"}
                                </span>
                                <span className="ml-auto text-[11px] font-mono text-muted-foreground">
                                  score: {classified.correctness_score.toFixed(2)}
                                </span>
                              </div>
                              <div className="h-1.5 w-full overflow-hidden rounded-full bg-muted">
                                <div
                                  className={`h-full rounded-full transition-all ${
                                    classified.correctness_score >= 0.8
                                      ? "bg-green-500"
                                      : classified.correctness_score >= 0.5
                                        ? "bg-yellow-500"
                                        : "bg-red-500"
                                  }`}
                                  style={{ width: `${classified.correctness_score * 100}%` }}
                                />
                              </div>
                              <p className="text-xs text-muted-foreground">{classified.explanation}</p>
                            </div>
                          )}
                        </div>
                      );
                    })}
                  </div>
                )}
              </div>
            );
          })}
        </section>
      )}
    </main>
  );
}
