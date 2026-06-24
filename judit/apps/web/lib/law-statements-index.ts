export type LawStatementPropRole =
  | "source"
  | "supporting"
  | "required_context"
  | "connector"
  | "via";

export type LawStatementPropRef = {
  propositionId: string;
  role: LawStatementPropRole;
  contextLocator?: string;
  contextResolutionStatus?: string;
  connectorKind?: string;
};

export type CompositionTraceIncorporation = {
  included_in_text: boolean;
  external_context: boolean;
  should_inline: boolean;
  should_split: boolean;
  reviewer_required: boolean;
};

export type CompositionTraceSpan = {
  order: number;
  text: string;
  start: number;
  end: number;
  role: string;
  proposition_ids: string[];
  context_locators: string[];
  source_fragment_ids: string[];
  source_locators: string[];
  support_status: string;
  incorporation: CompositionTraceIncorporation;
};

export type ContextIncorporationEntry = {
  locator: string;
  kind: string;
  resolution_status: string;
  proposition_ids: string[];
  material_role: string;
  incorporation: CompositionTraceIncorporation;
};

export type LawStatementRow = {
  id: string;
  statement_text: string;
  presentation_role: string;
  standalone_status: string;
  confidence: string;
  source_proposition_ids?: string[];
  supporting_proposition_ids?: string[];
  required_context?: Array<{
    kind?: string;
    locator?: string;
    resolution_status?: string;
    proposition_ids?: string[];
  }>;
  connector_context?: Array<{
    kind?: string;
    locator?: string;
    proposition_ids?: string[];
    via_proposition_ids?: string[];
    target_locator?: string;
    target_proposition_ids?: string[];
  }>;
  warnings?: string[];
  composition_trace?: CompositionTraceSpan[];
  context_incorporation?: ContextIncorporationEntry[];
};

export type PropositionRow = {
  id: string;
  proposition_text?: string;
  label?: string;
  short_name?: string;
  fragment_locator?: string;
  legal_effect_type?: string;
  proposition_tier?: string;
  source_record_id?: string;
  source_fragment_id?: string;
  extraction_debug_meta?: {
    evidence_quote?: string;
  };
};

export type SourceRow = {
  id: string;
  title?: string;
  citation?: string;
};

export type StatementQualityFlag =
  | "incomplete_standalone"
  | "unresolved_context"
  | "ambiguous_context"
  | "warnings"
  | "low_confidence"
  | "high_composition"
  | "weak_source_completeness";

export type StatementQualityAssessment = {
  uniquePropositionCount: number;
  refCount: number;
  flags: StatementQualityFlag[];
  issueLabels: string[];
  reviewScore: number;
};

export type StatementQualityPreset =
  | ""
  | "needs_review"
  | "incomplete"
  | "high_composition"
  | "unresolved_context";

export type StatementSortMode = "review_priority" | "proposition_count" | "text";

export function uniquePropositionIdsForStatement(statement: LawStatementRow): string[] {
  const ids = new Set<string>();
  for (const ref of propositionRefsForStatement(statement)) {
    ids.add(ref.propositionId);
  }
  return Array.from(ids);
}

export function assessStatementQuality(
  statement: LawStatementRow,
  options?: {
    minHighCompositionCount?: number;
    sourceCompletenessByPropositionId?: Map<string, string>;
  },
): StatementQualityAssessment {
  const uniqueIds = uniquePropositionIdsForStatement(statement);
  const refs = propositionRefsForStatement(statement);
  const minHighComposition = options?.minHighCompositionCount ?? 3;
  const completenessById = options?.sourceCompletenessByPropositionId;

  const flags: StatementQualityFlag[] = [];
  const issueLabels: string[] = [];
  let reviewScore = 0;

  if (statement.standalone_status !== "standalone") {
    flags.push("incomplete_standalone");
    issueLabels.push(`Incomplete (${statement.standalone_status.replaceAll("_", " ")})`);
    if (statement.standalone_status === "fragmentary" || statement.standalone_status === "relationship_only") {
      reviewScore += 6;
    } else if (statement.standalone_status === "context_dependent") {
      reviewScore += 5;
    } else {
      reviewScore += 3;
    }
  }

  const warningCount = (statement.warnings ?? []).filter((warning) => warning.trim()).length;
  if (warningCount > 0) {
    flags.push("warnings");
    issueLabels.push(`${warningCount} warning${warningCount === 1 ? "" : "s"}`);
    reviewScore += warningCount * 3;
  }

  if (statement.confidence === "low") {
    flags.push("low_confidence");
    issueLabels.push("Low confidence");
    reviewScore += 5;
  }

  let unresolvedCount = 0;
  let ambiguousCount = 0;
  for (const ctx of statement.required_context ?? []) {
    const status = String(ctx.resolution_status ?? "").trim();
    if (status === "unresolved") {
      unresolvedCount += 1;
    } else if (status === "ambiguous") {
      ambiguousCount += 1;
    }
  }
  if (unresolvedCount > 0) {
    flags.push("unresolved_context");
    issueLabels.push(`${unresolvedCount} unresolved context`);
    reviewScore += unresolvedCount * 5;
  }
  if (ambiguousCount > 0) {
    flags.push("ambiguous_context");
    issueLabels.push(`${ambiguousCount} ambiguous context`);
    reviewScore += ambiguousCount * 3;
  }

  if (uniqueIds.length >= minHighComposition) {
    flags.push("high_composition");
    issueLabels.push(`${uniqueIds.length} propositions`);
    reviewScore += 2 + Math.max(0, uniqueIds.length - minHighComposition);
  }

  if (completenessById) {
    const weakSourceCompleteness = (statement.source_proposition_ids ?? []).some((propositionId) => {
      const status = completenessById.get(propositionId);
      return status === "context_dependent" || status === "fragmentary";
    });
    if (weakSourceCompleteness) {
      flags.push("weak_source_completeness");
      issueLabels.push("Weak source proposition completeness");
      reviewScore += 4;
    }
  }

  return {
    uniquePropositionCount: uniqueIds.length,
    refCount: refs.length,
    flags,
    issueLabels,
    reviewScore,
  };
}

export function matchesQualityPreset(
  assessment: StatementQualityAssessment,
  preset: StatementQualityPreset,
): boolean {
  if (!preset) {
    return true;
  }
  switch (preset) {
    case "needs_review":
      return (
        assessment.flags.includes("incomplete_standalone") ||
        assessment.flags.includes("unresolved_context") ||
        assessment.flags.includes("ambiguous_context") ||
        assessment.flags.includes("warnings") ||
        assessment.flags.includes("low_confidence") ||
        assessment.flags.includes("weak_source_completeness")
      );
    case "incomplete":
      return assessment.flags.includes("incomplete_standalone");
    case "high_composition":
      return assessment.flags.includes("high_composition");
    case "unresolved_context":
      return (
        assessment.flags.includes("unresolved_context") ||
        assessment.flags.includes("ambiguous_context")
      );
    default:
      return true;
  }
}

export function sortStatements(
  statements: LawStatementRow[],
  sortMode: StatementSortMode,
  qualityById: Map<string, StatementQualityAssessment>,
): LawStatementRow[] {
  const rows = [...statements];
  rows.sort((left, right) => {
    const leftQuality = qualityById.get(left.id);
    const rightQuality = qualityById.get(right.id);
    if (sortMode === "proposition_count") {
      const countDiff =
        (rightQuality?.uniquePropositionCount ?? 0) - (leftQuality?.uniquePropositionCount ?? 0);
      if (countDiff !== 0) {
        return countDiff;
      }
    } else if (sortMode === "review_priority") {
      const scoreDiff = (rightQuality?.reviewScore ?? 0) - (leftQuality?.reviewScore ?? 0);
      if (scoreDiff !== 0) {
        return scoreDiff;
      }
      const countDiff =
        (rightQuality?.uniquePropositionCount ?? 0) - (leftQuality?.uniquePropositionCount ?? 0);
      if (countDiff !== 0) {
        return countDiff;
      }
    }
    return left.statement_text.localeCompare(right.statement_text);
  });
  return rows;
}

export type PropositionRefInstrumentGroup = {
  instrumentKey: string;
  refs: LawStatementPropRef[];
};

/** Preserve first-seen instrument order from `refs`. */
export function groupPropositionRefsByInstrument(
  refs: LawStatementPropRef[],
  instrumentKeyByPropositionId: Map<string, string>,
): PropositionRefInstrumentGroup[] {
  const order: string[] = [];
  const buckets = new Map<string, LawStatementPropRef[]>();
  for (const ref of refs) {
    const key = instrumentKeyByPropositionId.get(ref.propositionId) ?? "__unknown_instrument__";
    if (!buckets.has(key)) {
      order.push(key);
      buckets.set(key, []);
    }
    buckets.get(key)!.push(ref);
  }
  return order.map((instrumentKey) => ({
    instrumentKey,
    refs: buckets.get(instrumentKey) ?? [],
  }));
}

export function uniqueInstrumentKeysForStatement(
  statement: LawStatementRow,
  instrumentKeyByPropositionId: Map<string, string>,
): string[] {
  const keys = new Set<string>();
  for (const ref of propositionRefsForStatement(statement)) {
    const key = instrumentKeyByPropositionId.get(ref.propositionId) ?? "__unknown_instrument__";
    if (key !== "__unknown_instrument__") {
      keys.add(key);
    }
  }
  return Array.from(keys).sort((a, b) => a.localeCompare(b));
}

export function propositionRefsForStatement(statement: LawStatementRow): LawStatementPropRef[] {
  const out: LawStatementPropRef[] = [];
  const seen = new Set<string>();

  const push = (ref: LawStatementPropRef): void => {
    const key = `${ref.role}|${ref.propositionId}|${ref.contextLocator ?? ""}|${ref.connectorKind ?? ""}`;
    if (seen.has(key)) {
      return;
    }
    seen.add(key);
    out.push(ref);
  };

  for (const propositionId of statement.source_proposition_ids ?? []) {
    if (propositionId.trim()) {
      push({ propositionId, role: "source" });
    }
  }
  for (const propositionId of statement.supporting_proposition_ids ?? []) {
    if (propositionId.trim()) {
      push({ propositionId, role: "supporting" });
    }
  }
  for (const ctx of statement.required_context ?? []) {
    for (const propositionId of ctx.proposition_ids ?? []) {
      if (!propositionId.trim()) {
        continue;
      }
      push({
        propositionId,
        role: "required_context",
        contextLocator: ctx.locator,
        contextResolutionStatus: ctx.resolution_status,
      });
    }
  }
  for (const ctx of statement.connector_context ?? []) {
    for (const propositionId of ctx.proposition_ids ?? []) {
      if (!propositionId.trim()) {
        continue;
      }
      push({
        propositionId,
        role: "connector",
        contextLocator: ctx.locator,
        connectorKind: ctx.kind,
      });
    }
    for (const propositionId of ctx.via_proposition_ids ?? []) {
      if (!propositionId.trim()) {
        continue;
      }
      push({
        propositionId,
        role: "via",
        contextLocator: ctx.locator,
        connectorKind: ctx.kind,
      });
    }
  }
  return out;
}

export function buildStatementIndexes(statements: LawStatementRow[]): {
  statementsById: Map<string, LawStatementRow>;
  statementsByPropositionId: Map<string, LawStatementRow[]>;
} {
  const statementsById = new Map<string, LawStatementRow>();
  const statementsByPropositionId = new Map<string, LawStatementRow[]>();

  for (const statement of statements) {
    statementsById.set(statement.id, statement);
    for (const ref of propositionRefsForStatement(statement)) {
      const bucket = statementsByPropositionId.get(ref.propositionId) ?? [];
      if (!bucket.some((row) => row.id === statement.id)) {
        bucket.push(statement);
      }
      statementsByPropositionId.set(ref.propositionId, bucket);
    }
  }

  return { statementsById, statementsByPropositionId };
}

export function presentationRoleLabel(role: string): string {
  switch (role) {
    case "guidance_matching_candidate":
      return "Guidance match";
    case "procedural_or_enforcement_context":
      return "Procedural";
    case "supporting_definition":
      return "Definition";
    case "context_connector":
      return "Context connector";
    default:
      return role.replaceAll("_", " ");
  }
}

export function matchesStatementFilters(
  statement: LawStatementRow,
  filters: {
    search: string;
    presentationRole: string;
    standaloneStatus: string;
    beatriceOnly: boolean;
    beatriceStatementIds: Set<string>;
    qualityPreset: StatementQualityPreset;
    minPropositionCount: number;
    qualityById: Map<string, StatementQualityAssessment>;
  },
): boolean {
  if (filters.presentationRole && statement.presentation_role !== filters.presentationRole) {
    return false;
  }
  if (filters.standaloneStatus && statement.standalone_status !== filters.standaloneStatus) {
    return false;
  }
  if (filters.beatriceOnly && !filters.beatriceStatementIds.has(statement.id)) {
    return false;
  }
  const assessment =
    filters.qualityById.get(statement.id) ??
    assessStatementQuality(statement, { minHighCompositionCount: filters.minPropositionCount });
  if (filters.minPropositionCount > 1 && assessment.uniquePropositionCount < filters.minPropositionCount) {
    return false;
  }
  if (!matchesQualityPreset(assessment, filters.qualityPreset)) {
    return false;
  }
  const q = filters.search.trim().toLowerCase();
  if (!q) {
    return true;
  }
  const haystack = [
    statement.id,
    statement.statement_text,
    statement.presentation_role,
    statement.standalone_status,
    ...(statement.source_proposition_ids ?? []),
    ...(statement.supporting_proposition_ids ?? []),
  ]
    .join(" ")
    .toLowerCase();
  return haystack.includes(q);
}
