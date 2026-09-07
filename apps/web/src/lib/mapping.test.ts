/**
 * Run: npx --yes tsx --test apps/web/src/lib/mapping.test.ts
 */
import assert from "node:assert/strict";
import { describe, it } from "node:test";
import {
  acknowledgeMappingRisk,
  applyDestTypeChange,
  applyStructPolicyChange,
  applyReductionReason,
  reductionEvidenceGap,
  applyDeclaredSourceZone,
  applyTransformChange,
  assumeTimezoneAwaitingZone,
  declaredSourceZone,
  suggestedSourceZones,
  approveMappingHonestly,
  approveMappingsHonestly,
  buildPreflightMappings,
  canWidenMapping,
  countApproveEligible,
  createNewRiskChipLabel,
  editableFromPipelineMappings,
  engineStampedRiskChip,
  engineTransformToUi,
  formatCodeCrosswalk,
  formatColumnProfileStrip,
  inferLogicalFromSample,
  isSafeNormalizeMapping,
  mappingAckLabel,
  mappingAckTier,
  mappingHealthSummary,
  mappingRequiresRiskAck,
  mappingsFromAnalysis,
  mergeSignedRiskContracts,
  mergeStampedTargetTypes,
  parseCodeCrosswalk,
  uiTransformToEngine,
  widenMappingToVarchar,
  isControlTotalCandidate,
  isCodeCrosswalkCandidate,
  isCodeCrosswalkDeclared,
  type EditableMapping,
} from "./mapping.js";
import { needsMappingReview } from "./columnWorkbench.js";
import { typeFamily } from "./typeDisplay.js";

describe("transform SSOT round-trip", () => {
  it("preserves phone/currency/integer engine transforms through Map edit", () => {
    const editable = editableFromPipelineMappings(
      [
        { source: "amt", target: "amount", confidence: 0.92, transform: "currency", source_type: "VARCHAR", target_type: "DECIMAL" },
        { source: "qty", target: "qty", confidence: 0.95, transform: "integer", source_type: "VARCHAR", target_type: "INTEGER" },
        { source: "phone", target: "phone", confidence: 0.9, transform: "phone", source_type: "VARCHAR", target_type: "VARCHAR" },
        { source: "blob", target: "blob", confidence: 0.9, transform: "binary", source_type: "BINARY", target_type: "BYTEA" },
        { source: "doc", target: "doc", confidence: 0.9, transform: "json", source_type: "JSON", target_type: "JSONB" },
      ],
      [],
      ["amount", "qty", "phone", "blob", "doc"],
      0.75,
      { amount: "DECIMAL", qty: "INTEGER", phone: "VARCHAR", blob: "BYTEA", doc: "JSONB" },
    );
    assert.equal(editable[0].transform, "currency");
    assert.equal(editable[0].engineTransform, "currency");
    assert.equal(editable[1].transform, "cast_integer");
    assert.equal(editable[1].engineTransform, "integer");
    assert.equal(editable[2].transform, "phone");
    assert.equal(editable[3].transform, "binary");
    assert.equal(editable[4].transform, "parse_json");

    const pf = buildPreflightMappings([], editable);
    assert.equal(pf[0].transform, "currency");
    assert.equal(pf[1].transform, "integer");
    assert.equal(pf[2].transform, "phone");
    assert.equal(pf[3].transform, "binary");
    assert.equal(pf[4].transform, "json");
  });

  it("maps engine json → parse_json (not none)", () => {
    assert.equal(engineTransformToUi("json"), "parse_json");
    assert.equal(uiTransformToEngine("parse_json"), "json");
    assert.equal(uiTransformToEngine("cast_integer"), "integer");
  });

  it("operator transform change updates engineTransform", () => {
    const m: EditableMapping = {
      source: "a",
      target: "a",
      confidence: 0.9,
      approved: true,
      transform: "phone",
      engineTransform: "phone",
    };
    const next = applyTransformChange(m, "cast_number");
    assert.equal(next.transform, "cast_number");
    assert.equal(next.engineTransform, "decimal");
    assert.equal(next.approved, false);
  });
});

describe("declared source zone", () => {
  const zoneless: EditableMapping = {
    source: "created_at",
    target: "created_at",
    confidence: 0.95,
    approved: false,
    sourceType: "TIMESTAMP",
    targetType: "date",
    transform: "none",
  };

  it("serializes the named zone as the engine transform", () => {
    const declared = applyDeclaredSourceZone(zoneless, "Europe/Berlin");
    assert.equal(declared.transform, "assume_timezone");
    assert.equal(declared.engineTransform, "assume_timezone:Europe/Berlin");
    assert.equal(declaredSourceZone(declared), "Europe/Berlin");
    assert.equal(assumeTimezoneAwaitingZone(declared), false);
    assert.equal(uiTransformToEngine("assume_timezone", "assume_timezone:Europe/Berlin"), "assume_timezone:Europe/Berlin");
    assert.equal(buildPreflightMappings([], [declared])[0].transform, "assume_timezone:Europe/Berlin");
  });

  it("never lets an unnamed zone reach the engine", () => {
    const chosen = applyTransformChange(zoneless, "assume_timezone");
    assert.equal(assumeTimezoneAwaitingZone(chosen), true);
    assert.equal(chosen.engineTransform, undefined);
    assert.equal(uiTransformToEngine("assume_timezone", chosen.engineTransform), undefined);

    const cleared = applyDeclaredSourceZone(applyDeclaredSourceZone(zoneless, "UTC"), "  ");
    assert.equal(assumeTimezoneAwaitingZone(cleared), true);
    assert.equal(cleared.engineTransform, undefined);
  });

  it("round-trips an engine declaration back onto the control", () => {
    assert.equal(engineTransformToUi("assume_timezone:Asia/Kolkata"), "assume_timezone");
    const editable = editableFromPipelineMappings(
      [
        {
          source: "created_at",
          target: "created_at",
          confidence: 0.95,
          transform: "assume_timezone:Asia/Kolkata",
          source_type: "TIMESTAMP",
          target_type: "date",
        },
      ],
      [],
      ["created_at"],
      0.75,
      { created_at: "date" },
    );
    assert.equal(editable[0].transform, "assume_timezone");
    assert.equal(declaredSourceZone(editable[0]), "Asia/Kolkata");
  });

  it("keeps the zone when the operator reselects the control", () => {
    const declared = applyDeclaredSourceZone(zoneless, "UTC");
    assert.equal(declaredSourceZone(applyTransformChange(declared, "assume_timezone")), "UTC");
  });

  it("suggests UTC first and offers real zone names", () => {
    const zones = suggestedSourceZones();
    assert.equal(zones[0], "UTC");
    assert.ok(zones.includes("Asia/Kolkata"));
    assert.equal(new Set(zones).size, zones.length);
  });
});

describe("fail-closed Map approve", () => {
  it("does not auto-approve exact-name lossy_cast pairs", () => {
    const editable = editableFromPipelineMappings(
      [
        {
          source: "amount",
          target: "amount",
          confidence: 0.99,
          source_type: "DECIMAL",
          target_type: "INTEGER",
          fidelity: "lossy_cast",
          fidelity_reason: "precision collapse",
          type_narrowing: true,
          requires_review: false,
        },
      ],
      [],
      ["amount"],
      0.85,
      { amount: "INTEGER" },
    );
    assert.equal(editable[0].approved, false);
    assert.equal(editable[0].requiresReview, true);
  });

  it("approveMappingHonestly refuses lossy_cast even when operator Approve-all runs", () => {
    const next = approveMappingsHonestly([
      {
        source: "amount",
        target: "amount",
        confidence: 0.99,
        approved: false,
        fidelity: "lossy_cast",
        typeNarrowing: true,
        inferredType: "DECIMAL",
        destType: "INTEGER",
      },
    ]);
    assert.equal(next[0].approved, false);
    assert.equal(next[0].requiresReview, true);
  });

  it("acknowledgeMappingRisk stamps risk_acknowledged for G4", () => {
    const next = acknowledgeMappingRisk(
      {
        source: "amount",
        target: "amount",
        confidence: 0.99,
        approved: false,
        fidelity: "lossy_cast",
        typeNarrowing: true,
        inferredType: "DECIMAL",
        destType: "INTEGER",
        fidelityReason: "precision collapse",
      },
      {
        executionPolicy: "CAST_AND_CONTINUE",
        migrationId: "mig-42",
        table: "orders",
        planId: "mig-42",
      },
    );
    assert.equal(next.approved, true);
    assert.equal(next.riskAcknowledged, true);
    assert.equal(next.requiresReview, false);
    assert.ok(next.riskContract);
    assert.equal(next.riskContract?.execution_policy, "CAST_AND_CONTINUE");
    assert.equal(next.riskContract?.column, "amount");
    assert.equal(next.riskContract?.migration_id, "mig-42");
    assert.equal(next.riskContract?.table, "orders");
    assert.equal(next.riskContract?.loss_classification, "lossy_cast");
    const pf = buildPreflightMappings([], [next]);
    assert.equal(pf[0].risk_acknowledged, true);
    assert.equal(pf[0].fidelity, "lossy_cast");
    assert.ok(pf[0].risk_contract);
    assert.equal(pf[0].risk_contract?.execution_policy, "CAST_AND_CONTINUE");
  });

  it("approveMappingHonestly refuses boolean riskAcknowledged without Risk Contract", () => {
    const next = approveMappingHonestly({
      source: "amount",
      target: "amount",
      confidence: 0.99,
      approved: false,
      fidelity: "lossy_cast",
      typeNarrowing: true,
      inferredType: "DECIMAL",
      destType: "INTEGER",
      riskAcknowledged: true,
      // no riskContract — GA fail-closed
    });
    assert.equal(next.approved, false);
    assert.equal(next.requiresReview, true);
  });

  it("approve-all refuses mutate fidelity without risk ack", () => {
    const next = approveMappingsHonestly([
      {
        source: "amount",
        target: "amount",
        confidence: 0.99,
        approved: false,
        fidelity: "mutate",
        transform: "currency",
        inferredType: "VARCHAR",
        destType: "DECIMAL",
      },
    ]);
    assert.equal(next[0].approved, false);
    assert.equal(next[0].requiresReview, true);
    const acked = acknowledgeMappingRisk(next[0], {
      executionPolicy: "CAST_AND_CONTINUE",
    });
    assert.equal(acked.riskAcknowledged, true);
    assert.equal(acked.approved, true);
  });

  it("acknowledgeMappingRisk refuses hidden CAST_AND_CONTINUE default", () => {
    const next = acknowledgeMappingRisk({
      source: "amount",
      target: "amount",
      confidence: 0.99,
      approved: false,
      fidelity: "lossy_cast",
      inferredType: "DECIMAL",
      destType: "INTEGER",
    });
    assert.equal(next.approved, false);
    assert.equal(next.requiresReview, true);
    assert.equal(next.riskAcknowledged, undefined);
    assert.ok(String(next.reason || "").includes("execution policy"));
  });

  it("FAIL_JOB contract does not unlock Approve", () => {
    const next = acknowledgeMappingRisk(
      {
        source: "amount",
        target: "amount",
        confidence: 0.99,
        approved: false,
        fidelity: "lossy_cast",
        inferredType: "DECIMAL",
        destType: "INTEGER",
      },
      { executionPolicy: "FAIL_JOB" },
    );
    assert.equal(next.riskAcknowledged, true);
    assert.equal(next.approved, false);
    assert.equal(next.requiresReview, true);
  });

  it("mergeStampedTargetTypes hydrates Kernel destType from Validate", () => {
    const merged = mergeStampedTargetTypes(
      [{
        source: "Change_from_Previous_Year",
        target: "Change_from_Previous_Year",
        confidence: 0.9,
        approved: true,
        createNew: true,
        destType: "",
        assignmentStrategy: "create_compatible_new",
      }],
      [{
        source: "Change_from_Previous_Year",
        target: "Change_from_Previous_Year",
        target_type: "DOUBLE PRECISION",
        create_new: true,
        assignment_strategy: "create_compatible_new",
      }],
    );
    assert.equal(merged[0].destType, "DOUBLE PRECISION");
    assert.equal(merged[0].createNew, true);
    assert.equal(merged[0].existsInDestination, false);
  });

  it("mergeStampedTargetTypes refuses ambiguous source-only hydrate", () => {
    const merged = mergeStampedTargetTypes(
      [
        { source: "a", target: "a1", confidence: 0.9, approved: true, destType: "TEXT" },
        { source: "a", target: "a2", confidence: 0.9, approved: true, destType: "TEXT" },
      ],
      [
        { source: "a", target: "a1", target_type: "INTEGER" },
        { source: "a", target: "a2", target_type: "BIGINT" },
      ],
    );
    assert.equal(merged[0].destType, "INTEGER");
    assert.equal(merged[1].destType, "BIGINT");
  });

  it("mergeStampedTargetTypes clears stale createNew on bind_existing", () => {
    const merged = mergeStampedTargetTypes(
      [{
        source: "id",
        target: "id",
        confidence: 0.99,
        approved: true,
        createNew: true,
        existsInDestination: false,
        destType: "VARCHAR",
        assignmentStrategy: "create_compatible_new",
      }],
      [{
        source: "id",
        target: "id",
        target_type: "INTEGER",
        create_new: false,
        assignment_strategy: "bind_existing",
      }],
    );
    assert.equal(merged[0].destType, "INTEGER");
    assert.equal(merged[0].createNew, false);
    assert.equal(merged[0].existsInDestination, true);
    assert.equal(merged[0].assignmentStrategy, "bind_existing");
    const wire = buildPreflightMappings([], merged);
    assert.equal(wire[0].create_new, false);
  });

  it("mergeSignedRiskContracts echoes risk_id and signature", () => {
    const merged = mergeSignedRiskContracts(
      [{
        source: "amount",
        target: "amount",
        confidence: 0.99,
        approved: true,
        fidelity: "lossy_cast",
        riskAcknowledged: true,
        riskContract: {
          column: "amount",
          source_type: "DECIMAL",
          destination_type: "INTEGER",
          execution_policy: "CAST_AND_CONTINUE",
          approved_by: "map-operator",
          reason: "draft",
        },
      }],
      [{
        source: "amount",
        target: "amount",
        risk_acknowledged: true,
        risk_contract: {
          column: "amount",
          source_type: "DECIMAL",
          destination_type: "INTEGER",
          execution_policy: "CAST_AND_CONTINUE",
          approved_by: "ops",
          reason: "signed",
          risk_id: "mrc-abc",
          signature: "mrc-sha256:deadbeef",
        },
      }],
    );
    assert.equal(merged[0].riskContract?.risk_id, "mrc-abc");
    assert.equal(merged[0].riskContract?.signature, "mrc-sha256:deadbeef");
  });

  it("safe normalize (email/trim) Approves without Accept risk wording", () => {
    const email = {
      source: "customer_email",
      target: "customer_email",
      confidence: 0.93,
      approved: false,
      fidelity: "mutate" as const,
      transform: "email" as const,
      inferredType: "VARCHAR",
      destType: "TEXT",
    };
    assert.equal(isSafeNormalizeMapping(email), true);
    assert.equal(mappingRequiresRiskAck(email), false);
    assert.equal(mappingAckLabel(email), "Approve");
    assert.equal(engineStampedRiskChip(email), null);
    const next = approveMappingHonestly(email);
    assert.equal(next.approved, true);
    assert.equal(next.riskAcknowledged, undefined);
  });

  it("cast fidelity uses Review label; lossy uses Accept risk", () => {
    const castRow = {
      source: "order_date",
      target: "order_date",
      confidence: 0.9,
      approved: false,
      fidelity: "cast" as const,
      transform: "date_iso" as const,
      inferredType: "DATE",
      destType: "TEXT",
    };
    const lossy = {
      source: "order_amt",
      target: "order_amt",
      confidence: 0.7,
      approved: false,
      fidelity: "lossy_cast" as const,
      transform: "cast_integer" as const,
      inferredType: "DECIMAL",
      destType: "INTEGER",
      typeNarrowing: true,
    };
    assert.equal(mappingAckTier(castRow), "review");
    assert.equal(mappingAckLabel(castRow), "Review");
    assert.equal(mappingAckTier(lossy), "accept_risk");
    assert.equal(mappingAckLabel(lossy), "Sign Risk Contract");
  });
});

describe("existing DDL honesty", () => {
  it("forbids Widen on existing destination columns", () => {
    const m: EditableMapping = {
      source: "status",
      target: "status",
      confidence: 0.9,
      approved: false,
      existsInDestination: true,
      destType: "BOOLEAN",
      inferredType: "VARCHAR",
      sample: "active",
      semanticRole: "string_enum",
      transform: "cast_boolean",
    };
    assert.equal(canWidenMapping(m), false);
    const widened = widenMappingToVarchar(m);
    assert.equal(widened.destType, "BOOLEAN");
    assert.equal(widened.requiresReview, true);
    assert.match(widened.reason || "", /ALTER|remap/i);
  });

  it("flags dest type change on existing columns without rewriting physical type", () => {
    const m: EditableMapping = {
      source: "id",
      target: "id",
      confidence: 0.99,
      approved: true,
      existsInDestination: true,
      destType: "INTEGER",
    };
    const next = applyDestTypeChange(m, "VARCHAR");
    assert.equal(next.destType, "INTEGER");
    assert.equal(next.requiresReview, true);
    assert.match(next.reason || "", /Desired type VARCHAR/);
  });
});

describe("specialty + health banner", () => {
  it("marks VECTOR as identity specialty and needs review", () => {
    const editable = editableFromPipelineMappings(
      [{ source: "emb", target: "emb", confidence: 0.99, transform: "none", source_type: "VECTOR(768)", target_type: "VECTOR(768)" }],
      [],
      [],
      0.75,
    );
    assert.equal(editable[0].transform, "identity_specialty");
    assert.equal(editable[0].requiresReview, true);
    assert.equal(editable[0].approved, false);
  });

  it("reports empty and conflict health", () => {
    assert.equal(mappingHealthSummary([]).total, 0);
    assert.equal(mappingHealthSummary([]).weak, true);
    const bad: EditableMapping[] = [{
      source: "status",
      target: "status",
      confidence: 0.9,
      approved: false,
      requiresReview: true,
      existsInDestination: true,
      destType: "BOOLEAN",
      sample: "active",
      semanticRole: "string_enum",
      reason: "Existing BOOLEAN column cannot be changed from Map",
    }];
    const h = mappingHealthSummary(bad, 0.85);
    assert.ok(h.existingTypeConflict >= 1 || h.needsReview >= 1);
    assert.equal(h.weak, true);
  });
});

describe("STRUCT Map policy", () => {
  it("defaults JSON to store_as_json and round-trips struct_policy", () => {
    const editable = editableFromPipelineMappings(
      [{ source: "addr", target: "addr", confidence: 0.9, transform: "json", source_type: "JSON", target_type: "JSONB" }],
      [{ addr: '{"city":"Austin","zip":"78701","geo":{"lat":30}}' }],
      [],
      0.75,
    );
    assert.equal(editable[0].structPolicy, "store_as_json");
    assert.equal(editable[0].requiresReview, true);
    const pf = buildPreflightMappings([], editable);
    assert.equal(pf[0].struct_policy, "store_as_json");
  });

  it("flatten synthesizes parent_key children and drops nested objects", () => {
    const base: EditableMapping[] = [{
      source: "addr",
      target: "addr",
      confidence: 0.9,
      approved: false,
      inferredType: "JSON",
      destType: "JSONB",
      sample: '{"city":"Austin","zip":"78701","geo":{"lat":30},"tags":["a"]}',
      structPolicy: "store_as_json",
      transform: "parse_json",
      engineTransform: "json",
    }];
    const next = applyStructPolicyChange(base, 0, "flatten_top_level_keys");
    assert.equal(next[0].structPolicy, "flatten_top_level_keys");
    const sources = next.map((m) => m.source);
    assert.ok(sources.includes("addr_city"));
    assert.ok(sources.includes("addr_zip"));
    assert.ok(sources.includes("addr_tags"));
    assert.ok(!sources.includes("addr_geo"), "nested object stays on parent blob");
    const child = next.find((m) => m.source === "addr_city");
    assert.equal(child?.structDerived, true);
    assert.equal(child?.structParent, "addr");
    assert.equal(child?.sample, "Austin");
    assert.equal(child?.inferredType, "VARCHAR");
    const tags = next.find((m) => m.source === "addr_tags");
    assert.equal(tags?.inferredType, "ARRAY");

    const back = applyStructPolicyChange(next, 0, "store_as_json");
    assert.equal(back.length, 1);
    assert.equal(back[0].structPolicy, "store_as_json");
  });

  it("deep flatten promotes nested geo_lat", () => {
    const base: EditableMapping[] = [{
      source: "addr",
      target: "addr",
      confidence: 0.9,
      approved: false,
      inferredType: "JSON",
      destType: "JSONB",
      sample: '{"city":"Austin","geo":{"lat":30,"lon":-97}}',
      structPolicy: "store_as_json",
      transform: "parse_json",
    }];
    const next = applyStructPolicyChange(base, 0, "flatten_deep");
    const sources = next.map((m) => m.source);
    assert.ok(sources.includes("addr_city"));
    assert.ok(sources.includes("addr_geo_lat"));
    assert.ok(sources.includes("addr_geo_lon"));
  });

  it("fails closed when flatten underscore paths collide", () => {
    const base: EditableMapping[] = [{
      source: "addr",
      target: "addr",
      confidence: 0.9,
      approved: false,
      inferredType: "JSON",
      destType: "JSONB",
      // literal geo_lat and nested geo.lat both flatten to geo_lat
      sample: '{"geo_lat":1,"geo":{"lat":2}}',
      structPolicy: "store_as_json",
      transform: "parse_json",
    }];
    const next = applyStructPolicyChange(base, 0, "flatten_deep");
    assert.equal(next[0].requiresReview, true);
    assert.match(next[0].reason || "", /collision/i);
    assert.ok(!next.some((m) => m.source === "addr_geo_lat" && m.structDerived));
  });

  it("array explode synthesizes _elem child", () => {
    const base: EditableMapping[] = [{
      source: "tags",
      target: "tags",
      confidence: 0.9,
      approved: false,
      inferredType: "ARRAY",
      destType: "JSONB",
      sample: '["a","b"]',
      structPolicy: "store_as_json",
      transform: "parse_json",
    }];
    const next = applyStructPolicyChange(base, 0, "explode_rows");
    assert.equal(next[0].structPolicy, "explode_rows");
    assert.ok(next.some((m) => m.source === "tags_elem" && m.structDerived));
  });
});

describe("Approve honesty + type badges", () => {
  it("does not auto-approve specialty or flatten rows", () => {
    const rows: EditableMapping[] = [
      { source: "a", target: "a", confidence: 0.99, approved: false, inferredType: "VARCHAR" },
      {
        source: "emb",
        target: "emb",
        confidence: 0.99,
        approved: false,
        inferredType: "VECTOR(8)",
        transform: "identity_specialty",
        requiresReview: true,
      },
      {
        source: "addr_city",
        target: "addr_city",
        confidence: 0.8,
        approved: false,
        inferredType: "VARCHAR",
        structDerived: true,
        structParent: "addr",
        requiresReview: true,
      },
    ];
    const next = approveMappingsHonestly(rows);
    assert.equal(next[0].approved, true);
    assert.equal(next[1].approved, false);
    assert.equal(next[2].approved, false);
    assert.equal(countApproveEligible(rows), 1);
  });

  it("infers child types from samples", () => {
    assert.equal(inferLogicalFromSample("42"), "INTEGER");
    assert.equal(inferLogicalFromSample("true"), "BOOLEAN");
    assert.equal(inferLogicalFromSample("2024-01-15T10:00:00Z"), "TIMESTAMPTZ");
    assert.equal(inferLogicalFromSample("[1,2]"), "ARRAY");
  });

  it("classifies NUMBER(p,s) badges correctly", () => {
    assert.equal(typeFamily("NUMBER(38,0)"), "int");
    assert.equal(typeFamily("NUMBER(38,10)"), "decimal");
    assert.equal(typeFamily("VECTOR(768)"), "binary");
    assert.equal(typeFamily("TIMESTAMPTZ"), "temporal");
  });
});

describe("destination schema honesty", () => {
  it("does not invent create-new from empty dest columns", () => {
    const editable = editableFromPipelineMappings(
      [{
        source: "id",
        target: "id",
        confidence: 0.9,
        source_type: "INTEGER",
        target_type: "INTEGER",
        assignment_strategy: "pending_dest_schema",
        create_new: false,
        requires_review: true,
      }],
      [],
      [],
      0.75,
    );
    assert.equal(editable[0].createNew, false);
    assert.equal(editable[0].assignmentStrategy, "pending_dest_schema");
    assert.equal(editable[0].approved, false);
    assert.ok(editable[0].confidence <= 0.55);
  });

  it("honors confirmed identity_passthrough create-new without inventing from empty cols alone", () => {
    const editable = editableFromPipelineMappings(
      [{
        source: "id",
        target: "id",
        confidence: 0.92,
        source_type: "INTEGER",
        target_type: "INTEGER",
        assignment_strategy: "identity_passthrough",
        create_new: true,
      }],
      [],
      [],
      0.75,
    );
    assert.equal(editable[0].createNew, true);
    assert.ok(editable[0].confidence <= 0.93);
  });

  it("does not flatten create-new confidence to a 95% wall", () => {
    const rows = editableFromPipelineMappings([
      {
        source: "c_custkey",
        target: "c_custkey",
        confidence: 0.88,
        transform: "none",
        create_new: true,
        assignment_strategy: "identity_passthrough",
        source_type: "BIGINT",
        target_type: "NUMBER(38,0)",
        fidelity: "preserve",
      },
      {
        source: "c_acctbal",
        target: "c_acctbal",
        confidence: 0.91,
        transform: "none",
        create_new: true,
        assignment_strategy: "identity_passthrough",
        source_type: "DECIMAL(11,6)",
        target_type: "NUMBER(11,6)",
        fidelity: "preserve",
      },
    ]);
    assert.equal(rows[0].confidence, 0.88);
    assert.equal(rows[1].confidence, 0.91);
    assert.notEqual(rows[0].confidence, rows[1].confidence);
  });

  it("create-new Approve destType is the target_type Validate reads", () => {
    const pf = buildPreflightMappings([], [
      {
        source: "DEP_TIME",
        target: "DEP_TIME",
        confidence: 0.9,
        transform: "none",
        approved: true,
        requiresReview: false,
        isPii: false,
        createNew: true,
        existsInDestination: false,
        assignmentStrategy: "create_compatible_new",
        destType: "NUMBER(12,9)",
        inferredType: "NUMBER(9,6)",
      },
    ]);
    assert.equal(pf[0].target_type, "NUMBER(12,9)");
    assert.equal(pf[0].create_new, true);
  });

  it("caps create-new confidence in buildPreflightMappings before preflight", () => {
    const fromEditable = buildPreflightMappings([], [
      {
        source: "id",
        target: "id",
        confidence: 0.99,
        transform: "none",
        approved: true,
        requiresReview: false,
        isPii: false,
        createNew: true,
        assignmentStrategy: "create_compatible_new",
        inferredType: "INTEGER",
      },
    ]);
    assert.equal(fromEditable[0].create_new, true);
    assert.ok((fromEditable[0].confidence as number) <= 0.96);

    const fromColumns = buildPreflightMappings([
      {
        column_name: "email",
        confidence: 0.99,
        inferred_type: "VARCHAR",
        semantic_type: "email",
        is_pii: true,
        compliance: [],
      },
    ]);
    assert.equal(fromColumns[0].create_new, true);
    assert.ok((fromColumns[0].confidence as number) <= 0.96);
  });

  it("caps Map bootstrap identity when dest column is missing (even without createNew flag)", () => {
    const fromBootstrap = buildPreflightMappings([], [
      {
        source: "id",
        target: "id",
        confidence: 0.95,
        transform: "none",
        approved: true,
        requiresReview: false,
        isPii: false,
        existsInDestination: false,
        inferredType: "INTEGER",
      },
    ]);
    assert.equal(fromBootstrap[0].create_new, true);
    assert.ok((fromBootstrap[0].confidence as number) <= 0.96);
  });

  it("mappingsFromAnalysis caps create-new and pending dest honestly", () => {
    const cols = [{
      column_name: "id",
      confidence: 0.99,
      inferred_type: "INTEGER",
      is_pii: false,
      compliance: [],
    }];
    const unknownDest = mappingsFromAnalysis(cols);
    assert.ok(unknownDest[0].confidence <= 0.96);

    const pending = mappingsFromAnalysis(cols, undefined, []);
    assert.equal(pending[0].assignmentStrategy, "pending_dest_schema");
    assert.ok(pending[0].confidence <= 0.55);
    assert.equal(pending[0].createNew, undefined);

    const create = mappingsFromAnalysis(cols, undefined, ["other_col"]);
    assert.equal(create[0].existsInDestination, false);
    assert.equal(create[0].createNew, true);
    assert.ok(create[0].confidence <= 0.96);

    const existing = mappingsFromAnalysis(cols, undefined, ["id"]);
    assert.equal(existing[0].existsInDestination, true);
    assert.equal(existing[0].createNew, undefined);
    assert.ok(existing[0].confidence >= 0.95);
    // Bootstrap must not invent Approve — fidelity pipeline stamps first.
    assert.equal(existing[0].approved, false);
  });

  it("create-new destType uses catalog inferred_type, not sample semantic_type", () => {
    const cols = [{
      column_name: "C_CUSTKEY",
      confidence: 0.99,
      inferred_type: "DECIMAL(38,0)",
      semantic_type: "BIGINT",
      is_pii: false,
      compliance: [],
    }];
    const create = mappingsFromAnalysis(cols, undefined, ["other_col"]);
    assert.equal(create[0].createNew, true);
    assert.equal(create[0].destType, "DECIMAL(38,0)");
  });

  it("intentional omit is first-class Map policy", () => {
    const base: EditableMapping = {
      source: "ssn",
      target: "ssn",
      confidence: 0.99,
      approved: false,
      transform: "none",
      inferredType: "VARCHAR",
    };
    const omitted = applyTransformChange(base, "omit");
    assert.equal(omitted.transform, "omit");
    assert.equal(omitted.target, "");
    assert.equal(omitted.approved, true);
    const health = mappingHealthSummary([
      { source: "id", target: "id", confidence: 0.99, approved: true, transform: "none" },
      omitted,
    ]);
    assert.equal(health.intentionalOmit, 1);
    assert.equal(health.unmappedTarget, 0);
    assert.equal(health.weak, false);
    const payload = buildPreflightMappings([], [
      { source: "id", target: "id", confidence: 0.99, approved: true, transform: "none" },
      omitted,
    ]);
    assert.equal(payload[1].transform, "omit");
    assert.equal(payload[1].intentional_omit, true);
    assert.equal(payload[1].target, "");
  });

  it("carries the Map reduction reason to the G16 wire fields", () => {
    const omitted = applyTransformChange(
      { source: "fax", target: "fax", confidence: 0.99, approved: false, transform: "none" },
      "omit",
    );
    const withReason: EditableMapping = {
      ...applyReductionReason(omitted, "archive_only"),
      omitReasonText: "Kept for the 7-year retention obligation",
      archiveReference: "s3://audit-archive/legacy_customer/2026",
      retentionUntil: "2033-01-31",
      omitApprovedBy: "R. Mehta",
    };
    const payload = buildPreflightMappings([], [
      { source: "id", target: "id", confidence: 0.99, approved: true, transform: "none" },
      withReason,
    ]);

    // Recording the reason must not withdraw the omission itself.
    assert.equal(payload[1].transform, "omit");
    assert.equal(payload[1].intentional_omit, true);
    assert.equal(payload[1].target, "");
    assert.equal(payload[1].omit_reason, "archive_only");
    assert.equal(payload[1].omit_reason_text, "Kept for the 7-year retention obligation");
    assert.equal(payload[1].archive_reference, "s3://audit-archive/legacy_customer/2026");
    assert.equal(payload[1].retention_until, "2033-01-31");
    assert.equal(payload[1].omit_approved_by, "R. Mehta");
    // A carried column never ships reduction evidence.
    assert.equal(payload[0].omit_reason, undefined);
  });

  it("carries a declared code crosswalk on the G20 wire field", () => {
    const payload = buildPreflightMappings([], [
      {
        source: "status",
        target: "status",
        confidence: 0.99,
        approved: true,
        transform: "none",
        codeCrosswalk: { A: "active", B: "blocked" },
        codeCrosswalkSystem: "legacy_status→v2",
      },
    ]);
    assert.deepEqual(payload[0].code_crosswalk, { A: "active", B: "blocked" });
    assert.equal(payload[0].code_crosswalk_system, "legacy_status→v2");
  });

  it("does not ship a code crosswalk on an omitted column", () => {
    const omitted = applyTransformChange(
      {
        source: "status",
        target: "status",
        confidence: 0.99,
        approved: false,
        transform: "none",
        codeCrosswalk: { A: "active" },
      },
      "omit",
    );
    const payload = buildPreflightMappings([], [omitted]);
    assert.equal(payload[0].code_crosswalk, undefined);
  });

  it("parses OLD=NEW / OLD->NEW / OLD→NEW and ignores comments", () => {
    const parsed = parseCodeCrosswalk("# comment\nA=active\nB -> blocked\nC→closed\n");
    assert.deepEqual(parsed, { A: "active", B: "blocked", C: "closed" });
    assert.equal(formatCodeCrosswalk(parsed), "A=active\nB=blocked\nC=closed");
    assert.equal(parseCodeCrosswalk(""), undefined);
    assert.deepEqual(parseCodeCrosswalk("A=A"), { A: "A" });
  });

  it("never ships a reduction reason for a column that is carried again", () => {
    const omitted = applyReductionReason(
      applyTransformChange(
        { source: "fax", target: "fax", confidence: 0.99, approved: false, transform: "none" },
        "omit",
      ),
      "dropped_not_required",
    );
    const restored = applyTransformChange(
      { ...omitted, omitReasonText: "New screen has no fax", omitApprovedBy: "R. Mehta" },
      "none",
    );

    assert.equal(restored.omitReason, undefined);
    assert.equal(restored.omitReasonText, undefined);
    assert.equal(restored.omitApprovedBy, undefined);
    assert.equal(buildPreflightMappings([], [restored])[0].omit_reason, undefined);
  });

  it("drops reason-scoped evidence when the reason is cleared or unknown", () => {
    const omitted: EditableMapping = {
      source: "fax",
      target: "",
      confidence: 1,
      approved: true,
      transform: "omit",
      omitReason: "archive_only",
      archiveReference: "s3://audit-archive/legacy",
      retentionUntil: "2033-01-31",
    };

    const cleared = applyReductionReason(omitted, "");
    assert.equal(cleared.omitReason, undefined);
    assert.equal(cleared.archiveReference, undefined);
    assert.equal(cleared.retentionUntil, undefined);

    // An unknown code is not a reason — it must not silently keep the archive claim.
    const bogus = applyReductionReason(omitted, "dropped_because_i_said_so");
    assert.equal(bogus.omitReason, undefined);
    assert.equal(bogus.archiveReference, undefined);

    // Moving to a non-archive reason retires the archive claim with it.
    const moved = applyReductionReason(omitted, "dropped_obsolete");
    assert.equal(moved.omitReason, "dropped_obsolete");
    assert.equal(moved.archiveReference, undefined);
    assert.equal(moved.retentionUntil, undefined);
  });

  it("names the evidence G16 still needs, and only for omitted rows", () => {
    const omitted = applyTransformChange(
      { source: "fax", target: "fax", confidence: 0.99, approved: false, transform: "none" },
      "omit",
    );

    assert.match(reductionEvidenceGap(omitted) || "", /No reduction reason/);
    assert.match(
      reductionEvidenceGap(applyReductionReason(omitted, "dropped_obsolete")) || "",
      /needs a note/,
    );
    assert.match(
      reductionEvidenceGap({
        ...applyReductionReason(omitted, "archive_only"),
        omitReasonText: "Kept in the audit archive",
      }) || "",
      /archive/i,
    );
    assert.equal(
      reductionEvidenceGap({
        ...applyReductionReason(omitted, "archive_only"),
        omitReasonText: "Kept in the audit archive",
        archiveReference: "s3://audit-archive/legacy",
      }),
      null,
    );
    // Observed codes stand on the sample the engine checks, not on a note.
    assert.equal(reductionEvidenceGap(applyReductionReason(omitted, "dropped_empty")), null);
    // A carried column has no reduction to evidence.
    assert.equal(
      reductionEvidenceGap({ source: "id", target: "id", confidence: 0.99, approved: true, transform: "none" }),
      null,
    );
  });

  it("consumes pipeline create_new_risks stamp before Validate", () => {
    const editable = editableFromPipelineMappings(
      [{
        source: "created_at",
        target: "created_at",
        confidence: 0.92,
        source_type: "TIMESTAMPTZ",
        target_type: "TIMESTAMP",
        create_new: true,
        assignment_strategy: "identity_passthrough",
        create_new_risks: [{
          kind: "timezone_polarity",
          severity: "warn",
          message: "Create-new drops timezone polarity: TIMESTAMPTZ → TIMESTAMP.",
        }],
      }],
      [],
      [],
      0.75,
    );
    assert.equal(editable[0].createNew, true);
    assert.equal(editable[0].createNewRisks?.length, 1);
    assert.equal(editable[0].approved, false);
    assert.equal(mappingRequiresRiskAck(editable[0]), true);
    assert.equal(createNewRiskChipLabel(editable[0]), "TZ risk");
    assert.ok(engineStampedRiskChip(editable[0])?.label === "TZ risk");
  });

  it("shows the epoch ceiling of a MySQL TIMESTAMP as a review, not a contract", () => {
    const editable = editableFromPipelineMappings(
      [{
        source: "created_at",
        target: "created_at",
        confidence: 0.92,
        source_type: "TIMESTAMPTZ",
        target_type: "TIMESTAMP(6)",
        create_new: true,
        assignment_strategy: "identity_passthrough",
        create_new_risks: [{
          kind: "instant_range_cap",
          severity: "warn",
          message:
            "Create-new TIMESTAMPTZ → TIMESTAMP(6) keeps the instant but caps its "
            + "range to 1970-01-01 00:00:01 UTC .. 2038-01-19 03:14:07 UTC.",
        }],
      }],
      [],
      [],
      0.75,
    );
    assert.equal(createNewRiskChipLabel(editable[0]), "range risk");
    assert.equal(engineStampedRiskChip(editable[0])?.severity, "warn");
    assert.equal(mappingAckTier(editable[0]), "review");
  });

  it("cast fidelity never auto-approves as Ready without Accept risk", () => {
    const editable = editableFromPipelineMappings(
      [{
        source: "created_at",
        target: "created_at",
        confidence: 0.99,
        source_type: "TIMESTAMP",
        target_type: "TIMESTAMP",
        fidelity: "cast",
        fidelity_reason: "Parsed via datetime; unparseable values quarantine.",
      }],
      [],
      ["created_at"],
      0.75,
      { created_at: "TIMESTAMP" },
    );
    assert.equal(editable[0].approved, false);
    assert.equal(mappingRequiresRiskAck(editable[0]), true);
    assert.equal(engineStampedRiskChip(editable[0])?.label, "cast");
  });

  it("never invents Approve from high confidence alone", () => {
    const editable = editableFromPipelineMappings(
      [{
        source: "id",
        target: "id",
        confidence: 0.99,
        source_type: "INTEGER",
        target_type: "INTEGER",
        requires_review: false,
      }],
      [],
      ["id"],
      0.75,
      { id: "INTEGER" },
    );
    assert.equal(editable[0].approved, false);
    assert.equal(editable[0].requiresReview, false);
  });

  it("first Validate does not need Approve for INTEGER → existing INT4 after round", () => {
    // Engine path after apply_shape_type_ceilings: rounded decimal is INTEGER,
    // dest is the existing int4. Continue already treats preserve as not-Issues;
    // the G4 wire must not re-invent requires_review from the missing click.
    const dests = ["INT4", "INT", "INTEGER"] as const;
    for (const dest of dests) {
      const editable = editableFromPipelineMappings(
        [{
          source: "arr_time",
          target: "arr_time",
          confidence: 0.99,
          source_type: "INTEGER",
          target_type: dest,
          fidelity: "preserve",
          requires_review: false,
          transform: "none",
        }],
        [],
        ["arr_time"],
        0.85,
        { arr_time: dest },
      );
      assert.equal(editable[0].approved, false, dest);
      assert.equal(editable[0].requiresReview, false, dest);
      assert.equal(needsMappingReview(editable[0], 0.85), false, dest);
      const wire = buildPreflightMappings([], editable);
      assert.equal(wire[0].requires_review, false, dest);
      assert.equal(wire[0].user_override, false, dest);
      assert.equal(wire[0].source_type, "INTEGER", dest);
    }
  });

  it("does not treat pending dest schema as create-new Widen", () => {
    const m: EditableMapping = {
      source: "id",
      target: "id",
      confidence: 0.9,
      approved: false,
      existsInDestination: false,
      assignmentStrategy: "pending_dest_schema",
    };
    assert.equal(canWidenMapping(m), false);
  });

  it("specialty health clears after Accept risk + Approve", () => {
    const open: EditableMapping = {
      source: "emb",
      target: "emb",
      confidence: 0.99,
      approved: false,
      transform: "identity_specialty",
      inferredType: "VECTOR(3)",
      destType: "VECTOR(3)",
      fidelity: "mutate",
      existsInDestination: true,
    };
    const openHealth = mappingHealthSummary([open], 0.75);
    assert.ok(openHealth.specialtyIdentity >= 1);
    assert.equal(openHealth.weak, true);

    const cleared: EditableMapping = {
      ...open,
      approved: true,
      riskAcknowledged: true,
    };
    const clearedHealth = mappingHealthSummary([cleared], 0.75);
    assert.equal(clearedHealth.specialtyIdentity, 0);
    assert.equal(clearedHealth.weak, false);
    assert.equal(clearedHealth.ready, 1);
    assert.match(clearedHealth.headline, /ready/i);
  });

  it("does not invent destType from source when Studio schema is partial", () => {
    const editable = editableFromPipelineMappings(
      [
        {
          source: "id",
          target: "id",
          confidence: 0.95,
          transform: "none",
          source_type: "INTEGER",
          target_type: "INTEGER",
        },
        {
          source: "note",
          target: "note",
          confidence: 0.9,
          transform: "none",
          source_type: "VARCHAR",
          // no Map stamp — Studio also missing note
        },
      ],
      [],
      ["id", "note"],
      0.75,
      { id: "INTEGER" }, // partial Studio
    );
    assert.equal(editable[0].destType, "INTEGER");
    assert.equal(editable[1].destType, undefined);
    assert.equal(editable[1].requiresReview, true);

    // Preflight must not re-invent VARCHAR stamp after Approve path.
    const withExists = editable.map((m) => ({
      ...m,
      existsInDestination: true,
      approved: true,
    }));
    const pf = buildPreflightMappings([], withExists);
    assert.equal(pf[0].target_type, "INTEGER");
    assert.equal(pf[1].target_type, undefined);
  });
});

describe("column profile Map strip", () => {
  it("threads engine column_profile into EditableMapping", () => {
    const editable = editableFromPipelineMappings(
      [
        {
          source: "score",
          target: "score",
          confidence: 0.9,
          source_type: "DECIMAL",
          target_type: "NUMERIC(8,2)",
          column_profile: {
            null_rate: 0.1,
            min: 66.75,
            max: 100,
            observed_precision: 5,
            observed_scale: 2,
            numeric_kind: "fixed_decimal",
          },
        },
      ],
      [],
      [],
      0.75,
      {},
    );
    assert.equal(editable[0].columnProfile?.null_rate, 0.1);
    assert.equal(editable[0].columnProfile?.observed_scale, 2);
    const strip = formatColumnProfileStrip(editable[0].columnProfile);
    assert.ok(strip);
    assert.match(strip!, /null 10%/);
    assert.match(strip!, /p5,s2/);
    assert.match(strip!, /fixed/);
  });

  it("formatColumnProfileStrip returns null when empty", () => {
    assert.equal(formatColumnProfileStrip(undefined), null);
    assert.equal(formatColumnProfileStrip({}), null);
  });
});

describe("G21 control total wire", () => {
  it("carries control_total through buildPreflightMappings", () => {
    const payload = buildPreflightMappings([], [
      {
        source: "amount",
        target: "amount",
        confidence: 0.99,
        approved: true,
        transform: "none",
        destType: "DECIMAL(12,2)",
        controlTotal: true,
      },
      {
        source: "id",
        target: "id",
        confidence: 0.99,
        approved: true,
        transform: "none",
        destType: "INTEGER",
      },
    ]);
    assert.equal(payload[0].control_total, true);
    assert.equal(payload[1].control_total, undefined);
  });

  it("defaults MONEY carriers as control-total candidates", () => {
    const money: EditableMapping = {
      source: "balance",
      target: "balance",
      confidence: 0.99,
      approved: true,
      transform: "none",
      destType: "MONEY",
    };
    const id: EditableMapping = {
      source: "id",
      target: "id",
      confidence: 0.99,
      approved: true,
      transform: "none",
      destType: "INTEGER",
    };
    assert.equal(isControlTotalCandidate(money), true);
    assert.equal(isControlTotalCandidate(id), false);
  });

  it("lets an operator declare a crosswalk on a plain varchar code column", () => {
    const base = { confidence: 0.99, approved: true, transform: "none" as const };
    const status: EditableMapping = {
      ...base,
      source: "status",
      target: "status",
      inferredType: "VARCHAR(4)",
      destType: "VARCHAR(4)",
    };
    // No enum marker anywhere, yet G20 must be askable — this is the ordinary
    // mainframe status/code column.
    assert.equal(status.semanticRole, undefined);
    assert.equal(isCodeCrosswalkCandidate(status), true);
    // Candidacy is not a declaration: nothing is mapped until the operator says so.
    assert.equal(isCodeCrosswalkDeclared(status), false);
    assert.equal(
      isCodeCrosswalkDeclared({ ...status, codeCrosswalk: { A: "active" } }),
      true,
    );
    const amount: EditableMapping = {
      ...base,
      source: "amount",
      target: "amount",
      inferredType: "DECIMAL(12,2)",
      destType: "DECIMAL(12,2)",
    };
    assert.equal(isCodeCrosswalkCandidate(amount), false);
    const omitted: EditableMapping = {
      ...status,
      transform: "omit",
      target: "",
    };
    assert.equal(isCodeCrosswalkCandidate(omitted), false);
  });
});
