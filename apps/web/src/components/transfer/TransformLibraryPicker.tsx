import { useEffect, useMemo, useState } from "react";
import {
  compileTransformRecipe,
  listTransformLibrary,
  type TransformLibraryRecipe,
} from "../../lib/api";
import type { ShapeStepWire } from "../../lib/shape";

export interface TransformRecipeBinding {
  recipe_id: string;
  recipe_name: string;
  version: number;
  version_mode: "pin" | "follow";
  params: Record<string, unknown>;
}

interface TransformLibraryPickerProps {
  /** Source columns — the recipe's input contract is checked against them. */
  sourceColumns: string[];
  onApply: (steps: ShapeStepWire[], binding: TransformRecipeBinding) => void;
  onClear: () => void;
  /** The binding currently applied, when any — renders the pinned chip. */
  applied?: TransformRecipeBinding | null;
  disabled?: boolean;
}

/**
 * In-flow "From Library" selector — the (B) side of the two-transform model.
 *
 * Pick a named recipe → choose pin-vs-follow + a version → fill only its
 * declared parameters → Apply compiles it server-side into the exact shape
 * steps this editor and the engine both run. The shared definition is never
 * edited from here; a transfer can only rebind or re-parameterise.
 */
export function TransformLibraryPicker({
  sourceColumns,
  onApply,
  onClear,
  applied,
  disabled,
}: TransformLibraryPickerProps) {
  const [recipes, setRecipes] = useState<TransformLibraryRecipe[] | null>(null);
  const [loadError, setLoadError] = useState("");
  const [selectedId, setSelectedId] = useState("");
  const [mode, setMode] = useState<"pin" | "follow">("pin");
  const [pinVersion, setPinVersion] = useState<number | 0>(0);
  const [paramValues, setParamValues] = useState<Record<string, string>>({});
  const [busy, setBusy] = useState(false);
  const [applyError, setApplyError] = useState("");

  useEffect(() => {
    let cancelled = false;
    listTransformLibrary()
      .then((list) => {
        if (!cancelled) setRecipes(list);
      })
      .catch((err: Error) => {
        if (!cancelled) setLoadError(err.message);
      });
    return () => {
      cancelled = true;
    };
  }, []);

  const selected = useMemo(
    () => recipes?.find((r) => r.id === selectedId) ?? null,
    [recipes, selectedId],
  );

  const paramNames = useMemo(
    () => Object.keys(selected?.parameters || {}),
    [selected],
  );

  useEffect(() => {
    // Reset parameter entry when a different recipe is chosen — stale values
    // for another recipe's params must never leak into a new binding.
    setParamValues({});
    setApplyError("");
  }, [selectedId]);

  const apply = async () => {
    if (!selected) return;
    setBusy(true);
    setApplyError("");
    try {
      const params = Object.fromEntries(
        paramNames
          .filter((name) => paramValues[name] !== undefined && paramValues[name] !== "")
          .map((name) => [name, paramValues[name]]),
      );
      const result = await compileTransformRecipe(
        selected.id,
        { params, source_columns: sourceColumns },
        mode === "pin" && pinVersion ? pinVersion : undefined,
      );
      onApply(result.shape_recipe.steps as ShapeStepWire[], {
        recipe_id: result.recipe_id,
        recipe_name: result.recipe_name,
        version: result.bound_version,
        version_mode: mode,
        params,
      });
    } catch (err) {
      setApplyError(err instanceof Error ? err.message : "Recipe could not be applied");
    } finally {
      setBusy(false);
    }
  };

  return (
    <section className="df2-xform-library" aria-label="Apply a library transform">
      <div className="df2-xform-card-head">
        <span className="df2-xform-title">From Transform Library</span>
        {applied ? (
          <span className="df2-badge">
            {applied.recipe_name} · v{applied.version}
            {applied.version_mode === "follow" ? " · follows latest" : " · pinned"}
          </span>
        ) : null}
      </div>
      {loadError ? (
        <p className="df2-xform-note" role="status">
          {loadError}
        </p>
      ) : recipes === null ? (
        <p className="df2-xform-note">Loading library…</p>
      ) : recipes.length === 0 ? (
        <p className="df2-xform-note">
          No saved transforms yet — build the steps below, or save them to the
          Library so the next transfer can reuse them.
        </p>
      ) : (
        <>
          <div className="df2-label-hint" style={{ display: "flex", gap: 8, flexWrap: "wrap" }}>
            <label className="df2-sr-only" htmlFor="df2-lib-select">
              Library transform
            </label>
            <select
              id="df2-lib-select"
              className="df2-input"
              value={selectedId}
              disabled={disabled}
              onChange={(e) => setSelectedId(e.target.value)}
            >
              <option value="">Choose a library transform…</option>
              {recipes.map((r) => (
                <option key={r.id} value={r.id} disabled={r.status !== "active"}>
                  {r.name} · v{r.version}
                  {r.status !== "active" ? " (draft)" : ""}
                </option>
              ))}
            </select>
            <select
              aria-label="Version mode"
              className="df2-input"
              value={mode}
              disabled={disabled || !selected}
              onChange={(e) => setMode(e.target.value as "pin" | "follow")}
            >
              <option value="pin">Pin version</option>
              <option value="follow">Follow latest (re-resolves each scheduled run)</option>
            </select>
            {mode === "pin" && selected ? (
              <select
                aria-label="Pinned version"
                className="df2-input"
                value={pinVersion || selected.version}
                disabled={disabled}
                onChange={(e) => setPinVersion(Number(e.target.value))}
              >
                {Array.from({ length: selected.version }, (_, i) => i + 1).map((v) => (
                  <option key={v} value={v}>
                    v{v}
                  </option>
                ))}
              </select>
            ) : null}
          </div>
          {selected?.description ? (
            <p className="df2-xform-note">{selected.description}</p>
          ) : null}
          {mode === "follow" ? (
            <p className="df2-xform-note">
              Apply compiles the latest active version now. A one-off run uses
              those steps as applied; a schedule re-resolves the latest version
              at each run time.
            </p>
          ) : null}
          {paramNames.map((name) => (
            <label key={name} className="df2-label-hint" style={{ display: "block" }}>
              {name}
              {selected?.parameters?.[name]?.description
                ? ` — ${selected.parameters[name].description}`
                : ""}
              <input
                className="df2-input"
                value={paramValues[name] ?? ""}
                disabled={disabled}
                placeholder={String(
                  selected?.parameters?.[name]?.default ?? "required",
                )}
                onChange={(e) =>
                  setParamValues((prev) => ({ ...prev, [name]: e.target.value }))
                }
              />
            </label>
          ))}
          {applyError ? (
            <p className="df2-alert df2-alert-error" role="alert">
              {applyError}
            </p>
          ) : null}
          <div style={{ display: "flex", gap: 8 }}>
            <button
              type="button"
              className="df2-btn df2-btn-primary df2-btn-sm"
              disabled={disabled || busy || !selected}
              onClick={() => void apply()}
            >
              {busy ? "Applying…" : "Apply to steps"}
            </button>
            {applied ? (
              <button
                type="button"
                className="df2-btn df2-btn-ghost df2-btn-sm"
                disabled={disabled}
                onClick={onClear}
              >
                Clear binding
              </button>
            ) : null}
          </div>
        </>
      )}
    </section>
  );
}
