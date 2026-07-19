import { useEffect, useState } from "react";
import { DtIcon } from "../DtIcon";
import { StructurePreview } from "../ui/StructurePreview";
import type { StreamSchemaPreview } from "../../lib/sourceStreams";

interface MultiStreamSchemaPreviewProps {
  streams: StreamSchemaPreview[];
  connectorName?: string;
  activeName?: string;
  onActiveChange?: (name: string) => void;
  loading?: boolean;
}

export function MultiStreamSchemaPreview({
  streams,
  connectorName,
  activeName,
  onActiveChange,
  loading,
}: MultiStreamSchemaPreviewProps) {
  const firstOk = streams.find((s) => s.status === "ok")?.name;
  const firstName = streams[0]?.name;
  const [internalActive, setInternalActive] = useState(activeName || firstOk || firstName || "");

  useEffect(() => {
    if (activeName) {
      setInternalActive(activeName);
      return;
    }
    const preferred = streams.find((s) => s.status === "ok")?.name
      || streams.find((s) => s.status === "loading")?.name
      || streams[0]?.name
      || "";
    setInternalActive((prev) => (streams.some((s) => s.name === prev) ? prev : preferred));
  }, [activeName, streams]);

  const active = streams.find((s) => s.name === internalActive) || streams[0];
  const okCount = streams.filter((s) => s.status === "ok").length;
  const errCount = streams.filter((s) => s.status === "error").length;

  const select = (name: string) => {
    setInternalActive(name);
    onActiveChange?.(name);
  };

  if (!streams.length) {
    return (
      <div className="df2-structure-preview is-empty">
        <DtIcon name="database" size={20} />
        <p>Enter one or more table/collection names to preview schema.</p>
      </div>
    );
  }

  return (
    <div className="df2-multistream-preview">
      <div className="df2-multistream-preview-head">
        <div>
          <h4>Source schema</h4>
          <p>
            {streams.length} stream{streams.length === 1 ? "" : "s"}
            {connectorName ? ` · ${connectorName}` : ""}
            {okCount > 0 ? ` · ${okCount} ready` : ""}
            {errCount > 0 ? ` · ${errCount} failed` : ""}
            {loading ? " · reading…" : ""}
          </p>
        </div>
      </div>

      <div className="df2-multistream-tabs" role="tablist" aria-label="Source streams">
        {streams.map((stream) => {
          const selected = stream.name === active?.name;
          return (
            <button
              key={stream.name}
              type="button"
              role="tab"
              aria-selected={selected}
              className={`df2-multistream-tab ${selected ? "is-active" : ""} is-${stream.status}`}
              onClick={() => select(stream.name)}
              title={stream.error || stream.name}
            >
              <span className="df2-multistream-tab-name">{stream.name}</span>
              {stream.status === "loading" && <SpinnerDot />}
              {stream.status === "ok" && <DtIcon name="check" size={12} />}
              {stream.status === "error" && <DtIcon name="alert" size={12} />}
            </button>
          );
        })}
      </div>

      {active?.status === "loading" || (loading && active?.status !== "ok" && active?.status !== "error") ? (
        <div className="df2-multistream-panel is-loading">
          <DtIcon name="activity" size={18} />
          <p>Reading schema for <strong>{active?.name}</strong>…</p>
        </div>
      ) : active?.status === "error" ? (
        <div className="df2-multistream-panel is-error" role="alert">
          <DtIcon name="alert" size={18} />
          <div>
            <strong>Could not read <code>{active.name}</code></strong>
            <p>{active.error || "Verify the name exists on this connector and credentials allow access."}</p>
            <p className="df2-muted">
              Other streams can still be previewed in their tabs. Fix this name or remove it from the comma-separated list.
            </p>
          </div>
        </div>
      ) : active?.status === "ok" ? (
        <StructurePreview
          columns={active.columns}
          schema={active.schema}
          rows={active.rows}
          rowCount={active.rowEstimate}
          title={active.name}
          subtitle={`${active.columns.length} fields${active.rowEstimate != null ? ` · ~${active.rowEstimate.toLocaleString()} rows` : ""} · sample below`}
          fill
          showBadge
          allowJson
          className="df2-multistream-structure"
        />
      ) : (
        <div className="df2-multistream-panel">
          <p>Select a stream tab to preview its schema.</p>
        </div>
      )}
    </div>
  );
}

function SpinnerDot() {
  return <span className="df2-multistream-tab-spinner" aria-hidden />;
}
