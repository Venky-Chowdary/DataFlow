import { useCallback, useEffect, useMemo, useState } from "react";
import {
  FileDropzone,
  TransferSelectLayout,
  DestinationPicker,
  SourceTypePicker,
  useToast,
  type DestinationOption,
} from "@dataflow/design-system";
import { uploadFile } from "../lib/api";
import { fetchSavedConnectors } from "../lib/api";
import { SAMPLE_FILES } from "../lib/samples";
import { TRANSFER_TEMPLATES } from "../lib/transferModes";
import { TransferSelectionValidator } from "../lib/transfer/TransferSelectionValidator";
import {
  DATABASE_OPTIONS,
  FILE_FORMAT_OPTIONS,
  emptyEndpoint,
  type EndpointConfig,
  type FileFormat,
} from "../lib/types";

export interface TransferDraft {
  templateId: string;
  source: EndpointConfig;
  destination: EndpointConfig;
  exportFormat: FileFormat;
  apiUrl: string;
  sourceDbType: string;
  destDbType: string;
  destConnectorId: string;
  sourceConnectorId: string;
}

interface TransferSelectScreenProps {
  draft: TransferDraft;
  onDraftChange: (draft: TransferDraft) => void;
  onContinue: () => void;
}

const validator = new TransferSelectionValidator();

export function TransferSelectScreen({ draft, onDraftChange, onContinue }: TransferSelectScreenProps) {
  const { toast } = useToast();
  const [uploading, setUploading] = useState(false);
  const [savedConnectors, setSavedConnectors] = useState<{ id: string; name: string; type: string; role: string }[]>([]);

  const template = TRANSFER_TEMPLATES.find((t) => t.id === draft.templateId) ?? TRANSFER_TEMPLATES[0];

  useEffect(() => {
    fetchSavedConnectors()
      .then(setSavedConnectors)
      .catch(() => setSavedConnectors([]));
  }, []);

  const destOptions: DestinationOption[] = useMemo(() => {
    const saved = savedConnectors
      .filter((c) => c.role === "destination" || c.role === "both")
      .map((c) => ({ id: `saved:${c.id}`, label: c.name, type: "saved" as const, engine: c.type }));
    const engines = DATABASE_OPTIONS.map((d) => ({
      id: `engine:${d.id}`,
      label: d.label,
      type: "engine" as const,
      engine: d.id,
      status: (d.id === "postgresql" || d.id === "snowflake" ? "live" : "planned") as "live" | "planned",
    }));
    return [...saved, ...engines];
  }, [savedConnectors]);

  const destValue = draft.destConnectorId
    ? `saved:${draft.destConnectorId}`
    : draft.destDbType
      ? `engine:${draft.destDbType}`
      : "";

  function patch(partial: Partial<TransferDraft>) {
    onDraftChange({ ...draft, ...partial });
  }

  function handleTemplateChange(id: string) {
    const t = TRANSFER_TEMPLATES.find((x) => x.id === id)!;
    patch({
      templateId: id,
      source: emptyEndpoint(t.sourceKind, "Source"),
      destination:
        t.destKind === "file"
          ? { ...emptyEndpoint("file", "Destination"), exportFormat: t.destFormat ?? "csv" }
          : emptyEndpoint("database", "Destination"),
      exportFormat: t.destFormat ?? "csv",
      sourceDbType: t.sourceKind === "database" ? "postgresql" : "",
      destDbType: "",
      destConnectorId: "",
      sourceConnectorId: "",
      apiUrl: "",
    });
  }

  async function handleFile(file: File) {
    setUploading(true);
    try {
      const result = await uploadFile(file);
      patch({
        source: {
          ...emptyEndpoint("file", "Source"),
          kind: "file",
          connected: true,
          file: {
            fileName: result.filename,
            fileId: result.file_id,
            detectedFormat: result.format,
            format: "auto",
            encoding: result.encoding ?? "utf-8",
            rowCount: result.row_count,
            columns: result.columns,
            previewRows: result.preview_rows ?? [],
          },
        },
      });
    } catch (e) {
      toast({ title: "Upload failed", message: e instanceof Error ? e.message : "Parse error", tone: "error" });
    } finally {
      setUploading(false);
    }
  }

  const validation = validator.validateStep1(template, {
    templateId: draft.templateId,
    source: draft.source,
    destination: draft.destination,
    exportFormat: draft.exportFormat,
    apiUrl: draft.apiUrl,
    sourceConnectorId: draft.sourceConnectorId,
    destConnectorId: draft.destConnectorId,
    destDbType: draft.destDbType,
    sourceDbType: draft.sourceDbType,
  });

  const handleContinue = useCallback(() => {
    if (!validation.ok) {
      toast({ title: "Cannot continue", message: validation.message, tone: "error" });
      return;
    }
    onContinue();
  }, [validation, onContinue, toast]);

  const sourcePanel =
    template.sourceKind === "file" ? (
      <>
        <FileDropzone
          title="Drop your file here"
          hint="CSV · Excel · JSON · TSV"
          busy={uploading}
          fileName={draft.source.file.fileName}
          rowCount={draft.source.file.rowCount}
          onFileSelect={handleFile}
        />
        <div className="df-sample-links">
          {SAMPLE_FILES.slice(0, 3).map((s) => (
            <button
              key={s.id}
              type="button"
              className="df-sample-link"
              disabled={uploading}
              onClick={() =>
                fetch(`/samples/${s.filename}`)
                  .then((r) => r.blob())
                  .then((b) => handleFile(new File([b], s.filename)))
              }
            >
              Try {s.label}
            </button>
          ))}
        </div>
      </>
    ) : template.sourceKind === "database" ? (
      <SourceTypePicker
        value={draft.sourceDbType}
        onChange={(id) => patch({ sourceDbType: id })}
        options={DATABASE_OPTIONS.map((d) => ({ id: d.id, label: d.label }))}
      />
    ) : (
      <>
        <span className="df-label">API URL</span>
        <input
          className="df-input"
          value={draft.apiUrl}
          onChange={(e) => patch({ apiUrl: e.target.value })}
          placeholder="https://api.example.com/v1/data"
        />
      </>
    );

  const destinationPanel =
    template.destKind === "database" ? (
      <DestinationPicker
        label="Where should data go?"
        options={destOptions}
        value={destValue}
        onChange={(id) => {
          if (id.startsWith("saved:")) {
            patch({ destConnectorId: id.slice(6), destDbType: "" });
          } else {
            patch({ destDbType: id.slice(7), destConnectorId: "" });
          }
        }}
      />
    ) : (
      <>
        <span className="df-label">Export format</span>
        <select
          className="df-select"
          value={draft.exportFormat}
          onChange={(e) => patch({ exportFormat: e.target.value as FileFormat })}
        >
          {FILE_FORMAT_OPTIONS.filter((f) => f.id !== "auto").map((f) => (
            <option key={f.id} value={f.id}>
              {f.label}
            </option>
          ))}
        </select>
      </>
    );

  return (
    <>
      <p className="df-page-lead">Choose source and destination. Connection details are on the next step.</p>
      <TransferSelectLayout
        templates={TRANSFER_TEMPLATES.map((t) => ({ id: t.id, label: t.label }))}
        activeTemplateId={draft.templateId}
        onTemplateChange={handleTemplateChange}
        sourcePanel={sourcePanel}
        destinationPanel={destinationPanel}
        onContinue={handleContinue}
        continueDisabled={!validation.ok || uploading}
      />
    </>
  );
}
