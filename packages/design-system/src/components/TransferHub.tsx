import type { ReactNode } from "react";

export interface TransferHubTemplate {
  id: string;
  label: string;
  description: string;
}

interface TransferHubProps {
  templates: TransferHubTemplate[];
  activeTemplateId: string;
  onTemplateChange: (id: string) => void;
  operationLabel: string;
  operationHint: string;
  sourcePanel: ReactNode;
  destinationPanel: ReactNode;
  footer?: ReactNode;
  status?: ReactNode;
}

export function TransferHub({
  templates,
  activeTemplateId,
  onTemplateChange,
  operationLabel,
  operationHint,
  sourcePanel,
  destinationPanel,
  footer,
  status,
}: TransferHubProps) {
  return (
    <div className="df-transfer-hub">
      <div className="df-transfer-hub-templates" role="tablist" aria-label="Transfer type">
        {templates.map((t) => (
          <button
            key={t.id}
            type="button"
            role="tab"
            aria-selected={activeTemplateId === t.id}
            className={["df-transfer-template", activeTemplateId === t.id ? "df-transfer-template--active" : ""]
              .filter(Boolean)
              .join(" ")}
            onClick={() => onTemplateChange(t.id)}
          >
            <span className="df-transfer-template-label">{t.label}</span>
            <span className="df-transfer-template-desc">{t.description}</span>
          </button>
        ))}
      </div>

      <div className="df-transfer-hub-operation">
        <span className="df-transfer-hub-op-label">{operationLabel}</span>
        <span className="df-transfer-hub-op-hint">{operationHint}</span>
      </div>

      <div className="df-transfer-hub-panels">
        <div className="df-transfer-hub-panel">
          <div className="df-transfer-hub-panel-tag df-transfer-hub-panel-tag--source">Source</div>
          {sourcePanel}
        </div>
        <div className="df-transfer-hub-bridge" aria-hidden>
          <svg width="20" height="20" viewBox="0 0 20 20" fill="none">
            <path d="M4 10H14M14 10L11 7M14 10L11 13" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" />
          </svg>
        </div>
        <div className="df-transfer-hub-panel">
          <div className="df-transfer-hub-panel-tag df-transfer-hub-panel-tag--dest">Destination</div>
          {destinationPanel}
        </div>
      </div>

      {status}
      {footer}
    </div>
  );
}
