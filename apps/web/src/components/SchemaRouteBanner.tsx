import { ConnectorIcon } from "../app/brand-icons";
import { DtIcon } from "./DtIcon";
import { Spinner } from "./LoadingState";

interface SchemaRouteBannerProps {
  sourceLabel: string;
  sourceSubtitle: string;
  sourceType: string;
  destLabel: string;
  destSubtitle: string;
  destType: string;
  destSchemaLoading?: boolean;
  destFieldCount?: number;
  sourceColumnCount?: number;
  mappingCount?: number;
}

/** Clear source → destination context for the schema mapping step. */
export function SchemaRouteBanner({
  sourceLabel,
  sourceSubtitle,
  sourceType,
  destLabel,
  destSubtitle,
  destType,
  destSchemaLoading = false,
  destFieldCount,
  sourceColumnCount,
  mappingCount,
}: SchemaRouteBannerProps) {
  return (
    <div className="df2-schema-route" aria-label="Schema mapping route">
      <div className="df2-schema-route-endpoint df2-schema-route-source">
        <div className="df2-schema-route-icon">
          <ConnectorIcon id={sourceType} size={22} />
        </div>
        <div className="df2-schema-route-text">
          <span className="df2-schema-route-kind">Source</span>
          <strong className="df2-schema-route-name" title={sourceLabel}>{sourceLabel}</strong>
          <span className="df2-schema-route-detail">{sourceSubtitle}</span>
          {sourceColumnCount != null && sourceColumnCount > 0 && (
            <span className="df2-schema-route-meta">{sourceColumnCount} columns from source</span>
          )}
        </div>
      </div>

      <div className="df2-schema-route-bridge" aria-hidden>
        <span className="df2-schema-route-arrow">→</span>
        {mappingCount != null && mappingCount > 0 && (
          <span className="df2-schema-route-bridge-meta">{mappingCount} mapped</span>
        )}
      </div>

      <div className="df2-schema-route-endpoint df2-schema-route-dest">
        <div className="df2-schema-route-icon">
          <ConnectorIcon id={destType} size={22} />
        </div>
        <div className="df2-schema-route-text">
          <span className="df2-schema-route-kind">Destination</span>
          <strong className="df2-schema-route-name" title={destLabel}>{destLabel}</strong>
          {destSchemaLoading ? (
            <span className="df2-schema-route-detail df2-schema-route-loading">
              <Spinner size="sm" label="Loading destination schema" />
              Reading destination schema…
            </span>
          ) : (
            <span className="df2-schema-route-detail">{destSubtitle}</span>
          )}
          {!destSchemaLoading && destFieldCount != null && destFieldCount > 0 && (
            <span className="df2-schema-route-meta df2-schema-route-meta-dest">
              <DtIcon name="database" size={12} />
              {destFieldCount} existing fields loaded
            </span>
          )}
        </div>
      </div>
    </div>
  );
}
