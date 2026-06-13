export interface DatabaseFormValues {
  type: string;
  host: string;
  port: number;
  database: string;
  username: string;
  password: string;
  schema: string;
  connectionString: string;
  ssl: boolean;
  warehouse: string;
}

interface DatabaseOption {
  id: string;
  label: string;
  defaultPort: number;
}

interface DatabaseConnectionFormProps {
  values: DatabaseFormValues;
  onChange: (values: DatabaseFormValues) => void;
  databaseOptions: DatabaseOption[];
  useConnectionString?: boolean;
  onToggleConnectionString?: (use: boolean) => void;
}

export function DatabaseConnectionForm({
  values,
  onChange,
  databaseOptions,
  useConnectionString = false,
  onToggleConnectionString,
}: DatabaseConnectionFormProps) {
  function patch(partial: Partial<DatabaseFormValues>) {
    onChange({ ...values, ...partial });
  }

  function onTypeChange(type: string) {
    const opt = databaseOptions.find((d) => d.id === type);
    patch({ type, port: opt?.defaultPort ?? values.port });
  }

  return (
    <div className="df-form-stack">
      <div className="df-form-field">
        <span className="df-label">Database type</span>
        <select className="df-select" value={values.type} onChange={(e) => onTypeChange(e.target.value)}>
          {databaseOptions.map((d) => (
            <option key={d.id} value={d.id}>
              {d.label}
            </option>
          ))}
        </select>
      </div>

      {onToggleConnectionString && (
        <label className="df-checkbox-label">
          <input
            type="checkbox"
            checked={useConnectionString}
            onChange={(e) => onToggleConnectionString(e.target.checked)}
          />
          Use connection string
        </label>
      )}

      {useConnectionString ? (
        <div className="df-form-field">
          <span className="df-label">Connection string</span>
          <input
            className="df-input"
            value={values.connectionString}
            onChange={(e) => patch({ connectionString: e.target.value })}
            placeholder="postgresql://user:pass@host:5432/dbname"
          />
        </div>
      ) : (
        <>
          <div className="df-form-row">
            <div className="df-form-field">
              <span className="df-label">{values.type === "snowflake" ? "Account" : "Host"}</span>
              <input
                className="df-input"
                value={values.host}
                onChange={(e) => patch({ host: e.target.value })}
                placeholder={values.type === "snowflake" ? "xy12345.us-east-1" : "localhost"}
              />
            </div>
            <div className="df-form-field df-form-field--narrow">
              <span className="df-label">Port</span>
              <input
                className="df-input"
                type="number"
                value={values.port}
                onChange={(e) => patch({ port: Number(e.target.value) })}
              />
            </div>
            <div className="df-form-field">
              <span className="df-label">Database</span>
              <input
                className="df-input"
                value={values.database}
                onChange={(e) => patch({ database: e.target.value })}
              />
            </div>
          </div>
          <div className="df-form-row">
            <div className="df-form-field">
              <span className="df-label">Username</span>
              <input
                className="df-input"
                value={values.username}
                onChange={(e) => patch({ username: e.target.value })}
                autoComplete="off"
              />
            </div>
            <div className="df-form-field">
              <span className="df-label">Password</span>
              <input
                className="df-input"
                type="password"
                value={values.password}
                onChange={(e) => patch({ password: e.target.value })}
                autoComplete="off"
              />
            </div>
            <div className="df-form-field">
              <span className="df-label">Schema</span>
              <input
                className="df-input"
                value={values.schema}
                onChange={(e) => patch({ schema: e.target.value })}
                placeholder={values.type === "snowflake" ? "PUBLIC" : "public"}
              />
            </div>
          </div>
          {values.type === "snowflake" && (
            <div className="df-form-field">
              <span className="df-label">Warehouse</span>
              <input
                className="df-input"
                value={values.warehouse}
                onChange={(e) => patch({ warehouse: e.target.value })}
                placeholder="COMPUTE_WH"
              />
            </div>
          )}
          <label className="df-checkbox-label">
            <input type="checkbox" checked={values.ssl} onChange={(e) => patch({ ssl: e.target.checked })} />
            SSL / TLS
          </label>
        </>
      )}
    </div>
  );
}
