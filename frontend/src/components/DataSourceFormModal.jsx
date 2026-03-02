import { useEffect, useMemo, useState } from "react";

const INITIAL_FORM = {
  source_type: "",
  conn_secret_ref: "",
  is_active: true,
};

const SECRET_PLACEHOLDER_BY_TYPE = {
  postgres: 'json:{"host":"localhost","port":5432,"dbname":"db","user":"postgres","password":"***"}',
  postgresql: 'json:{"host":"localhost","port":5432,"dbname":"db","user":"postgres","password":"***"}',
  sqlserver: 'json:{"host":"sqlserver.local","port":1433,"database":"db","user":"sa","password":"***"}',
  sql_server: 'json:{"host":"sqlserver.local","port":1433,"database":"db","user":"sa","password":"***"}',
  mssql: 'json:{"host":"sqlserver.local","port":1433,"database":"db","user":"sa","password":"***"}',
  mysql: 'json:{"host":"mysql.local","port":3306,"database":"db","user":"root","password":"***"}',
  oracle: 'json:{"host":"oracle.local","port":1521,"service_name":"ORCL","user":"system","password":"***"}',
  power_bi: 'json:{"access_token":"<token>","api_url":"https://api.powerbi.com/v1.0/myorg/groups?$top=1"}',
  fabric: 'json:{"access_token":"<token>","api_url":"https://api.fabric.microsoft.com/v1/workspaces?top=1"}',
};

export default function DataSourceFormModal({
  open,
  sourceCatalog,
  initialData = null,
  title = "Nova fonte de dados do tenant",
  submitLabel = "Cadastrar",
  testLabel = "Testar conexao",
  testing = false,
  onTest,
  onClose,
  onSubmit,
}) {
  const [form, setForm] = useState(INITIAL_FORM);

  useEffect(() => {
    if (!open) return;
    if (initialData) {
      setForm({
        source_type: initialData.source_type || sourceCatalog?.[0]?.code || "",
        conn_secret_ref: initialData.conn_secret_ref || "",
        is_active: Boolean(initialData.is_active),
      });
      return;
    }
    setForm({
      ...INITIAL_FORM,
      source_type: sourceCatalog?.[0]?.code || "",
    });
  }, [open, sourceCatalog, initialData]);

  const canSubmit = useMemo(
    () => form.source_type.trim().length > 0 && form.conn_secret_ref.trim().length > 2,
    [form.source_type, form.conn_secret_ref]
  );
  const secretPlaceholder = SECRET_PLACEHOLDER_BY_TYPE[form.source_type] || "json:{...} ou enc:<ciphertext>";

  if (!open) return null;

  const submit = (event) => {
    event.preventDefault();
    if (!canSubmit) return;
    onSubmit({
      id: initialData?.id,
      source_type: form.source_type,
      conn_secret_ref: form.conn_secret_ref.trim(),
      is_active: form.is_active,
    });
  };

  return (
    <div className="modal-overlay" role="dialog" aria-modal="true">
      <div className="modal-card form-modal">
        <header className="modal-header">
          <h3>{title}</h3>
        </header>
        <form className="modal-content form-grid" onSubmit={submit}>
          <label>
            Tipo da fonte
            <select
              value={form.source_type}
              onChange={(e) => setForm((prev) => ({ ...prev, source_type: e.target.value }))}
            >
              {sourceCatalog.map((item) => (
                <option key={item.code} value={item.code}>
                  {item.name}
                </option>
              ))}
            </select>
          </label>
          <label>
            Referencia do segredo de conexao
            <input
              value={form.conn_secret_ref}
              onChange={(e) => setForm((prev) => ({ ...prev, conn_secret_ref: e.target.value }))}
              placeholder={secretPlaceholder}
            />
          </label>
          <label className="checkbox-inline">
            <input
              type="checkbox"
              checked={form.is_active}
              onChange={(e) => setForm((prev) => ({ ...prev, is_active: e.target.checked }))}
            />
            Fonte ativa
          </label>
          <div className="modal-actions">
            <button type="button" className="btn btn-secondary" onClick={onClose}>
              Cancelar
            </button>
            <button
              type="button"
              className="btn btn-secondary"
              onClick={() =>
                onTest?.({
                  source_type: form.source_type,
                  conn_secret_ref: form.conn_secret_ref.trim(),
                })
              }
              disabled={!canSubmit || testing}
            >
              {testing ? "Testando..." : testLabel}
            </button>
            <button type="submit" className="btn btn-primary" disabled={!canSubmit}>
              {submitLabel}
            </button>
          </div>
        </form>
      </div>
    </div>
  );
}
