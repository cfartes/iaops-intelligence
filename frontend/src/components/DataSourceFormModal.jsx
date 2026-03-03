import { useEffect, useMemo, useState } from "react";

const INITIAL_FORM = {
  source_type: "",
  is_active: true,
  profile: {},
};

const PROFILE_BY_TYPE = {
  postgres: {
    fields: [
      { key: "host", label: "Host", required: true },
      { key: "port", label: "Porta", required: true, type: "number", defaultValue: 5432 },
      { key: "dbname", label: "Banco", required: true },
      { key: "user", label: "Usuario", required: true },
      { key: "password", label: "Senha", required: true, type: "password" },
    ],
  },
  postgresql: {
    fields: [
      { key: "host", label: "Host", required: true },
      { key: "port", label: "Porta", required: true, type: "number", defaultValue: 5432 },
      { key: "dbname", label: "Banco", required: true },
      { key: "user", label: "Usuario", required: true },
      { key: "password", label: "Senha", required: true, type: "password" },
    ],
  },
  mysql: {
    fields: [
      { key: "host", label: "Host", required: true },
      { key: "port", label: "Porta", required: true, type: "number", defaultValue: 3306 },
      { key: "database", label: "Banco", required: true },
      { key: "user", label: "Usuario", required: true },
      { key: "password", label: "Senha", required: true, type: "password" },
    ],
  },
  sqlserver: {
    fields: [
      { key: "host", label: "Host", required: true },
      { key: "port", label: "Porta", required: true, type: "number", defaultValue: 1433 },
      { key: "database", label: "Banco", required: true },
      { key: "user", label: "Usuario", required: true },
      { key: "password", label: "Senha", required: true, type: "password" },
    ],
  },
  sql_server: {
    fields: [
      { key: "host", label: "Host", required: true },
      { key: "port", label: "Porta", required: true, type: "number", defaultValue: 1433 },
      { key: "database", label: "Banco", required: true },
      { key: "user", label: "Usuario", required: true },
      { key: "password", label: "Senha", required: true, type: "password" },
    ],
  },
  mssql: {
    fields: [
      { key: "host", label: "Host", required: true },
      { key: "port", label: "Porta", required: true, type: "number", defaultValue: 1433 },
      { key: "database", label: "Banco", required: true },
      { key: "user", label: "Usuario", required: true },
      { key: "password", label: "Senha", required: true, type: "password" },
    ],
  },
  oracle: {
    fields: [
      { key: "host", label: "Host", required: true },
      { key: "port", label: "Porta", required: true, type: "number", defaultValue: 1521 },
      { key: "service_name", label: "Service Name", required: true },
      { key: "user", label: "Usuario", required: true },
      { key: "password", label: "Senha", required: true, type: "password" },
    ],
  },
  power_bi: {
    fields: [
      { key: "access_token", label: "Access Token", required: true, type: "password" },
      { key: "api_url", label: "API URL", required: false, defaultValue: "https://api.powerbi.com/v1.0/myorg/groups?$top=1" },
    ],
  },
  fabric: {
    fields: [
      { key: "access_token", label: "Access Token", required: true, type: "password" },
      { key: "api_url", label: "API URL", required: false, defaultValue: "https://api.fabric.microsoft.com/v1/workspaces?top=1" },
    ],
  },
  default: {
    fields: [
      { key: "host", label: "Host/Endpoint", required: true },
      { key: "port", label: "Porta (opcional)", required: false, type: "number" },
      { key: "database", label: "Banco/Projeto (opcional)", required: false },
      { key: "user", label: "Usuario (opcional)", required: false },
      { key: "password", label: "Senha/Token (opcional)", required: false, type: "password" },
    ],
  },
};

function parseSecretRef(connSecretRef) {
  const raw = String(connSecretRef || "").trim();
  if (!raw) return {};
  try {
    if (raw.startsWith("json:")) {
      const parsed = JSON.parse(raw.slice(5));
      return parsed && typeof parsed === "object" ? parsed : {};
    }
    if (raw.startsWith("{") && raw.endsWith("}")) {
      const parsed = JSON.parse(raw);
      return parsed && typeof parsed === "object" ? parsed : {};
    }
  } catch (_) {
    // ignore parse errors and start with empty profile
  }
  return {};
}

function getProfileTemplate(sourceType, existing = {}) {
  const key = String(sourceType || "").trim().toLowerCase();
  const def = PROFILE_BY_TYPE[key] || PROFILE_BY_TYPE.default;
  const profile = {};
  for (const field of def.fields) {
    const value = existing?.[field.key];
    if (value !== undefined && value !== null && String(value).trim() !== "") {
      profile[field.key] = String(value);
    } else if (field.defaultValue !== undefined) {
      profile[field.key] = String(field.defaultValue);
    } else {
      profile[field.key] = "";
    }
  }
  return profile;
}

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
  const sourceTypeKey = String(form.source_type || "").trim().toLowerCase();
  const profileDef = PROFILE_BY_TYPE[sourceTypeKey] || PROFILE_BY_TYPE.default;

  useEffect(() => {
    if (!open) return;
    if (initialData) {
      const parsedSecret = parseSecretRef(initialData.conn_secret_ref);
      setForm({
        source_type: initialData.source_type || sourceCatalog?.[0]?.code || "",
        is_active: Boolean(initialData.is_active),
        profile: getProfileTemplate(initialData.source_type || sourceCatalog?.[0]?.code || "", parsedSecret),
      });
      return;
    }
    const defaultType = sourceCatalog?.[0]?.code || "";
    setForm({
      ...INITIAL_FORM,
      source_type: defaultType,
      profile: getProfileTemplate(defaultType),
    });
  }, [open, sourceCatalog, initialData]);

  const canSubmit = useMemo(() => {
    if (!form.source_type.trim()) return false;
    return profileDef.fields.every((field) => {
      if (!field.required) return true;
      const value = String(form.profile?.[field.key] || "").trim();
      return value.length > 0;
    });
  }, [form.source_type, form.profile, profileDef.fields]);

  if (!open) return null;

  const buildConnSecretRef = () => {
    const payload = {};
    for (const field of profileDef.fields) {
      const raw = form.profile?.[field.key];
      const value = String(raw ?? "").trim();
      if (!value) continue;
      if (field.type === "number") {
        const asNumber = Number(value);
        payload[field.key] = Number.isFinite(asNumber) ? asNumber : value;
      } else {
        payload[field.key] = value;
      }
    }
    return `json:${JSON.stringify(payload)}`;
  };

  const submit = (event) => {
    event.preventDefault();
    if (!canSubmit) return;
    onSubmit({
      id: initialData?.id,
      source_type: form.source_type,
      conn_secret_ref: buildConnSecretRef(),
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
              onChange={(e) => {
                const nextType = e.target.value;
                setForm((prev) => ({
                  ...prev,
                  source_type: nextType,
                  profile: getProfileTemplate(nextType, prev.profile),
                }));
              }}
            >
              {sourceCatalog.map((item) => (
                <option key={item.code} value={item.code}>
                  {item.name}
                </option>
              ))}
            </select>
          </label>
          {profileDef.fields.map((field) => (
            <label key={field.key}>
              {field.label}
              <input
                type={field.type === "password" ? "password" : field.type === "number" ? "number" : "text"}
                value={String(form.profile?.[field.key] || "")}
                onChange={(e) =>
                  setForm((prev) => ({
                    ...prev,
                    profile: {
                      ...(prev.profile || {}),
                      [field.key]: e.target.value,
                    },
                  }))
                }
              />
            </label>
          ))}
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
                  conn_secret_ref: buildConnSecretRef(),
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
