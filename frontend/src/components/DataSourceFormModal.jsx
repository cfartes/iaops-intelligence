import { useEffect, useMemo, useState } from "react";
import useModalBehavior from "./useModalBehavior";

const INITIAL_FORM = {
  source_type: "",
  is_active: true,
  rag_enabled: false,
  rag_context_text: "",
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

const ALLOWED_CLASSIFICATIONS = new Set(["identifier", "sensitive", "financial", "temporal", "attribute", "metric"]);

function buildDefaultRagModel() {
  return { tenant_id: "global_default", entities: [] };
}

function parseRagJsonMaybe(rawText) {
  const text = String(rawText || "").trim();
  if (!text) return null;
  const candidates = [text];
  if (text.startsWith("json:")) candidates.unshift(text.slice(5).trim());
  for (const candidate of candidates) {
    try {
      const parsed = JSON.parse(candidate);
      if (parsed && typeof parsed === "object" && Array.isArray(parsed.entities)) {
        return parsed;
      }
    } catch (_) {
      // noop
    }
  }
  return null;
}

function ragModelToFriendlyText(modelInput) {
  const model = modelInput && typeof modelInput === "object" ? modelInput : buildDefaultRagModel();
  const entities = Array.isArray(model.entities) ? model.entities : [];
  const lines = [];
  lines.push("# Modelo RAG IAOps (texto amigavel)");
  lines.push("# Preencha os nomes amigaveis e sinonimos para ajudar o Chat BI.");
  lines.push("");
  for (const entity of entities) {
    const tableName = String(entity?.table_name || "").trim();
    if (!tableName) continue;
    lines.push(`## Tabela: ${tableName}`);
    lines.push(`Nome amigavel: ${String(entity?.friendly_name || "").trim() || tableName}`);
    lines.push(`Descricao: ${String(entity?.description || "").trim() || "-"}`);
    lines.push("");
    lines.push("### Campos");
    lines.push("| Campo tecnico | Nome amigavel | Tipo | Sinonimos |");
    lines.push("| --- | --- | --- | --- |");
    const cols = Array.isArray(entity?.columns) ? entity.columns : [];
    if (cols.length === 0) {
      lines.push("| id | Identificador | int | codigo |");
    } else {
      for (const col of cols) {
        const name = String(col?.name || "").trim();
        if (!name) continue;
        const friendly = String(col?.friendly_name || "").trim() || name;
        const type = String(col?.type || "").trim() || "text";
        const syn = Array.isArray(col?.synonyms)
          ? col.synonyms
              .map((item) => String(item || "").trim())
              .filter(Boolean)
              .join("; ")
          : "";
        lines.push(`| ${name} | ${friendly} | ${type} | ${syn || "-"} |`);
      }
    }
    lines.push("");
    lines.push("### Relacionamentos");
    lines.push("| Tabela destino | Condicao de join | Descricao |");
    lines.push("| --- | --- | --- |");
    const rels = Array.isArray(entity?.relationships) ? entity.relationships : [];
    if (rels.length === 0) {
      lines.push("| - | - | - |");
    } else {
      for (const rel of rels) {
        const toTable = String(rel?.to_table || "").trim() || "-";
        const joinCondition = String(rel?.join_condition || "").trim() || "-";
        const description = String(rel?.description || "").trim() || "-";
        lines.push(`| ${toTable} | ${joinCondition} | ${description} |`);
      }
    }
    lines.push("");
  }
  if (entities.length === 0) {
    lines.push("## Tabela: vendas.fact_vendas");
    lines.push("Nome amigavel: Vendas");
    lines.push("Descricao: Contem os registros de transacoes de vendas.");
    lines.push("");
    lines.push("### Campos");
    lines.push("| Campo tecnico | Nome amigavel | Tipo | Sinonimos |");
    lines.push("| --- | --- | --- | --- |");
    lines.push("| vlr_total | Valor da Venda | decimal | faturamento; preco; quanto custou |");
    lines.push("| dt_mov | Data da Transacao | date | quando; dia da venda; periodo |");
    lines.push("");
    lines.push("### Relacionamentos");
    lines.push("| Tabela destino | Condicao de join | Descricao |");
    lines.push("| --- | --- | --- |");
    lines.push("| dim_clientes | fact_vendas.cliente_id = dim_clientes.id | Relaciona uma venda a um cliente especifico |");
  }
  return lines.join("\n").trim();
}

function parseMarkdownTableRow(trimmedLine) {
  if (!trimmedLine.startsWith("|")) return null;
  const parts = trimmedLine
    .split("|")
    .map((item) => item.trim())
    .filter((_, idx, arr) => !(idx === 0 || idx === arr.length - 1));
  if (parts.length < 3) return null;
  if (parts.every((value) => /^-+$/.test(value.replace(/\s+/g, "")))) return null;
  return parts;
}

function parseRagFriendlyTextToModel(rawText) {
  const direct = parseRagJsonMaybe(rawText);
  if (direct) {
    return { model: direct, errors: [], warnings: [] };
  }
  const text = String(rawText || "").trim();
  if (!text) return { model: buildDefaultRagModel(), errors: [], warnings: [] };

  const errors = [];
  const warnings = [];
  const entities = [];
  let current = null;
  let section = "";

  const lines = text.split(/\r?\n/);
  for (let idx = 0; idx < lines.length; idx += 1) {
    const lineNo = idx + 1;
    const line = String(lines[idx] || "");
    const trimmed = line.trim();
    if (!trimmed || trimmed.startsWith("# ")) continue;

    const tableMatch = /^##\s*tabela\s*:\s*(.+)$/i.exec(trimmed);
    if (tableMatch) {
      const tableName = String(tableMatch[1] || "").trim();
      if (!tableName) {
        errors.push(`Linha ${lineNo}: nome da tabela vazio.`);
        current = null;
        continue;
      }
      current = {
        table_name: tableName,
        friendly_name: "",
        description: "",
        columns: [],
        relationships: [],
      };
      entities.push(current);
      section = "";
      continue;
    }

    if (!current) {
      continue;
    }

    const nameMatch = /^nome\s+amigavel\s*:\s*(.+)$/i.exec(trimmed);
    if (nameMatch) {
      current.friendly_name = String(nameMatch[1] || "").trim();
      continue;
    }
    const descMatch = /^descricao\s*:\s*(.+)$/i.exec(trimmed);
    if (descMatch) {
      current.description = String(descMatch[1] || "").trim();
      continue;
    }

    if (/^###\s*campos\s*$/i.test(trimmed)) {
      section = "columns";
      continue;
    }
    if (/^###\s*relacionamentos\s*$/i.test(trimmed)) {
      section = "relationships";
      continue;
    }

    const row = parseMarkdownTableRow(trimmed);
    if (!row) continue;

    if (section === "columns" && row.length >= 4) {
      const colName = String(row[0] || "").trim();
      if (!colName || colName === "Campo tecnico") continue;
      const synonyms = String(row[3] || "")
        .split(";")
        .map((item) => String(item || "").trim())
        .filter(Boolean);
      current.columns.push({
        name: colName,
        friendly_name: String(row[1] || "").trim() || colName,
        type: String(row[2] || "").trim() || "text",
        synonyms,
      });
      continue;
    }
    if (section === "relationships" && row.length >= 3) {
      const toTable = String(row[0] || "").trim();
      if (!toTable || toTable === "Tabela destino" || toTable === "-") continue;
      current.relationships.push({
        to_table: toTable,
        join_condition: String(row[1] || "").trim(),
        description: String(row[2] || "").trim(),
      });
    }
  }

  if (entities.length === 0) {
    errors.push("Nenhuma tabela foi identificada. Use o modelo textual de RAG.");
  }
  for (const entity of entities) {
    if (!entity.friendly_name) {
      entity.friendly_name = entity.table_name;
      warnings.push(`Tabela ${entity.table_name}: nome amigavel nao informado; foi usado o nome tecnico.`);
    }
  }
  return {
    model: { tenant_id: "global_default", entities },
    errors,
    warnings,
  };
}

function validateRagText(rawText) {
  const parsed = parseRagFriendlyTextToModel(rawText);
  return { errors: parsed.errors || [], warnings: parsed.warnings || [] };
}

function normalizeRagText(rawText) {
  const parsed = parseRagFriendlyTextToModel(rawText);
  if (parsed.errors?.length) return String(rawText || "").trim();
  return ragModelToFriendlyText(parsed.model);
}

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
  useModalBehavior({ open, onClose });
  const [form, setForm] = useState(INITIAL_FORM);
  const [loadingRagFile, setLoadingRagFile] = useState(false);
  const [ragUploadMode, setRagUploadMode] = useState("replace");
  const [ragPreview, setRagPreview] = useState("");
  const [showRagSection, setShowRagSection] = useState(false);
  const sourceTypeKey = String(form.source_type || "").trim().toLowerCase();
  const profileDef = PROFILE_BY_TYPE[sourceTypeKey] || PROFILE_BY_TYPE.default;
  const ragValidation = useMemo(() => validateRagText(form.rag_context_text), [form.rag_context_text]);

  useEffect(() => {
    if (!open) return;
    setRagUploadMode("replace");
    setRagPreview("");
    if (initialData) {
      const parsedSecret = parseSecretRef(initialData.conn_secret_ref);
      const initialRagRaw = String(initialData.rag_context_text || "").trim();
      const parsedRag = parseRagJsonMaybe(initialRagRaw);
      const ragDisplayText = parsedRag ? ragModelToFriendlyText(parsedRag) : initialRagRaw;
      const hasRagText = Boolean(String(ragDisplayText || "").trim());
      const ragEnabled = Boolean(initialData.rag_enabled);
      setForm({
        source_type: initialData.source_type || sourceCatalog?.[0]?.code || "",
        is_active: Boolean(initialData.is_active),
        rag_enabled: ragEnabled,
        rag_context_text: ragDisplayText,
        profile: getProfileTemplate(initialData.source_type || sourceCatalog?.[0]?.code || "", parsedSecret),
      });
      setShowRagSection(ragEnabled || hasRagText);
      return;
    }
    const defaultType = sourceCatalog?.[0]?.code || "";
    setForm({
      ...INITIAL_FORM,
      source_type: defaultType,
      rag_enabled: false,
      rag_context_text: "",
      profile: getProfileTemplate(defaultType),
    });
    setShowRagSection(false);
  }, [open, sourceCatalog, initialData]);

  const canSubmit = useMemo(() => {
    if (!form.source_type.trim()) return false;
    const profileOk = profileDef.fields.every((field) => {
      if (!field.required) return true;
      const value = String(form.profile?.[field.key] || "").trim();
      return value.length > 0;
    });
    if (!profileOk) return false;
    if (Boolean(form.rag_enabled) && ragValidation.errors.length > 0) return false;
    return true;
  }, [form.source_type, form.profile, profileDef.fields, form.rag_enabled, ragValidation.errors.length]);

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
    const ragParsed = parseRagFriendlyTextToModel(form.rag_context_text);
    const ragJson = String(form.rag_context_text || "").trim()
      ? JSON.stringify(ragParsed.model || buildDefaultRagModel())
      : null;
    onSubmit({
      id: initialData?.id,
      source_type: form.source_type,
      conn_secret_ref: buildConnSecretRef(),
      is_active: form.is_active,
      rag_enabled: Boolean(form.rag_enabled),
      rag_context_text: ragJson,
    });
  };

  const handleRagFileUpload = async (event) => {
    const file = event.target.files?.[0];
    if (!file) return;
    setLoadingRagFile(true);
    try {
      const text = await file.text();
      const cleanText = String(text || "").trim();
      setForm((prev) => ({
        ...prev,
        rag_context_text:
          ragUploadMode === "append" && String(prev.rag_context_text || "").trim()
            ? `${String(prev.rag_context_text || "").trim()}\n\n${cleanText}`
            : cleanText,
        rag_enabled: true,
      }));
      setRagPreview(cleanText.slice(0, 3000));
    } finally {
      setLoadingRagFile(false);
      event.target.value = "";
    }
  };

  const handleDownloadRagTemplate = () => {
    const template = ragModelToFriendlyText(buildDefaultRagModel());
    const blob = new Blob([template], { type: "text/markdown;charset=utf-8" });
    const url = URL.createObjectURL(blob);
    const anchor = document.createElement("a");
    anchor.href = url;
    anchor.download = "modelo_rag_iaops.md";
    document.body.appendChild(anchor);
    anchor.click();
    anchor.remove();
    URL.revokeObjectURL(url);
  };

  const handleAutoFixRag = () => {
    const normalized = normalizeRagText(form.rag_context_text);
    setForm((prev) => ({
      ...prev,
      rag_context_text: normalized,
      rag_enabled: true,
    }));
    setRagPreview(normalized.slice(0, 3000));
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
          <label className="checkbox-inline">
            <input
              type="checkbox"
              checked={Boolean(form.rag_enabled)}
              onChange={(e) => {
                const checked = e.target.checked;
                setForm((prev) => ({ ...prev, rag_enabled: checked }));
                if (checked) setShowRagSection(true);
              }}
            />
            Habilitar contexto RAG para esta fonte
          </label>
          <div className="inline-form">
            <button type="button" className="btn btn-secondary btn-small" onClick={() => setShowRagSection((prev) => !prev)}>
              {showRagSection ? "Ocultar configuracao RAG" : "Configurar RAG (opcional)"}
            </button>
          </div>
          {showRagSection ? (
            <>
          <label>
            Contexto RAG (descricao de tabelas/campos e regras de negocio)
            <textarea
              rows={6}
              value={String(form.rag_context_text || "")}
              onChange={(e) => setForm((prev) => ({ ...prev, rag_context_text: e.target.value }))}
              placeholder={
                "Ex.: payment.amount = valor recebido; rental.return_date = data da devolucao; " +
                "regra: considerar somente pagamentos concluídos."
              }
            />
          </label>
          <label>
            Upload de arquivo RAG
            <input type="file" accept=".txt,.md,.csv,.json,text/plain,application/json" onChange={handleRagFileUpload} />
            <small>{loadingRagFile ? "Carregando arquivo..." : "O arquivo preenche o contexto RAG automaticamente."}</small>
          </label>
          <label>
            Estrategia de importacao RAG
            <select value={ragUploadMode} onChange={(e) => setRagUploadMode(e.target.value)}>
              <option value="replace">Substituir contexto atual</option>
              <option value="append">Anexar ao contexto atual</option>
            </select>
          </label>
          {ragPreview ? (
            <label>
              Preview do arquivo RAG
              <textarea rows={4} value={ragPreview} readOnly />
            </label>
          ) : null}
          {Boolean(form.rag_enabled) && (ragValidation.errors.length > 0 || ragValidation.warnings.length > 0) ? (
            <div className="form-note">
              {ragValidation.errors.length > 0 ? (
                <>
                  <strong>Erros de formato RAG</strong>
                  <ul>
                    {ragValidation.errors.slice(0, 8).map((item) => (
                      <li key={item}>{item}</li>
                    ))}
                  </ul>
                </>
              ) : null}
              {ragValidation.warnings.length > 0 ? (
                <>
                  <strong>Avisos RAG</strong>
                  <ul>
                    {ragValidation.warnings.slice(0, 8).map((item) => (
                      <li key={item}>{item}</li>
                    ))}
                  </ul>
                </>
              ) : null}
            </div>
          ) : null}
            </>
          ) : null}
          <div className="modal-actions">
            <button type="button" className="btn btn-secondary" onClick={onClose}>
              Cancelar
            </button>
            <button type="button" className="btn btn-secondary" onClick={handleDownloadRagTemplate}>
              Baixar template RAG
            </button>
            <button type="button" className="btn btn-secondary" onClick={handleAutoFixRag} disabled={!String(form.rag_context_text || "").trim()}>
              Corrigir automaticamente
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
