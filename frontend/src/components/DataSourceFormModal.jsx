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

function validateRagText(rawText) {
  const text = String(rawText || "");
  if (!text.trim()) {
    return { errors: [], warnings: [] };
  }
  const lines = text.split(/\r?\n/);
  const errors = [];
  const warnings = [];
  let inTableBlock = false;
  let inRulesBlock = false;
  let currentColumn = "";
  let foundStructuredBlock = false;
  let foundKeyValue = false;

  const pushError = (line, message) => errors.push(`Linha ${line}: ${message}`);
  const pushWarning = (line, message) => warnings.push(`Linha ${line}: ${message}`);

  for (let idx = 0; idx < lines.length; idx += 1) {
    const lineNo = idx + 1;
    const line = lines[idx];
    const trimmed = line.trim();
    if (!trimmed || trimmed.startsWith("# ")) continue;

    if (trimmed.startsWith("##")) {
      inTableBlock = false;
      inRulesBlock = false;
      currentColumn = "";
      const tableHeader = /^##\s*tabela\s*:\s*([A-Za-z_][A-Za-z0-9_]*)\.([A-Za-z_][A-Za-z0-9_]*)$/i.exec(trimmed);
      if (tableHeader) {
        inTableBlock = true;
        foundStructuredBlock = true;
        continue;
      }
      if (/^##\s*regras_negocio\s*$/i.test(trimmed)) {
        inRulesBlock = true;
        foundStructuredBlock = true;
        continue;
      }
      pushWarning(lineNo, "Cabecalho nao reconhecido. Use '## tabela: schema.tabela' ou '## regras_negocio'.");
      continue;
    }

    if (/^-\s*coluna\s*:/i.test(trimmed)) {
      foundStructuredBlock = true;
      if (!inTableBlock) {
        pushError(lineNo, "Item de coluna fora de bloco de tabela.");
        continue;
      }
      const col = trimmed.split(":").slice(1).join(":").trim();
      if (!col || !/^[A-Za-z_][A-Za-z0-9_]*$/.test(col)) {
        pushError(lineNo, "Nome de coluna invalido.");
        continue;
      }
      currentColumn = col;
      continue;
    }

    if (/^-\s+/.test(trimmed)) {
      if (!inRulesBlock && !inTableBlock) {
        pushWarning(lineNo, "Item de lista sem bloco definido.");
      }
      continue;
    }

    const kvMatch = /^([A-Za-z_][A-Za-z0-9_.]*)\s*[:=]\s*(.+)$/.exec(trimmed);
    if (kvMatch) {
      foundKeyValue = true;
      const key = String(kvMatch[1] || "").toLowerCase();
      const value = String(kvMatch[2] || "").trim();
      if (!value) {
        pushError(lineNo, "Valor vazio na definicao de chave.");
        continue;
      }
      if (key === "classificacao") {
        if (!currentColumn) {
          pushWarning(lineNo, "Classificacao sem coluna ativa no bloco atual.");
        }
        if (!ALLOWED_CLASSIFICATIONS.has(value.toLowerCase())) {
          pushError(
            lineNo,
            `Classificacao invalida '${value}'. Use: ${Array.from(ALLOWED_CLASSIFICATIONS).join(", ")}.`
          );
        }
      }
      if ((key === "descricao" || key === "description") && !currentColumn && !inTableBlock) {
        pushWarning(lineNo, "Descricao fora de bloco de tabela.");
      }
      continue;
    }

    pushError(lineNo, "Formato nao reconhecido para RAG.");
  }

  if (!foundStructuredBlock && !foundKeyValue) {
    pushWarning(1, "Texto sem estrutura detectada. Use o template RAG para melhor qualidade.");
  }
  return { errors, warnings };
}

function normalizeRagText(rawText) {
  const text = String(rawText || "").trim();
  if (!text) return "";
  const lines = text.split(/\r?\n/);
  const tables = new Map();
  const rules = [];
  const extras = [];
  let currentTableKey = "";
  let currentColumnName = "";
  let inRulesBlock = false;

  const ensureTable = (schemaName, tableName) => {
    const key = `${schemaName}.${tableName}`;
    if (!tables.has(key)) {
      tables.set(key, {
        schema: schemaName,
        table: tableName,
        description: "",
        columns: new Map(),
      });
    }
    return tables.get(key);
  };

  for (const rawLine of lines) {
    const line = String(rawLine || "");
    const trimmed = line.trim();
    if (!trimmed || trimmed.startsWith("# ")) continue;

    const tableHeader = /^##\s*tabela\s*:\s*([A-Za-z_][A-Za-z0-9_]*)\.([A-Za-z_][A-Za-z0-9_]*)$/i.exec(trimmed);
    if (tableHeader) {
      const schema = tableHeader[1];
      const table = tableHeader[2];
      currentTableKey = `${schema}.${table}`;
      currentColumnName = "";
      inRulesBlock = false;
      ensureTable(schema, table);
      continue;
    }
    if (/^##\s*regras_negocio\s*$/i.test(trimmed)) {
      currentTableKey = "";
      currentColumnName = "";
      inRulesBlock = true;
      continue;
    }
    if (/^-\s*coluna\s*:/i.test(trimmed)) {
      if (!currentTableKey) {
        extras.push(trimmed);
        continue;
      }
      const col = trimmed.split(":").slice(1).join(":").trim();
      if (!col) continue;
      const table = tables.get(currentTableKey);
      if (!table.columns.has(col)) {
        table.columns.set(col, { name: col, description: "", classification: "" });
      }
      currentColumnName = col;
      continue;
    }
    if (/^-\s+/.test(trimmed)) {
      const value = trimmed.replace(/^-+\s*/, "").trim();
      if (!value) continue;
      if (inRulesBlock) {
        rules.push(value);
      } else {
        extras.push(value);
      }
      continue;
    }
    const kvMatch = /^([A-Za-z_][A-Za-z0-9_.]*)\s*[:=]\s*(.+)$/.exec(trimmed);
    if (!kvMatch) {
      extras.push(trimmed);
      continue;
    }
    const key = String(kvMatch[1] || "").toLowerCase();
    const value = String(kvMatch[2] || "").trim();
    if (!value) continue;
    if (key === "descricao" || key === "description") {
      if (currentTableKey && currentColumnName) {
        const table = tables.get(currentTableKey);
        const col = table.columns.get(currentColumnName);
        col.description = value;
      } else if (currentTableKey) {
        tables.get(currentTableKey).description = value;
      } else {
        rules.push(value);
      }
      continue;
    }
    if (key === "classificacao" || key === "classification") {
      if (currentTableKey && currentColumnName) {
        const normalized = value.toLowerCase();
        const table = tables.get(currentTableKey);
        const col = table.columns.get(currentColumnName);
        col.classification = ALLOWED_CLASSIFICATIONS.has(normalized) ? normalized : "";
      } else {
        extras.push(`${key}: ${value}`);
      }
      continue;
    }
    if (key.split(".").length === 3) {
      const [schema, table, column] = key.split(".");
      const tab = ensureTable(schema, table);
      if (!tab.columns.has(column)) {
        tab.columns.set(column, { name: column, description: value, classification: "" });
      } else if (!tab.columns.get(column).description) {
        tab.columns.get(column).description = value;
      }
      continue;
    }
    if (key === "regra" || key === "rule") {
      rules.push(value);
      continue;
    }
    extras.push(`${key}: ${value}`);
  }

  if (tables.size === 0 && rules.length === 0 && extras.length === 0) {
    return text;
  }

  const out = [];
  out.push("# Template RAG - IAOps");
  out.push("# Ajustado automaticamente");
  out.push("");
  for (const table of tables.values()) {
    out.push(`## tabela: ${table.schema}.${table.table}`);
    if (table.description) {
      out.push(`descricao: ${table.description}`);
      out.push("");
    }
    for (const col of table.columns.values()) {
      out.push(`- coluna: ${col.name}`);
      if (col.description) {
        out.push(`  descricao: ${col.description}`);
      }
      if (col.classification) {
        out.push(`  classificacao: ${col.classification}`);
      }
      out.push("");
    }
  }
  out.push("## regras_negocio");
  if (rules.length === 0 && extras.length === 0) {
    out.push("- Definir regras de negocio para melhorar o entendimento da LLM.");
  } else {
    for (const rule of rules) {
      out.push(`- ${rule}`);
    }
    for (const extra of extras) {
      out.push(`- Observacao: ${extra}`);
    }
  }
  return out.join("\n").trim();
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
  const sourceTypeKey = String(form.source_type || "").trim().toLowerCase();
  const profileDef = PROFILE_BY_TYPE[sourceTypeKey] || PROFILE_BY_TYPE.default;
  const ragValidation = useMemo(() => validateRagText(form.rag_context_text), [form.rag_context_text]);

  useEffect(() => {
    if (!open) return;
    setRagUploadMode("replace");
    setRagPreview("");
    if (initialData) {
      const parsedSecret = parseSecretRef(initialData.conn_secret_ref);
      setForm({
        source_type: initialData.source_type || sourceCatalog?.[0]?.code || "",
        is_active: Boolean(initialData.is_active),
        rag_enabled: Boolean(initialData.rag_enabled),
        rag_context_text: String(initialData.rag_context_text || ""),
        profile: getProfileTemplate(initialData.source_type || sourceCatalog?.[0]?.code || "", parsedSecret),
      });
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
    onSubmit({
      id: initialData?.id,
      source_type: form.source_type,
      conn_secret_ref: buildConnSecretRef(),
      is_active: form.is_active,
      rag_enabled: Boolean(form.rag_enabled),
      rag_context_text: String(form.rag_context_text || "").trim() || null,
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
    const template = [
      "# Template RAG - IAOps",
      "# Use este arquivo para descrever tabelas, colunas e regras de negocio.",
      "",
      "## tabela: vendas.vendas",
      "descricao: Registra as vendas realizadas por cliente.",
      "",
      "- coluna: id",
      "  descricao: Identificador unico da venda",
      "  classificacao: identifier",
      "",
      "- coluna: cliente_id",
      "  descricao: Codigo do cliente (FK da tabela vendas.cliente)",
      "  classificacao: identifier",
      "",
      "- coluna: data_venda",
      "  descricao: Data da venda",
      "  classificacao: temporal",
      "",
      "- coluna: valor_total",
      "  descricao: Valor total da venda",
      "  classificacao: financial",
      "",
      "## regras_negocio",
      "- Considerar apenas registros com status='ativo' para analises gerenciais.",
      "- Datas em timezone America/Sao_Paulo.",
    ].join("\n");
    const blob = new Blob([template], { type: "text/markdown;charset=utf-8" });
    const url = URL.createObjectURL(blob);
    const anchor = document.createElement("a");
    anchor.href = url;
    anchor.download = "rag_template_iaops.md";
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
              onChange={(e) => setForm((prev) => ({ ...prev, rag_enabled: e.target.checked }))}
            />
            Habilitar contexto RAG para esta fonte
          </label>
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
              <textarea rows={6} value={ragPreview} readOnly />
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
