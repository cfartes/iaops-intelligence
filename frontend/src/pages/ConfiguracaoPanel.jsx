import { useEffect, useState } from "react";
import {
  beginMfaSetup,
  disableMfa,
  enableMfa,
  getAdminLlmConfig,
  getMfaStatus,
  getTenantLlmConfig,
  listAdminLlmProviders,
  listTenantLlmProviders,
  updateTenantLlmConfig,
  updateAdminLlmConfig,
} from "../api/mcpApi";
import AppLlmConfigModal from "../components/AppLlmConfigModal";
import MfaCodeModal from "../components/MfaCodeModal";
import TenantLlmConfigModal from "../components/TenantLlmConfigModal";

export default function ConfiguracaoPanel({ onSystemMessage }) {
  const [mfa, setMfa] = useState(null);
  const [loading, setLoading] = useState(false);
  const [setupInfo, setSetupInfo] = useState(null);
  const [modalMode, setModalMode] = useState(null);
  const [submitting, setSubmitting] = useState(false);
  const [llmProviders, setLlmProviders] = useState([]);
  const [llmConfig, setLlmConfig] = useState(null);
  const [llmDenied, setLlmDenied] = useState(false);
  const [llmModalOpen, setLlmModalOpen] = useState(false);
  const [tenantLlmProviders, setTenantLlmProviders] = useState([]);
  const [tenantLlmConfig, setTenantLlmConfig] = useState(null);
  const [tenantLlmModalOpen, setTenantLlmModalOpen] = useState(false);

  const loadStatus = async () => {
    setLoading(true);
    try {
      const data = await getMfaStatus();
      setMfa(data.mfa || null);
    } catch (error) {
      onSystemMessage("error", "Falha ao carregar MFA", error.message);
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    loadStatus();
    loadLlmAdmin();
    loadTenantLlmConfig();
  }, []);

  const loadLlmAdmin = async () => {
    try {
      const [providersData, configData] = await Promise.all([listAdminLlmProviders(), getAdminLlmConfig()]);
      setLlmProviders(providersData.providers || []);
      setLlmConfig(configData.config || null);
      setLlmDenied(false);
    } catch (error) {
      setLlmDenied(true);
    }
  };

  const loadTenantLlmConfig = async () => {
    try {
      const [providersData, cfgData] = await Promise.all([listTenantLlmProviders(), getTenantLlmConfig()]);
      setTenantLlmProviders(providersData.providers || []);
      setTenantLlmConfig(cfgData.config || null);
    } catch (error) {
      onSystemMessage("error", "Falha ao carregar LLM do tenant", error.message);
    }
  };

  const startSetup = async () => {
    setSubmitting(true);
    try {
      const data = await beginMfaSetup({ issuer: "IAOps Governance" });
      setSetupInfo(data.setup);
      setModalMode("enable");
    } catch (error) {
      onSystemMessage("error", "Falha ao iniciar setup MFA", error.message);
    } finally {
      setSubmitting(false);
    }
  };

  const submitEnable = async ({ otp_code }) => {
    setSubmitting(true);
    try {
      await enableMfa({ otp_code });
      setModalMode(null);
      setSetupInfo(null);
      onSystemMessage("success", "MFA habilitado", "MFA TOTP habilitado com sucesso.");
      await loadStatus();
    } catch (error) {
      onSystemMessage("error", "Falha ao habilitar MFA", error.message);
    } finally {
      setSubmitting(false);
    }
  };

  const submitDisable = async ({ otp_code }) => {
    setSubmitting(true);
    try {
      await disableMfa({ otp_code });
      setModalMode(null);
      setSetupInfo(null);
      onSystemMessage("success", "MFA desabilitado", "MFA desabilitado para seu usuario.");
      await loadStatus();
    } catch (error) {
      onSystemMessage("error", "Falha ao desabilitar MFA", error.message);
    } finally {
      setSubmitting(false);
    }
  };

  const submitLlmConfig = async (payload) => {
    setSubmitting(true);
    try {
      const data = await updateAdminLlmConfig(payload);
      setLlmConfig(data.config || null);
      setLlmModalOpen(false);
      onSystemMessage("success", "LLM do app atualizada", "Configuracao da LLM padrao atualizada com sucesso.");
    } catch (error) {
      onSystemMessage("error", "Falha ao atualizar LLM do app", error.message);
    } finally {
      setSubmitting(false);
    }
  };

  const submitTenantLlmConfig = async (payload) => {
    setSubmitting(true);
    try {
      const data = await updateTenantLlmConfig(payload);
      setTenantLlmConfig(data.config || null);
      setTenantLlmModalOpen(false);
      onSystemMessage("success", "LLM do tenant atualizada", "Configuracao de LLM do tenant salva com sucesso.");
    } catch (error) {
      onSystemMessage("error", "Falha ao salvar LLM do tenant", error.message);
    } finally {
      setSubmitting(false);
    }
  };

  return (
    <section className="page-panel">
      <header>
        <h2>Configuracao</h2>
        <p>MFA por usuario com TOTP (ativacao voluntaria).</p>
      </header>
      {loading ? (
        <p className="empty-state">Carregando configuracao MFA...</p>
      ) : (
        <>
          <div className="metric-grid">
            <article className="metric-card">
              <h4>Status MFA</h4>
              <p>{mfa?.enabled ? "Habilitado" : "Desabilitado"}</p>
            </article>
            <article className="metric-card">
              <h4>Setup pendente</h4>
              <p>{mfa?.has_pending_setup ? "Sim" : "Nao"}</p>
            </article>
          </div>

          <div className="page-actions">
            <button type="button" className="btn btn-primary" onClick={startSetup} disabled={submitting}>
              Ativar MFA
            </button>
            <button
              type="button"
              className="btn btn-secondary"
              onClick={() => setModalMode("disable")}
              disabled={submitting || !mfa?.enabled}
            >
              Desativar MFA
            </button>
            <button type="button" className="btn btn-secondary" onClick={loadStatus}>
              Atualizar Status
            </button>
          </div>
        </>
      )}

      <section className="catalog-block">
        <header>
          <h3>LLM do Tenant</h3>
        </header>
        <div className="table-wrap">
          <table className="data-table">
            <tbody>
              <tr>
                <th>Usar LLM do app</th>
                <td>{tenantLlmConfig?.use_app_default_llm ? "Sim" : "Nao"}</td>
              </tr>
              <tr>
                <th>Provedor</th>
                <td>{tenantLlmConfig?.provider_name || "-"}</td>
              </tr>
              <tr>
                <th>Modelo</th>
                <td>{tenantLlmConfig?.model_code || "-"}</td>
              </tr>
              <tr>
                <th>Endpoint</th>
                <td>{tenantLlmConfig?.endpoint_url || "-"}</td>
              </tr>
            </tbody>
          </table>
        </div>
        <div className="page-actions">
          <button type="button" className="btn btn-primary" onClick={() => setTenantLlmModalOpen(true)} disabled={submitting}>
            Configurar LLM do Tenant
          </button>
          <button type="button" className="btn btn-secondary" onClick={loadTenantLlmConfig}>
            Atualizar LLM Tenant
          </button>
        </div>
      </section>

      <section className="catalog-block">
        <header>
          <h3>LLM Padrao do App (Superadmin)</h3>
        </header>
        {llmDenied ? (
          <p className="empty-state">Acesso restrito a superadmin.</p>
        ) : (
          <>
            <div className="table-wrap">
              <table className="data-table">
                <tbody>
                  <tr>
                    <th>Provedor</th>
                    <td>{llmConfig?.provider_name || "-"}</td>
                  </tr>
                  <tr>
                    <th>Modelo</th>
                    <td>{llmConfig?.model_code || "-"}</td>
                  </tr>
                  <tr>
                    <th>Endpoint</th>
                    <td>{llmConfig?.endpoint_url || "-"}</td>
                  </tr>
                  <tr>
                    <th>Secret Ref</th>
                    <td>{llmConfig?.secret_ref || "-"}</td>
                  </tr>
                </tbody>
              </table>
            </div>
            <div className="page-actions">
              <button type="button" className="btn btn-primary" onClick={() => setLlmModalOpen(true)} disabled={submitting}>
                Configurar LLM do App
              </button>
              <button type="button" className="btn btn-secondary" onClick={loadLlmAdmin}>
                Atualizar LLM
              </button>
            </div>
          </>
        )}
      </section>

      <MfaCodeModal
        open={modalMode === "enable"}
        title="Ativar MFA TOTP"
        subtitle="Escaneie o secret/URI no app autenticador e informe o codigo para ativar."
        setupInfo={setupInfo}
        submitLabel="Confirmar Ativacao"
        loading={submitting}
        onClose={() => {
          if (!submitting) setModalMode(null);
        }}
        onSubmit={submitEnable}
      />

      <MfaCodeModal
        open={modalMode === "disable"}
        title="Desativar MFA TOTP"
        subtitle="Informe o codigo TOTP atual para confirmar a desativacao."
        setupInfo={null}
        submitLabel="Confirmar Desativacao"
        loading={submitting}
        onClose={() => {
          if (!submitting) setModalMode(null);
        }}
        onSubmit={submitDisable}
      />

      <AppLlmConfigModal
        open={llmModalOpen}
        providers={llmProviders}
        initialConfig={llmConfig}
        loading={submitting}
        onClose={() => {
          if (!submitting) setLlmModalOpen(false);
        }}
        onSubmit={submitLlmConfig}
      />

      <TenantLlmConfigModal
        open={tenantLlmModalOpen}
        providers={tenantLlmProviders}
        initialConfig={tenantLlmConfig}
        loading={submitting}
        onClose={() => {
          if (!submitting) setTenantLlmModalOpen(false);
        }}
        onSubmit={submitTenantLlmConfig}
      />
    </section>
  );
}
