import { useEffect, useState } from "react";
import {
  adminResetMfa,
  createTenant,
  getSetupProgress,
  getTenantLimits,
  listAccessUsers,
  listAuthSessions,
  listClientTenants,
  revokeAuthSession,
  updateTenantStatus,
} from "../api/mcpApi";
import ConfirmActionModal from "../components/ConfirmActionModal";
import TenantFormModal from "../components/TenantFormModal";
import { tUi } from "../i18n/uiText";

function translateLimitMessage(rawMessage) {
  const text = String(rawMessage || "").trim();
  const normalized = text.toLowerCase();
  if (normalized.includes("limite de tenants ativos")) {
    return `${tUi("access.limit.tenants", "Limite de tenants ativos atingido no plano atual.")} ${tUi("access.limit.actionHint", "Desative um registro existente ou altere o plano para continuar.")}`;
  }
  if (normalized.includes("limite de usuarios ativos") || normalized.includes("limite de usuários ativos")) {
    return `${tUi("access.limit.users", "Limite de usuarios ativos atingido no plano atual.")} ${tUi("access.limit.actionHint", "Desative um registro existente ou altere o plano para continuar.")}`;
  }
  return text;
}

export default function AccessPanel({ onSystemMessage }) {
  const [users, setUsers] = useState([]);
  const [loading, setLoading] = useState(false);
  const [pendingResetUser, setPendingResetUser] = useState(null);
  const [tenants, setTenants] = useState([]);
  const [limits, setLimits] = useState(null);
  const [pendingTenantAction, setPendingTenantAction] = useState(null);
  const [tenantModalOpen, setTenantModalOpen] = useState(false);
  const [submitting, setSubmitting] = useState(false);
  const [tenantSetupById, setTenantSetupById] = useState({});
  const [sessions, setSessions] = useState([]);
  const [sessionScope, setSessionScope] = useState("self");
  const [sessionRole, setSessionRole] = useState("viewer");
  const [sessionsLoading, setSessionsLoading] = useState(false);
  const [pendingSessionRevoke, setPendingSessionRevoke] = useState(null);

  const loadUsers = async () => {
    setLoading(true);
    try {
      const data = await listAccessUsers();
      setUsers(data.users || []);
    } catch (error) {
      onSystemMessage("error", tUi("access.fail.users", "Falha ao listar usuarios"), error.message);
    } finally {
      setLoading(false);
    }
  };

  const loadTenants = async () => {
    try {
      const [tenantData, limitData] = await Promise.all([listClientTenants(), getTenantLimits()]);
      const rows = tenantData.tenants || [];
      setTenants(rows);
      setLimits(limitData.limits || null);
      await loadTenantSetup(rows);
    } catch (error) {
      onSystemMessage("error", tUi("access.fail.tenants", "Falha ao listar tenants"), error.message);
    }
  };

  const loadTenantSetup = async (rows) => {
    const settled = await Promise.allSettled(
      rows.map(async (item) => {
        const data = await getSetupProgress(item.id);
        return [item.id, data?.progress || null];
      })
    );
    const next = {};
    settled.forEach((entry) => {
      if (entry.status !== "fulfilled") return;
      const [tenantId, progress] = entry.value;
      next[tenantId] = progress;
    });
    setTenantSetupById(next);
  };

  const setupStatus = (tenantId) => {
    const snapshot = tenantSetupById[tenantId]?.snapshot || {};
    const counts = snapshot.counts || {};
    const done = Number(counts.done || 0);
    const partial = Number(counts.partial || 0);
    const blocked = Number(counts.blocked || 0);
    if (done >= 4) return tUi("access.setup.done", "Concluido");
    if (blocked > 0) return tUi("access.setup.blocked", "Bloqueado");
    if (partial > 0 || done > 0) return tUi("access.setup.partial", "Parcial");
    return tUi("access.setup.pending", "Pendente");
  };

  useEffect(() => {
    loadUsers();
    loadTenants();
    loadSessions();
  }, []);

  const loadSessions = async () => {
    setSessionsLoading(true);
    try {
      const data = await listAuthSessions();
      setSessions(data.sessions || []);
      setSessionScope(data.scope || "self");
      setSessionRole(data.actor_role || "viewer");
    } catch (error) {
      onSystemMessage("error", tUi("access.fail.sessions", "Falha ao listar sessoes ativas"), error.message);
    } finally {
      setSessionsLoading(false);
    }
  };

  const confirmReset = async () => {
    if (!pendingResetUser) return;
    setSubmitting(true);
    try {
      await adminResetMfa({ target_user_id: pendingResetUser.user_id });
      setPendingResetUser(null);
      onSystemMessage(
        "success",
        tUi("access.ok.reset.title", "MFA resetado"),
        tUi("access.ok.reset.message", `MFA resetado para ${pendingResetUser.email}.`, { email: pendingResetUser.email })
      );
      await loadUsers();
      await loadTenants();
    } catch (error) {
      onSystemMessage("error", tUi("access.fail.reset", "Falha no reset de MFA"), error.message);
    } finally {
      setSubmitting(false);
    }
  };

  const submitTenantCreate = async (payload) => {
    setSubmitting(true);
    try {
      await createTenant(payload);
      setTenantModalOpen(false);
      onSystemMessage(
        "success",
        tUi("access.ok.createTenant.title", "Tenant criado"),
        tUi("access.ok.createTenant.message", `Tenant ${payload.name} criado com sucesso.`, { name: payload.name })
      );
      await loadTenants();
    } catch (error) {
      onSystemMessage("error", tUi("access.fail.createTenant", "Falha ao criar tenant"), translateLimitMessage(error.message));
    } finally {
      setSubmitting(false);
    }
  };

  const confirmTenantStatusChange = async () => {
    if (!pendingTenantAction) return;
    setSubmitting(true);
    try {
      await updateTenantStatus({
        tenant_id: pendingTenantAction.tenant.id,
        status: pendingTenantAction.nextStatus,
      });
      setPendingTenantAction(null);
      onSystemMessage(
        "success",
        tUi("access.ok.updateTenant.title", "Tenant atualizado"),
        tUi("access.ok.updateTenant.message", `Tenant ${pendingTenantAction.tenant.name} atualizado para ${pendingTenantAction.nextStatus}.`, {
          name: pendingTenantAction.tenant.name,
          status: pendingTenantAction.nextStatus,
        })
      );
      await loadTenants();
    } catch (error) {
      onSystemMessage("error", tUi("access.fail.updateTenant", "Falha ao atualizar tenant"), translateLimitMessage(error.message));
    } finally {
      setSubmitting(false);
    }
  };

  const formatEpoch = (value) => {
    const num = Number(value || 0);
    if (!num) return "-";
    return new Date(num * 1000).toLocaleString();
  };

  const confirmSessionRevoke = async () => {
    if (!pendingSessionRevoke) return;
    setSubmitting(true);
    try {
      await revokeAuthSession({ session_token: pendingSessionRevoke.session_token });
      setPendingSessionRevoke(null);
      onSystemMessage(
        "success",
        tUi("access.ok.revokeSession.title", "Sessao revogada"),
        tUi("access.ok.revokeSession.message", "Sessao de {email} foi revogada.", { email: pendingSessionRevoke.email })
      );
      await loadSessions();
    } catch (error) {
      onSystemMessage("error", tUi("access.fail.revokeSession", "Falha ao revogar sessao"), error.message);
    } finally {
      setSubmitting(false);
    }
  };

  return (
    <section className="page-panel">
      <header>
        <h2>{tUi("access.header.title", "Acesso")}</h2>
        <p>{tUi("access.header.subtitle", "Gestao de usuarios por tenant e reset MFA (admin/owner).")}</p>
      </header>
      <div className="page-actions">
        <button type="button" className="btn btn-secondary" onClick={loadUsers}>
          {tUi("access.refreshUsers", "Atualizar Usuarios")}
        </button>
      </div>
      {loading ? (
        <p className="empty-state">{tUi("access.loadingUsers", "Carregando usuarios...")}</p>
      ) : users.length === 0 ? (
        <p className="empty-state">{tUi("access.emptyUsers", "Nenhum usuario encontrado.")}</p>
      ) : (
        <div className="table-wrap">
          <table className="data-table">
            <thead>
              <tr>
                <th>User ID</th>
                <th>Nome</th>
                <th>Email</th>
                <th>Role</th>
                <th>Status</th>
                <th>MFA</th>
                <th>Acoes</th>
              </tr>
            </thead>
            <tbody>
              {users.map((item) => (
                <tr key={item.user_id}>
                  <td>{item.user_id}</td>
                  <td>{item.full_name}</td>
                  <td>{item.email}</td>
                  <td>{item.role}</td>
                  <td>{item.is_active ? tUi("access.status.active", "Ativo") : tUi("access.status.inactive", "Inativo")}</td>
                  <td>{item.mfa_enabled ? tUi("access.mfa.enabled", "Habilitado") : tUi("access.mfa.disabled", "Desabilitado")}</td>
                  <td>
                    <button
                      type="button"
                      className="btn btn-small btn-secondary"
                      onClick={() => setPendingResetUser(item)}
                    >
                      {tUi("access.resetMfa", "Reset MFA")}
                    </button>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}

      <section className="catalog-block">
        <div className="section-header">
          <h3>{tUi("access.tenants.title", "Tenants do cliente")}</h3>
          <button type="button" className="btn btn-primary btn-small" onClick={() => setTenantModalOpen(true)}>
            {tUi("access.tenants.new", "Novo Tenant")}
          </button>
        </div>
        <p className="muted">
          {tUi("access.tenants.limit", `Limite do plano: ${limits?.active_tenants ?? 0}/${limits?.max_tenants ?? 0} tenants ativos.`, {
            active: limits?.active_tenants ?? 0,
            max: limits?.max_tenants ?? 0,
          })}
        </p>
        <p className="muted">
          {`Fontes ativas: ${limits?.active_data_sources ?? 0}/${limits?.max_data_sources_per_client ?? 0} (cliente) | ${limits?.active_data_sources_tenant ?? 0}/${limits?.max_data_sources_per_tenant ?? 0} (tenant atual).`}
        </p>
        {tenants.length === 0 ? (
          <p className="empty-state">{tUi("access.tenants.empty", "Nenhum tenant encontrado.")}</p>
        ) : (
          <div className="table-wrap">
            <table className="data-table">
              <thead>
                <tr>
                  <th>ID</th>
                  <th>Nome</th>
                  <th>Slug</th>
                  <th>Status</th>
                  <th>{tUi("access.setup.column", "Setup")}</th>
                  <th>Acoes</th>
                </tr>
              </thead>
              <tbody>
                {tenants.map((item) => (
                  <tr key={item.id}>
                    <td>{item.id}</td>
                    <td>{item.name}</td>
                    <td>{item.slug}</td>
                    <td>{item.status}</td>
                    <td>
                      <span className="chip">{setupStatus(item.id)}</span>
                    </td>
                    <td>
                      <button
                        type="button"
                        className="btn btn-small btn-secondary"
                        onClick={() =>
                          setPendingTenantAction({
                            tenant: item,
                            nextStatus: item.status === "active" ? "disabled" : "active",
                          })
                        }
                      >
                        {item.status === "active"
                          ? tUi("access.tenant.disable", "Desabilitar")
                          : tUi("access.tenant.reactivate", "Reativar")}
                      </button>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
      </section>

      <section className="catalog-block">
        <div className="section-header">
          <h3>{tUi("access.sessions.title", "Sessoes ativas")}</h3>
          <button type="button" className="btn btn-secondary btn-small" onClick={loadSessions}>
            {tUi("common.refresh", "Atualizar")}
          </button>
        </div>
        <p className="muted">
          {tUi("access.sessions.scope", "Escopo: {scope} | Perfil: {role}", {
            scope:
              sessionScope === "client"
                ? tUi("access.sessions.scope.client", "cliente")
                : tUi("access.sessions.scope.user", "usuario"),
            role: sessionRole,
          })}
        </p>
        {sessionsLoading ? (
          <p className="empty-state">{tUi("access.sessions.loading", "Carregando sessoes...")}</p>
        ) : sessions.length === 0 ? (
          <p className="empty-state">{tUi("access.sessions.empty", "Nenhuma sessao ativa.")}</p>
        ) : (
          <div className="table-wrap">
            <table className="data-table">
              <thead>
                <tr>
                  <th>{tUi("access.sessions.col.email", "Email")}</th>
                  <th>{tUi("access.sessions.col.role", "Role")}</th>
                  <th>{tUi("access.sessions.col.tenant", "Tenant")}</th>
                  <th>{tUi("access.sessions.col.issued", "Emitida em")}</th>
                  <th>{tUi("access.sessions.col.lastSeen", "Ultima atividade")}</th>
                  <th>{tUi("access.sessions.col.expires", "Expira em")}</th>
                  <th>{tUi("access.sessions.col.actions", "Acoes")}</th>
                </tr>
              </thead>
              <tbody>
                {sessions.map((item) => (
                  <tr key={item.session_token}>
                    <td>
                      {item.email}
                      {item.is_current ? tUi("access.sessions.currentTag", " (atual)") : ""}
                    </td>
                    <td>{item.role}</td>
                    <td>{item.tenant_name || item.tenant_id}</td>
                    <td>{formatEpoch(item.issued_at_epoch)}</td>
                    <td>{formatEpoch(item.last_seen_epoch)}</td>
                    <td>{formatEpoch(item.session_expires_at_epoch)}</td>
                    <td>
                      <button
                        type="button"
                        className="btn btn-small btn-secondary"
                        onClick={() => setPendingSessionRevoke(item)}
                        disabled={submitting}
                      >
                        {item.is_current
                          ? tUi("access.sessions.endCurrent", "Encerrar atual")
                          : tUi("access.sessions.revoke", "Revogar")}
                      </button>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
      </section>

      <ConfirmActionModal
        open={Boolean(pendingResetUser)}
        title={tUi("access.modal.reset.title", "Resetar MFA do usuario")}
        message={
          pendingResetUser
            ? tUi(
                "access.modal.reset.message",
                `Resetar MFA de ${pendingResetUser.email}? O usuario precisara configurar novamente.`,
                { email: pendingResetUser.email }
              )
            : ""
        }
        confirmLabel={tUi("access.modal.reset.confirm", "Resetar MFA")}
        loading={submitting}
        onConfirm={confirmReset}
        onClose={() => {
          if (!submitting) setPendingResetUser(null);
        }}
      />

      <ConfirmActionModal
        open={Boolean(pendingTenantAction)}
        title={tUi("access.modal.status.title", "Atualizar status do tenant")}
        message={
          pendingTenantAction
            ? tUi(
                "access.modal.status.message",
                `Deseja ${pendingTenantAction.nextStatus === "active" ? "reativar" : "desabilitar"} o tenant ${
                  pendingTenantAction.tenant.name
                }?`,
                {
                  action:
                    pendingTenantAction.nextStatus === "active"
                      ? tUi("access.tenant.reactivate.lower", "reativar")
                      : tUi("access.tenant.disable.lower", "desabilitar"),
                  name: pendingTenantAction.tenant.name,
                }
              )
            : ""
        }
        confirmLabel={tUi("common.confirm", "Confirmar")}
        loading={submitting}
        onConfirm={confirmTenantStatusChange}
        onClose={() => {
          if (!submitting) setPendingTenantAction(null);
        }}
      />

      <ConfirmActionModal
        open={Boolean(pendingSessionRevoke)}
        title={tUi("access.sessions.modal.title", "Revogar sessao")}
        message={
          pendingSessionRevoke
            ? tUi("access.sessions.modal.message", "Deseja revogar a sessao de {email}{current}?", {
                email: pendingSessionRevoke.email,
                current: pendingSessionRevoke.is_current ? tUi("access.sessions.currentTag", " (atual)") : "",
              })
            : ""
        }
        confirmLabel={tUi("common.confirm", "Confirmar")}
        loading={submitting}
        onConfirm={confirmSessionRevoke}
        onClose={() => {
          if (!submitting) setPendingSessionRevoke(null);
        }}
      />

      <TenantFormModal
        open={tenantModalOpen}
        loading={submitting}
        onClose={() => {
          if (!submitting) setTenantModalOpen(false);
        }}
        onSubmit={submitTenantCreate}
      />
    </section>
  );
}
