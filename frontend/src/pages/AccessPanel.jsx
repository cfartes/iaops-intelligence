import { useEffect, useState } from "react";
import { adminResetMfa, createTenant, getSetupProgress, getTenantLimits, listAccessUsers, listClientTenants, updateTenantStatus } from "../api/mcpApi";
import ConfirmActionModal from "../components/ConfirmActionModal";
import TenantFormModal from "../components/TenantFormModal";
import { tUi } from "../i18n/uiText";

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
  }, []);

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
      onSystemMessage("error", tUi("access.fail.createTenant", "Falha ao criar tenant"), error.message);
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
      onSystemMessage("error", tUi("access.fail.updateTenant", "Falha ao atualizar tenant"), error.message);
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
