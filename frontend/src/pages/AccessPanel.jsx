import { useEffect, useState } from "react";
import { adminResetMfa, createTenant, getTenantLimits, listAccessUsers, listClientTenants, updateTenantStatus } from "../api/mcpApi";
import ConfirmActionModal from "../components/ConfirmActionModal";
import TenantFormModal from "../components/TenantFormModal";

export default function AccessPanel({ onSystemMessage }) {
  const [users, setUsers] = useState([]);
  const [loading, setLoading] = useState(false);
  const [pendingResetUser, setPendingResetUser] = useState(null);
  const [tenants, setTenants] = useState([]);
  const [limits, setLimits] = useState(null);
  const [pendingTenantAction, setPendingTenantAction] = useState(null);
  const [tenantModalOpen, setTenantModalOpen] = useState(false);
  const [submitting, setSubmitting] = useState(false);

  const loadUsers = async () => {
    setLoading(true);
    try {
      const data = await listAccessUsers();
      setUsers(data.users || []);
    } catch (error) {
      onSystemMessage("error", "Falha ao listar usuarios", error.message);
    } finally {
      setLoading(false);
    }
  };

  const loadTenants = async () => {
    try {
      const [tenantData, limitData] = await Promise.all([listClientTenants(), getTenantLimits()]);
      setTenants(tenantData.tenants || []);
      setLimits(limitData.limits || null);
    } catch (error) {
      onSystemMessage("error", "Falha ao listar tenants", error.message);
    }
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
      onSystemMessage("success", "MFA resetado", `MFA resetado para ${pendingResetUser.email}.`);
      await loadUsers();
      await loadTenants();
    } catch (error) {
      onSystemMessage("error", "Falha no reset de MFA", error.message);
    } finally {
      setSubmitting(false);
    }
  };

  const submitTenantCreate = async (payload) => {
    setSubmitting(true);
    try {
      await createTenant(payload);
      setTenantModalOpen(false);
      onSystemMessage("success", "Tenant criado", `Tenant ${payload.name} criado com sucesso.`);
      await loadTenants();
    } catch (error) {
      onSystemMessage("error", "Falha ao criar tenant", error.message);
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
        "Tenant atualizado",
        `Tenant ${pendingTenantAction.tenant.name} atualizado para ${pendingTenantAction.nextStatus}.`
      );
      await loadTenants();
    } catch (error) {
      onSystemMessage("error", "Falha ao atualizar tenant", error.message);
    } finally {
      setSubmitting(false);
    }
  };

  return (
    <section className="page-panel">
      <header>
        <h2>Acesso</h2>
        <p>Gestao de usuarios por tenant e reset MFA (admin/owner).</p>
      </header>
      <div className="page-actions">
        <button type="button" className="btn btn-secondary" onClick={loadUsers}>
          Atualizar Usuarios
        </button>
      </div>
      {loading ? (
        <p className="empty-state">Carregando usuarios...</p>
      ) : users.length === 0 ? (
        <p className="empty-state">Nenhum usuario encontrado.</p>
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
                  <td>{item.is_active ? "Ativo" : "Inativo"}</td>
                  <td>{item.mfa_enabled ? "Habilitado" : "Desabilitado"}</td>
                  <td>
                    <button
                      type="button"
                      className="btn btn-small btn-secondary"
                      onClick={() => setPendingResetUser(item)}
                    >
                      Reset MFA
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
          <h3>Tenants do cliente</h3>
          <button type="button" className="btn btn-primary btn-small" onClick={() => setTenantModalOpen(true)}>
            Novo Tenant
          </button>
        </div>
        <p className="muted">
          Limite do plano: {limits?.active_tenants ?? 0}/{limits?.max_tenants ?? 0} tenants ativos.
        </p>
        {tenants.length === 0 ? (
          <p className="empty-state">Nenhum tenant encontrado.</p>
        ) : (
          <div className="table-wrap">
            <table className="data-table">
              <thead>
                <tr>
                  <th>ID</th>
                  <th>Nome</th>
                  <th>Slug</th>
                  <th>Status</th>
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
                        {item.status === "active" ? "Desabilitar" : "Reativar"}
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
        title="Resetar MFA do usuario"
        message={
          pendingResetUser
            ? `Resetar MFA de ${pendingResetUser.email}? O usuario precisara configurar novamente.`
            : ""
        }
        confirmLabel="Resetar MFA"
        loading={submitting}
        onConfirm={confirmReset}
        onClose={() => {
          if (!submitting) setPendingResetUser(null);
        }}
      />

      <ConfirmActionModal
        open={Boolean(pendingTenantAction)}
        title="Atualizar status do tenant"
        message={
          pendingTenantAction
            ? `Deseja ${pendingTenantAction.nextStatus === "active" ? "reativar" : "desabilitar"} o tenant ${
                pendingTenantAction.tenant.name
              }?`
            : ""
        }
        confirmLabel="Confirmar"
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
