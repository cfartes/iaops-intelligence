import { useEffect, useState } from "react";
import {
  generateBillingInstallment,
  listBillingInstallments,
  payBillingInstallment,
} from "../api/mcpApi";

export default function InstallmentsPanel({ onSystemMessage }) {
  const [items, setItems] = useState([]);
  const [statusFilter, setStatusFilter] = useState("");
  const [showGenerateModal, setShowGenerateModal] = useState(false);
  const [dueDate, setDueDate] = useState("");

  const loadItems = async () => {
    try {
      const data = await listBillingInstallments(statusFilter || undefined);
      setItems(data.installments || []);
    } catch (error) {
      onSystemMessage("error", "Falha parcelas", error.message);
    }
  };

  useEffect(() => {
    loadItems();
  }, [statusFilter]);

  const generateInstallment = async () => {
    if (!dueDate) return;
    try {
      await generateBillingInstallment({ due_date: dueDate });
      setShowGenerateModal(false);
      onSystemMessage("success", "Parcelas", "Parcela gerada com sucesso.");
      await loadItems();
    } catch (error) {
      onSystemMessage("error", "Falha parcelas", error.message);
    }
  };

  const payInstallment = async (id) => {
    try {
      await payBillingInstallment({ installment_id: id });
      onSystemMessage("success", "Parcelas", "Pagamento registrado.");
      await loadItems();
    } catch (error) {
      onSystemMessage("error", "Falha parcelas", error.message);
    }
  };

  return (
    <section className="page-panel">
      <header>
        <h2>Parcelas (Owner)</h2>
        <p>Geracao de parcelas, baixa e acompanhamento de atraso.</p>
      </header>
      <div className="page-actions">
        <button type="button" className="btn btn-primary" onClick={() => setShowGenerateModal(true)}>
          Gerar Parcela (Modal)
        </button>
        <select value={statusFilter} onChange={(e) => setStatusFilter(e.target.value)}>
          <option value="">Status: todos</option>
          <option value="open">open</option>
          <option value="paid">paid</option>
        </select>
        <button type="button" className="btn btn-secondary" onClick={loadItems}>
          Atualizar
        </button>
      </div>
      <div className="table-wrap">
        <table className="data-table">
          <thead><tr><th>ID</th><th>Vencimento</th><th>Valor (cent)</th><th>Status</th><th>Pago em</th><th>Acoes</th></tr></thead>
          <tbody>
            {items.map((item) => (
              <tr key={item.id}>
                <td>{item.id}</td>
                <td>{item.due_date || "-"}</td>
                <td>{item.amount_cents}</td>
                <td>{item.status}</td>
                <td>{item.paid_at || "-"}</td>
                <td>
                  {item.status !== "paid" ? (
                    <button type="button" className="btn btn-small btn-secondary" onClick={() => payInstallment(item.id)}>
                      Baixar pagamento
                    </button>
                  ) : "-"}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>

      {showGenerateModal ? (
        <div className="modal-overlay" role="dialog" aria-modal="true">
          <div className="modal-card form-modal">
            <header className="modal-header"><h3>Gerar parcela</h3></header>
            <div className="modal-content form-grid">
              <label>Data de vencimento (YYYY-MM-DD)
                <input value={dueDate} onChange={(e) => setDueDate(e.target.value)} placeholder="2026-04-10" />
              </label>
              <div className="modal-actions">
                <button type="button" className="btn btn-secondary" onClick={() => setShowGenerateModal(false)}>Cancelar</button>
                <button type="button" className="btn btn-primary" onClick={generateInstallment}>Gerar</button>
              </div>
            </div>
          </div>
        </div>
      ) : null}
    </section>
  );
}

