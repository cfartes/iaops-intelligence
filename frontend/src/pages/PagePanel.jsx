import { tUi } from "../i18n/uiText";

export default function PagePanel({ title, subtitle, onOpenCreate, onShowAlert }) {
  return (
    <section className="page-panel">
      <header>
        <h2>{title}</h2>
        <p>{subtitle}</p>
      </header>
      <div className="page-actions">
        <button type="button" className="btn btn-primary" onClick={onOpenCreate}>
          {tUi("panel.newRecord", "Novo Cadastro (Modal)")}
        </button>
        <button type="button" className="btn btn-secondary" onClick={onShowAlert}>
          {tUi("panel.showSystem", "Exibir Mensagem do Sistema")}
        </button>
      </div>
    </section>
  );
}
