export default function PagePanel({ title, subtitle, onOpenCreate, onShowAlert }) {
  return (
    <section className="page-panel">
      <header>
        <h2>{title}</h2>
        <p>{subtitle}</p>
      </header>
      <div className="page-actions">
        <button type="button" className="btn btn-primary" onClick={onOpenCreate}>
          Novo Cadastro (Modal)
        </button>
        <button type="button" className="btn btn-secondary" onClick={onShowAlert}>
          Exibir Mensagem do Sistema
        </button>
      </div>
    </section>
  );
}