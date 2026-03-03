export default function SideMenu({ items, activeKey, onSelect, logoutLabel, onLogout }) {
  return (
    <aside className="side-menu">
      <div className="brand">
        <h1>IAOps</h1>
        <p>Governance</p>
      </div>
      <nav>
        {items.map((item) => (
          <button
            type="button"
            key={item.key}
            className={activeKey === item.key ? "menu-item active" : "menu-item"}
            onClick={() => onSelect(item.key)}
          >
            {item.label}
          </button>
        ))}
        {onLogout ? (
          <button type="button" className="menu-item menu-item-logout" onClick={onLogout}>
            {logoutLabel || "Sair"}
          </button>
        ) : null}
      </nav>
    </aside>
  );
}
