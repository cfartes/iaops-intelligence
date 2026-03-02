export default function SideMenu({ items, activeKey, onSelect }) {
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
      </nav>
    </aside>
  );
}