import type { StatusTile } from "../types";

const TILE_CLASS: Record<StatusTile["status"], string> = {
  active: "df-status-tile--active",
  warning: "df-status-tile--warning",
  broken: "df-status-tile--broken",
  idle: "df-status-tile--idle",
};

interface StatusTileGridProps {
  tiles: StatusTile[];
  title?: string;
}

export function StatusTileGrid({ tiles, title }: StatusTileGridProps) {
  return (
    <div className="df-status-tile-section">
      {title && <div className="df-section-title">{title}</div>}
      <div className="df-status-tile-grid">
        {tiles.map((tile) => (
          <div
            key={tile.id}
            className={["df-status-tile", TILE_CLASS[tile.status]].join(" ")}
            title={tile.label}
          >
            <span className="df-status-tile-count">{tile.count ?? "—"}</span>
            <span className="df-status-tile-label">{tile.label}</span>
          </div>
        ))}
      </div>
    </div>
  );
}
