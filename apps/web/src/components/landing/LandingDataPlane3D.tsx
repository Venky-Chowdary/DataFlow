import { useEffect, useRef, type CSSProperties } from "react";

/**
 * Full-bleed dimensional data plane — source rack → governed engine → warehouse,
 * with live canvas packet flow. Built for Azure-weight product marketing.
 */
export function LandingDataPlane3D() {
  const canvasRef = useRef<HTMLCanvasElement>(null);
  const wrapRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    const canvas = canvasRef.current;
    const wrap = wrapRef.current;
    if (!canvas || !wrap) return;

    const prefersReduced = window.matchMedia("(prefers-reduced-motion: reduce)").matches;
    const ctx = canvas.getContext("2d");
    if (!ctx) return;

    let raf = 0;
    let running = true;
    const packets: { t: number; lane: number; speed: number; size: number; trail: { x: number; y: number }[] }[] = [];

    const resize = () => {
      const dpr = Math.min(window.devicePixelRatio || 1, 2);
      const { width, height } = wrap.getBoundingClientRect();
      canvas.width = Math.max(1, Math.floor(width * dpr));
      canvas.height = Math.max(1, Math.floor(height * dpr));
      canvas.style.width = `${width}px`;
      canvas.style.height = `${height}px`;
      ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
    };

    resize();
    const ro = new ResizeObserver(resize);
    ro.observe(wrap);

    for (let i = 0; i < 22; i++) {
      packets.push({
        t: Math.random(),
        lane: i % 4,
        speed: 0.16 + Math.random() * 0.24,
        size: 2 + Math.random() * 2.6,
        trail: [],
      });
    }

    const sample = (p: (typeof packets)[0], width: number, height: number) => {
      const x0 = width * 0.16;
      const x1 = width * 0.5;
      const x2 = width * 0.84;
      const baseY = height * (0.4 + p.lane * 0.075);
      const clamped = Math.min(1, Math.max(0, p.t));
      const arc = Math.sin(clamped * Math.PI) * (26 + p.lane * 5);
      let x: number;
      if (p.t < 0.5) x = x0 + (x1 - x0) * (p.t / 0.5);
      else x = x1 + (x2 - x1) * ((p.t - 0.5) / 0.5);
      return { x, y: baseY - arc };
    };

    const tick = () => {
      if (!running) return;
      const { width, height } = wrap.getBoundingClientRect();
      ctx.clearRect(0, 0, width, height);

      if (!prefersReduced) {
        for (const p of packets) {
          p.t += p.speed * 0.016;
          if (p.t > 1.2) {
            p.t = -0.08;
            p.trail = [];
          }
          const pos = sample(p, width, height);
          p.trail.push(pos);
          if (p.trail.length > 10) p.trail.shift();

          if (p.trail.length > 1) {
            ctx.beginPath();
            ctx.moveTo(p.trail[0].x, p.trail[0].y);
            for (let i = 1; i < p.trail.length; i++) ctx.lineTo(p.trail[i].x, p.trail[i].y);
            ctx.strokeStyle = "rgba(13, 148, 136, 0.22)";
            ctx.lineWidth = p.size * 0.9;
            ctx.lineCap = "round";
            ctx.stroke();
          }

          const glow = ctx.createRadialGradient(pos.x, pos.y, 0, pos.x, pos.y, p.size * 6);
          glow.addColorStop(0, "rgba(13, 148, 136, 0.95)");
          glow.addColorStop(0.4, "rgba(20, 184, 166, 0.35)");
          glow.addColorStop(1, "rgba(13, 148, 136, 0)");
          ctx.fillStyle = glow;
          ctx.beginPath();
          ctx.arc(pos.x, pos.y, p.size * 6, 0, Math.PI * 2);
          ctx.fill();

          ctx.fillStyle = "#0f766e";
          ctx.beginPath();
          ctx.arc(pos.x, pos.y, p.size, 0, Math.PI * 2);
          ctx.fill();
        }
      }

      raf = requestAnimationFrame(tick);
    };

    raf = requestAnimationFrame(tick);
    return () => {
      running = false;
      cancelAnimationFrame(raf);
      ro.disconnect();
    };
  }, []);

  return (
    <div className="lp-d3 lp-d3--flat" ref={wrapRef} aria-hidden>
      <div className="lp-d3-stage">
        <div className="lp-d3-stage-grid" />

        <div className="lp-d3-world">
          <ServerRack
            className="lp-d3-unit lp-d3-unit--source"
            title="Source"
            badge="PostgreSQL"
            live={4}
            total={6}
          />

          <div className="lp-d3-unit lp-d3-unit--engine">
            <div className="lp-d3-engine">
              <div className="lp-d3-engine-lid" />
              <div className="lp-d3-engine-body">
                <strong>DataFlow</strong>
                <span>governed engine</span>
                <div className="lp-d3-engine-bars">
                  <i style={{ "--p": "0.9" } as CSSProperties} />
                  <i style={{ "--p": "0.72" } as CSSProperties} />
                  <i style={{ "--p": "0.86" } as CSSProperties} />
                </div>
              </div>
              <div className="lp-d3-engine-side" />
            </div>
            <div className="lp-d3-orbit" />
            <div className="lp-d3-orbit lp-d3-orbit--2" />
          </div>

          <ServerRack
            className="lp-d3-unit lp-d3-unit--dest"
            title="Destination"
            badge="Snowflake"
            live={5}
            total={6}
            dest
          />
        </div>

        <aside className="lp-d3-hud lp-d3-hud--map">
          <header><span className="lp-d3-led" /> Semantic map</header>
          <p><code>order_amt</code> → <b>payment_amount</b></p>
          <p><code>cust_id</code> → <b>customer_key</b></p>
          <div className="lp-d3-bar"><span /></div>
          <footer>96% confidence</footer>
        </aside>

        <aside className="lp-d3-hud lp-d3-hud--proof">
          <header><span className="lp-d3-led is-ok" /> Job Theater</header>
          <ul>
            <li><span>Preflight</span><em>8 / 8</em></li>
            <li><span>Write</span><em>12,480</em></li>
            <li><span>Reconcile</span><em>match</em></li>
          </ul>
        </aside>

        <canvas className="lp-d3-packets" ref={canvasRef} />
      </div>
    </div>
  );
}

function ServerRack({
  className,
  title,
  badge,
  live,
  total,
  dest = false,
}: {
  className: string;
  title: string;
  badge: string;
  live: number;
  total: number;
  dest?: boolean;
}) {
  return (
    <div className={className}>
      <div className="lp-d3-server">
        <div className="lp-d3-server-top" />
        <div className="lp-d3-server-front">
          <span className="lp-d3-server-title">{title}</span>
          {Array.from({ length: total }, (_, i) => (
            <span
              key={i}
              className={`lp-d3-slot ${i < live ? "is-on" : ""} ${dest ? "is-dest" : ""}`}
              style={{ "--i": i } as CSSProperties}
            >
              <em /><em /><em /><em />
            </span>
          ))}
        </div>
        <div className="lp-d3-server-side" />
      </div>
      <div className="lp-d3-server-badge">{badge}</div>
    </div>
  );
}
