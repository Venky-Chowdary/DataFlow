import { useEffect, useRef } from "react";

/**
 * Clean enterprise hero stage: source → map → destination with living packet trails.
 * Replaces the heavier rack/3D composition with a product-forward motion plane.
 */
export function LandingHeroFlow() {
  const canvasRef = useRef<HTMLCanvasElement>(null);
  const wrapRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    const canvas = canvasRef.current;
    const wrap = wrapRef.current;
    if (!canvas || !wrap) return;

    const reduced = window.matchMedia("(prefers-reduced-motion: reduce)").matches;
    const ctx = canvas.getContext("2d");
    if (!ctx) return;

    let raf = 0;
    let running = true;
    let t0 = performance.now();

    type Packet = { t: number; lane: number; speed: number };
    const packets: Packet[] = Array.from({ length: 14 }, (_, i) => ({
      t: Math.random(),
      lane: i % 3,
      speed: 0.12 + Math.random() * 0.18,
    }));

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

    const pathY = (lane: number, h: number) => h * (0.38 + lane * 0.12);

    const tick = (now: number) => {
      if (!running) return;
      const { width, height } = wrap.getBoundingClientRect();
      ctx.clearRect(0, 0, width, height);

      const elapsed = (now - t0) / 1000;
      const x0 = width * 0.18;
      const x1 = width * 0.5;
      const x2 = width * 0.82;

      // Soft guide curves
      for (let lane = 0; lane < 3; lane++) {
        const y = pathY(lane, height);
        ctx.beginPath();
        ctx.moveTo(x0, y);
        ctx.bezierCurveTo(x0 + width * 0.12, y - 18, x1 - width * 0.1, y + 18, x1, y);
        ctx.bezierCurveTo(x1 + width * 0.1, y - 14, x2 - width * 0.1, y + 14, x2, y);
        ctx.strokeStyle = "rgba(13, 148, 136, 0.14)";
        ctx.lineWidth = 1.5;
        ctx.stroke();
      }

      if (!reduced) {
        for (const p of packets) {
          p.t += p.speed * 0.016;
          if (p.t > 1.15) p.t = -0.05;
          const u = Math.min(1, Math.max(0, p.t));
          const yBase = pathY(p.lane, height);
          const bob = Math.sin((u + elapsed * 0.4) * Math.PI * 2) * 6;
          let x: number;
          let y: number;
          if (u < 0.5) {
            const s = u / 0.5;
            x = x0 + (x1 - x0) * s;
            y = yBase + bob - Math.sin(s * Math.PI) * 16;
          } else {
            const s = (u - 0.5) / 0.5;
            x = x1 + (x2 - x1) * s;
            y = yBase + bob - Math.sin(s * Math.PI) * 12;
          }

          const glow = ctx.createRadialGradient(x, y, 0, x, y, 14);
          glow.addColorStop(0, "rgba(15, 118, 110, 0.85)");
          glow.addColorStop(0.45, "rgba(20, 184, 166, 0.28)");
          glow.addColorStop(1, "rgba(13, 148, 136, 0)");
          ctx.fillStyle = glow;
          ctx.beginPath();
          ctx.arc(x, y, 14, 0, Math.PI * 2);
          ctx.fill();

          ctx.fillStyle = "#0f766e";
          ctx.beginPath();
          ctx.arc(x, y, 3.2, 0, Math.PI * 2);
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
    <div className="lp-hero-flow" ref={wrapRef} aria-hidden>
      <div className="lp-hero-flow-stage">
        <div className="lp-hero-flow-glow" />
        <canvas className="lp-hero-flow-canvas" ref={canvasRef} />

        <article className="lp-hero-flow-card lp-hero-flow-card--source">
          <header>
            <span className="lp-hero-flow-dot is-src" />
            Source
          </header>
          <strong>PostgreSQL</strong>
          <p>orders · 12.4k rows</p>
          <ul>
            <li>order_amt</li>
            <li>cust_email</li>
            <li>cust_id</li>
          </ul>
        </article>

        <article className="lp-hero-flow-card lp-hero-flow-card--engine">
          <header>
            <span className="lp-hero-flow-dot is-eng" />
            Governed engine
          </header>
          <strong>Map · Preflight · Prove</strong>
          <div className="lp-hero-flow-meters">
            <div><span>Semantic map</span><em>96%</em></div>
            <div><span>Preflight</span><em>8 / 8</em></div>
            <div><span>Checksum</span><em>match</em></div>
          </div>
          <div className="lp-hero-flow-pulse" />
        </article>

        <article className="lp-hero-flow-card lp-hero-flow-card--dest">
          <header>
            <span className="lp-hero-flow-dot is-dst" />
            Destination
          </header>
          <strong>Snowflake</strong>
          <p>ANALYTICS.ORDERS</p>
          <ul>
            <li>payment_amount</li>
            <li>email</li>
            <li>customer_key</li>
          </ul>
        </article>
      </div>
    </div>
  );
}
