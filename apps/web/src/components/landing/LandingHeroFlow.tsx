import { useEffect, useRef } from "react";
import { useInView } from "../../hooks/useInView";

/**
 * Hero product proof: source → governed engine → destination
 * over a living data-wave field (canvas). Compact — no empty void.
 */
export function LandingHeroFlow() {
  const canvasRef = useRef<HTMLCanvasElement>(null);
  const { ref: wrapRef, inView } = useInView<HTMLDivElement>("100px 0px");
  const inViewRef = useRef(inView);
  inViewRef.current = inView;

  useEffect(() => {
    const canvas = canvasRef.current;
    const wrap = wrapRef.current;
    if (!canvas || !wrap) return;

    const reduced = window.matchMedia("(prefers-reduced-motion: reduce)").matches;
    const ctx = canvas.getContext("2d", { alpha: true });
    if (!ctx) return;

    let raf = 0;
    let running = true;
    let t0 = performance.now();

    type Packet = { t: number; lane: number; speed: number; hue: number };
    const packets: Packet[] = Array.from({ length: 10 }, (_, i) => ({
      t: Math.random(),
      lane: i % 4,
      speed: 0.08 + Math.random() * 0.14,
      hue: i % 3,
    }));

    const resize = () => {
      const dpr = Math.min(window.devicePixelRatio || 1, 1.75);
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

    const colors = ["#2dd4bf", "#14b8a6", "#f59e0b"];

    const tick = (now: number) => {
      if (!running) return;
      if (!inViewRef.current) {
        raf = requestAnimationFrame(tick);
        return;
      }

      const { width, height } = wrap.getBoundingClientRect();
      ctx.clearRect(0, 0, width, height);

      const elapsed = (now - t0) / 1000;

      // Soft field wash
      const grad = ctx.createLinearGradient(0, 0, width, height);
      grad.addColorStop(0, "rgba(45, 212, 191, 0.08)");
      grad.addColorStop(0.55, "rgba(15, 118, 110, 0.04)");
      grad.addColorStop(1, "rgba(245, 158, 11, 0.06)");
      ctx.fillStyle = grad;
      ctx.fillRect(0, 0, width, height);

      // Multi-lane wave ribbons
      for (let lane = 0; lane < 4; lane++) {
        const yBase = height * (0.22 + lane * 0.2);
        const amp = 6 + lane * 2;
        ctx.beginPath();
        for (let x = 0; x <= width; x += 6) {
          const y =
            yBase +
            Math.sin(x * 0.018 + elapsed * (0.9 + lane * 0.15) + lane) * amp +
            Math.sin(x * 0.007 + elapsed * 0.4) * 3;
          if (x === 0) ctx.moveTo(x, y);
          else ctx.lineTo(x, y);
        }
        ctx.strokeStyle =
          lane === 1 ? "rgba(245, 158, 11, 0.28)" : "rgba(45, 212, 191, 0.28)";
        ctx.lineWidth = lane === 1 ? 1.75 : 1.25;
        ctx.stroke();
      }

      // Vertical pulse columns (data ticks)
      for (let i = 0; i < 7; i++) {
        const x = width * (0.12 + i * 0.12);
        const pulse = 0.35 + 0.65 * (0.5 + 0.5 * Math.sin(elapsed * 1.6 + i));
        ctx.strokeStyle = `rgba(94, 234, 212, ${0.08 + pulse * 0.12})`;
        ctx.lineWidth = 1;
        ctx.beginPath();
        ctx.moveTo(x, height * 0.12);
        ctx.lineTo(x, height * 0.88);
        ctx.stroke();
      }

      if (!reduced) {
        for (const p of packets) {
          p.t += p.speed * 0.016;
          if (p.t > 1.12) p.t = -0.05;
          const u = Math.min(1, Math.max(0, p.t));
          const yBase = height * (0.22 + p.lane * 0.2);
          const amp = 6 + p.lane * 2;
          const x = width * (0.06 + u * 0.88);
          const y =
            yBase +
            Math.sin(x * 0.018 + elapsed * (0.9 + p.lane * 0.15) + p.lane) * amp +
            Math.sin((u + elapsed * 0.5) * Math.PI * 2) * 3;

          const c = colors[p.hue];
          ctx.fillStyle = `${c}33`;
          ctx.beginPath();
          ctx.arc(x, y, 8, 0, Math.PI * 2);
          ctx.fill();

          ctx.fillStyle = c;
          ctx.beginPath();
          ctx.arc(x, y, 2.8, 0, Math.PI * 2);
          ctx.fill();
        }

        // Center seal pulse
        const cx = width * 0.5;
        const cy = height * 0.5;
        const seal = 0.5 + 0.5 * Math.sin(elapsed * 2.2);
        ctx.strokeStyle = `rgba(45, 212, 191, ${0.2 + seal * 0.35})`;
        ctx.lineWidth = 1.5;
        ctx.beginPath();
        ctx.arc(cx, cy, 10 + seal * 4, 0, Math.PI * 2);
        ctx.stroke();
        ctx.fillStyle = "#2dd4bf";
        ctx.beginPath();
        ctx.arc(cx, cy, 3.2, 0, Math.PI * 2);
        ctx.fill();
      }

      raf = requestAnimationFrame(tick);
    };

    raf = requestAnimationFrame(tick);
    return () => {
      running = false;
      cancelAnimationFrame(raf);
      ro.disconnect();
    };
  }, [wrapRef]);

  return (
    <div className="lp-hero-flow" aria-hidden>
      <div className="lp-hero-flow-stage">
        <div className="lp-hero-flow-atmosphere" aria-hidden>
          <span className="lp-hero-flow-grid" />
          <span className="lp-hero-flow-orb lp-hero-flow-orb--a" />
          <span className="lp-hero-flow-orb lp-hero-flow-orb--b" />
        </div>

        <div className="lp-hero-flow-rail">
          <article className="lp-hero-flow-card lp-hero-flow-card--source">
            <header>
              <span className="lp-hero-flow-dot is-src" />
              Source
            </header>
            <strong>PostgreSQL</strong>
            <p>orders · three amount columns</p>
            <ul>
              <li>order_amt</li>
              <li>pay_amt</li>
              <li>tax_amt</li>
            </ul>
          </article>

          <article className="lp-hero-flow-card lp-hero-flow-card--engine">
            <header>
              <span className="lp-hero-flow-dot is-eng" />
              Governed engine
            </header>
            <strong>Propose · Review · Pin</strong>
            <ul className="lp-hero-flow-edges">
              <li>
                <code>order_amt</code>
                <span>→</span>
                <code>total_amount</code>
                <em>pin</em>
              </li>
              <li>
                <code>pay_amt</code>
                <span>→</span>
                <code>payment_amount</code>
                <em>pin</em>
              </li>
              <li className="is-review">
                <code>tax_amt</code>
                <span>→</span>
                <code>tax_amount</code>
                <em>review</em>
              </li>
            </ul>
            <div className="lp-hero-flow-meters">
              <div>
                <span>Same role</span>
                <em>not identity</em>
              </div>
              <div>
                <span>G4 Map</span>
                <em className="is-warn">holds 1</em>
              </div>
            </div>
          </article>

          <article className="lp-hero-flow-card lp-hero-flow-card--dest">
            <header>
              <span className="lp-hero-flow-dot is-dst" />
              Destination
            </header>
            <strong>Snowflake</strong>
            <p>ANALYTICS.ORDERS</p>
            <ul>
              <li>total_amount</li>
              <li>payment_amount</li>
              <li>tax_amount</li>
            </ul>
          </article>
        </div>

        <div className="lp-hero-flow-waveband" ref={wrapRef}>
          <canvas className="lp-hero-flow-canvas" ref={canvasRef} />
          <div className="lp-hero-flow-waveband-label">
            <span>Live packet path</span>
            <strong>source → map → destination</strong>
          </div>
        </div>

        <footer className="lp-hero-flow-proof">
          <span>
            <em>amt</em> is a family, not a column
          </span>
          <span>
            <em>G4</em> holds until Map confirms
          </span>
          <span>
            Illustration of the mapper — not a live run
          </span>
        </footer>
      </div>
    </div>
  );
}
