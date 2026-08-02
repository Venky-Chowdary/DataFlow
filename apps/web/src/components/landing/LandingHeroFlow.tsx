import { useEffect, useRef } from "react";
import { useInView } from "../../hooks/useInView";

/**
 * Clean enterprise hero stage: source → map → destination with living packet trails.
 * Animation pauses when off-screen to keep landing scroll smooth.
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

    type Packet = { t: number; lane: number; speed: number };
    const packets: Packet[] = Array.from({ length: 6 }, (_, i) => ({
      t: Math.random(),
      lane: i % 2,
      speed: 0.1 + Math.random() * 0.12,
    }));

    const resize = () => {
      const dpr = Math.min(window.devicePixelRatio || 1, 1.5);
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

    const tick = (now: number) => {
      if (!running) return;
      if (!inViewRef.current) {
        raf = requestAnimationFrame(tick);
        return;
      }

      const { width, height } = wrap.getBoundingClientRect();
      ctx.clearRect(0, 0, width, height);

      const elapsed = (now - t0) / 1000;
      const yMid = height * 0.42;
      const x0 = width * 0.18;
      const x1 = width * 0.5;
      const x2 = width * 0.82;

      for (let lane = 0; lane < 2; lane++) {
        const y = yMid + (lane === 0 ? -10 : 10);
        ctx.beginPath();
        ctx.moveTo(x0, y);
        ctx.bezierCurveTo(x0 + width * 0.1, y - 8, x1 - width * 0.08, y + 8, x1, y);
        ctx.bezierCurveTo(x1 + width * 0.08, y - 8, x2 - width * 0.1, y + 8, x2, y);
        ctx.strokeStyle = "rgba(13, 148, 136, 0.18)";
        ctx.lineWidth = 1.5;
        ctx.stroke();
      }

      if (!reduced) {
        for (const p of packets) {
          p.t += p.speed * 0.016;
          if (p.t > 1.15) p.t = -0.05;
          const u = Math.min(1, Math.max(0, p.t));
          const yBase = yMid + (p.lane === 0 ? -10 : 10);
          const bob = Math.sin((u + elapsed * 0.4) * Math.PI * 2) * 4;
          let x: number;
          let y: number;
          if (u < 0.5) {
            const s = u / 0.5;
            x = x0 + (x1 - x0) * s;
            y = yBase + bob - Math.sin(s * Math.PI) * 10;
          } else {
            const s = (u - 0.5) / 0.5;
            x = x1 + (x2 - x1) * s;
            y = yBase + bob - Math.sin(s * Math.PI) * 8;
          }

          ctx.fillStyle = "rgba(15, 118, 110, 0.2)";
          ctx.beginPath();
          ctx.arc(x, y, 7, 0, Math.PI * 2);
          ctx.fill();

          ctx.fillStyle = "#0f766e";
          ctx.beginPath();
          ctx.arc(x, y, 2.6, 0, Math.PI * 2);
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
  }, [wrapRef]);

  return (
    <div className="lp-hero-flow" ref={wrapRef} aria-hidden>
      <div className="lp-hero-flow-stage">
        <canvas className="lp-hero-flow-canvas" ref={canvasRef} />

        <div className="lp-hero-flow-rail">
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
              <div>
                <span>Semantic map</span>
                <em>96%</em>
              </div>
              <div>
                <span>Preflight</span>
                <em>8 / 8</em>
              </div>
              <div>
                <span>Checksum</span>
                <em>match</em>
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
              <li>payment_amount</li>
              <li>email</li>
              <li>customer_key</li>
            </ul>
          </article>
        </div>
      </div>
    </div>
  );
}
