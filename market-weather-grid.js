// Market Weather Grid — Canvas Rendering Engine
// Angelrow Trading Systems — angelrow.co.uk

(function () {
  'use strict';

  // ── Configuration ─────────────────────────────────────────────────────────

  const CONFIG = {
    DATA_URL:          'data/grid-data.json',
    POLL_INTERVAL:     60000,          // 60 seconds
    FRESH_THRESHOLD:   20 * 60 * 1000, // 20 minutes
    STALE_THRESHOLD:   60 * 60 * 1000, // 60 minutes
    DEFAULT_TRAIL:     12,
    MIN_TRAIL:         4,
    MAX_TRAIL:         80,
    CLEAN_THRESHOLD:   0.35,
    CAUTION_THRESHOLD: 0.70,
    DANGER_BOOST:      0.15,  // added to bear+expansion cells
    SAFE_REDUCTION:    0.05,  // subtracted from bull+compression cells
    GRID_SIZE:         5,
    ANIMATION_SPEED:   0.06,  // lerp factor per frame
  };

  // ── Grid axis labels ──────────────────────────────────────────────────────

  const Y_LABELS = ['STRONG BULL', 'LEAN BULL', 'NEUTRAL', 'LEAN BEAR', 'STRONG BEAR'];
  const X_LABELS = ['COMPRESSION', 'BELOW AVG', 'NORMAL', 'ABOVE AVG', 'EXPANSION'];

  // ── Direction labels ──────────────────────────────────────────────────────

  function directionLabel(d) {
    if (d > 0.6)  return { text: 'STRONG BULL', color: '#00d4aa' };
    if (d > 0.2)  return { text: 'LEAN BULL',   color: '#66e0c4' };
    if (d > -0.2) return { text: 'NEUTRAL',     color: '#4d9fff' };
    if (d > -0.6) return { text: 'LEAN BEAR',   color: '#ff8a6a' };
    return               { text: 'STRONG BEAR',  color: '#ff4d6a' };
  }

  function magnitudeLabel(m) {
    if (m > 0.6)  return { text: 'EXPANSION',   color: '#ff4d6a' };
    if (m > 0.2)  return { text: 'ABOVE AVG',   color: '#ffb020' };
    if (m > -0.2) return { text: 'NORMAL',       color: '#4d9fff' };
    if (m > -0.6) return { text: 'BELOW AVG',   color: '#66e0c4' };
    return               { text: 'COMPRESSION',  color: '#00d4aa' };
  }

  // ── State ─────────────────────────────────────────────────────────────────

  let gridData = [];
  let trailLength = CONFIG.DEFAULT_TRAIL;

  // Animation state
  let currentDotX = 0.5;  // 0–1 normalised canvas coords
  let currentDotY = 0.5;
  let targetDotX = 0.5;
  let targetDotY = 0.5;
  let animating = false;
  let currentVolScore = 0;
  let targetVolScore = 0;

  // Canvas
  const canvas = document.getElementById('gridCanvas');
  const ctx = canvas.getContext('2d');
  let dpr = window.devicePixelRatio || 1;

  // ── Canvas sizing ─────────────────────────────────────────────────────────

  function resizeCanvas() {
    const rect = canvas.parentElement.getBoundingClientRect();
    const w = rect.width - 40; // account for padding
    const h = Math.min(w * 0.7, 650);
    canvas.style.width = w + 'px';
    canvas.style.height = h + 'px';
    canvas.width = w * dpr;
    canvas.height = h * dpr;
    ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
    draw();
  }

  // ── Colour helpers ────────────────────────────────────────────────────────

  function lerpColor(a, b, t) {
    return [
      a[0] + (b[0] - a[0]) * t,
      a[1] + (b[1] - a[1]) * t,
      a[2] + (b[2] - a[2]) * t,
    ];
  }

  function heatmapColor(score) {
    // 0–0.35: cool blues/teals
    // 0.35–0.70: warm ambers
    // 0.70–1.0: deep reds
    let r, g, b;
    if (score < CONFIG.CLEAN_THRESHOLD) {
      const t = score / CONFIG.CLEAN_THRESHOLD;
      [r, g, b] = lerpColor([10, 25, 45], [18, 40, 65], t);
    } else if (score < CONFIG.CAUTION_THRESHOLD) {
      const t = (score - CONFIG.CLEAN_THRESHOLD) / (CONFIG.CAUTION_THRESHOLD - CONFIG.CLEAN_THRESHOLD);
      [r, g, b] = lerpColor([80, 60, 15], [200, 100, 20], t);
    } else {
      const t = (score - CONFIG.CAUTION_THRESHOLD) / (1 - CONFIG.CAUTION_THRESHOLD);
      [r, g, b] = lerpColor([140, 20, 15], [220, 30, 25], Math.min(t, 1));
    }
    return `rgb(${Math.round(r)},${Math.round(g)},${Math.round(b)})`;
  }

  function cellDangerModifier(row, col) {
    // row 0 = strong bull (top), row 4 = strong bear (bottom)
    // col 0 = compression (left), col 4 = expansion (right)
    const isBearish = row >= 3;
    const isBullish = row <= 1;
    const isExpansion = col >= 3;
    const isCompression = col <= 1;

    let mod = 0;
    if (isBearish && isExpansion) mod += CONFIG.DANGER_BOOST;
    if (isBullish && isCompression) mod -= CONFIG.SAFE_REDUCTION;
    // Slight boost for extreme corners
    if (row === 4 && col === 4) mod += 0.05;
    if (row === 0 && col === 0) mod -= 0.03;
    return mod;
  }

  function dotColor(dir) {
    if (dir > 0.1) return { h: 142, s: 70, l: 50 }; // green (bull)
    if (dir < -0.1) return { h: 0, s: 70, l: 55 };   // red (bear)
    return { h: 210, s: 70, l: 55 };                   // blue (neutral)
  }

  // ── Coordinate mapping ────────────────────────────────────────────────────

  function getGridArea() {
    const w = canvas.width / dpr;
    const h = canvas.height / dpr;
    const left = 90;
    const right = w - 30;
    const top = 20;
    const bottom = h - 40;
    return { left, right, top, bottom, w: right - left, h: bottom - top };
  }

  function dataToCanvas(dir, mag) {
    // dir: -1 (bear/bottom) to +1 (bull/top)
    // mag: -1 (compression/left) to +1 (expansion/right)
    const g = getGridArea();
    const x = g.left + ((mag + 1) / 2) * g.w;
    const y = g.top + ((1 - dir) / 2) * g.h;  // invert: bull=top
    return { x, y };
  }

  // ── Drawing ───────────────────────────────────────────────────────────────

  function draw() {
    const w = canvas.width / dpr;
    const h = canvas.height / dpr;
    ctx.clearRect(0, 0, w, h);

    const g = getGridArea();
    const cellW = g.w / CONFIG.GRID_SIZE;
    const cellH = g.h / CONFIG.GRID_SIZE;

    const volScore = currentVolScore;

    // Draw heatmap cells
    for (let row = 0; row < CONFIG.GRID_SIZE; row++) {
      for (let col = 0; col < CONFIG.GRID_SIZE; col++) {
        const cx = g.left + col * cellW;
        const cy = g.top + row * cellH;
        const modifier = cellDangerModifier(row, col);
        const adjusted = Math.max(0, Math.min(1, volScore + modifier));

        ctx.fillStyle = heatmapColor(adjusted);
        ctx.fillRect(cx, cy, cellW, cellH);

        // Cell borders
        ctx.strokeStyle = 'rgba(255,255,255,0.06)';
        ctx.lineWidth = 1;

        // NO TRADE cells: dashed borders
        if (adjusted > CONFIG.CAUTION_THRESHOLD) {
          ctx.setLineDash([4, 3]);
          ctx.strokeStyle = 'rgba(255,80,80,0.3)';
        } else {
          ctx.setLineDash([]);
        }
        ctx.strokeRect(cx, cy, cellW, cellH);
        ctx.setLineDash([]);

        // Extreme NO TRADE: hazard hatch
        if (adjusted > 0.85) {
          drawHazardHatch(cx, cy, cellW, cellH);
        }
      }
    }

    // Axis labels
    ctx.font = '500 9px JetBrains Mono, monospace';
    ctx.textAlign = 'right';
    ctx.textBaseline = 'middle';
    for (let i = 0; i < CONFIG.GRID_SIZE; i++) {
      const y = g.top + (i + 0.5) * cellH;
      ctx.fillStyle = i === 2 ? 'rgba(255,255,255,0.4)' : 'rgba(255,255,255,0.25)';
      ctx.fillText(Y_LABELS[i], g.left - 8, y);
    }

    ctx.textAlign = 'center';
    ctx.textBaseline = 'top';
    for (let i = 0; i < CONFIG.GRID_SIZE; i++) {
      const x = g.left + (i + 0.5) * cellW;
      ctx.fillStyle = i === 2 ? 'rgba(255,255,255,0.4)' : 'rgba(255,255,255,0.25)';
      ctx.fillText(X_LABELS[i], x, g.bottom + 8);
    }

    // Y-axis title
    ctx.save();
    ctx.translate(12, g.top + g.h / 2);
    ctx.rotate(-Math.PI / 2);
    ctx.textAlign = 'center';
    ctx.textBaseline = 'middle';
    ctx.font = '700 9px JetBrains Mono, monospace';
    ctx.fillStyle = 'rgba(255,255,255,0.2)';
    ctx.fillText('DIRECTION', 0, 0);
    ctx.restore();

    // X-axis title
    ctx.font = '700 9px JetBrains Mono, monospace';
    ctx.fillStyle = 'rgba(255,255,255,0.2)';
    ctx.textAlign = 'center';
    ctx.textBaseline = 'top';
    ctx.fillText('EXPECTED MAGNITUDE', g.left + g.w / 2, g.bottom + 24);

    // Outer border
    ctx.strokeStyle = 'rgba(255,255,255,0.1)';
    ctx.lineWidth = 1.5;
    ctx.strokeRect(g.left, g.top, g.w, g.h);

    // Draw trail and dot
    if (gridData.length > 0) {
      drawTrail();
      drawDot();
    }
  }

  function drawHazardHatch(x, y, w, h) {
    ctx.save();
    ctx.beginPath();
    ctx.rect(x, y, w, h);
    ctx.clip();
    ctx.strokeStyle = 'rgba(255,60,60,0.12)';
    ctx.lineWidth = 1;
    const step = 10;
    for (let i = -h; i < w + h; i += step) {
      ctx.beginPath();
      ctx.moveTo(x + i, y);
      ctx.lineTo(x + i - h, y + h);
      ctx.stroke();
    }
    ctx.restore();
  }

  function drawTrail() {
    const points = gridData.slice(-trailLength);
    if (points.length < 2) return;

    // Trail line
    for (let i = 1; i < points.length; i++) {
      const p0 = dataToCanvas(points[i - 1].dir, points[i - 1].mag);
      const p1 = dataToCanvas(points[i].dir, points[i].mag);
      const progress = i / points.length;
      const alpha = 0.08 + progress * 0.35;
      const width = 0.5 + progress * 2;

      ctx.beginPath();
      ctx.moveTo(p0.x, p0.y);
      ctx.lineTo(p1.x, p1.y);
      ctx.strokeStyle = `rgba(0,220,200,${alpha})`;
      ctx.lineWidth = width;
      ctx.stroke();
    }

    // Trail dots
    for (let i = 0; i < points.length - 1; i++) {
      const p = dataToCanvas(points[i].dir, points[i].mag);
      const progress = (i + 1) / points.length;
      const alpha = 0.1 + progress * 0.4;
      const radius = 1.5 + progress * 1.5;

      ctx.beginPath();
      ctx.arc(p.x, p.y, radius, 0, Math.PI * 2);
      ctx.fillStyle = `rgba(0,220,200,${alpha})`;
      ctx.fill();
    }
  }

  function drawDot() {
    if (gridData.length === 0) return;

    const latest = gridData[gridData.length - 1];
    const x = currentDotX;
    const y = currentDotY;
    const pos = dataToCanvas(
      y * 2 - 1,  // convert back from 0-1 to -1..1 for mapping
      0 // placeholder
    );
    // Actually use raw pixel position from lerp
    const px = (() => {
      const g = getGridArea();
      return {
        x: g.left + currentDotX * g.w,
        y: g.top + currentDotY * g.h
      };
    })();

    const col = dotColor(latest.dir);
    const hsl = `hsl(${col.h},${col.s}%,${col.l}%)`;
    const volScore = currentVolScore;

    // Glow — enlarges in danger zones
    const glowSize = 12 + volScore * 20;
    const glowAlpha = 0.15 + volScore * 0.2;
    const grad = ctx.createRadialGradient(px.x, px.y, 0, px.x, px.y, glowSize);
    grad.addColorStop(0, `hsla(${col.h},${col.s}%,${col.l}%,${glowAlpha})`);
    grad.addColorStop(1, `hsla(${col.h},${col.s}%,${col.l}%,0)`);
    ctx.beginPath();
    ctx.arc(px.x, px.y, glowSize, 0, Math.PI * 2);
    ctx.fillStyle = grad;
    ctx.fill();

    // Warning ring in NO TRADE zone
    if (volScore >= CONFIG.CAUTION_THRESHOLD) {
      ctx.beginPath();
      ctx.arc(px.x, px.y, 14 + volScore * 6, 0, Math.PI * 2);
      ctx.setLineDash([3, 3]);
      ctx.strokeStyle = `hsla(0,70%,55%,${0.3 + (volScore - 0.7) * 0.5})`;
      ctx.lineWidth = 1.5;
      ctx.stroke();
      ctx.setLineDash([]);
    }

    // Main dot
    ctx.beginPath();
    ctx.arc(px.x, px.y, 6, 0, Math.PI * 2);
    ctx.fillStyle = hsl;
    ctx.fill();

    // Dot border
    ctx.beginPath();
    ctx.arc(px.x, px.y, 6, 0, Math.PI * 2);
    ctx.strokeStyle = 'rgba(255,255,255,0.3)';
    ctx.lineWidth = 1;
    ctx.stroke();

    // Inner highlight
    ctx.beginPath();
    ctx.arc(px.x - 1.5, px.y - 1.5, 2, 0, Math.PI * 2);
    ctx.fillStyle = 'rgba(255,255,255,0.25)';
    ctx.fill();
  }

  // ── Animation loop ────────────────────────────────────────────────────────

  function animate() {
    let needsRedraw = false;

    // Lerp dot position
    const dx = targetDotX - currentDotX;
    const dy = targetDotY - currentDotY;
    if (Math.abs(dx) > 0.001 || Math.abs(dy) > 0.001) {
      currentDotX += dx * CONFIG.ANIMATION_SPEED;
      currentDotY += dy * CONFIG.ANIMATION_SPEED;
      needsRedraw = true;
    } else {
      currentDotX = targetDotX;
      currentDotY = targetDotY;
    }

    // Lerp volScore
    const dv = targetVolScore - currentVolScore;
    if (Math.abs(dv) > 0.001) {
      currentVolScore += dv * CONFIG.ANIMATION_SPEED;
      needsRedraw = true;
    } else {
      currentVolScore = targetVolScore;
    }

    if (needsRedraw) {
      draw();
    }

    requestAnimationFrame(animate);
  }

  // ── Data processing ───────────────────────────────────────────────────────

  function updateFromData() {
    if (gridData.length === 0) {
      document.getElementById('awaitingMsg').style.display = 'block';
      canvas.style.display = 'none';
      return;
    }

    document.getElementById('awaitingMsg').style.display = 'none';
    canvas.style.display = 'block';

    const latest = gridData[gridData.length - 1];

    // Set animation targets
    // Map dir (-1..1) and mag (-1..1) to 0..1 canvas space
    targetDotX = (latest.mag + 1) / 2;
    targetDotY = (1 - latest.dir) / 2; // invert: bull=top=0
    targetVolScore = latest.volScore;

    // If first load, snap immediately
    if (gridData.length === 1 || (currentDotX === 0.5 && currentDotY === 0.5 && currentVolScore === 0)) {
      currentDotX = targetDotX;
      currentDotY = targetDotY;
      currentVolScore = targetVolScore;
    }

    // Update data bar
    updateDataBar(latest);

    // Update status panels
    updateStatusPanels(latest);

    // Update freshness indicator
    updateFreshness(latest.timestamp);

    draw();
  }

  function updateDataBar(d) {
    const ts = new Date(d.timestamp);
    const dateStr = ts.toLocaleDateString('en-GB', { day: '2-digit', month: 'short', year: '2-digit' });
    const timeStr = ts.toLocaleTimeString('en-GB', { hour: '2-digit', minute: '2-digit' });

    document.getElementById('dbDate').textContent = dateStr + ' ' + timeStr;
    document.getElementById('dbSpy').textContent = '$' + d.spy.toFixed(2);

    const vixEl = document.getElementById('dbVix');
    vixEl.textContent = d.vix.toFixed(1);
    vixEl.className = 'data-val ' + (d.vix < 16 ? 'vix-low' : d.vix < 25 ? 'vix-mid' : 'vix-high');

    document.getElementById('dbIvr').textContent = d.ivr.toFixed(0) + '%';
    document.getElementById('dbEm').textContent = '\u00B1' + d.em + '%';
  }

  function updateStatusPanels(d) {
    // Direction
    const dir = directionLabel(d.dir);
    const stDir = document.getElementById('stDirection');
    stDir.textContent = dir.text;
    stDir.style.color = dir.color;
    document.getElementById('stDirSub').textContent = 'Score: ' + d.dir.toFixed(3);

    // Magnitude
    const mag = magnitudeLabel(d.mag);
    const stMag = document.getElementById('stMagnitude');
    stMag.textContent = mag.text;
    stMag.style.color = mag.color;
    document.getElementById('stMagSub').textContent = 'Score: ' + d.mag.toFixed(3);

    // Momentum (delta of last 2 points)
    const stMom = document.getElementById('stMomentum');
    const stMomSub = document.getElementById('stMomSub');
    if (gridData.length >= 2) {
      const prev = gridData[gridData.length - 2];
      const dirDelta = d.dir - prev.dir;
      const magDelta = d.mag - prev.mag;
      let momText = '';
      let momColor = '#4d9fff';
      const arrows = [];

      if (Math.abs(dirDelta) > 0.01) {
        if (dirDelta > 0) { arrows.push('\u2191 BULL'); momColor = '#00d4aa'; }
        else { arrows.push('\u2193 BEAR'); momColor = '#ff4d6a'; }
      }
      if (Math.abs(magDelta) > 0.01) {
        if (magDelta > 0) { arrows.push('\u2192 VOL RISING'); }
        else { arrows.push('\u2190 VOL FALLING'); }
      }

      momText = arrows.length > 0 ? arrows.join(' ') : 'STABLE';
      if (arrows.length === 0) momColor = '#4d9fff';

      stMom.textContent = momText;
      stMom.style.color = momColor;
      stMomSub.textContent = '\u0394dir: ' + (dirDelta > 0 ? '+' : '') + dirDelta.toFixed(3) +
        ' | \u0394mag: ' + (magDelta > 0 ? '+' : '') + magDelta.toFixed(3);
    } else {
      stMom.textContent = 'PENDING';
      stMom.style.color = 'var(--text-muted)';
      stMomSub.textContent = 'Need 2+ data points';
    }

    // Vol Regime
    const stReg = document.getElementById('stRegime');
    const stRegSub = document.getElementById('stRegSub');
    const volBar = document.getElementById('volBarFill');

    if (d.volScore < CONFIG.CLEAN_THRESHOLD) {
      stReg.textContent = 'CLEAN';
      stReg.style.color = '#00d4aa';
      stRegSub.textContent = 'Trade normal size';
      volBar.style.background = '#00d4aa';
    } else if (d.volScore < CONFIG.CAUTION_THRESHOLD) {
      stReg.textContent = 'CAUTION';
      stReg.style.color = '#ffb020';
      stRegSub.textContent = 'Reduce position size';
      volBar.style.background = '#ffb020';
    } else {
      stReg.textContent = 'NO TRADE';
      stReg.style.color = '#ff4d6a';
      stRegSub.textContent = 'Sit out entirely';
      volBar.style.background = '#ff4d6a';
    }
    volBar.style.width = (d.volScore * 100) + '%';
  }

  function updateFreshness(timestamp) {
    const age = Date.now() - new Date(timestamp).getTime();
    const dot = document.getElementById('liveDot');
    const text = document.getElementById('liveText');

    dot.className = 'live-dot';
    text.className = 'live-text';

    if (age < CONFIG.FRESH_THRESHOLD) {
      dot.classList.add('live');
      text.classList.add('live');
      text.textContent = 'LIVE';
    } else if (age < CONFIG.STALE_THRESHOLD) {
      dot.classList.add('stale');
      text.classList.add('stale');
      text.textContent = 'STALE';
    } else {
      dot.classList.add('offline');
      text.classList.add('offline');
      text.textContent = 'OFFLINE';
    }
  }

  // ── Data fetching ─────────────────────────────────────────────────────────

  async function fetchData() {
    try {
      // Cache-busting query param
      const url = CONFIG.DATA_URL + '?t=' + Date.now();
      const resp = await fetch(url);
      if (!resp.ok) throw new Error('HTTP ' + resp.status);
      const data = await resp.json();
      if (Array.isArray(data)) {
        gridData = data;
        updateFromData();
      }
    } catch (e) {
      console.warn('Failed to fetch grid data:', e);
      // If we have no data at all, show awaiting message
      if (gridData.length === 0) {
        document.getElementById('awaitingMsg').style.display = 'block';
        canvas.style.display = 'none';
      }
    }
  }

  // ── Slider ────────────────────────────────────────────────────────────────

  function setupSlider() {
    const slider = document.getElementById('trailSlider');
    const label = document.getElementById('trailValue');

    function formatTrailTime(pts) {
      const minutes = pts * 15;
      if (minutes < 60) return pts + ' pts \u00B7 ~' + minutes + 'min';
      const hours = minutes / 60;
      if (hours < 8) return pts + ' pts \u00B7 ~' + hours.toFixed(1) + 'h';
      const days = hours / 6.5; // ~6.5 trading hours/day
      return pts + ' pts \u00B7 ~' + days.toFixed(1) + 'd';
    }

    slider.value = trailLength;
    label.textContent = formatTrailTime(trailLength);

    slider.addEventListener('input', function () {
      trailLength = parseInt(this.value);
      label.textContent = formatTrailTime(trailLength);
      draw();
    });
  }

  // ── Init ──────────────────────────────────────────────────────────────────

  function init() {
    resizeCanvas();
    setupSlider();
    fetchData();

    // Start animation loop
    requestAnimationFrame(animate);

    // Poll for new data
    setInterval(fetchData, CONFIG.POLL_INTERVAL);

    // Resize handler
    window.addEventListener('resize', resizeCanvas);
  }

  // Wait for DOM
  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', init);
  } else {
    init();
  }

})();
