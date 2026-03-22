// Monte Carlo Simulation — CSP & Bull Put Spread Strategy Analysis
// Angelrow Limited — angelrow.co.uk

const MC = (() => {
  'use strict';

  const SIMS = 10000;
  const RISK = 0.02;

  let mcCharts     = {};
  let mcDataAll    = null;  // all CSP/Bull Put trades (including errors)
  let mcResults    = null;
  let mcStats      = null;
  let _mode        = 'clean'; // 'clean' | 'all' | 'errors' — source of truth

  // ── Maths helpers ─────────────────────────────────────────────────────────

  function pctile(arr, p) {
    const sorted = [...arr].sort((a, b) => a - b);
    const idx = (p / 100) * (sorted.length - 1);
    const lo = Math.floor(idx), hi = Math.ceil(idx);
    return sorted[lo] + (sorted[hi] - sorted[lo]) * (idx - lo);
  }

  function mean(arr) {
    return arr.reduce((s, v) => s + v, 0) / arr.length;
  }

  function median(arr) {
    const s = [...arr].sort((a, b) => a - b);
    const m = Math.floor(s.length / 2);
    return s.length % 2 ? s[m] : (s[m - 1] + s[m]) / 2;
  }

  // ── CSV parsing ───────────────────────────────────────────────────────────

  function parseCSVLine(line) {
    const result = [];
    let cell = '', inQ = false;
    for (let i = 0; i < line.length; i++) {
      const c = line[i];
      if (c === '"') { inQ = !inQ; }
      else if (c === ',' && !inQ) { result.push(cell); cell = ''; }
      else { cell += c; }
    }
    result.push(cell);
    return result;
  }

  function parsePct(s) {
    if (!s || s === '—' || s.trim() === '') return null;
    const n = parseFloat(s.replace(/^\((.+)\)$/, '-$1').replace(/[^0-9.\-]/g, ''));
    return isNaN(n) ? null : n;
  }

  function parseDollar(s) {
    if (!s || s === '—' || s.trim() === '') return null;
    const n = parseFloat(s.replace(/^\((.+)\)$/, '-$1').replace(/[^0-9.\-]/g, ''));
    return isNaN(n) ? null : Math.abs(n);
  }

  // Try new underscore filename first, fall back to legacy space name
  async function fetchCSV() {
    const names = ['Trade_Log-Table_1.csv', 'Trade Log-Table 1.csv'];
    for (const name of names) {
      const res = await fetch(encodeURI(name));
      if (res.ok) return res.text();
    }
    throw new Error('CSV not found (tried Trade_Log-Table_1.csv and Trade Log-Table 1.csv)');
  }

  // Loads ALL CSP + Bull Put trades; flags error trades rather than dropping them
  async function loadAllTrades() {
    const text = await fetchCSV();
    const lines = text.trim().split('\n').filter(l => l.trim());
    if (lines.length < 2) throw new Error('CSV appears empty');

    const headers = parseCSVLine(lines[0]).map(h =>
      h.trim().toLowerCase().replace(/[^a-z0-9]+/g, '_').replace(/^_|_$/g, '')
    );

    const fi = (...names) => headers.findIndex(h => names.some(n => h === n || h.includes(n)));

    const cStrategy  = fi('strategy')                        >= 0 ? fi('strategy')                        : 3;
    const cPnlPct    = fi('p_l_', 'pnl_pct', 'p_l_%')       >= 0 ? fi('p_l_', 'pnl_pct', 'p_l_%')       : 13;
    const cWinLoss   = fi('win_loss', 'win')                 >= 0 ? fi('win_loss', 'win')                 : 16;
    const cPremium   = fi('entry_cost', 'premium', 'credit') >= 0 ? fi('entry_cost', 'premium', 'credit') : 10;
    const cMistakes  = fi('mistakes')                        >= 0 ? fi('mistakes')                        : 28;

    const trades = [];

    for (let i = 1; i < lines.length; i++) {
      const c = parseCSVLine(lines[i]);
      const g = idx => (c[idx] || '').toString().trim();

      const strategy  = g(cStrategy);
      const mistakes  = g(cMistakes);

      const strat_lc  = strategy.toLowerCase();
      const isCsp     = strat_lc.includes('cash secured put') || strat_lc.includes('csp');
      const isBullPut = strat_lc.includes('bull put spread');
      if (!isCsp && !isBullPut) continue;

      const pnlPct  = parsePct(g(cPnlPct));
      const winLoss = g(cWinLoss).toLowerCase();
      const premium = parseDollar(g(cPremium));

      if (pnlPct === null) continue;
      if (winLoss !== 'win' && winLoss !== 'loss') continue;

      trades.push({
        strategy,
        pnlPct,
        isWin:       winLoss === 'win',
        isError:     mistakes.toUpperCase().startsWith('ERROR'),  // case-insensitive
        premium:     premium || 0,
        mistakesRaw: mistakes  // stored for the diagnostic panel
      });
    }

    // Use most recent 50 if > 200 total (CSV is date-ordered)
    return trades.length > 200 ? trades.slice(-50) : trades;
  }

  function activeTrades() {
    if (!mcDataAll) return [];
    if (_mode === 'all')    return mcDataAll;
    if (_mode === 'errors') return mcDataAll.filter(t => t.isError);
    return mcDataAll.filter(t => !t.isError);
  }

  function syncModeButtons() {
    document.querySelectorAll('.mc-mode-btn').forEach(b => {
      b.classList.toggle('active', b.dataset.mode === _mode);
    });
  }

  // ── Statistics ────────────────────────────────────────────────────────────

  function calcStats(trades) {
    const wins   = trades.filter(t => t.isWin);
    const losses = trades.filter(t => !t.isWin);

    const winPcts  = wins.map(t => t.pnlPct);
    const lossPcts = losses.map(t => t.pnlPct);

    const avg = arr => arr.length ? arr.reduce((s, v) => s + v, 0) / arr.length : 0;

    const winRate    = trades.length ? wins.length / trades.length : 0;
    const avgWin     = avg(winPcts);
    const avgLoss    = avg(lossPcts);
    const maxWin     = winPcts.length  ? Math.max(...winPcts)  : 0;
    const maxLoss    = lossPcts.length ? Math.min(...lossPcts) : 0;
    const expectancy = winRate * avgWin + (1 - winRate) * avgLoss;

    const premiums      = trades.filter(t => t.premium > 0).map(t => t.premium);
    const avgPremium    = avg(premiums);
    const medianPremium = premiums.length ? median(premiums) : 0;

    return {
      total: trades.length,
      wins: wins.length, losses: losses.length,
      winRate, lossRate: 1 - winRate,
      avgWin, avgLoss, maxWin, maxLoss,
      expectancy, avgPremium, medianPremium
    };
  }

  // ── Monte Carlo — bootstrap resampling ────────────────────────────────────
  // Each simulated trade draws a P&L directly from the actual trade pool
  // (sampling with replacement). No parametric distribution assumed.

  function runMonteCarlo(trades, startCapital, tradesPerYear) {
    const n = trades.length;

    const endValues    = new Float64Array(SIMS);
    const maxDrawdowns = new Float64Array(SIMS);
    const maxConLosses = new Int32Array(SIMS);

    for (let sim = 0; sim < SIMS; sim++) {
      let account = startCapital;
      let peak    = startCapital;
      let maxDD   = 0;
      let consL   = 0;
      let maxCons = 0;

      for (let t = 0; t < tradesPerYear; t++) {
        // Bootstrap: draw one actual trade outcome at random (with replacement)
        const trade  = trades[Math.floor(Math.random() * n)];
        const pnlPct = trade.pnlPct;

        account += account * RISK * (pnlPct / 100);
        if (account <= 0) { account = 0; break; }

        if (account > peak) peak = account;
        const dd = (peak - account) / peak * 100;
        if (dd > maxDD) maxDD = dd;

        if (!trade.isWin) { consL++; if (consL > maxCons) maxCons = consL; }
        else consL = 0;
      }

      endValues[sim]    = account;
      maxDrawdowns[sim] = maxDD;
      maxConLosses[sim] = maxCons;
    }

    const ev  = Array.from(endValues);
    const mdd = Array.from(maxDrawdowns);
    const mcl = Array.from(maxConLosses);

    const probProfit   = ev.filter(v => v > startCapital).length / SIMS * 100;
    const probHalfLoss = ev.filter(v => v < startCapital * 0.5).length / SIMS * 100;

    const sortedEV = [...ev].sort((a, b) => a - b);
    const pctReturns = Array.from({ length: 99 }, (_, i) => {
      const idx = Math.floor(((i + 1) / 100) * (sortedEV.length - 1));
      return (sortedEV[idx] - startCapital) / startCapital * 100;
    });

    return {
      endValues: ev, maxDrawdowns: mdd, maxConLosses: mcl,
      p5:  pctile(ev, 5),  p25: pctile(ev, 25), p50: pctile(ev, 50),
      p75: pctile(ev, 75), p95: pctile(ev, 95),
      meanVal: mean(ev),
      probProfit, probLoss: 100 - probProfit, probHalfLoss,
      ddMedian: pctile(mdd, 50), ddP95: pctile(mdd, 95),
      consMedian: pctile(mcl, 50), consP95: pctile(mcl, 95),
      pctReturns
    };
  }

  // ── Formatting ────────────────────────────────────────────────────────────

  function $d(v) {
    const abs = Math.abs(Math.round(v));
    return (v < 0 ? '-$' : '$') + abs.toLocaleString('en-GB');
  }

  function $pct(v, dec = 1) {
    return (v >= 0 ? '+' : '') + v.toFixed(dec) + '%';
  }

  // ── Render sections ───────────────────────────────────────────────────────

  function renderStats(s, mode, errorCount) {
    const modeNote = mode === 'all'
      ? `<tr><td colspan="2" style="font-size:11px;color:var(--amber);padding:6px 10px 2px">⚠ ${errorCount} error trade${errorCount !== 1 ? 's' : ''} included</td></tr>`
      : mode === 'errors'
      ? `<tr><td colspan="2" style="font-size:11px;color:var(--red);padding:6px 10px 2px">⚠ Error trades only — ${s.total} trades</td></tr>`
      : '';
    const errNote = modeNote;
    document.getElementById('mc-stats-body').innerHTML = `
      ${errNote}
      <tr><td>Total Trades (CSP + Bull Put)</td><td class="mc-mono mc-neutral">${s.total}</td></tr>
      <tr><td>Wins</td><td class="mc-mono mc-win">${s.wins}</td></tr>
      <tr><td>Losses</td><td class="mc-mono mc-loss">${s.losses}</td></tr>
      <tr class="mc-hi"><td><strong>Win Rate</strong></td><td class="mc-mono mc-win"><strong>${(s.winRate * 100).toFixed(1)}%</strong></td></tr>
      <tr><td>Loss Rate</td><td class="mc-mono mc-loss">${(s.lossRate * 100).toFixed(1)}%</td></tr>
      <tr class="mc-hi"><td><strong>Average Win</strong></td><td class="mc-mono mc-win"><strong>${$pct(s.avgWin)}</strong></td></tr>
      <tr class="mc-hi"><td><strong>Average Loss</strong></td><td class="mc-mono mc-loss"><strong>${$pct(s.avgLoss)}</strong></td></tr>
      <tr><td>Max Win</td><td class="mc-mono mc-win">${$pct(s.maxWin)}</td></tr>
      <tr><td>Max Loss</td><td class="mc-mono mc-loss">${$pct(s.maxLoss)}</td></tr>
      <tr class="mc-hi"><td><strong>Expectancy / Trade</strong></td><td class="mc-mono ${s.expectancy >= 0 ? 'mc-win' : 'mc-loss'}"><strong>${$pct(s.expectancy)}</strong></td></tr>
      <tr><td>Avg Premium Collected</td><td class="mc-mono mc-neutral">${$d(s.avgPremium)}</td></tr>
      <tr><td>Median Premium Collected</td><td class="mc-mono mc-neutral">${$d(s.medianPremium)}</td></tr>
    `;
  }

  function renderResults(r, startCapital) {
    const ret = v => $pct((v - startCapital) / startCapital * 100);
    const cls = v => v >= startCapital ? 'mc-win' : 'mc-loss';
    document.getElementById('mc-results-body').innerHTML = `
      <tr><td>5th Percentile <span class="mc-tag">Worst 5%</span></td><td class="mc-mono mc-loss">${$d(r.p5)}</td><td class="mc-mono mc-loss">${ret(r.p5)}</td></tr>
      <tr><td>25th Percentile</td><td class="mc-mono ${cls(r.p25)}">${$d(r.p25)}</td><td class="mc-mono ${cls(r.p25)}">${ret(r.p25)}</td></tr>
      <tr class="mc-hi"><td><strong>50th Percentile <span class="mc-tag">Median</span></strong></td><td class="mc-mono mc-win"><strong>${$d(r.p50)}</strong></td><td class="mc-mono mc-win"><strong>${ret(r.p50)}</strong></td></tr>
      <tr><td>75th Percentile</td><td class="mc-mono mc-win">${$d(r.p75)}</td><td class="mc-mono mc-win">${ret(r.p75)}</td></tr>
      <tr><td>95th Percentile <span class="mc-tag">Best 5%</span></td><td class="mc-mono mc-win">${$d(r.p95)}</td><td class="mc-mono mc-win">${ret(r.p95)}</td></tr>
      <tr><td>Mean Outcome</td><td class="mc-mono ${cls(r.meanVal)}">${$d(r.meanVal)}</td><td class="mc-mono ${cls(r.meanVal)}">${ret(r.meanVal)}</td></tr>
    `;
    const pf = document.getElementById('mc-prob-profit');
    const pl = document.getElementById('mc-prob-loss');
    const ph = document.getElementById('mc-prob-halfloss');
    if (pf) pf.textContent = r.probProfit.toFixed(1) + '%';
    if (pl) pl.textContent = r.probLoss.toFixed(1) + '%';
    if (ph) ph.textContent = r.probHalfLoss.toFixed(1) + '%';
  }

  function renderRisk(r, s) {
    const el = id => document.getElementById(id);
    if (el('mc-dd-median'))   el('mc-dd-median').textContent   = r.ddMedian.toFixed(1) + '%';
    if (el('mc-dd-p95'))      el('mc-dd-p95').textContent      = r.ddP95.toFixed(1) + '%';
    if (el('mc-cons-median')) el('mc-cons-median').textContent = Math.round(r.consMedian);
    if (el('mc-cons-p95'))    el('mc-cons-p95').textContent    = Math.round(r.consP95);

    const consLo = Math.max(1, Math.round(r.consMedian));
    const consHi = Math.round(r.consP95);
    const ddLo   = r.ddMedian.toFixed(0);
    const ddHi   = r.ddP95.toFixed(0);
    const pt     = el('mc-psych-text');
    if (pt) pt.innerHTML =
      `You <strong>WILL</strong> experience <strong>${consLo}–${consHi} consecutive losses</strong> ` +
      `and a <strong>${ddLo}–${ddHi}% drawdown</strong> at some point this year. ` +
      `This is not a failure — it is statistically inevitable with these parameters. ` +
      `Your edge (<strong>${$pct(s.expectancy)} expectancy/trade</strong>) remains intact through these periods. ` +
      `Stay mechanical: ${(RISK * 100).toFixed(0)}% risk per trade, follow your plan, and the math favours you in ` +
      `<strong>${r.probProfit.toFixed(0)}%</strong> of all simulated scenarios.`;
  }

  // ── Charts ────────────────────────────────────────────────────────────────

  function buildBins(data, numBins) {
    const min = Math.min(...data);
    const max = Math.max(...data);
    const size = (max - min) / numBins;
    const counts = Array(numBins).fill(0);
    const centers = Array.from({ length: numBins }, (_, i) => min + (i + 0.5) * size);
    data.forEach(v => {
      const idx = Math.min(numBins - 1, Math.max(0, Math.floor((v - min) / size)));
      counts[idx]++;
    });
    return { centers, counts, min, max, size };
  }

  function killChart(id) {
    if (mcCharts[id]) { mcCharts[id].destroy(); delete mcCharts[id]; }
  }

  const baseOpts = {
    responsive: true,
    maintainAspectRatio: false,
    animation: { duration: 500 },
    plugins: {
      legend: { display: false },
      tooltip: {
        backgroundColor: 'rgba(17,22,30,0.95)',
        borderColor: 'rgba(255,255,255,0.1)',
        borderWidth: 1,
        titleColor: '#8a95a5',
        bodyColor: '#eaf0f6',
        padding: 10
      }
    },
    scales: {
      x: {
        ticks: { color: '#5a6577', font: { size: 10 }, maxTicksLimit: 8 },
        grid: { color: 'rgba(255,255,255,0.04)' },
        border: { color: 'rgba(255,255,255,0.06)' }
      },
      y: {
        ticks: { color: '#5a6577', font: { size: 10 } },
        grid: { color: 'rgba(255,255,255,0.04)' },
        border: { color: 'rgba(255,255,255,0.06)' },
        title: { display: true, text: 'Simulations', color: '#5a6577', font: { size: 10 } }
      }
    }
  };

  function renderCharts(r, startCapital) {
    // ── Chart 1: Ending account values ──────────────────────────────────────
    const ev = buildBins(r.endValues, 100);
    killChart('mc-c1');
    mcCharts['mc-c1'] = new Chart(document.getElementById('mc-c1'), {
      type: 'bar',
      data: {
        labels: ev.centers.map(v => v.toFixed(0)),
        datasets: [{
          data: ev.counts,
          backgroundColor: ev.centers.map(v =>
            v >= startCapital ? 'rgba(0,212,170,0.65)' : 'rgba(255,77,106,0.65)'
          ),
          borderWidth: 0, barPercentage: 1.0, categoryPercentage: 1.0
        }]
      },
      options: {
        ...baseOpts,
        plugins: {
          ...baseOpts.plugins,
          tooltip: { ...baseOpts.plugins.tooltip, callbacks: {
            title: c => `~${$d(parseFloat(c[0].label))}`,
            label: c => `${c.raw.toLocaleString()} simulations`
          }}
        },
        scales: {
          x: { ...baseOpts.scales.x, ticks: { ...baseOpts.scales.x.ticks,
            callback: (_, i) => { const v = ev.centers[i]; return v !== undefined ? `$${(v / 1000).toFixed(0)}k` : ''; }
          }},
          y: { ...baseOpts.scales.y }
        }
      }
    });

    // ── Chart 2: Max drawdowns ───────────────────────────────────────────────
    const dd = buildBins(r.maxDrawdowns, 50);
    killChart('mc-c2');
    mcCharts['mc-c2'] = new Chart(document.getElementById('mc-c2'), {
      type: 'bar',
      data: {
        labels: dd.centers.map(v => v.toFixed(1)),
        datasets: [{
          data: dd.counts,
          backgroundColor: dd.centers.map(v =>
            v <= r.ddMedian ? 'rgba(0,212,170,0.55)' :
            v <= r.ddP95   ? 'rgba(255,176,32,0.55)' : 'rgba(255,77,106,0.65)'
          ),
          borderWidth: 0, barPercentage: 1.0, categoryPercentage: 1.0
        }]
      },
      options: {
        ...baseOpts,
        plugins: {
          ...baseOpts.plugins,
          tooltip: { ...baseOpts.plugins.tooltip, callbacks: {
            title: c => `~${parseFloat(c[0].label).toFixed(1)}% drawdown`,
            label: c => `${c.raw.toLocaleString()} simulations`
          }}
        },
        scales: {
          x: { ...baseOpts.scales.x, ticks: { ...baseOpts.scales.x.ticks,
            callback: (_, i) => { const v = dd.centers[i]; return v !== undefined ? `${v.toFixed(0)}%` : ''; }
          }},
          y: { ...baseOpts.scales.y }
        }
      }
    });

    // ── Chart 3: Consecutive losses ──────────────────────────────────────────
    const maxCons = Math.max(...r.maxConLosses);
    const consCounts = Array(maxCons + 1).fill(0);
    r.maxConLosses.forEach(v => { if (v <= maxCons) consCounts[v]++; });
    const consLabels = Array.from({ length: maxCons + 1 }, (_, i) => i.toString());

    killChart('mc-c3');
    mcCharts['mc-c3'] = new Chart(document.getElementById('mc-c3'), {
      type: 'bar',
      data: {
        labels: consLabels,
        datasets: [{
          data: consCounts,
          backgroundColor: consLabels.map(l => {
            const v = parseInt(l);
            return v <= r.consMedian ? 'rgba(0,212,170,0.55)' :
                   v <= r.consP95   ? 'rgba(255,176,32,0.55)' : 'rgba(255,77,106,0.65)';
          }),
          borderWidth: 0, barPercentage: 0.8, categoryPercentage: 0.9
        }]
      },
      options: {
        ...baseOpts,
        plugins: {
          ...baseOpts.plugins,
          tooltip: { ...baseOpts.plugins.tooltip, callbacks: {
            title: c => `${c[0].label} consecutive losses`,
            label: c => `${c.raw.toLocaleString()} simulations`
          }}
        },
        scales: {
          x: { ...baseOpts.scales.x, title: { display: true, text: 'Max Consecutive Losses', color: '#5a6577', font: { size: 10 } } },
          y: { ...baseOpts.scales.y }
        }
      }
    });

    // ── Chart 4: Percentile return curve ─────────────────────────────────────
    killChart('mc-c4');
    mcCharts['mc-c4'] = new Chart(document.getElementById('mc-c4'), {
      type: 'line',
      data: {
        datasets: [{
          data: r.pctReturns.map((v, i) => ({ x: i + 1, y: parseFloat(v.toFixed(2)) })),
          borderColor: '#00d4aa',
          borderWidth: 2,
          backgroundColor: 'rgba(0,212,170,0.08)',
          fill: { target: 'origin', above: 'rgba(0,212,170,0.13)', below: 'rgba(255,77,106,0.13)' },
          tension: 0.4,
          pointRadius: 0
        }]
      },
      options: {
        ...baseOpts,
        plugins: {
          ...baseOpts.plugins,
          tooltip: { ...baseOpts.plugins.tooltip, mode: 'index', intersect: false, callbacks: {
            title: c => `${c[0].parsed.x}th percentile`,
            label: c => `Return: ${c.parsed.y >= 0 ? '+' : ''}${c.parsed.y.toFixed(1)}%`
          }}
        },
        scales: {
          x: { type: 'linear', ...baseOpts.scales.x,
            title: { display: true, text: 'Percentile', color: '#5a6577', font: { size: 10 } },
            ticks: { ...baseOpts.scales.x.ticks, callback: v => `${v}%` }
          },
          y: { ...baseOpts.scales.y,
            title: { display: true, text: 'Return (%)', color: '#5a6577', font: { size: 10 } },
            ticks: { ...baseOpts.scales.y.ticks, callback: v => `${v >= 0 ? '+' : ''}${v}%` }
          }
        }
      }
    });
  }

  // ── Status helpers ────────────────────────────────────────────────────────

  function setStatus(msg, type) {
    const el = document.getElementById('mc-status');
    if (!el) return;
    el.textContent = msg;
    el.className = 'mc-status mc-status-' + type;
  }

  function updateDataCount(trades) {
    const el = document.getElementById('mc-data-count');
    if (!el) return;
    const errCount = mcDataAll ? mcDataAll.filter(t => t.isError).length : 0;
    if (_mode === 'errors') el.textContent = `${trades.length} error trade${trades.length !== 1 ? 's' : ''}`;
    else if (_mode === 'all') el.textContent = `${trades.length} trades (${errCount} errors incl.)`;
    else el.textContent = `${trades.length} clean trades`;
  }

  // ── Diagnostic panel (shown before simulation runs) ───────────────────────

  function renderDiagnostic(allTrades, activeTrds) {
    const el = document.getElementById('mc-diagnostic');
    if (!el) return;

    const errorCount = allTrades.filter(t => t.isError).length;
    const cleanCount = allTrades.length - errorCount;

    // Unique non-empty mistakes values (up to 10 for display)
    const uniqueMistakes = [...new Set(
      allTrades.map(t => t.mistakesRaw).filter(v => v)
    )].slice(0, 10);

    const modeLabel = _mode === 'all' ? 'All Trades' : _mode === 'errors' ? 'Errors Only' : 'Clean Only';
    const modeColor = _mode === 'errors' ? 'var(--red)' : _mode === 'all' ? 'var(--amber)' : 'var(--accent)';

    const mistakesSamples = uniqueMistakes.length
      ? uniqueMistakes.map(v => {
          const isErr = v.toUpperCase().startsWith('ERROR');
          const display = v.length > 35 ? v.slice(0, 35) + '…' : v;
          return `<span class="mc-diag-val ${isErr ? 'mc-diag-val-err' : 'mc-diag-val-ok'}">${display}</span>`;
        }).join('')
      : '<span style="color:var(--red);font-size:11px">⚠ Mistakes column empty — check CSV column index</span>';

    el.innerHTML = `
      <div class="mc-diag-bar">
        <div class="mc-diag-counts">
          <span class="mc-diag-item">Total loaded: <strong>${allTrades.length}</strong></span>
          <span class="mc-diag-sep">·</span>
          <span class="mc-diag-item mc-diag-clean">Clean: <strong>${cleanCount}</strong></span>
          <span class="mc-diag-sep">·</span>
          <span class="mc-diag-item mc-diag-err">Errors: <strong>${errorCount}</strong></span>
          <span class="mc-diag-sep">·</span>
          <span class="mc-diag-item" style="color:${modeColor}">Active (${modeLabel}): <strong>${activeTrds.length}</strong></span>
        </div>
        <div class="mc-diag-mistakes">
          <span class="mc-diag-label">Mistakes column values: </span>
          ${mistakesSamples}
        </div>
      </div>`;
  }

  // ── Public: toggle panel ──────────────────────────────────────────────────

  function toggle() {
    const content = document.getElementById('mc-content');
    const btn     = document.getElementById('mc-toggle-btn');
    if (!content) return;
    const isHidden = content.style.display === 'none' || content.style.display === '';
    content.style.display = isHidden ? 'block' : 'none';
    if (btn) btn.textContent = isHidden ? 'Hide Analysis ▲' : 'Show Monte Carlo Analysis ▼';
    if (isHidden && !mcResults) run();
  }

  // ── Public: run ───────────────────────────────────────────────────────────

  async function run() {
    const capitalInput  = document.getElementById('mc-capital');
    const tradesInput   = document.getElementById('mc-trades');
    const startCapital  = parseFloat((capitalInput ? capitalInput.value : '50000').replace(/[^0-9.]/g, '')) || 50000;
    const tradesPerYear = parseInt(tradesInput ? tradesInput.value : '250') || 250;

    setStatus('Loading trade data…', 'loading');

    try {
      if (!mcDataAll) {
        mcDataAll = await loadAllTrades();
      }

      const trades = activeTrades();

      if (trades.length < 5) {
        setStatus(`Insufficient data: ${trades.length} CSP/Bull Put trades found (need ≥ 5)`, 'error');
        return;
      }

      updateDataCount(trades);

      mcStats = calcStats(trades);
      const errCount = trades.filter(t => t.isError).length;

      // Show diagnostic + stats BEFORE the heavy simulation so the user can
      // verify filtering is correct while the simulation is running
      renderDiagnostic(mcDataAll, trades);
      renderStats(mcStats, _mode, errCount);
      setStatus(`Running ${SIMS.toLocaleString()} bootstrap simulations…`, 'loading');

      // Yield to UI before heavy computation
      await new Promise(resolve => setTimeout(resolve, 30));

      mcResults = runMonteCarlo(trades, startCapital, tradesPerYear);

      renderResults(mcResults, startCapital);
      renderRisk(mcResults, mcStats);
      renderCharts(mcResults, startCapital);

      const cfgEl = document.getElementById('mc-sim-config');
      if (cfgEl) cfgEl.textContent =
        `Starting Capital: ${$d(startCapital)} · Trades/Year: ${tradesPerYear} · Risk/Trade: ${(RISK * 100).toFixed(0)}% · Simulations: ${SIMS.toLocaleString()} · Method: Bootstrap`;

      setStatus(`Complete — ${trades.length} trades · ${SIMS.toLocaleString()} scenarios`, 'ok');
    } catch (err) {
      console.error('[MC]', err);
      setStatus(`Error: ${err.message}`, 'error');
    }
  }

  // ── Public: rerun (clear cache) ───────────────────────────────────────────

  function rerun() {
    mcDataAll = null;
    mcResults = null;
    mcStats   = null;
    run();
  }

  // ── Public: setMode — switch between clean/all/errors without refetching CSV

  function setMode(mode) {
    _mode     = mode;
    syncModeButtons();
    mcResults = null;
    mcStats   = null;
    run();
  }

  return { toggle, run, rerun, setMode };
})();
