const http = require('http');
const https = require('https');
const fs = require('fs');
const path = require('path');
const url = require('url');

const PORT = process.env.PORT || 3000;
const ANTHROPIC_API_KEY = process.env.ANTHROPIC_API_KEY;

const rateLimitMap = new Map();
const RATE_LIMIT_WINDOW = 60 * 60 * 1000;
const RATE_LIMIT_MAX = 15;

function getRateLimit(ip) {
  const now = Date.now();
  const entry = rateLimitMap.get(ip) || { count: 0, resetAt: now + RATE_LIMIT_WINDOW };
  if (now > entry.resetAt) { entry.count = 0; entry.resetAt = now + RATE_LIMIT_WINDOW; }
  entry.count++;
  rateLimitMap.set(ip, entry);
  return { count: entry.count, resetAt: entry.resetAt, limit: RATE_LIMIT_MAX };
}

function getClientIP(req) {
  return (req.headers['x-forwarded-for'] || req.socket.remoteAddress || '').split(',')[0].trim();
}

const MIME = {
  '.html': 'text/html; charset=utf-8',
  '.js': 'application/javascript',
  '.css': 'text/css',
  '.json': 'application/json',
  '.ico': 'image/x-icon',
};

function sendJSON(res, status, obj) {
  const body = JSON.stringify(obj);
  res.writeHead(status, { 'Content-Type': 'application/json', 'Content-Length': Buffer.byteLength(body) });
  res.end(body);
}

function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }

// ── FUND REGISTRY ──────────────────────────────────────────────────────────
// Loaded from AMFI at startup — covers all 15,000+ schemes
let fundRegistry = []; // [{schemeCode, schemeName, nameLower}]
let registryLoaded = false;
let registryLoading = false;

function httpsGet(hostname, reqPath, timeout = 30000) {
  return new Promise((resolve, reject) => {
    const req = https.request({
      hostname, path: reqPath, method: 'GET',
      headers: { 'User-Agent': 'FundAudit/4.0', 'Accept': 'application/json' }
    }, res => {
      let data = '';
      res.on('data', c => data += c);
      res.on('end', () => resolve({ status: res.statusCode, body: data }));
    });
    req.on('error', reject);
    req.setTimeout(timeout, () => { req.destroy(); reject(new Error(`Timeout ${timeout}ms`)); });
    req.end();
  });
}

async function loadFundRegistry() {
  if (registryLoaded || registryLoading) return;
  registryLoading = true;
  console.log('[Registry] Loading all AMFI fund schemes from mfapi.in...');
  try {
    const r = await httpsGet('api.mfapi.in', '/mf', 60000);
    if (r.status !== 200) throw new Error(`HTTP ${r.status}`);
    const all = JSON.parse(r.body);
    fundRegistry = all.map(s => ({
      schemeCode: s.schemeCode,
      schemeName: s.schemeName,
      nameLower: s.schemeName.toLowerCase()
    }));
    registryLoaded = true;
    console.log(`[Registry] Loaded ${fundRegistry.length} schemes`);
  } catch(e) {
    console.error('[Registry] Failed to load:', e.message);
    registryLoading = false;
  }
}

// Smart fund matching — finds best Regular Growth plan for any fund name
function findBestMatch(userInput) {
  if (!fundRegistry.length) return null;

  const input = userInput.toLowerCase().trim();

  // Extract key words — remove common noise words
  const noise = ['fund', 'mutual', 'plan', 'option', 'scheme', 'the', 'india', 'indian'];
  const words = input.split(/\s+/).filter(w => w.length > 2 && !noise.includes(w));

  // Score each fund scheme
  const scored = fundRegistry.map(f => {
    const n = f.nameLower;
    let score = 0;

    // Must contain all key words
    const allWordsMatch = words.every(w => n.includes(w));
    if (!allWordsMatch) return { ...f, score: 0 };

    score += words.length * 10; // base score for matching all words

    // Strongly prefer Regular Growth plans
    if (n.includes('regular')) score += 20;
    if (n.includes('growth')) score += 15;
    if (n.includes('gr ') || n.endsWith('gr')) score += 10;

    // Penalise Direct plans (we want Regular for Regular plan users)
    if (n.includes('direct')) score -= 30;
    if (n.includes(' - direct')) score -= 30;

    // Penalise dividend/IDCW plans
    if (n.includes('idcw') || n.includes('dividend') || n.includes('payout')) score -= 20;

    // Penalise bonus/other variants
    if (n.includes('bonus') || n.includes('annual') || n.includes('quarterly')) score -= 10;

    // Reward exact phrase matches
    if (n.includes(input)) score += 50;

    return { ...f, score };
  });

  // Sort by score descending
  scored.sort((a, b) => b.score - a.score);

  const best = scored[0];
  return best && best.score > 10 ? best : null;
}

// ── NAV COMPUTATION ────────────────────────────────────────────────────────
function parseNavDate(str) {
  if (!str) return null;
  const months = { jan:0,feb:1,mar:2,apr:3,may:4,jun:5,jul:6,aug:7,sep:8,oct:9,nov:10,dec:11 };
  const p = str.split('-');
  if (p.length !== 3) return null;
  const day = parseInt(p[0]);
  let month, year;
  if (isNaN(parseInt(p[1]))) {
    month = months[p[1].toLowerCase()];
    year = parseInt(p[2]);
  } else {
    month = parseInt(p[1]) - 1;
    year = parseInt(p[2]);
  }
  if (isNaN(day) || month == null || isNaN(year)) return null;
  return new Date(year, month, day);
}

function navAt(navData, targetDate) {
  // navData is sorted newest first
  let best = null, bestDiff = Infinity;
  for (const d of navData) {
    const nd = parseNavDate(d.date);
    if (!nd) continue;
    const diff = Math.abs(nd - targetDate);
    if (diff < bestDiff) { bestDiff = diff; best = parseFloat(d.nav); }
    if (nd < targetDate && bestDiff < 5 * 86400000) break; // found close enough past date
  }
  return best;
}

function computeCAGR(startNav, endNav, years) {
  if (!startNav || !endNav || years <= 0) return null;
  return ((Math.pow(endNav / startNav, 1 / years) - 1) * 100).toFixed(2);
}

function yearsAgo(n) {
  const d = new Date();
  d.setFullYear(d.getFullYear() - n);
  return d;
}

function calYearReturn(navData, year) {
  const s = navAt(navData, new Date(year, 0, 2));  // Jan 2
  const e = navAt(navData, new Date(year, 11, 30)); // Dec 30
  if (!s || !e) return null;
  return (((e - s) / s) * 100).toFixed(1);
}

async function getLiveFundData(fund) {
  const investAmt = parseFloat(fund.amt.replace(/[₹,\s]/g, '')) || 0;
  const fmt = v => new Intl.NumberFormat('en-IN', { style: 'currency', currency: 'INR', maximumFractionDigits: 0 }).format(v);

  // Find scheme
  let match = findBestMatch(fund.name);
  let schemeCode = match?.schemeCode;
  let schemeName = match?.schemeName;

  // Fallback: try mfapi search if registry didn't work
  if (!schemeCode) {
    console.log(`  [search fallback] ${fund.name}`);
    try {
      const q = encodeURIComponent(fund.name);
      const r = await httpsGet('api.mfapi.in', `/mf/search?q=${q}`);
      if (r.status === 200) {
        const schemes = JSON.parse(r.body);
        const best = schemes.find(s => {
          const n = s.schemeName.toLowerCase();
          return n.includes('regular') && n.includes('growth');
        }) || schemes.find(s => !s.schemeName.toLowerCase().includes('direct')) || schemes[0];
        if (best) { schemeCode = best.schemeCode; schemeName = best.schemeName; }
      }
    } catch(e) { console.warn(`  search failed: ${e.message}`); }
  }

  if (!schemeCode) {
    return { fund, error: `"${fund.name}" not found in AMFI database` };
  }

  // Fetch NAV history
  console.log(`  [NAV] ${schemeCode} — ${schemeName}`);
  const r = await httpsGet('api.mfapi.in', `/mf/${schemeCode}`);
  if (r.status !== 200) return { fund, error: `NAV fetch failed (HTTP ${r.status})` };

  const mfData = JSON.parse(r.body);
  const navData = mfData.data;
  schemeName = schemeName || mfData.meta.scheme_name;

  const latestNav = parseFloat(navData[0].nav);
  const latestDate = navData[0].date;

  // Trailing returns
  const nav1y = navAt(navData, yearsAgo(1));
  const nav3y = navAt(navData, yearsAgo(3));
  const nav5y = navAt(navData, yearsAgo(5));

  const ret1y = computeCAGR(nav1y, latestNav, 1);
  const ret3y = computeCAGR(nav3y, latestNav, 3);
  const ret5y = computeCAGR(nav5y, latestNav, 5);

  // Investment tracking
  const investDate = parseNavDate(fund.date);
  const navOnInvest = investDate ? navAt(navData, investDate) : null;
  const yearsHeld = investDate ? (Date.now() - investDate) / (365.25 * 24 * 3600 * 1000) : null;
  const currentValue = navOnInvest ? (investAmt * latestNav / navOnInvest) : null;
  const investCAGR = navOnInvest && yearsHeld ? computeCAGR(navOnInvest, latestNav, yearsHeld) : null;
  const absReturn = navOnInvest ? (((latestNav - navOnInvest) / navOnInvest) * 100).toFixed(2) : null;
  const gain = currentValue ? (currentValue - investAmt) : null;

  // Calendar year returns
  const BM = { 2020: 15.2, 2021: 24.1, 2022: 4.8, 2023: 22.3, 2024: 12.8, 2025: 6.5 };
  const calReturns = {};
  for (const yr of [2020, 2021, 2022, 2023, 2024, 2025]) {
    const r = calYearReturn(navData, yr);
    calReturns[yr] = r;
    calReturns[`${yr}Beat`] = r !== null ? parseFloat(r) > BM[yr] : false;
  }

  console.log(`    ✓ 1Y:${ret1y}% 3Y:${ret3y}% 5Y:${ret5y}% | Invested:₹${navOnInvest?.toFixed(2)} → Now:₹${latestNav}`);

  return {
    fund, schemeCode, schemeName,
    fundHouse: mfData.meta.fund_house,
    category: mfData.meta.scheme_category,
    latestNav, latestDate, navOnInvest,
    ret1y, ret3y, ret5y, calReturns,
    investAmt, currentValue, investCAGR, absReturn, gain,
    currentValueFmt: currentValue ? fmt(currentValue) : null,
    gainFmt: gain ? fmt(gain) : null,
    investAmtFmt: fmt(investAmt),
  };
}

// ── ANTHROPIC ──────────────────────────────────────────────────────────────
async function callAnthropic(messages, retries = 3) {
  const payload = { model: 'claude-sonnet-4-5', max_tokens: 7000, messages };
  const postData = JSON.stringify(payload);
  const opts = {
    hostname: 'api.anthropic.com', port: 443, path: '/v1/messages', method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      'x-api-key': ANTHROPIC_API_KEY,
      'anthropic-version': '2023-06-01',
      'Content-Length': Buffer.byteLength(postData)
    }
  };
  for (let attempt = 1; attempt <= retries; attempt++) {
    const result = await new Promise((resolve, reject) => {
      const req = https.request(opts, res => {
        let data = '';
        res.on('data', c => data += c);
        res.on('end', () => resolve({ status: res.statusCode, body: data }));
      });
      req.on('error', reject);
      req.setTimeout(120000, () => { req.destroy(); reject(new Error('Anthropic timeout')); });
      req.write(postData);
      req.end();
    });
    const parsed = JSON.parse(result.body);
    if (result.status === 529 || result.status === 500) {
      if (attempt < retries) { await sleep(attempt * 20000); continue; }
      throw new Error('AI service busy. Please retry in a few minutes.');
    }
    if (result.status === 429) throw new Error(parsed.error?.message || 'Rate limit. Wait 1 min and retry.');
    if (result.status !== 200) throw new Error(parsed.error?.message || `API error ${result.status}`);
    return parsed;
  }
}

// ── MAIN ANALYSIS ──────────────────────────────────────────────────────────
async function runAnalysis(funds) {
  const fmt = v => new Intl.NumberFormat('en-IN', { style: 'currency', currency: 'INR', maximumFractionDigits: 0 }).format(v);
  const totalInvested = funds.reduce((s, f) => s + (parseFloat(f.amt.replace(/[₹,\s]/g, '')) || 0), 0);

  // Ensure registry is loaded
  if (!registryLoaded) {
    console.log('[Analysis] Waiting for fund registry...');
    await loadFundRegistry();
  }

  // Phase 1: Fetch real AMFI data for all funds
  console.log(`\n[Phase 1] Fetching AMFI data for ${funds.length} funds`);
  const results = [];
  for (const fund of funds) {
    console.log(`  → ${fund.name}`);
    try {
      const data = await getLiveFundData(fund);
      results.push(data);
    } catch(e) {
      console.error(`  ✗ ${e.message}`);
      results.push({ fund, error: e.message });
    }
  }

  const fetched = results.filter(r => !r.error && r.ret1y);
  console.log(`[Phase 1] Done: ${fetched.length}/${funds.length} funds fetched successfully`);

  // Compute portfolio totals from real data
  const totalCurrentValue = results.reduce((s, r) => s + (r.currentValue || 0), 0);
  const hasAll = results.every(r => r.currentValue);

  // Build live data summary
  const liveDataStr = results.map(r => {
    if (r.error) return `FUND: ${r.fund.name}\nStatus: NOT FOUND — ${r.error}\nUse your knowledge for this fund.\n`;
    const c = r.calReturns;
    return `FUND: ${r.fund.name}
AMFI Name: ${r.schemeName}
Category: ${r.category}
Latest NAV: ₹${r.latestNav} (${r.latestDate})
NAV on ${r.fund.date}: ₹${r.navOnInvest?.toFixed(4) || 'N/A'}
Amount invested: ${r.investAmtFmt}
Current value: ${r.currentValueFmt || 'N/A'}
Gain: ${r.gainFmt || 'N/A'}
Absolute return: ${r.absReturn || 'N/A'}%
CAGR since investment: ${r.investCAGR || 'N/A'}%
1Y trailing CAGR: ${r.ret1y || 'N/A'}%
3Y trailing CAGR: ${r.ret3y || 'N/A'}%
5Y trailing CAGR: ${r.ret5y || 'N/A'}%
Calendar returns (from actual NAV):
  2020: ${c[2020]||'N/A'}% (BM: 15.2%) ${c['2020Beat']?'✓ BEAT':'✗ LAGGED'}
  2021: ${c[2021]||'N/A'}% (BM: 24.1%) ${c['2021Beat']?'✓ BEAT':'✗ LAGGED'}
  2022: ${c[2022]||'N/A'}% (BM:  4.8%) ${c['2022Beat']?'✓ BEAT':'✗ LAGGED'}
  2023: ${c[2023]||'N/A'}% (BM: 22.3%) ${c['2023Beat']?'✓ BEAT':'✗ LAGGED'}
  2024: ${c[2024]||'N/A'}% (BM: 12.8%) ${c['2024Beat']?'✓ BEAT':'✗ LAGGED'}
  2025: ${c[2025]||'N/A'}% (BM:  6.5%) ${c['2025Beat']?'✓ BEAT':'✗ LAGGED'}`;
  }).join('\n\n---\n\n');

  // Build pre-filled JSON template
  const fundsJSON = results.map(r => {
    const c = r.calReturns || {};
    const p5 = parseFloat(r.ret5y) || 0;
    const p3 = parseFloat(r.ret3y) || 0;
    const p1 = parseFloat(r.ret1y) || 0;
    return `{"name":"${r.fund.name}","manager":"FILL_REAL_NAME","tenureYrs":0,"tenureFlag":false,"cagr5y":"${r.ret5y||'FILL'}%","cagr3y":"${r.ret3y||'FILL'}%","ret1y":"${r.ret1y||'FILL'}%","sharpe":"FILL","beta":"FILL","stddev":"FILL","alpha":"FILL","ter":"FILL","aum":"FILL","quality":"FILL","decision":"FILL","perf5yVal":${p5},"perf3yVal":${p3},"ret1yVal":${p1},"sharpeVal":0,"calendarReturns":{"2020":"${c[2020]||'X'}%","2020Beat":${!!c['2020Beat']},"2021":"${c[2021]||'X'}%","2021Beat":${!!c['2021Beat']},"2022":"${c[2022]||'X'}%","2022Beat":${!!c['2022Beat']},"2023":"${c[2023]||'X'}%","2023Beat":${!!c['2023Beat']},"2024":"${c[2024]||'X'}%","2024Beat":${!!c['2024Beat']},"2025":"${c[2025]||'X'}%","2025Beat":${!!c['2025Beat']}},"quartile":"FILL","quartileLabel":"FILL","rolling1yAvg":"FILL","rolling1yBeatPct":"FILL","rolling1yWorst":"FILL","rolling3yAvg":"FILL","rolling3yBeatPct":"FILL","rolling3yMin":"FILL","realReturn":"FILL","estCurrentValue":"${r.currentValueFmt||'FILL'}","gainAmt":"${r.gainFmt||'FILL'}","ltcgTax":"FILL","netProceeds":"FILL","breakEvenMonths":3}`;
  }).join(',');

  // Phase 2: Claude fills remaining fields
  const prompt = `You are a CFA-level Indian mutual fund analyst. Below is REAL data fetched live from AMFI's official NAV database (mfapi.in). All return figures and current values are computed from actual historical NAV prices — they are 100% accurate.

═══ LIVE AMFI DATA (from actual NAV prices) ═══
${liveDataStr}

Portfolio invested: ${fmt(totalInvested)}
Portfolio current value: ${hasAll ? fmt(totalCurrentValue) : 'see individual funds above'}
Benchmark: Nifty 100 TRI — 5Y: 13.2% | 3Y: 14.0% | 1Y: +0.8%
CPI: 6.2% | Risk-free rate: 6.5%

═══ YOUR TASK ═══
The 1Y/3Y/5Y CAGR and calendar return values are PRE-FILLED from real AMFI data.
DO NOT change any return percentage that is already filled.
Only fill these using your knowledge:
- manager (real fund manager name for this specific fund as of 2026)
- tenureYrs (years this manager has managed this fund)
- tenureFlag (true if under 2 years)
- sharpe, beta, stddev, alpha (3Y trailing, from Value Research)
- ter (expense ratio for Regular plan, from AMFI)
- aum (Assets under Management in Crores, from AMFI)
- quality (Strong/Average/Weak based on performance vs benchmark)
- decision (Hold/Switch/Exit based on real data above)
- quartile, quartileLabel (peer category ranking)
- rolling1yAvg, rolling1yBeatPct, rolling1yWorst (estimates)
- rolling3yAvg, rolling3yBeatPct, rolling3yMin (estimates)
- realReturn (1Y return minus 6.2% CPI)
- ltcgTax (12.5% on gain above ₹1.25L, after LTCG exemption)
- netProceeds (currentValue minus ltcgTax)
- All FILL_REAL_NAME fields with actual manager names
- All CALC fields with computed values

Return ONLY valid JSON, no markdown, no text outside JSON:

{"summary":{"totalInvested":"${fmt(totalInvested)}","currentValue":"${hasAll ? fmt(totalCurrentValue) : 'CALC'}","blendedCAGR":"CALC_FROM_REAL","alphaBM":"CALC_VS_BENCHMARK","realReturn":"CALC_MINUS_CPI","annualTER":"CALC_TOTAL","fundsBeatBM":"X/${funds.length}","uniqueStocks":"~X","healthScore":"X.X/10","healthVerdict":"ONE_LINE_VERDICT","overlapPct":"X%","keyFlags":["SPECIFIC_FINDING_WITH_REAL_NUMBERS","SPECIFIC_FINDING","SPECIFIC_FINDING","SPECIFIC_FINDING"]},"funds":[${fundsJSON}],"benchmark":{"cagr5y":"13.2%","cagr3y":"14.0%","ret1y":"+0.8%","sharpe":"0.95","beta":"1.00","stddev":"12.8%","rolling1yAvg":"13.8%","rolling3yAvg":"14.4%","calendarReturns":{"2020":"+15.2%","2021":"+24.1%","2022":"+4.8%","2023":"+22.3%","2024":"+12.8%","2025":"+6.5%"}},"risk":{"blendedBeta":"X","bfsiPct":"X%","top5StocksPct":"X%","midSmallPct":"X%","uniqueStocks":"~X","stddev":"X%","maxDrawdown":"~-X%","downsideCap":"~X%","upsideCap":"~X%","stressScenarios":[{"label":"Bull +15%","impact":"CALC","pct":"+X%"},{"label":"Flat 3Y","impact":"CALC","pct":"-X%"},{"label":"Correction -20%","impact":"CALC","pct":"-X%"},{"label":"Crash -30%","impact":"CALC","pct":"-X%"}]},"sectors":[{"name":"BFSI","pct":35,"flag":true},{"name":"IT","pct":14,"flag":false},{"name":"Energy","pct":11,"flag":false},{"name":"Industrials","pct":10,"flag":false},{"name":"Consumer","pct":9,"flag":false},{"name":"Others","pct":21,"flag":false}],"overlap":{"overallPct":"X%","verdict":"X","topStocks":[{"stock":"HDFC Bank","funds":"X funds","avgWt":"X%","risk":"Very High"},{"stock":"ICICI Bank","funds":"X funds","avgWt":"X%","risk":"Very High"},{"stock":"Reliance","funds":"X funds","avgWt":"X%","risk":"High"},{"stock":"Infosys","funds":"X funds","avgWt":"X%","risk":"Moderate"},{"stock":"L&T","funds":"X funds","avgWt":"X%","risk":"Moderate"}]},"projections":{"corpus":"${hasAll ? fmt(totalCurrentValue) : fmt(totalInvested * 1.7)}","rows":[{"label":"Current portfolio","cagr":"CALC_BLENDED","y5":"CALC","y10":"CALC","y15":"CALC","y20":"CALC","type":"bad"},{"label":"Nifty 100 Index","cagr":"13.2%","y5":"CALC","y10":"CALC","y15":"CALC","y20":"CALC","type":"mid"},{"label":"Recommended portfolio","cagr":"~16%","y5":"CALC","y10":"CALC","y15":"CALC","y20":"CALC","type":"good"}],"gap20y":"CALC"},"recommended":[{"name":"Nippon India Large Cap","cat":"Large Cap","alloc":"25%","amt":"CALC","cagr5y":"15.98%","sharpe":"0.89","ter":"0.65%","role":"Core anchor"},{"name":"HDFC Mid-Cap Opp.","cat":"Mid Cap","alloc":"30%","amt":"CALC","cagr5y":"18.7%","sharpe":"0.82","ter":"0.75%","role":"Growth kicker"},{"name":"PPFAS Flexicap","cat":"Flexi Cap","alloc":"25%","amt":"CALC","cagr5y":"17.3%","sharpe":"0.88","ter":"0.59%","role":"Intl diversifier"},{"name":"Motilal Nifty 50 Index","cat":"Index","alloc":"20%","amt":"CALC","cagr5y":"13.5%","sharpe":"0.94","ter":"0.11%","role":"Passive core"}],"execution":[{"step":"Step 1 — April 2026","color":"bad","detail":"Exit worst performer. Use ₹1.25L LTCG exemption this FY."},{"step":"Step 2 — April 2027","color":"warn","detail":"Exit next underperformer with fresh ₹1.25L exemption."},{"step":"Step 3 — Oct 2027+","color":"ok","detail":"Annual rebalance. Exit Q3/Q4 funds for 2 years running."}],"scorecard":[{"label":"Performance consistency","score":X,"note":"Based on real AMFI returns vs benchmark"},{"label":"Diversification","score":X,"note":"Overlap % and category spread"},{"label":"Risk control","score":X,"note":"Downside vs upside capture ratio"},{"label":"Cost efficiency","score":X,"note":"TER vs real alpha generated"},{"label":"Overall health","score":X,"note":"Specific action required"}]}`;

  const response = await callAnthropic([{ role: 'user', content: prompt }]);
  return (response.content || []).filter(b => b.type === 'text').map(b => b.text).join('');
}

// ── HTTP SERVER ────────────────────────────────────────────────────────────
const server = http.createServer((req, res) => {
  const { pathname } = url.parse(req.url, true);

  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type');
  if (req.method === 'OPTIONS') { res.writeHead(204); res.end(); return; }

  if (pathname === '/health') {
    sendJSON(res, 200, {
      ok: true,
      key: !!ANTHROPIC_API_KEY,
      mode: 'amfi-v4-all-funds',
      registry: registryLoaded ? `${fundRegistry.length} schemes loaded` : 'loading...'
    });
    return;
  }

  if (pathname === '/api/debug') {
    httpsGet('api.mfapi.in', '/mf/119598/latest', 10000)
      .then(r => {
        let nav = null;
        try { nav = JSON.parse(r.body); } catch {}
        sendJSON(res, 200, {
          amfiReachable: r.status === 200,
          httpStatus: r.status,
          fundName: nav?.meta?.scheme_name,
          latestNAV: nav?.data?.[0]?.nav,
          registryLoaded,
          registrySize: fundRegistry.length
        });
      })
      .catch(e => sendJSON(res, 200, { amfiReachable: false, error: e.message, registryLoaded, registrySize: fundRegistry.length }));
    return;
  }

  if (pathname === '/api/search' && req.method === 'GET') {
    const q = url.parse(req.url, true).query.q || '';
    if (!q) { sendJSON(res, 400, { error: 'No query' }); return; }
    const match = findBestMatch(q);
    sendJSON(res, 200, { query: q, match, registrySize: fundRegistry.length });
    return;
  }

  if (pathname === '/api/analyse' && req.method === 'POST') {
    const ip = getClientIP(req);
    const rl = getRateLimit(ip);
    if (rl.count > rl.limit) { sendJSON(res, 429, { error: 'Rate limit reached. Try again later.' }); return; }
    if (!ANTHROPIC_API_KEY) { sendJSON(res, 500, { error: 'API key not configured on server.' }); return; }

    let body = '';
    req.on('data', c => { body += c; });
    req.on('end', async () => {
      let payload;
      try { payload = JSON.parse(body); } catch { sendJSON(res, 400, { error: 'Invalid JSON' }); return; }
      if (!payload.funds?.length) { sendJSON(res, 400, { error: 'No funds provided' }); return; }

      try {
        console.log(`[${new Date().toISOString()}] ${payload.funds.length} funds from ${ip}`);
        const text = await runAnalysis(payload.funds);
        sendJSON(res, 200, { content: [{ type: 'text', text }] });
      } catch(e) {
        console.error('Analysis failed:', e.message);
        sendJSON(res, 500, { error: e.message || 'Analysis failed. Please retry.' });
      }
    });
    return;
  }

  // Static files
  let filePath = path.join(__dirname, pathname === '/' ? 'index.html' : pathname.replace(/^\//, ''));
  if (!filePath.startsWith(__dirname)) { res.writeHead(403); res.end(); return; }
  const mime = MIME[path.extname(filePath)] || 'text/html; charset=utf-8';
  fs.readFile(filePath, (err, data) => {
    if (err) {
      fs.readFile(path.join(__dirname, 'index.html'), (e2, d2) => {
        if (e2) { res.writeHead(404); res.end('Not found'); }
        else { res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8' }); res.end(d2); }
      });
    } else { res.writeHead(200, { 'Content-Type': mime }); res.end(data); }
  });
});

// Start server and immediately begin loading fund registry
server.listen(PORT, () => {
  console.log(`FundAudit AMFI-v4 on port ${PORT} | API key: ${!!ANTHROPIC_API_KEY}`);
  loadFundRegistry(); // async, runs in background
});
