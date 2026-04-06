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

// ── HTTPS GET ──────────────────────────────────────────────────────────────
function httpsGet(hostname, reqPath, timeout = 20000) {
  return new Promise((resolve, reject) => {
    const req = https.request({
      hostname, path: reqPath, method: 'GET',
      headers: { 'User-Agent': 'FundAudit/5.0', 'Accept': 'application/json' }
    }, res => {
      let data = '';
      res.on('data', c => data += c);
      res.on('end', () => resolve({ status: res.statusCode, body: data }));
    });
    req.on('error', reject);
    req.setTimeout(timeout, () => { req.destroy(); reject(new Error(`Timeout after ${timeout}ms`)); });
    req.end();
  });
}

// ── SEARCH FUND ON MFAPI ──────────────────────────────────────────────────
// Uses /mf/search which is fast — returns only matching schemes, not all 15k
async function searchFund(fundName) {
  // Try multiple query variations to handle typos and abbreviations
  const queries = generateQueries(fundName);

  for (const query of queries) {
    try {
      const q = encodeURIComponent(query);
      const r = await httpsGet('api.mfapi.in', `/mf/search?q=${q}`, 15000);
      if (r.status !== 200) continue;

      const schemes = JSON.parse(r.body);
      if (!schemes || schemes.length === 0) continue;

      // Pick best match — prefer Regular Growth
      const best = pickBestScheme(schemes, fundName);
      if (best) {
        console.log(`  [found] "${query}" → ${best.schemeName} (${best.schemeCode})`);
        return best;
      }
    } catch(e) {
      console.warn(`  [search error] ${query}: ${e.message}`);
    }
  }
  return null;
}

// Generate multiple search queries to handle variations
function generateQueries(fundName) {
  const name = fundName.trim();
  const queries = [name];

  // Common abbreviation expansions
  const expansions = {
    'pru ': 'prudential ',
    'pru.': 'prudential',
    'pudential': 'prudential',
    'hdfc mc': 'hdfc mid cap',
    'bnp': 'bnp paribas',
    'flexi cap': 'flexicap',
    'flexicap': 'flexi cap',
    'multi cap': 'multicap',
    'multicap': 'multi cap',
    'mid cap': 'midcap',
    'midcap': 'mid cap',
    'small cap': 'smallcap',
    'smallcap': 'small cap',
    'large cap': 'largecap',
    'largecap': 'large cap',
  };

  let expanded = name.toLowerCase();
  for (const [abbr, full] of Object.entries(expansions)) {
    if (expanded.includes(abbr)) {
      queries.push(name.toLowerCase().replace(abbr, full));
    }
  }

  // Also try just the first 3 meaningful words
  const words = name.split(/\s+/).filter(w =>
    w.length > 2 &&
    !['fund', 'the', 'and', 'growth', 'regular', 'direct', 'plan', 'option'].includes(w.toLowerCase())
  );
  if (words.length >= 2) queries.push(words.slice(0, 3).join(' '));
  if (words.length >= 1) queries.push(words[0]); // AMC name alone as last resort

  return [...new Set(queries)]; // deduplicate
}

function pickBestScheme(schemes, userInput) {
  const input = userInput.toLowerCase();

  const scored = schemes.map(s => {
    const n = s.schemeName.toLowerCase();
    let score = 0;

    // Prefer Regular plan
    if (n.includes('regular')) score += 25;
    if (n.includes('growth') || n.includes('- gr ') || n.endsWith('- gr')) score += 20;

    // Penalise Direct, IDCW, dividend, institutional, bonus variants
    if (n.includes('direct')) score -= 40;
    if (n.includes('idcw') || n.includes('dividend') || n.includes('payout')) score -= 30;
    if (n.includes('bonus') || n.includes('weekly') || n.includes('monthly') || n.includes('quarterly')) score -= 20;
    if (n.includes('institutional') || n.includes('- i -') || n.includes('- ii -')) score -= 50;
    if (n.includes('series') || n.includes('fof') || n.includes('fund of fund')) score -= 30;

    // Reward keyword matches — but penalise EXTRA words not in user query
    const userWords = input.split(/\s+/).filter(w => w.length > 2 &&
      !['fund', 'plan', 'option', 'regular', 'growth', 'direct', 'india', 'the'].includes(w));
    for (const w of userWords) {
      if (n.includes(w)) score += 15;
    }

    // Penalise scheme names with extra category words not in user input
    // e.g. user typed "large cap" but scheme has "large & mid cap"
    if (!input.includes('mid') && n.includes('mid cap')) score -= 35;
    if (!input.includes('small') && n.includes('small cap')) score -= 35;
    if (!input.includes('flexi') && n.includes('flexi')) score -= 20;
    if (!input.includes('multi') && n.includes('multi cap')) score -= 20;
    if (!input.includes('balanced') && n.includes('balanced')) score -= 20;
    if (!input.includes('hybrid') && n.includes('hybrid')) score -= 20;

    return { ...s, score };
  });

  scored.sort((a, b) => b.score - a.score);
  const best = scored[0];
  return best && best.score > 0 ? best : null;
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
  return isNaN(day) || month == null || isNaN(year) ? null : new Date(year, month, day);
}

function navAt(navData, targetDate) {
  let best = null, bestDiff = Infinity;
  for (const d of navData) {
    const nd = parseNavDate(d.date);
    if (!nd) continue;
    const diff = Math.abs(nd - targetDate);
    if (diff < bestDiff) { bestDiff = diff; best = parseFloat(d.nav); }
    // Stop if we've gone past the target and found something close
    if (nd < targetDate && bestDiff < 7 * 86400000) break;
  }
  return best;
}

function cagr(start, end, years) {
  if (!start || !end || years <= 0) return null;
  return ((Math.pow(end / start, 1 / years) - 1) * 100).toFixed(2);
}

function yearsAgo(n) {
  const d = new Date();
  d.setFullYear(d.getFullYear() - n);
  return d;
}

function calYearReturn(navData, year) {
  const s = navAt(navData, new Date(year, 0, 3));
  const e = navAt(navData, new Date(year, 11, 29));
  if (!s || !e) return null;
  return (((e - s) / s) * 100).toFixed(1);
}

async function getLiveFundData(fund) {
  const investAmt = parseFloat(fund.amt.replace(/[₹,\s]/g, '')) || 0;
  const fmt = v => new Intl.NumberFormat('en-IN', { style: 'currency', currency: 'INR', maximumFractionDigits: 0 }).format(v);

  // Step 1: Search for fund scheme
  const scheme = await searchFund(fund.name);
  if (!scheme) return { fund, error: `"${fund.name}" not found in AMFI` };

  // Step 2: Fetch NAV history (limit to 6 years for speed)
  console.log(`  [NAV] Fetching ${scheme.schemeCode} - ${scheme.schemeName}`);
  let navData, fundMeta;

  // Fetch full NAV history
  const r = await httpsGet('api.mfapi.in', `/mf/${scheme.schemeCode}`, 25000);
  if (r.status !== 200) return { fund, error: `NAV fetch failed HTTP ${r.status}` };
  const mfParsed = JSON.parse(r.body);
  navData = mfParsed.data;
  fundMeta = mfParsed.meta;
  if (!navData || navData.length < 5) return { fund, error: 'Insufficient NAV data' };

  const latestNav = parseFloat(navData[0].nav);
  const latestDate = navData[0].date;

  // Step 3: Compute trailing returns
  const nav1y = navAt(navData, yearsAgo(1));
  const nav3y = navAt(navData, yearsAgo(3));
  const nav5y = navAt(navData, yearsAgo(5));
  const ret1y = cagr(nav1y, latestNav, 1);
  const ret3y = cagr(nav3y, latestNav, 3);
  const ret5y = cagr(nav5y, latestNav, 5);

  // Step 4: Investment tracking
  const investDate = parseNavDate(fund.date);
  const navOnInvest = investDate ? navAt(navData, investDate) : null;
  const yearsHeld = investDate ? (Date.now() - investDate) / (365.25 * 24 * 3600 * 1000) : null;
  const currentValue = navOnInvest ? (investAmt * latestNav / navOnInvest) : null;
  const investCAGR = navOnInvest && yearsHeld ? cagr(navOnInvest, latestNav, yearsHeld) : null;
  const absReturn = navOnInvest ? (((latestNav - navOnInvest) / navOnInvest) * 100).toFixed(2) : null;
  const gain = currentValue ? (currentValue - investAmt) : null;

  // Step 5: Calendar year returns
  const BM = { 2020: 15.2, 2021: 24.1, 2022: 4.8, 2023: 22.3, 2024: 12.8, 2025: 6.5 };
  const calReturns = {};
  for (const yr of [2020, 2021, 2022, 2023, 2024, 2025]) {
    const rv = calYearReturn(navData, yr);
    calReturns[yr] = rv;
    calReturns[`${yr}Beat`] = rv !== null ? parseFloat(rv) > BM[yr] : false;
  }

  console.log(`  [✓] 1Y:${ret1y}% 3Y:${ret3y}% 5Y:${ret5y}% NAV:₹${latestNav}`);

  return {
    fund, schemeCode: scheme.schemeCode,
    schemeName: scheme.schemeName,
    fundHouse: fundMeta?.fund_house,
    category: fundMeta?.scheme_category,
    latestNav, latestDate, navOnInvest,
    ret1y, ret3y, ret5y, calReturns,
    investAmt, currentValue, investCAGR, absReturn, gain,
    currentValueFmt: currentValue ? fmt(currentValue) : null,
    gainFmt: gain ? fmt(Math.abs(gain)) : null,
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
      req.setTimeout(240000, () => { req.destroy(); reject(new Error('Anthropic timeout — please retry')); });
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

// ── ANALYSIS ───────────────────────────────────────────────────────────────
async function runAnalysis(funds) {
  const fmt = v => new Intl.NumberFormat('en-IN', { style: 'currency', currency: 'INR', maximumFractionDigits: 0 }).format(v);
  const totalInvested = funds.reduce((s, f) => s + (parseFloat(f.amt.replace(/[₹,\s]/g, '')) || 0), 0);

  console.log(`\n[Phase 1] Fetching AMFI data for ${funds.length} funds IN PARALLEL`);
  // Fetch all funds simultaneously — much faster than sequential
  const results = await Promise.all(
    funds.map(async fund => {
      console.log(`  → ${fund.name}`);
      try {
        return await getLiveFundData(fund);
      } catch(e) {
        console.error(`  ✗ ${fund.name}: ${e.message}`);
        return { fund, error: e.message };
      }
    })
  );

  const fetched = results.filter(r => !r.error && r.ret1y);
  console.log(`[Phase 1] Done: ${fetched.length}/${funds.length} fetched`);

  const totalCurrentValue = results.reduce((s, r) => s + (r.currentValue || 0), 0);
  const hasAll = results.every(r => r.currentValue);

  // Build live data string
  const liveDataStr = results.map(r => {
    if (r.error) return `FUND: ${r.fund.name}\nStatus: NOT FOUND (${r.error})\nUse your best knowledge for this fund.\n`;
    const c = r.calReturns;
    const BM = { 2020: 15.2, 2021: 24.1, 2022: 4.8, 2023: 22.3, 2024: 12.8, 2025: 6.5 };
    return `FUND: ${r.fund.name}
AMFI Scheme: ${r.schemeName} (code: ${r.schemeCode})
Category: ${r.category}
Latest NAV: ₹${r.latestNav} as of ${r.latestDate}
NAV on purchase date (${r.fund.date}): ₹${r.navOnInvest?.toFixed(4) || 'N/A'}
Amount invested: ${r.investAmtFmt}
Current value: ${r.currentValueFmt || 'N/A'}
Absolute return: ${r.absReturn || 'N/A'}%
CAGR since purchase: ${r.investCAGR || 'N/A'}%
1Y trailing CAGR: ${r.ret1y || 'N/A'}%
3Y trailing CAGR: ${r.ret3y || 'N/A'}%
5Y trailing CAGR: ${r.ret5y || 'N/A'}%
Calendar returns vs Nifty 100 TRI:
  2020: ${c[2020]||'N/A'}% vs ${BM[2020]}% → ${c['2020Beat']?'BEAT':'LAGGED'}
  2021: ${c[2021]||'N/A'}% vs ${BM[2021]}% → ${c['2021Beat']?'BEAT':'LAGGED'}
  2022: ${c[2022]||'N/A'}% vs ${BM[2022]}% → ${c['2022Beat']?'BEAT':'LAGGED'}
  2023: ${c[2023]||'N/A'}% vs ${BM[2023]}% → ${c['2023Beat']?'BEAT':'LAGGED'}
  2024: ${c[2024]||'N/A'}% vs ${BM[2024]}% → ${c['2024Beat']?'BEAT':'LAGGED'}
  2025: ${c[2025]||'N/A'}% vs ${BM[2025]}% → ${c['2025Beat']?'BEAT':'LAGGED'}`;
  }).join('\n\n---\n\n');

  // Build pre-filled funds JSON
  const fundsJSON = results.map(r => {
    const c = r.calReturns || {};
    const p5 = parseFloat(r.ret5y) || 0;
    const p3 = parseFloat(r.ret3y) || 0;
    const p1 = parseFloat(r.ret1y) || 0;
    return `{"name":"${r.fund.name}","manager":"FILL","tenureYrs":0,"tenureFlag":false,"cagr5y":"${r.ret5y||'FILL'}%","cagr3y":"${r.ret3y||'FILL'}%","ret1y":"${r.ret1y||'FILL'}%","sharpe":"FILL","beta":"FILL","stddev":"FILL","alpha":"FILL","ter":"FILL","aum":"FILL","quality":"FILL","decision":"FILL","perf5yVal":${p5},"perf3yVal":${p3},"ret1yVal":${p1},"sharpeVal":0,"calendarReturns":{"2020":"${c[2020]||'X'}%","2020Beat":${!!c['2020Beat']},"2021":"${c[2021]||'X'}%","2021Beat":${!!c['2021Beat']},"2022":"${c[2022]||'X'}%","2022Beat":${!!c['2022Beat']},"2023":"${c[2023]||'X'}%","2023Beat":${!!c['2023Beat']},"2024":"${c[2024]||'X'}%","2024Beat":${!!c['2024Beat']},"2025":"${c[2025]||'X'}%","2025Beat":${!!c['2025Beat']}},"quartile":"FILL","quartileLabel":"FILL","rolling1yAvg":"FILL","rolling1yBeatPct":"FILL","rolling1yWorst":"FILL","rolling3yAvg":"FILL","rolling3yBeatPct":"FILL","rolling3yMin":"FILL","realReturn":"FILL","estCurrentValue":"${r.currentValueFmt||'FILL'}","gainAmt":"${r.gainFmt||'FILL'}","ltcgTax":"FILL","netProceeds":"FILL","breakEvenMonths":3}`;
  }).join(',');

  console.log(`[Phase 2] Calling Claude for analysis`);
  const prompt = `You are a CFA-level Indian mutual fund analyst. REAL AMFI NAV data is below — use these exact return figures.

LIVE AMFI DATA:
${liveDataStr}

Total invested: ${fmt(totalInvested)} | Current: ${hasAll ? fmt(totalCurrentValue) : 'per fund above'}
Benchmark Nifty 100 TRI: 5Y=13.2% 3Y=14.0% 1Y=+0.8% | CPI=6.2% | LTCG=12.5% above ₹1.25L/FY

Return ONLY valid JSON. CAGRs and calendar returns are PRE-FILLED — do not change them. Fill FILL/CALC with real values.

{"summary":{"totalInvested":"${fmt(totalInvested)}","currentValue":"${hasAll ? fmt(totalCurrentValue) : 'CALC'}","blendedCAGR":"CALC","alphaBM":"CALC","realReturn":"CALC","annualTER":"CALC","fundsBeatBM":"X/${funds.length}","uniqueStocks":"~X","healthScore":"X/10","healthVerdict":"SHORT","overlapPct":"X%","keyFlags":["FINDING1","FINDING2","FINDING3","FINDING4"]},"funds":[${fundsJSON}],"benchmark":{"cagr5y":"13.2%","cagr3y":"14.0%","ret1y":"+0.8%","sharpe":"0.95","beta":"1.00","stddev":"12.8%","rolling1yAvg":"13.8%","rolling3yAvg":"14.4%","calendarReturns":{"2020":"+15.2%","2021":"+24.1%","2022":"+4.8%","2023":"+22.3%","2024":"+12.8%","2025":"+6.5%"}},"risk":{"blendedBeta":"X","bfsiPct":"X%","top5StocksPct":"X%","midSmallPct":"X%","uniqueStocks":"~X","stddev":"X%","maxDrawdown":"~-X%","downsideCap":"~X%","upsideCap":"~X%","stressScenarios":[{"label":"Bull +15%","impact":"CALC","pct":"+X%"},{"label":"Flat 3Y","impact":"CALC","pct":"-X%"},{"label":"Correction -20%","impact":"CALC","pct":"-X%"},{"label":"Crash -30%","impact":"CALC","pct":"-X%"}]},"sectors":[{"name":"BFSI","pct":35,"flag":true},{"name":"IT","pct":14,"flag":false},{"name":"Energy","pct":11,"flag":false},{"name":"Industrials","pct":10,"flag":false},{"name":"Consumer","pct":9,"flag":false},{"name":"Others","pct":21,"flag":false}],"overlap":{"overallPct":"X%","verdict":"X","topStocks":[{"stock":"HDFC Bank","funds":"X","avgWt":"X%","risk":"Very High"},{"stock":"ICICI Bank","funds":"X","avgWt":"X%","risk":"Very High"},{"stock":"Reliance","funds":"X","avgWt":"X%","risk":"High"},{"stock":"Infosys","funds":"X","avgWt":"X%","risk":"Moderate"},{"stock":"L&T","funds":"X","avgWt":"X%","risk":"Moderate"}]},"projections":{"corpus":"${hasAll ? fmt(totalCurrentValue) : fmt(totalInvested*1.7)}","rows":[{"label":"Current portfolio","cagr":"CALC","y5":"CALC","y10":"CALC","y15":"CALC","y20":"CALC","type":"bad"},{"label":"Nifty 100 Index","cagr":"13.2%","y5":"CALC","y10":"CALC","y15":"CALC","y20":"CALC","type":"mid"},{"label":"Recommended portfolio","cagr":"~16%","y5":"CALC","y10":"CALC","y15":"CALC","y20":"CALC","type":"good"}],"gap20y":"CALC"},"recommended":[{"name":"Nippon India Large Cap","cat":"Large Cap","alloc":"25%","amt":"CALC","cagr5y":"15.98%","sharpe":"0.89","ter":"0.65%","role":"Core anchor"},{"name":"HDFC Mid-Cap Opp.","cat":"Mid Cap","alloc":"30%","amt":"CALC","cagr5y":"18.7%","sharpe":"0.82","ter":"0.75%","role":"Growth kicker"},{"name":"PPFAS Flexicap","cat":"Flexi Cap","alloc":"25%","amt":"CALC","cagr5y":"17.3%","sharpe":"0.88","ter":"0.59%","role":"Intl diversifier"},{"name":"Motilal Nifty 50 Index","cat":"Index","alloc":"20%","amt":"CALC","cagr5y":"13.5%","sharpe":"0.94","ter":"0.11%","role":"Passive core"}],"execution":[{"step":"Step 1 — Now","color":"bad","detail":"Exit worst fund. Use ₹1.25L LTCG exemption this FY."},{"step":"Step 2 — April 2027","color":"warn","detail":"Exit next underperformer with fresh ₹1.25L exemption."},{"step":"Step 3 — Oct 2027+","color":"ok","detail":"Annual rebalance. Exit Q3/Q4 funds 2 years running."}],"scorecard":[{"label":"Performance consistency","score":X,"note":"AMFI beat rate"},{"label":"Diversification","score":X,"note":"Overlap and spread"},{"label":"Risk control","score":X,"note":"Downside capture"},{"label":"Cost efficiency","score":X,"note":"TER vs alpha"},{"label":"Overall health","score":X,"note":"Key action"}]}`
  const response = await callAnthropic([{ role: 'user', content: prompt }]);
  const text = (response.content || []).filter(b => b.type === 'text').map(b => b.text).join('');
  console.log(`[Phase 2] Done. ${text.length} chars`);
  return text;
}

// ── HTTP SERVER ────────────────────────────────────────────────────────────
const server = http.createServer((req, res) => {
  const { pathname } = url.parse(req.url, true);

  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type');
  if (req.method === 'OPTIONS') { res.writeHead(204); res.end(); return; }

  if (pathname === '/health') {
    sendJSON(res, 200, { ok: true, key: !!ANTHROPIC_API_KEY, mode: 'amfi-v5-search' });
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
          testFund: nav?.meta?.scheme_name || 'parse error',
          latestNAV: nav?.data?.[0]?.nav
        });
      })
      .catch(e => sendJSON(res, 200, { amfiReachable: false, error: e.message }));
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
        // Hard 3-minute timeout so server never gets stuck
        const text = await Promise.race([
          runAnalysis(payload.funds),
          new Promise((_, reject) => setTimeout(() => reject(new Error('Analysis took too long. Please retry.')), 270000))
        ]);
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

// Start server immediately — no startup delay
server.listen(PORT, () => {
  console.log(`FundAudit AMFI-v5 on port ${PORT} | key:${!!ANTHROPIC_API_KEY}`);
  console.log('Ready. Fund data fetched on-demand per analysis request.');
});
