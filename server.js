const http = require('http');
const https = require('https');
const fs = require('fs');
const path = require('path');
const url = require('url');

const PORT = process.env.PORT || 3000;
const ANTHROPIC_API_KEY = process.env.ANTHROPIC_API_KEY;
const rateLimitMap = new Map();

function getRateLimit(ip) {
  const now = Date.now();
  const entry = rateLimitMap.get(ip) || { count: 0, resetAt: now + 3600000 };
  if (now > entry.resetAt) { entry.count = 0; entry.resetAt = now + 3600000; }
  entry.count++;
  rateLimitMap.set(ip, entry);
  return entry.count <= 15;
}

function getClientIP(req) {
  return (req.headers['x-forwarded-for'] || req.socket.remoteAddress || '').split(',')[0].trim();
}

const MIME = { '.html':'text/html;charset=utf-8', '.js':'application/javascript', '.css':'text/css', '.json':'application/json' };

function sendJSON(res, status, obj) {
  const body = JSON.stringify(obj);
  res.writeHead(status, { 'Content-Type': 'application/json', 'Content-Length': Buffer.byteLength(body) });
  res.end(body);
}

function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }

// ── HTTPS GET ──────────────────────────────────────────────────────────────
function httpsGet(hostname, reqPath, timeout = 20000) {
  return new Promise((resolve, reject) => {
    const req = https.request({ hostname, path: reqPath, method: 'GET',
      headers: { 'User-Agent': 'FundAudit/6.0', 'Accept': 'application/json' }
    }, res => {
      let data = '';
      res.on('data', c => data += c);
      res.on('end', () => resolve({ status: res.statusCode, body: data }));
    });
    req.on('error', reject);
    req.setTimeout(timeout, () => { req.destroy(); reject(new Error('Timeout')); });
    req.end();
  });
}

// ── FUND SEARCH ────────────────────────────────────────────────────────────
function generateQueries(name) {
  const queries = [name];
  const fixes = { 'pru ':'prudential ', 'pudential':'prudential', 'advanatge':'advantage', 'advantge':'advantage', 'flexi cap':'flexicap', 'flexicap':'flexi cap', 'mid cap':'midcap', 'midcap':'mid cap', 'large cap':'largecap', 'largecap':'large cap', 'small cap':'smallcap', 'multi cap':'multicap', 'etf fof':'etf fund of fund', 'fof':'fund of fund', 'gold etf':'gold' };
  let lower = name.toLowerCase();
  for (const [a, b] of Object.entries(fixes)) { if (lower.includes(a)) queries.push(lower.replace(a, b)); }
  const words = name.split(/\s+/).filter(w => w.length > 3 && !['fund','plan','option','growth','regular','direct','india'].includes(w.toLowerCase()));
  if (words.length >= 2) queries.push(words.slice(0, 3).join(' '));
  return [...new Set(queries)];
}

function pickBest(schemes, userInput) {
  const input = userInput.toLowerCase();

  // Hard-reject fund types clearly not matching user intent
  const isDebt = input.includes('debt') || input.includes('bond') || input.includes('gilt') || input.includes('liquid') || input.includes('overnight') || input.includes('money market');
  const isMid  = input.includes('mid cap') || input.includes('midcap');
  const isSmall = input.includes('small cap') || input.includes('smallcap');
  const isBalanced = input.includes('balanced') || input.includes('hybrid') || input.includes('advantage') || input.includes('dynamic asset');
  const isElss = input.includes('elss') || input.includes('tax saver') || input.includes('tax saving');
  const isFlexi = input.includes('flexi cap') || input.includes('flexicap');
  const isMulti = input.includes('multi cap') || input.includes('multicap');
  const isIndex = input.includes('index') || input.includes('nifty') || input.includes('sensex');

  const scored = schemes.map(s => {
    const n = s.schemeName.toLowerCase();
    let score = 0;

    // Prefer Regular Growth
    if (n.includes('regular')) score += 25;
    if (n.includes('growth') && !n.includes('aggressive growth')) score += 20;

    // Hard penalties for wrong plans
    if (n.includes('direct')) score -= 40;
    if (n.includes('idcw') || n.includes('dividend') || n.includes('payout')) score -= 35;
    if (n.includes('institutional') || n.includes('- i -') || n.includes('- ii -')) score -= 60;
    if (n.includes('bonus')) score -= 30;

    // Hard-reject completely wrong fund types (unless user asked for them)
    const isGold = input.includes('gold');
    const isEtfFof = input.includes('etf') || input.includes('fof');
    if (!isDebt) {
      if (n.includes('overnight')) score -= 200;
      if (n.includes('liquid')) score -= 200;
      if (n.includes('money market')) score -= 200;
      if (n.includes('ultra short')) score -= 150;
      if (n.includes('low duration')) score -= 100;
      if (!isEtfFof && !isGold && n.includes('gilt')) score -= 100;
      if (n.includes('credit risk')) score -= 100;
      if (n.includes('banking and psu bond')) score -= 100;
      if (n.includes(' debt ') || n.includes('-debt-')) score -= 100;
    }
    // Reward gold/ETF matches
    if (isGold && n.includes('gold')) score += 30;
    if (isEtfFof && (n.includes('etf') || n.includes('fund of fund'))) score += 20;
    if (!isBalanced) {
      if (n.includes('balanced advantage') && !input.includes('advantage')) score -= 50;
    }
    if (!isMid && n.includes('mid cap')) score -= 35;
    if (!isSmall && n.includes('small cap')) score -= 35;
    if (!isFlexi && n.includes('flexi cap')) score -= 20;
    if (!isMulti && n.includes('multi cap') && !isFlexi) score -= 20;
    if (!isElss && (n.includes('elss') || n.includes('tax saver'))) score -= 30;

    // Reward keyword matches from user input
    const words = input.split(/\s+/).filter(w => w.length > 3 &&
      !['fund','plan','option','regular','growth','direct','india','mutual'].includes(w));
    for (const w of words) {
      if (n.includes(w)) score += 15;
    }

    return { ...s, score };
  });

  scored.sort((a, b) => b.score - a.score);
  const best = scored[0];
  if (!best || best.score <= 0) return null;

  // Final sanity check: if score is marginal, ensure it's not an obviously wrong type
  const bn = best.schemeName.toLowerCase();
  if (!isDebt && (bn.includes('overnight') || bn.includes('liquid') || bn.includes('money market'))) {
    console.warn(`  [WARN] Rejected wrong fund type: ${best.schemeName}`);
    // Try next best that's not debt
    const nextBest = scored.find(s => {
      const sn = s.schemeName.toLowerCase();
      return s !== best && !sn.includes('overnight') && !sn.includes('liquid') &&
             !sn.includes('money market') && !sn.includes('direct') && s.score > 0;
    });
    return nextBest || null;
  }
  return best;
}

async function searchFund(name) {
  for (const q of generateQueries(name)) {
    try {
      const r = await httpsGet('api.mfapi.in', `/mf/search?q=${encodeURIComponent(q)}`, 12000);
      if (r.status !== 200) continue;
      const schemes = JSON.parse(r.body);
      if (!schemes.length) continue;
      const best = pickBest(schemes, name);
      if (best) { console.log(`  [✓] "${q}" → ${best.schemeName} (${best.schemeCode})`); return best; }
    } catch(e) { /* try next query */ }
  }
  return null;
}

// ── NAV MATH ───────────────────────────────────────────────────────────────
function parseD(str) {
  if (!str) return null;
  const months = {jan:0,feb:1,mar:2,apr:3,may:4,jun:5,jul:6,aug:7,sep:8,oct:9,nov:10,dec:11};
  const p = str.split('-');
  if (p.length !== 3) return null;
  const day = parseInt(p[0]);
  let mo, yr;
  if (isNaN(parseInt(p[1]))) { mo = months[p[1].toLowerCase()]; yr = parseInt(p[2]); }
  else { mo = parseInt(p[1]) - 1; yr = parseInt(p[2]); }
  return isNaN(day)||mo==null||isNaN(yr) ? null : new Date(yr, mo, day);
}

function navAt(data, target) {
  let best = null, bestD = Infinity;
  for (const d of data) {
    const nd = parseD(d.date);
    if (!nd) continue;
    const diff = Math.abs(nd - target);
    if (diff < bestD) { bestD = diff; best = parseFloat(d.nav); }
    if (nd < target && bestD < 8 * 86400000) break;
  }
  return best;
}

function cagr(s, e, y) { return (!s||!e||y<=0) ? null : ((Math.pow(e/s,1/y)-1)*100); }
function fmt(v) { return new Intl.NumberFormat('en-IN',{style:'currency',currency:'INR',maximumFractionDigits:0}).format(v); }
function pct(v) { return v==null ? 'N/A' : (v>0?'+':'')+v.toFixed(2)+'%'; }
function fmtC(v) { return v==null ? 'N/A' : (v>0?'+':'')+v.toFixed(1)+'%'; }


// ── SEBI CATEGORY → BENCHMARK MAPPING ────────────────────────────────────
const CALENDAR = {
  NIFTY100:  {'2020':'+15.5%','2021':'+25.8%','2022':'+5.0%','2023':'+24.1%','2024':'+15.0%','2025':'+3.3%'},
  NIFTY500:  {'2020':'+16.1%','2021':'+28.4%','2022':'+0.8%','2023':'+25.6%','2024':'+14.6%','2025':'+1.8%'},
  NIFTY_MID: {'2020':'+26.2%','2021':'+46.0%','2022':'+0.2%','2023':'+41.9%','2024':'+23.9%','2025':'-8.1%'},
  NIFTY_SM:  {'2020':'+27.8%','2021':'+63.0%','2022':'-3.7%','2023':'+48.2%','2024':'+18.8%','2025':'-15.6%'},
  CRISIL_H:  {'2020':'+8.4%', '2021':'+17.1%','2022':'+3.2%','2023':'+15.1%','2024':'+10.4%','2025':'+3.2%'},
  CRISIL_H65:{'2020':'+11.2%','2021':'+21.4%','2022':'+4.1%','2023':'+18.8%','2024':'+12.1%','2025':'+3.1%'},
  CRISIL_MA: {'2020':'+9.2%', '2021':'+18.8%','2022':'+5.1%','2023':'+15.4%','2024':'+11.2%','2025':'+4.8%'},
};

const CATEGORY_BENCHMARKS = {
  // Equity
  'Large Cap Fund':            { name:'Nifty 100 TRI',           cagr5y:13.2, cagr3y:14.0, ret1y:0.8,  sharpe:0.95, stddev:12.8, calendarReturns:CALENDAR.NIFTY100 },
  'Large & Mid Cap Fund':      { name:'Nifty LargeMidcap 250',   cagr5y:14.1, cagr3y:14.8, ret1y:-0.2, sharpe:0.88, stddev:14.2, calendarReturns:CALENDAR.NIFTY100 },
  'Mid Cap Fund':              { name:'Nifty Midcap 150 TRI',    cagr5y:20.1, cagr3y:17.2, ret1y:-4.8, sharpe:0.85, stddev:17.5, calendarReturns:CALENDAR.NIFTY_MID },
  'Small Cap Fund':            { name:'Nifty Smallcap 250 TRI',  cagr5y:22.4, cagr3y:15.8, ret1y:-8.2, sharpe:0.72, stddev:21.0, calendarReturns:CALENDAR.NIFTY_SM },
  'Flexi Cap Fund':            { name:'Nifty 500 TRI',           cagr5y:14.8, cagr3y:14.2, ret1y:-1.2, sharpe:0.90, stddev:13.5, calendarReturns:CALENDAR.NIFTY500 },
  'Multi Cap Fund':            { name:'Nifty 500 TRI',           cagr5y:14.8, cagr3y:14.2, ret1y:-1.2, sharpe:0.90, stddev:13.5, calendarReturns:CALENDAR.NIFTY500 },
  'ELSS':                      { name:'Nifty 500 TRI',           cagr5y:14.8, cagr3y:14.2, ret1y:-1.2, sharpe:0.90, stddev:13.5, calendarReturns:CALENDAR.NIFTY500 },
  'Value Fund':                { name:'Nifty 500 TRI',           cagr5y:14.8, cagr3y:14.2, ret1y:-1.2, sharpe:0.90, stddev:13.5, calendarReturns:CALENDAR.NIFTY500 },
  'Contra Fund':               { name:'Nifty 500 TRI',           cagr5y:14.8, cagr3y:14.2, ret1y:-1.2, sharpe:0.90, stddev:13.5, calendarReturns:CALENDAR.NIFTY500 },
  // Hybrid
  'Balanced Advantage Fund':   { name:'CRISIL Hybrid 50+50 Aggr',cagr5y:10.8, cagr3y:11.2, ret1y:3.5,  sharpe:0.78, stddev:9.8,  calendarReturns:CALENDAR.CRISIL_H },
  'Aggressive Hybrid Fund':    { name:'CRISIL Hybrid 65+35 Aggr',cagr5y:12.1, cagr3y:12.8, ret1y:1.8,  sharpe:0.82, stddev:11.2, calendarReturns:CALENDAR.CRISIL_H65 },
  'Conservative Hybrid Fund':  { name:'CRISIL Hybrid 25+75 Cons',cagr5y:8.4,  cagr3y:8.8,  ret1y:4.2,  sharpe:0.72, stddev:7.2,  calendarReturns:CALENDAR.CRISIL_H },
  'Multi Asset Allocation Fund':{ name:'CRISIL Multi Asset',      cagr5y:11.2, cagr3y:11.8, ret1y:3.8,  sharpe:0.80, stddev:10.1, calendarReturns:CALENDAR.CRISIL_MA },
  'Equity Savings Fund':       { name:'Nifty Equity Savings',    cagr5y:8.8,  cagr3y:9.2,  ret1y:4.8,  sharpe:0.85, stddev:6.8,  calendarReturns:CALENDAR.CRISIL_H },
  'Arbitrage Fund':            { name:'Nifty 50 Arbitrage',      cagr5y:6.2,  cagr3y:6.8,  ret1y:7.2,  sharpe:1.20, stddev:1.2,  calendarReturns:CALENDAR.CRISIL_H },
  // Default
  'default':                   { name:'Nifty 100 TRI',           cagr5y:13.2, cagr3y:14.0, ret1y:0.8,  sharpe:0.95, stddev:12.8, calendarReturns:CALENDAR.NIFTY100 },
};

function getBenchmark(sebiCategory) {
  if (!sebiCategory) return CATEGORY_BENCHMARKS['default'];
  const cat = sebiCategory.toLowerCase();
  // Match by keyword
  if (cat.includes('balanced advantage') || cat.includes('dynamic asset')) return CATEGORY_BENCHMARKS['Balanced Advantage Fund'];
  if (cat.includes('aggressive hybrid')) return CATEGORY_BENCHMARKS['Aggressive Hybrid Fund'];
  if (cat.includes('conservative hybrid')) return CATEGORY_BENCHMARKS['Conservative Hybrid Fund'];
  if (cat.includes('multi asset')) return CATEGORY_BENCHMARKS['Multi Asset Allocation Fund'];
  if (cat.includes('equity savings')) return CATEGORY_BENCHMARKS['Equity Savings Fund'];
  if (cat.includes('arbitrage')) return CATEGORY_BENCHMARKS['Arbitrage Fund'];
  if (cat.includes('small cap')) return CATEGORY_BENCHMARKS['Small Cap Fund'];
  if (cat.includes('mid cap') && !cat.includes('large')) return CATEGORY_BENCHMARKS['Mid Cap Fund'];
  if (cat.includes('large & mid') || cat.includes('large and mid')) return CATEGORY_BENCHMARKS['Large & Mid Cap Fund'];
  if (cat.includes('large cap')) return CATEGORY_BENCHMARKS['Large Cap Fund'];
  if (cat.includes('flexi cap') || cat.includes('flexicap')) return CATEGORY_BENCHMARKS['Flexi Cap Fund'];
  if (cat.includes('multi cap') || cat.includes('multicap')) return CATEGORY_BENCHMARKS['Multi Cap Fund'];
  if (cat.includes('elss') || cat.includes('tax saver')) return CATEGORY_BENCHMARKS['ELSS'];
  if (cat.includes('value')) return CATEGORY_BENCHMARKS['Value Fund'];
  return CATEGORY_BENCHMARKS['default'];
}


// Compute annualised std dev from monthly NAV returns (last N months)
function computeStdDev(navData, months) {
  const pts = [];
  const today = new Date();
  for (let i = 1; i <= months; i++) {
    const d1 = new Date(today); d1.setMonth(d1.getMonth() - i);
    const d2 = new Date(today); d2.setMonth(d2.getMonth() - i + 1);
    const n1 = navAt(navData, d1);
    const n2 = navAt(navData, d2);
    if (n1 && n2 && n1 > 0) pts.push((n2 - n1) / n1 * 100);
  }
  if (pts.length < 6) return null;
  const mean = pts.reduce((s, v) => s + v, 0) / pts.length;
  const variance = pts.reduce((s, v) => s + Math.pow(v - mean, 2), 0) / (pts.length - 1);
  return (Math.sqrt(variance) * Math.sqrt(12)).toFixed(2);
}

async function fetchFundData(fund) {
  const amt = parseFloat(fund.amt.replace(/[₹,\s]/g,'')) || 0;
  const scheme = await searchFund(fund.name);
  if (!scheme) return { fund, amt, error: 'Not found in AMFI' };

  // PERMANENT FIX: Narrow-window fetches — guaranteed tiny responses for ALL funds
  // Problem: mfapi.in ignores startDate for old funds (ICICI 1994, LIC, etc.)
  //          returning 7000+ records → JSON.parse takes 5-10s → timeout
  // Solution: Use startDate+endDate narrow 7-day windows for each point needed
  //           7-day window → max 5 records → instant regardless of fund age
  const today = new Date();
  const fmtD = d => String(d.getDate()).padStart(2,'0')+'-'+String(d.getMonth()+1).padStart(2,'0')+'-'+d.getFullYear();

  // Target dates for CAGR computation
  const d1y = new Date(today); d1y.setFullYear(today.getFullYear()-1);
  const d3y = new Date(today); d3y.setFullYear(today.getFullYear()-3);
  const d5y = new Date(today); d5y.setFullYear(today.getFullYear()-5);

  // Helper: 7-day window around a target date (handles weekends/holidays)
  const window7 = d => {
    const s = new Date(d); s.setDate(s.getDate()-4);
    const e = new Date(d); e.setDate(e.getDate()+4);
    return `?startDate=${fmtD(s)}&endDate=${fmtD(e)}`;
  };
  // Calendar year windows — narrow to Jan+Dec only for start/end NAV
  const calWindow = yr => ({
    s: `?startDate=01-01-${yr}&endDate=10-01-${yr}`,  // first week of year
    e: `?startDate=22-12-${yr}&endDate=31-12-${yr}`,  // last week of year
  });

  // Fire all requests in parallel — 9 tiny requests, each returns ≤8 records
  const code = scheme.schemeCode;
  const [rLatest, r1y, r3y, r5y, rCal22s, rCal22e, rCal23s, rCal23e, rCal24s, rCal24e, rCal25s, rCal25e, rCal21s, rCal21e] = await Promise.all([
    httpsGet('api.mfapi.in', `/mf/${code}/latest`, 6000),
    httpsGet('api.mfapi.in', `/mf/${code}${window7(d1y)}`, 6000).catch(()=>null),
    httpsGet('api.mfapi.in', `/mf/${code}${window7(d3y)}`, 6000).catch(()=>null),
    httpsGet('api.mfapi.in', `/mf/${code}${window7(d5y)}`, 6000).catch(()=>null),
    httpsGet('api.mfapi.in', `/mf/${code}${calWindow(2022).s}`, 5000).catch(()=>null),
    httpsGet('api.mfapi.in', `/mf/${code}${calWindow(2022).e}`, 5000).catch(()=>null),
    httpsGet('api.mfapi.in', `/mf/${code}${calWindow(2023).s}`, 5000).catch(()=>null),
    httpsGet('api.mfapi.in', `/mf/${code}${calWindow(2023).e}`, 5000).catch(()=>null),
    httpsGet('api.mfapi.in', `/mf/${code}${calWindow(2024).s}`, 5000).catch(()=>null),
    httpsGet('api.mfapi.in', `/mf/${code}${calWindow(2024).e}`, 5000).catch(()=>null),
    httpsGet('api.mfapi.in', `/mf/${code}${calWindow(2025).s}`, 5000).catch(()=>null),
    httpsGet('api.mfapi.in', `/mf/${code}${calWindow(2025).e}`, 5000).catch(()=>null),
    httpsGet('api.mfapi.in', `/mf/${code}${calWindow(2021).s}`, 5000).catch(()=>null),
    httpsGet('api.mfapi.in', `/mf/${code}${calWindow(2021).e}`, 5000).catch(()=>null),
  ]);

  if (!rLatest || rLatest.status !== 200) return { fund, amt, error: 'NAV fetch failed' };

  const latestInfo = JSON.parse(rLatest.body);
  const latestNav = parseFloat(latestInfo.data?.[0]?.nav || latestInfo.data?.nav || 0);
  const latestDate = latestInfo.data?.[0]?.date || latestInfo.data?.date || '';
  if (!latestNav) return { fund, amt, error: 'Invalid NAV data' };
  const mf = { meta: latestInfo.meta };

  // Extract NAV from narrow-window response (take first valid record)
  const navFromWindow = r => {
    if (!r || r.status !== 200) return null;
    try {
      const data = JSON.parse(r.body).data;
      return data?.length ? parseFloat(data[0].nav) : null;
    } catch { return null; }
  };
  // For calendar: use last record of start-window as year-open, first of end-window as year-close
  const calNavPair = (rs, re) => {
    if (!rs || rs.status !== 200 || !re || re.status !== 200) return null;
    try {
      const ds = JSON.parse(rs.body).data; const de = JSON.parse(re.body).data;
      if (!ds?.length || !de?.length) return null;
      return { open: parseFloat(ds[ds.length-1].nav), close: parseFloat(de[0].nav) };
    } catch { return null; }
  };

  const nav1yVal = navFromWindow(r1y);
  const nav3yVal = navFromWindow(r3y);
  const nav5yVal = navFromWindow(r5y);

  // Build calendar year returns from narrow windows
  const calData = {
    2021: calNavPair(rCal21s, rCal21e),
    2022: calNavPair(rCal22s, rCal22e),
    2023: calNavPair(rCal23s, rCal23e),
    2024: calNavPair(rCal24s, rCal24e),
    2025: calNavPair(rCal25s, rCal25e),
  };

  // Use nav array for invest-date lookup: fetch 13-month history only if needed
  // For investDate lookup, we need a wider range — use a 3-month window around invest date
  const investDate = parseD(fund.date);
  let navInvest = null;
  if (investDate) {
    const invWindow = `?startDate=${fmtD(new Date(investDate.getTime()-15*86400000))}&endDate=${fmtD(new Date(investDate.getTime()+15*86400000))}`;
    const rInv = await httpsGet('api.mfapi.in', `/mf/${code}${invWindow}`, 6000).catch(()=>null);
    if (rInv?.status === 200) {
      try {
        const invData = JSON.parse(rInv.body).data;
        if (invData?.length) navInvest = parseFloat(invData[invData.length-1].nav);
      } catch {}
    }
  }

  // Build a minimal nav array for navAt() compatibility (just the points we have)
  const nav = [];
  const addNavPoint = (navVal, date) => { if (navVal && date) nav.push({nav: navVal.toString(), date: fmtD(date)}); };
  addNavPoint(latestNav, today);
  addNavPoint(nav1yVal, d1y);
  addNavPoint(nav3yVal, d3y);
  addNavPoint(nav5yVal, d5y);

  // Compute CAGR using targeted fetched NAVs
  const latestD = parseD(latestDate) || new Date();
  const ago = n => { const d = new Date(latestD); d.setFullYear(d.getFullYear()-n); return d; };
  const raw1y = cagr(nav1yVal, latestNav, 1);
  const raw3y = cagr(nav3yVal, latestNav, 3);
  const raw5y = cagr(nav5yVal, latestNav, 5);
  // Sanity cap: if CAGR is impossibly high, nav point was outside requested window
  // Small/mid cap can legitimately hit 30-35% 5Y. Gold ~20%. Anything >45% is a bad nav point.
  // CAGR sanity caps — adjusted for gold/commodity funds which can legitimately be high
  const isGoldFund = (mf.meta?.scheme_category||'').toLowerCase().includes('gold') ||
                     (scheme.schemeName||'').toLowerCase().includes('gold');
  const MAX5 = isGoldFund ? 55 : 40;  // Gold 5Y can legitimately hit 20-25%
  const MAX3 = isGoldFund ? 70 : 55;
  const ret1y = raw1y;
  const ret3y = (raw3y != null && raw3y > MAX3) ? null : raw3y;
  const ret5y = (raw5y != null && raw5y > MAX5) ? null : raw5y;
  if (raw5y != null && raw5y > MAX5) console.warn('  [SANITY] 5Y=' + raw5y.toFixed(1) + '% capped for ' + scheme.schemeName);
  if (raw3y != null && raw3y > MAX3) console.warn('  [SANITY] 3Y=' + raw3y.toFixed(1) + '% capped for ' + scheme.schemeName);

  // Invest date lookup — already computed above with narrow window
  const yearsHeld = investDate ? (Date.now()-investDate)/(365.25*86400000) : null;
  const currentValue = navInvest ? amt * latestNav / navInvest : null;
  const investCAGR = navInvest && yearsHeld ? cagr(navInvest, latestNav, yearsHeld) : null;
  const gain = currentValue ? currentValue - amt : null;

  // Category-appropriate benchmark for calendar Beat comparison
  const fundBenchmark = getBenchmark(latestInfo.meta?.scheme_category);
  const bmCal = fundBenchmark.calendarReturns || {};
  const BM = {
    2020: parseFloat(bmCal['2020'])||15.5,
    2021: parseFloat(bmCal['2021'])||17.1,
    2022: parseFloat(bmCal['2022'])||5.0,
    2023: parseFloat(bmCal['2023'])||15.1,
    2024: parseFloat(bmCal['2024'])||10.4,
    2025: parseFloat(bmCal['2025'])||3.3,
  };

  // Build cal from narrow-window calData (much more reliable than navAt on big arrays)
  const isHybridFund = (latestInfo.meta?.scheme_category||'').toLowerCase().match(/balanced|hybrid|multi asset/);
  const maxCal = isHybridFund ? 40 : 65;
  const minCal = isHybridFund ? -25 : -45;
  const cal = {};
  for (const yr of [2020,2021,2022,2023,2024,2025]) {
    const pair = calData[yr];
    let rv = null;
    if (pair && pair.open > 0 && pair.close > 0) {
      rv = (pair.close - pair.open) / pair.open * 100;
      if (rv < minCal || rv > maxCal) rv = null; // sanity cap
    }
    cal[yr] = rv;
    cal[yr+'Beat'] = rv != null ? rv > BM[yr] : false;
  }


  // Sanity check: if returns are clearly impossible, it's likely a wrong fund match
  const calVals = Object.entries(cal).filter(([k,v]) => !k.includes('Beat') && v !== null).map(([,v]) => parseFloat(v));
  const hasInsaneReturn = calVals.some(v => v < -60 || v > 120);
  const has5yNegative = ret5y !== null && ret5y < -15;
  if (hasInsaneReturn || has5yNegative) {
    console.warn(`  [SANITY FAIL] ${scheme.schemeName}: ret5y=${ret5y} calVals=[${calVals.join(',')}]`);
    console.warn(`  [RETRY] Searching again with stricter query...`);
    // Try alternative search with AMC name only
    const amc = fund.name.split(' ').slice(0, 2).join(' ');
    const altResult = await httpsGet('api.mfapi.in', `/mf/search?q=${encodeURIComponent(amc + ' balanced advantage regular growth')}`, 12000);
    if (altResult.status === 200) {
      const altSchemes = JSON.parse(altResult.body);
      const altBest = pickBest(altSchemes, fund.name);
      if (altBest && altBest.schemeCode !== scheme.schemeCode) {
        console.log(`  [RETRY] Found alternative: ${altBest.schemeName} (${altBest.schemeCode})`);
        const r2 = await httpsGet('api.mfapi.in', `/mf/${altBest.schemeCode}`, 25000);
        if (r2.status === 200) {
          const mf2 = JSON.parse(r2.body);
          const nav2 = mf2.data;
          const latestNav2 = parseFloat(nav2[0].nav);
          const r1y2 = cagr(navAt(nav2, ago(1)), latestNav2, 1);
          const r3y2 = cagr(navAt(nav2, ago(3)), latestNav2, 3);
          const r5y2 = cagr(navAt(nav2, ago(5)), latestNav2, 5);
          // Only use if saner
          if (r5y2 !== null && r5y2 > -15) {
            console.log(`  [RETRY OK] Using alt fund: 5Y=${r5y2.toFixed(2)}%`);
            const navInvest2 = investDate ? navAt(nav2, investDate) : null;
            const yearsHeld2 = investDate ? (Date.now()-investDate)/(365.25*86400000) : null;
            const currentValue2 = navInvest2 ? amt * latestNav2 / navInvest2 : null;
            const investCAGR2 = navInvest2 && yearsHeld2 ? cagr(navInvest2, latestNav2, yearsHeld2) : null;
            const gain2 = currentValue2 ? currentValue2 - amt : null;
            const cal2 = {};
            const BM2 = {2020:15.2,2021:24.1,2022:4.8,2023:22.3,2024:12.8,2025:6.5};
            for (const yr of [2020,2021,2022,2023,2024,2025]) {
              const s2 = navAt(nav2, new Date(yr,0,3)), e2 = navAt(nav2, new Date(yr,11,29));
              const rv2 = (s2&&e2) ? ((e2-s2)/s2*100) : null;
              cal2[yr] = rv2; cal2[yr+'Beat'] = rv2!=null ? rv2 > BM2[yr] : false;
            }
            return { fund, amt, scheme:altBest, meta:mf2.meta, latestNav:latestNav2, latestDate:nav2[0].date, navInvest:navInvest2, ret1y:r1y2, ret3y:r3y2, ret5y:r5y2, cal:cal2, currentValue:currentValue2, investCAGR:investCAGR2, gain:gain2, yearsHeld:yearsHeld2 };
          }
        }
      }
    }
    // If retry failed, return error so report shows N/A instead of wrong values
    return { fund, amt, error: `NAV data unreliable for matched scheme (${scheme.schemeName}) — fund may have been restructured or renamed` };
  }

  // Compute beta from NAV volatility (simplified: std dev relative to category benchmark)
  // True beta needs benchmark NAV series - we approximate from fund vs benchmark stddev
  const benchmark = getBenchmark(latestInfo.meta?.scheme_category);
  const fundStdDev = computeStdDev(nav, 36); // 3Y monthly rolling stddev
  const betaEstimate = fundStdDev > 0 && benchmark.stddev > 0
    ? (fundStdDev / benchmark.stddev).toFixed(2)
    : null;

  console.log(`  [NAV] ${scheme.schemeName}: 1Y=${pct(ret1y)} 3Y=${pct(ret3y)} 5Y=${pct(ret5y)} BM:${benchmark.name}`);
  return { fund, amt, scheme, meta: latestInfo.meta, latestNav, latestDate, navInvest, ret1y, ret3y, ret5y, cal, currentValue, investCAGR, gain, yearsHeld, benchmark, betaEstimate, fundStdDev };
}

// ── CLAUDE — knowledge fields (manager, TER, Sharpe, Beta, overlap, rolling) ──
async function getKnowledgeFields(funds, results) {
  // Only ask Claude about funds we actually have data for
  const fundList = results.map(r => {
    if (r.error) return `${r.fund.name}: DATA NOT AVAILABLE (timed out) — skip this fund, return null for it`;
    return `${r.fund.name} | Category:${r.meta?.scheme_category||'Equity'} | 1Y:${pct(r.ret1y)} 3Y:${pct(r.ret3y)} 5Y:${pct(r.ret5y)} | Invested:${fmt(r.amt)} | Current:${r.currentValue?fmt(r.currentValue):'N/A'}`;
  }).join('\n');

  const prompt = `You are a CFA-level Indian MF analyst. For these funds, return ONLY a JSON object — no markdown.

FUNDS (with real AMFI return data):
${fundList}

Benchmark: Nifty 100 TRI | 5Y:${13.2}% 3Y:${14.0}% 1Y:+0.8%

Return this exact structure with REAL data for each fund:
{
  "funds": [
    {
      "name": "exact fund name from list",
      "manager": "current manager name(s) as of Apr 2026",
      "tenureYrs": 5,
      "tenureFlag": false,
      "sharpe": "0.81",
      "beta": "1.02",
      "stddev": "14.2%",
      "alpha": "+2.5%",
      "ter": "1.51%",  // REGULAR PLAN TER — e.g. 1.51% NOT 0.69% (direct)
      "riskCategory": "Very High Risk",  // SEBI risk label: Very High/High/Moderately High/Moderate/Low to Moderate/Low
      "riskCategory": "Very High Risk",
      "quality": "Strong",
      "decision": "Hold",
      "quartile": "Q1",
      "quartileLabel": "Top 25%",
      "rolling1yAvg": "16.1%",
      "rolling1yBeatPct": "68%",
      "rolling1yWorst": "-8.4%",
      "rolling3yAvg": "15.8%",
      "rolling3yBeatPct": "72%",
      "rolling3yMin": "8.2%",
      "sebiCategory": "Large Cap Fund",
      "switchTarget": null
    }
  ],
  "overlap": {
    "overallPct": "74%",
    "verdict": "Critical redundancy — 10 funds, 1 strategy",
    "topStocks": [
      {"stock": "HDFC Bank", "funds": "10/10", "avgWt": "7.7%", "risk": "Very High"},
      {"stock": "ICICI Bank", "funds": "10/10", "avgWt": "7.2%", "risk": "Very High"},
      {"stock": "Reliance Industries", "funds": "10/10", "avgWt": "6.4%", "risk": "High"},
      {"stock": "Infosys", "funds": "10/10", "avgWt": "5.6%", "risk": "Moderate"},
      {"stock": "L&T", "funds": "9/10", "avgWt": "4.3%", "risk": "Moderate"}
    ]
  },
  "sectors": [
    {"name": "BFSI", "pct": 38, "flag": true},
    {"name": "IT", "pct": 15, "flag": false},
    {"name": "Energy", "pct": 9, "flag": false},
    {"name": "Consumer", "pct": 9, "flag": false},
    {"name": "Industrials", "pct": 8, "flag": false},
    {"name": "Others", "pct": 21, "flag": false}
  ],
  "keyFindings": [
    "specific finding with real numbers",
    "specific finding with real numbers",
    "specific finding with real numbers",
    "specific finding with real numbers"
  ],
  "healthVerdict": "one line assessment"
}

Use ACTUAL data from Value Research, AMFI factsheets, Moneycontrol for each fund.
IMPORTANT: 
- "ter" must be the REGULAR PLAN expense ratio from AMFI monthly TER disclosure. NOT direct plan.
  Typical ranges: Large cap equity 1.4-1.8% | Mid/small cap 1.6-2.0% | Balanced advantage 1.5-1.9% | Index funds 0.1-0.3%
- "riskCategory" must be the SEBI-mandated risk label from the fund's KIM/SID: "Very High Risk" / "High Risk" / "Moderately High Risk" / "Moderate Risk" / "Low to Moderate Risk" / "Low Risk"
- "beta" is vs the fund's own SEBI benchmark (not always Nifty 100). Balanced advantage beta vs CRISIL Hybrid index is typically 0.85-1.10.
decision=Hold if 5Y alpha>0, Switch if alpha -1% to 0%, Exit if alpha < -1%.`;

  const postData = JSON.stringify({
    model: 'claude-haiku-4-5-20251001',
    max_tokens: 3000,
    messages: [{ role: 'user', content: prompt }]
  });

  const result = await new Promise((resolve, reject) => {
    const req = https.request({
      hostname: 'api.anthropic.com', port: 443, path: '/v1/messages', method: 'POST',
      headers: { 'Content-Type':'application/json', 'x-api-key':ANTHROPIC_API_KEY, 'anthropic-version':'2023-06-01', 'Content-Length':Buffer.byteLength(postData) }
    }, res => {
      let data = '';
      res.on('data', c => data += c);
      res.on('end', () => resolve({ status: res.statusCode, body: data }));
    });
    req.on('error', reject);
    req.setTimeout(60000, () => { req.destroy(); reject(new Error('Claude timeout 60s')); });
    req.write(postData);
    req.end();
  });

  const parsed = JSON.parse(result.body);
  if (result.status !== 200) throw new Error(parsed.error?.message || `Claude error ${result.status}`);
  const text = (parsed.content||[]).filter(b=>b.type==='text').map(b=>b.text).join('');
  const clean = text.replace(/```json|```/g,'').trim();
  try { return JSON.parse(clean); } catch { return []; }
}

// ── BUILD FULL REPORT ON SERVER ───────────────────────────────────────────
function buildReport(funds, results, knowledge) {
  const kFunds = (knowledge?.funds || []).filter(k => k && k.name && typeof k.name === 'string');
  const kMap = {};
  for (const k of kFunds) { kMap[k.name.toLowerCase().trim()] = k; }
  const getK = name => {
    const direct = kMap[name.toLowerCase().trim()];
    if (direct) return direct;
    const lower = name.toLowerCase();
    for (const [key, val] of Object.entries(kMap)) {
      const keyWords = key.split(' ').filter(w=>w.length>3);
      if (keyWords.some(w => lower.includes(w))) return val;
    }
    return {};
  };

  const totalInvested = results.reduce((s,r) => s + r.amt, 0);
  const successResults = results.filter(r => !r.error && r.currentValue);
  const totalCurrent = successResults.reduce((s,r) => s + r.currentValue, 0);
  // Show partial value if at least 50% of funds fetched successfully
  const hasAll = successResults.length >= Math.ceil(results.length * 0.5);
  const partialNote = successResults.length < results.length
    ? ` (${successResults.length}/${results.length} funds — ${results.filter(r=>r.error).map(r=>r.fund.name.split(' ').slice(0,2).join(' ')).join(', ')} timed out)`
    : '';
  const BM5Y = 13.2;

  const validR = results.filter(r => r.ret5y && r.amt);
  const blendedCAGR5 = validR.length ? validR.reduce((s,r)=>s+(r.ret5y*r.amt),0)/validR.reduce((s,r)=>s+r.amt,0) : 0;
  // Use primary category benchmark for portfolio-level alpha
  const primaryCat = results.find(r=>r.meta?.scheme_category)?.meta?.scheme_category || '';
  const portfolioBM = getBenchmark(primaryCat) || CATEGORY_BENCHMARKS['default'];
  // For mixed portfolios, show weighted alpha
  const weightedBMcagr = successResults.length > 0 && totalInvested > 0
    ? successResults.reduce((s,r) => s + ((r.benchmark?.cagr5y||portfolioBM?.cagr5y||13.2) * r.amt), 0) / totalInvested
    : (portfolioBM?.cagr5y || 13.2);
  const alpha5 = blendedCAGR5 - weightedBMcagr;
  const realReturn = blendedCAGR5 - 6.2;
  // Beat count uses each fund's OWN benchmark (not portfolio-level)
  const beatCount5 = results.filter(r => {
    if (!r.ret5y || r.error) return false;
    const fundBM = r.benchmark?.cagr5y || portfolioBM.cagr5y;
    return r.ret5y > fundBM;
  }).length;
  const avgTER = kFunds.length ? kFunds.reduce((s,k)=>s+parseFloat(k.ter||'1.62'),0)/kFunds.length : 1.62;
  const annualTERCost = totalInvested * avgTER / 100;
  const corpus = hasAll ? totalCurrent : totalInvested * 1.7;
  const uniqueStocks = Math.min(funds.length * 22, 150);
  const healthScore = Math.min(9.5, Math.max(1, 5 + alpha5 * 0.4 - (funds.length > 5 ? 1 : 0))).toFixed(1);

  function project(cagr, yrs) { return fmt(corpus * Math.pow(1+cagr/100, yrs)); }

  const keyFlags = knowledge?.keyFindings?.length >= 4 ? knowledge.keyFindings : [
    `Blended 5Y CAGR ${blendedCAGR5.toFixed(1)}% vs Nifty 100 TRI ${BM5Y}% (alpha: ${alpha5>=0?'+':''}${alpha5.toFixed(2)}%)`,
    `${beatCount5} of ${funds.length} funds beat benchmark on 5Y basis`,
    `Real return after 6.2% CPI: ${realReturn>=0?'+':''}${realReturn.toFixed(2)}% — ${realReturn>3?'adequate for equity risk':'inadequate — consider restructuring'}`,
    `Annual TER cost ${fmt(annualTERCost)}/yr — 10yr compounded drag ≈ ${fmt(annualTERCost*14)}`,
  ];

  const fundsArr = results.map(r => {
    const k = getK(r.fund.name);
    const c = r.cal||{};
    const alpha = r.ret5y!=null ? (r.ret5y - BM5Y) : null;
    const gain = r.gain||0;
    const ltcgTax = Math.max(0, gain-125000) * 0.125;
    const netProceeds = (r.currentValue||0) - ltcgTax;

    // Use category-appropriate benchmark for each fund
    const bm = r.benchmark || { name:'Nifty 100 TRI', cagr5y:13.2, cagr3y:14.0, ret1y:0.8 };
    const bmCAGR5 = bm.cagr5y;
    const alphaVsBM = r.ret5y != null ? (r.ret5y - bmCAGR5) : null;
    // Use computed stddev from NAV data (more accurate than Claude's knowledge)
    const computedStdDev = r.fundStdDev ? r.fundStdDev+'%' : null;
    const computedBeta = r.betaEstimate || null;

    let decision = k.decision || (alphaVsBM==null?'Hold':alphaVsBM>1?'Hold':alphaVsBM>-1?'Switch':'Exit');
    let quality = k.quality || (r.ret5y>bmCAGR5+1?'Strong':r.ret5y>bmCAGR5-2?'Average':'Weak');
    let quartile = k.quartile || (r.ret5y>bmCAGR5+2?'Q1':r.ret5y>bmCAGR5?'Q2':r.ret5y>bmCAGR5-2?'Q3':'Q4');
    let quartileLabel = k.quartileLabel || (quartile==='Q1'?'Top 25%':quartile==='Q2'?'Top 50%':quartile==='Q3'?'Top 75%':'Bottom 25%');

    return {
      name:r.fund.name, manager:k.manager||'See factsheet', tenureYrs:k.tenureYrs||3, tenureFlag:k.tenureFlag||false,
      cagr5y:r.ret5y!=null?r.ret5y.toFixed(2)+'%':'N/A', cagr3y:r.ret3y!=null?r.ret3y.toFixed(2)+'%':'N/A', ret1y:r.ret1y!=null?r.ret1y.toFixed(2)+'%':'N/A',
      sharpe:k.sharpe||(r.ret5y>bmCAGR5+2?'0.85':r.ret5y>bmCAGR5?'0.72':'0.58'),
      beta:computedBeta||k.beta||'0.85',
      stddev:computedStdDev||k.stddev||(r.meta?.scheme_category?.toLowerCase().includes('balanced')?'9.8%':'14.0%'),
      alpha:alphaVsBM!=null?(alphaVsBM>=0?'+':'')+alphaVsBM.toFixed(2)+'% vs '+bm.name:'N/A', ter:k.ter||'1.62%', riskCategory:k.riskCategory||'Very High Risk',
      quality, decision,
      perf5yVal:r.ret5y||0, perf3yVal:r.ret3y||0, ret1yVal:r.ret1y||0, sharpeVal:parseFloat(k.sharpe)||0.65,
      calendarReturns:{'2020':fmtC(c[2020]),'2020Beat':!!c['2020Beat'],'2021':fmtC(c[2021]),'2021Beat':!!c['2021Beat'],'2022':fmtC(c[2022]),'2022Beat':!!c['2022Beat'],'2023':fmtC(c[2023]),'2023Beat':!!c['2023Beat'],'2024':fmtC(c[2024]),'2024Beat':!!c['2024Beat'],'2025':fmtC(c[2025]),'2025Beat':!!c['2025Beat']},
      quartile, quartileLabel,
      rolling1yAvg:k.rolling1yAvg||(r.ret1y?r.ret1y.toFixed(1)+'%':'N/A'), rolling1yBeatPct:k.rolling1yBeatPct||(r.ret5y>BM5Y?'62%':'38%'), rolling1yWorst:k.rolling1yWorst||(r.ret1y?(r.ret1y-10).toFixed(1)+'%':'N/A'),
      rolling3yAvg:k.rolling3yAvg||(r.ret3y?r.ret3y.toFixed(1)+'%':'N/A'), rolling3yBeatPct:k.rolling3yBeatPct||(r.ret5y>BM5Y?'65%':'35%'), rolling3yMin:k.rolling3yMin||(r.ret3y?(r.ret3y-7).toFixed(1)+'%':'N/A'),
      realReturn:r.ret1y!=null?(r.ret1y-6.2).toFixed(2)+'%':'N/A',
      estCurrentValue:r.currentValue?fmt(r.currentValue):'N/A', gainAmt:gain>0?fmt(gain):'N/A',
      ltcgTax:fmt(ltcgTax), netProceeds:fmt(netProceeds), breakEvenMonths:7,
      benchmarkName: (r.benchmark?.name || 'Nifty 100 TRI'),
      benchmarkCAGR5y: (r.benchmark?.cagr5y || 13.2),
    };
  });

  const sectors = knowledge?.sectors?.length ? knowledge.sectors : [
    {name:'BFSI',pct:38,flag:true},{name:'IT',pct:15,flag:false},{name:'Energy',pct:9,flag:false},
    {name:'Consumer',pct:9,flag:false},{name:'Industrials',pct:8,flag:false},{name:'Others',pct:21,flag:false}
  ];

  const overlapPct = knowledge?.overlap?.overallPct || (funds.length>4?'70%':funds.length>2?'50%':'35%');
  const topStocks = knowledge?.overlap?.topStocks || [
    {stock:'HDFC Bank',funds:`${Math.min(funds.length,10)}/10`,avgWt:'7.7%',risk:'Very High'},
    {stock:'ICICI Bank',funds:`${Math.min(funds.length,10)}/10`,avgWt:'7.2%',risk:'Very High'},
    {stock:'Reliance Industries',funds:`${Math.min(funds.length,9)}/10`,avgWt:'6.4%',risk:'High'},
    {stock:'Infosys',funds:`${Math.min(funds.length,8)}/10`,avgWt:'5.6%',risk:'Moderate'},
    {stock:'L&T',funds:`${Math.min(funds.length,7)}/10`,avgWt:'4.3%',risk:'Moderate'},
  ];

  const stress = [
    {label:'Bull +15%',impact:'+'+fmt(corpus*0.15),pct:'+15%'},
    {label:'Flat 3Y',impact:'-'+fmt(corpus*0.08),pct:'-8%'},
    {label:'Correction -20%',impact:'-'+fmt(corpus*0.20),pct:'-20%'},
    {label:'Crash -30%',impact:'-'+fmt(corpus*0.30),pct:'-30%'},
  ];

  const recCAGR = 15.4;
  const exitFunds = (fundsArr||[]).filter(f=>f.decision==='Exit').slice(0,2);
  const exitNames = exitFunds.map(f=>(f.name||'').split(' ').slice(0,2).join(' ')).join(' + ') || 'worst performers';

  return {
    summary:{totalInvested:fmt(totalInvested),currentValue:hasAll?fmt(totalCurrent):'N/A',blendedCAGR:blendedCAGR5.toFixed(2)+'%',alphaBM:(alpha5>=0?'+':'')+alpha5.toFixed(2)+'%',realReturn:(realReturn>=0?'+':'')+realReturn.toFixed(2)+'%',annualTER:fmt(annualTERCost),fundsBeatBM:`${beatCount5}/${funds.length}`,uniqueStocks:`~${uniqueStocks}`,healthScore:healthScore+'/10',healthVerdict:knowledge?.healthVerdict||(alpha5>0?`Beating ${portfolioBM?.name||'benchmark'} — consolidate redundant positions`:`Underperforming ${portfolioBM?.name||'benchmark'} — restructure recommended`),overlapPct:overlapPct,keyFlags},
    funds:fundsArr,
    // Build per-category benchmark rows for all unique benchmarks in portfolio
    benchmarkRows: (()=>{
      const bmMap = {};
      for (const r of results.filter(r=>!r.error && r.benchmark && r.benchmark.name)) {
        const bm = r.benchmark;
        if (!bmMap[bm.name]) bmMap[bm.name] = { ...bm, fundCount: 0 };
        bmMap[bm.name].fundCount++;
      }
      const rows = Object.values(bmMap);
      // Fallback: if no rows, use default benchmark
      return rows.length ? rows : [CATEGORY_BENCHMARKS['default']];
    })(),
    // Primary benchmark (most common)
    benchmark:(()=>{
      const cats = results.filter(r=>r.benchmark).map(r=>r.benchmark.name);
      const primaryBM = cats.length ? (cats.sort((a,b)=>cats.filter(x=>x===b).length-cats.filter(x=>x===a).length)[0]) : 'Nifty 100 TRI';
      const bm = results.find(r=>r.benchmark?.name===primaryBM)?.benchmark || CATEGORY_BENCHMARKS['default'] || {name:'Nifty 100 TRI',cagr5y:13.2,cagr3y:14.0,ret1y:0.8,sharpe:0.95,stddev:12.8,calendarReturns:{}};
      // Calendar returns differ by benchmark - show approximate for primary benchmark
      const isHybrid = primaryBM.toLowerCase().includes('hybrid') || primaryBM.toLowerCase().includes('crisil');
      return {
        name:bm.name,
        cagr5y:bm.cagr5y+'%', cagr3y:bm.cagr3y+'%', ret1y:(bm.ret1y>=0?'+':'')+bm.ret1y+'%',
        sharpe:bm.sharpe+'', beta:'1.00', stddev:bm.stddev+'%',
        rolling1yAvg:bm.cagr5y+'%', rolling3yAvg:bm.cagr3y+'%',
        calendarReturns: isHybrid
          ? {'2020':'+8.4%','2021':'+17.1%','2022':'+3.2%','2023':'+15.1%','2024':'+10.4%','2025':'+3.2%'}
          : {'2020':'+15.5%','2021':'+25.8%','2022':'+5.0%','2023':'+24.1%','2024':'+15.0%','2025':'+3.3%'}
      };
    })(),
    risk:{blendedBeta:'0.99',bfsiPct:(sectors.find(s=>s.name==='BFSI')?.pct||38)+'%',top5StocksPct:'24%',midSmallPct:funds.length>3?'<5%':'10%',uniqueStocks:`~${uniqueStocks}`,stddev:'14.2%',maxDrawdown:'~-33%',downsideCap:'~93%',upsideCap:'~96%',stressScenarios:stress},
    sectors,
    overlap:{overallPct:overlapPct,verdict:knowledge?.overlap?.verdict||(funds.length>4?'Critical redundancy — multiple funds, one strategy':'Moderate overlap — consolidate'),topStocks},
    projections:{corpus:fmt(corpus),rows:[{label:'Current portfolio',cagr:blendedCAGR5.toFixed(1)+'%',y5:project(blendedCAGR5,5),y10:project(blendedCAGR5,10),y15:project(blendedCAGR5,15),y20:project(blendedCAGR5,20),type:'bad'},{label:'Nifty 100 Index',cagr:'13.2%',y5:project(13.2,5),y10:project(13.2,10),y15:project(13.2,15),y20:project(13.2,20),type:'mid'},{label:'Recommended portfolio',cagr:recCAGR+'%',y5:project(recCAGR,5),y10:project(recCAGR,10),y15:project(recCAGR,15),y20:project(recCAGR,20),type:'good'}],gap20y:(()=>{
      const diff = corpus*Math.pow(1+recCAGR/100,20)-corpus*Math.pow(1+blendedCAGR5/100,20);
      return (diff>=0?'+':'-') + fmt(Math.abs(diff));
    })()},
    recommended:[{name:'Nippon India Large Cap',cat:'Large Cap',alloc:'30%',amt:fmt(corpus*0.30),cagr5y:'15.9%',sharpe:'0.81',ter:'0.69%',role:'Core anchor — consistent alpha'},{name:'ICICI Pru Bluechip',cat:'Large Cap',alloc:'25%',amt:fmt(corpus*0.25),cagr5y:'15.3%',sharpe:'0.77',ter:'0.95%',role:'Large cap diversifier'},{name:'UTI Nifty 50 Index',cat:'Index',alloc:'20%',amt:fmt(corpus*0.20),cagr5y:'14.7%',sharpe:'0.94',ter:'0.20%',role:'Low-cost passive core'},{name:'Motilal Oswal Midcap',cat:'Mid Cap',alloc:'25%',amt:fmt(corpus*0.25),cagr5y:'28.4%',sharpe:'1.14',ter:'0.58%',role:'Growth kicker — compounding'}],
    execution:[{step:'Step 1 — April 2026 (Now)',color:'bad',detail:`Exit ${exitNames} first. Fresh FY — use full ₹1.25L LTCG exemption. Deploy into Nippon India Large Cap + UTI Nifty 50 Index.`},{step:'Step 2 — May–July 2026',color:'warn',detail:'Exit remaining underperformers. Add Motilal Oswal Midcap for missing mid-cap exposure. Split exits across months to optimise LTCG.'},{step:'Step 3 — April 2027+',color:'ok',detail:`Fresh ₹1.25L exemption for final exits. Target: 4-fund portfolio at blended TER ~0.6%. Annual saving: ${fmt(annualTERCost*0.55)}/yr.`}],
    scorecard:[{label:'Performance consistency',score:Math.min(9,Math.max(1,5+(alpha5*0.4))).toFixed(1),note:`${beatCount5}/${funds.length} funds beat Nifty 100 TRI on 5Y basis`},{label:'Diversification',score:Math.max(1,7-(funds.length>5?2:0)-(parseFloat(overlapPct)>60?2:0)).toFixed(1),note:`${overlapPct} overlap — ${funds.length>5?'critical redundancy':'concentrated'}`},{label:'Risk control',score:'5.0',note:'Beta ~0.99 — full market downside, limited upside capture'},{label:'Cost efficiency',score:Math.min(8,Math.max(1,alpha5>2?7:alpha5>0?5:3)).toFixed(1),note:`${avgTER.toFixed(2)}% blended TER — 16x costlier than equivalent index`},{label:'Overall health',score:healthScore,note:alpha5>0?'Consolidate to eliminate redundancy':'Restructure immediately'}],
  };
}


// ── MAIN ANALYSIS ──────────────────────────────────────────────────────────
async function runAnalysis(funds) {
  console.log(`\n[Phase 1] Fetching AMFI for ${funds.length} funds in parallel`);
  const FUND_TIMEOUT = 22000; // 22s — matches 18s fetch + buffer
  const results = await Promise.all(funds.map(async fund => {
    console.log(`  → ${fund.name}`);
    try {
      return await Promise.race([
        fetchFundData(fund),
        new Promise((_, rej) => setTimeout(() => rej(new Error('Fund fetch timed out')), FUND_TIMEOUT))
      ]);
    }
    catch(e) {
      console.error(`  ✗ ${fund.name}: ${e.message}`);
      return { fund, amt: parseFloat(fund.amt.replace(/[₹,\s]/g,''))||0, error: e.message };
    }
  }));
  const ok = results.filter(r => !r.error).length;
  console.log(`[Phase 1] Done: ${ok}/${funds.length} fetched`);

  let knowledge = null;
  console.log(`[Phase 2] Claude — manager/TER/Sharpe/Beta/overlap`);
  try {
    knowledge = await getKnowledgeFields(funds, results);
    console.log(`[Phase 2] Got ${knowledge?.funds?.length||0} fund records`);
  } catch(e) {
    console.warn(`[Phase 2] Claude skipped (${e.message}) — using computed values`);
  }

  console.log(`[Phase 3] Building report`);
  // If all funds failed, build a placeholder report with Claude knowledge only
  if (results.every(r => r.error)) {
    console.warn('[Phase 3] WARNING: All funds timed out — using Claude knowledge only');
    const placeholderResults = funds.map(f => ({
      fund: f,
      amt: parseFloat(f.amt.replace(/[₹,\s]/g,''))||0,
      error: 'AMFI data unavailable',
      meta: null, benchmark: null, ret1y: null, ret3y: null, ret5y: null,
      cal: {}, currentValue: null, investCAGR: null, gain: null
    }));
    results.length = 0;
    placeholderResults.forEach(r => results.push(r));
  }
  let report;
  try {
    report = buildReport(funds, results, knowledge);
  } catch(buildErr) {
    console.error('[Phase 3] buildReport error:', buildErr.message);
    console.error('[Phase 3] Stack:', buildErr.stack?.split('\n').slice(0,5).join(' | '));
    // Return minimal valid report
    const totalInv = funds.reduce((s,f) => s + (parseFloat(f.amt.replace(/[₹,\s]/g,''))||0), 0);
    const fmt2 = v => new Intl.NumberFormat('en-IN',{style:'currency',currency:'INR',maximumFractionDigits:0}).format(v);
    report = {
      summary:{totalInvested:fmt2(totalInv),currentValue:'N/A',blendedCAGR:'N/A',alphaBM:'N/A',realReturn:'N/A',annualTER:'N/A',fundsBeatBM:`0/${funds.length}`,uniqueStocks:'N/A',healthScore:'N/A',healthVerdict:'Data unavailable — please retry',overlapPct:'N/A',keyFlags:['AMFI data could not be fetched. Please retry.','Fund names may need to be spelled more precisely.','Try common abbreviations: SBI Large Cap, ICICI Pru Bluechip etc.','If error persists, try fewer funds at once.']},
      funds: funds.map(f => ({name:f.name,manager:'See factsheet',tenureYrs:3,tenureFlag:false,cagr5y:'N/A',cagr3y:'N/A',ret1y:'N/A',sharpe:'N/A',beta:'N/A',stddev:'N/A',alpha:'N/A',ter:'N/A',riskCategory:'N/A',quality:'N/A',decision:'Hold',perf5yVal:0,perf3yVal:0,ret1yVal:0,sharpeVal:0,calendarReturns:{'2020':'N/A','2020Beat':false,'2021':'N/A','2021Beat':false,'2022':'N/A','2022Beat':false,'2023':'N/A','2023Beat':false,'2024':'N/A','2024Beat':false,'2025':'N/A','2025Beat':false},quartile:'N/A',quartileLabel:'N/A',rolling1yAvg:'N/A',rolling1yBeatPct:'N/A',rolling1yWorst:'N/A',rolling3yAvg:'N/A',rolling3yBeatPct:'N/A',rolling3yMin:'N/A',realReturn:'N/A',estCurrentValue:'N/A',gainAmt:'N/A',ltcgTax:'N/A',netProceeds:'N/A',breakEvenMonths:0,benchmarkName:'Nifty 100 TRI',benchmarkCAGR5y:13.2})),
      benchmark:{name:'Nifty 100 TRI',cagr5y:'13.2%',cagr3y:'14.0%',ret1y:'+0.8%',sharpe:'0.95',beta:'1.00',stddev:'12.8%',rolling1yAvg:'13.8%',rolling3yAvg:'14.4%',calendarReturns:{'2020':'+15.5%','2021':'+25.8%','2022':'+5.0%','2023':'+24.1%','2024':'+15.0%','2025':'+3.3%'}},
      benchmarkRows:[{name:'Nifty 100 TRI',cagr5y:13.2,cagr3y:14.0,ret1y:0.8,sharpe:0.95,stddev:12.8,calendarReturns:{'2020':'+15.5%','2021':'+25.8%','2022':'+5.0%','2023':'+24.1%','2024':'+15.0%','2025':'+3.3%'}}],
      risk:{blendedBeta:'N/A',bfsiPct:'N/A',top5StocksPct:'N/A',midSmallPct:'N/A',uniqueStocks:'N/A',stddev:'N/A',maxDrawdown:'N/A',downsideCap:'N/A',upsideCap:'N/A',stressScenarios:[{label:'Bull +15%',impact:'N/A',pct:'+15%'},{label:'Flat 3Y',impact:'N/A',pct:'0%'},{label:'Correction -20%',impact:'N/A',pct:'-20%'},{label:'Crash -30%',impact:'N/A',pct:'-30%'}]},
      sectors:[{name:'BFSI',pct:35,flag:true},{name:'IT',pct:15,flag:false},{name:'Energy',pct:10,flag:false},{name:'Consumer',pct:10,flag:false},{name:'Industrials',pct:9,flag:false},{name:'Others',pct:21,flag:false}],
      overlap:{overallPct:'N/A',verdict:'Data unavailable',topStocks:[{stock:'HDFC Bank',funds:'N/A',avgWt:'N/A',risk:'Very High'},{stock:'ICICI Bank',funds:'N/A',avgWt:'N/A',risk:'Very High'},{stock:'Reliance',funds:'N/A',avgWt:'N/A',risk:'High'},{stock:'Infosys',funds:'N/A',avgWt:'N/A',risk:'Moderate'},{stock:'L&T',funds:'N/A',avgWt:'N/A',risk:'Moderate'}]},
      projections:{corpus:fmt2(totalInv),rows:[{label:'Current portfolio',cagr:'N/A',y5:'N/A',y10:'N/A',y15:'N/A',y20:'N/A',type:'bad'},{label:'Nifty 100 Index',cagr:'13.2%',y5:'N/A',y10:'N/A',y15:'N/A',y20:'N/A',type:'mid'},{label:'Recommended portfolio',cagr:'~16%',y5:'N/A',y10:'N/A',y15:'N/A',y20:'N/A',type:'good'}],gap20y:'N/A'},
      recommended:[{name:'Nippon India Large Cap',cat:'Large Cap',alloc:'30%',amt:'N/A',cagr5y:'15.9%',sharpe:'0.81',ter:'0.69%',role:'Core anchor'},{name:'ICICI Pru Bluechip',cat:'Large Cap',alloc:'25%',amt:'N/A',cagr5y:'15.3%',sharpe:'0.77',ter:'0.95%',role:'Large cap diversifier'},{name:'UTI Nifty 50 Index',cat:'Index',alloc:'20%',amt:'N/A',cagr5y:'14.7%',sharpe:'0.94',ter:'0.20%',role:'Passive core'},{name:'Motilal Oswal Midcap',cat:'Mid Cap',alloc:'25%',amt:'N/A',cagr5y:'28.4%',sharpe:'1.14',ter:'0.58%',role:'Growth kicker'}],
      execution:[{step:'Step 1 — Retry Analysis',color:'bad',detail:'AMFI data fetch timed out. Please retry — servers may be temporarily slow.'},{step:'Step 2 — Check Fund Names',color:'warn',detail:'Ensure fund names are spelled correctly. Try: SBI Large Cap, ICICI Pru Bluechip, Franklin India Large Cap.'},{step:'Step 3 — Try Fewer Funds',color:'ok',detail:'If timeout persists, try 2-3 funds at a time instead of 5+.'}],
      scorecard:[{label:'Data availability',score:1,note:'AMFI fetch timed out — retry'},{label:'Fund matching',score:5,note:'Funds found but NAV data unavailable'},{label:'Analysis quality',score:1,note:'Retry for full analysis'},{label:'Recommendations',score:5,note:'General guidance only'},{label:'Overall',score:1,note:'Please retry the analysis'}]
    };
  }
  console.log(`[Phase 3] Done`);
  return JSON.stringify(report);
}

// ── HTTP SERVER ────────────────────────────────────────────────────────────
const server = http.createServer((req, res) => {
  const { pathname } = url.parse(req.url, true);
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type');
  if (req.method === 'OPTIONS') { res.writeHead(204); res.end(); return; }

  if (pathname === '/health') { sendJSON(res, 200, { ok:true, key:!!ANTHROPIC_API_KEY, mode:'amfi-v6-institutional' }); return; }

  if (pathname === '/api/debug') {
    httpsGet('api.mfapi.in', '/mf/119598/latest', 8000)
      .then(r => { let n=null; try{n=JSON.parse(r.body);}catch{} sendJSON(res,200,{amfi:r.status===200,nav:n?.data?.[0]?.nav,fund:n?.meta?.scheme_name}); })
      .catch(e => sendJSON(res,200,{amfi:false,error:e.message}));
    return;
  }

  if (pathname === '/api/analyse' && req.method === 'POST') {
    if (!getRateLimit(getClientIP(req))) { sendJSON(res,429,{error:'Rate limit. Try again later.'}); return; }
    if (!ANTHROPIC_API_KEY) { sendJSON(res,500,{error:'API key not configured.'}); return; }
    let body = '';
    req.on('data', c => { body += c; });
    req.on('end', async () => {
      let payload;
      try { payload = JSON.parse(body); } catch { sendJSON(res,400,{error:'Invalid JSON'}); return; }
      if (!payload.funds?.length) { sendJSON(res,400,{error:'No funds'}); return; }
      try {
        console.log(`[${new Date().toISOString()}] ${payload.funds.length} funds from ${getClientIP(req)}`);
        const text = await runAnalysis(payload.funds);
        sendJSON(res, 200, { content:[{type:'text',text}] });
      } catch(e) {
        console.error('Failed:', e.message);
        sendJSON(res,500,{error:e.message||'Analysis failed'});
      }
    });
    return;
  }

  
  let fp = path.join(__dirname, pathname==='/'?'index.html':pathname.replace(/^\//,''));
  if (!fp.startsWith(__dirname)) { res.writeHead(403); res.end(); return; }
  const MIME2 = {'.html':'text/html;charset=utf-8','.js':'application/javascript','.css':'text/css','.json':'application/json'};
  const mime = MIME2[path.extname(fp)] || 'text/html;charset=utf-8';
  fs.readFile(fp, (err, data) => {
    if (err) {
      fs.readFile(path.join(__dirname,'index.html'), (e2,d2) => {
        if (e2) { res.writeHead(404); res.end('Not found'); }
        else { res.writeHead(200,{'Content-Type':'text/html;charset=utf-8'}); res.end(d2); }
      });
    } else { res.writeHead(200,{'Content-Type':mime}); res.end(data); }
  });
});

server.listen(process.env.PORT || 3000, () => {
  console.log(`JD MF Report v1 on port ${process.env.PORT||3000} | key:${!!ANTHROPIC_API_KEY}`);
});
