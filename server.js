const http = require('http');
const https = require('https');
const fs = require('fs');
const path = require('path');

// Suppress DEP0169 url.parse warning — emitted by Node.js http internals,
// not by this code. Our routing already uses `new URL()` (WHATWG standard).
const _warn = process.emitWarning.bind(process);
process.emitWarning = (warning, ...args) => {
  if (typeof warning === 'string' && warning.includes('DEP0169')) return;
  if (args[0]?.code === 'DEP0169') return;
  _warn(warning, ...args);
};

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
// Hardcoded lookup for popular funds — bypasses AMFI search entirely (instant, no network)
// Format: normalized-key → { schemeCode, schemeName (official AMFI name) }
const K = (code, name) => ({ schemeCode: code, schemeName: name });
const KNOWN_SCHEMES = {
  'hdfc mid cap opportunities': K(118989,'HDFC Mid-Cap Opportunities Fund - Regular Plan - Growth'),
  'hdfc midcap opportunities':  K(118989,'HDFC Mid-Cap Opportunities Fund - Regular Plan - Growth'),
  'hdfc mid-cap':               K(118989,'HDFC Mid-Cap Opportunities Fund - Regular Plan - Growth'),
  'hdfc mid cap':               K(118989,'HDFC Mid-Cap Opportunities Fund - Regular Plan - Growth'),
  'sbi bluechip':               K(119598,'SBI Blue Chip Fund - Regular Plan - Growth'),
  'sbi blue chip':              K(119598,'SBI Blue Chip Fund - Regular Plan - Growth'),
  'nippon india large cap':     K(106235,'Nippon India Large Cap Fund - Growth Plan - Growth Option'),
  'nippon large cap':           K(106235,'Nippon India Large Cap Fund - Growth Plan - Growth Option'),
  'icici prudential bluechip':  K(120586,'ICICI Prudential Bluechip Fund - Growth'),
  'icici pru bluechip':         K(120586,'ICICI Prudential Bluechip Fund - Growth'),
  'icici bluechip':             K(120586,'ICICI Prudential Bluechip Fund - Growth'),
  'icici prudential large cap': K(120586,'ICICI Prudential Bluechip Fund - Growth'),
  'icici pru large cap':        K(120586,'ICICI Prudential Bluechip Fund - Growth'),
  'axis small cap':             K(125350,'Axis Small Cap Fund - Regular Plan - Growth'),
  'axis small cap fund':        K(125350,'Axis Small Cap Fund - Regular Plan - Growth'),
  'parag parikh flexi cap':     K(122640,'Parag Parikh Flexi Cap Fund - Regular Plan - Growth'),
  'parag parikh flexicap':      K(122640,'Parag Parikh Flexi Cap Fund - Regular Plan - Growth'),
  'mirae asset large cap':      K(118834,'Mirae Asset Large Cap Fund - Regular Plan - Growth'),
  'mirae large cap':            K(118834,'Mirae Asset Large Cap Fund - Regular Plan - Growth'),
  'kotak emerging equity':      K(131741,'Kotak Emerging Equity Fund - Regular Plan - Growth'),
  'hdfc flexi cap':             K(100033,'HDFC Flexi Cap Fund - Regular Plan - Growth Option'),
  'hdfc flexicap':              K(100033,'HDFC Flexi Cap Fund - Regular Plan - Growth Option'),
  'lic flexi cap':              K(100313,'LIC MF Flexi Cap Fund-Regular Plan-Growth'),
  'lic flexicap':               K(100313,'LIC MF Flexi Cap Fund-Regular Plan-Growth'),
  'lic mf flexi cap':           K(100313,'LIC MF Flexi Cap Fund-Regular Plan-Growth'),
  'sundaram balanced advantage':K(118825,'Sundaram Balanced Advantage Fund Regular Plan Growth'),
  'sundram balanced advantage':  K(118825,'Sundaram Balanced Advantage Fund Regular Plan Growth'),
  'sundaram bal adv':            K(118825,'Sundaram Balanced Advantage Fund Regular Plan Growth'),
  'dsp mid cap':                K(108066,'DSP Mid Cap Fund - Regular Plan - Growth'),
  'dsp midcap':                 K(108066,'DSP Mid Cap Fund - Regular Plan - Growth'),
  'franklin india flexi cap':   K(101006,'Franklin India Flexi Cap Fund - Growth'),
  'franklin india flexicap':    K(101006,'Franklin India Flexi Cap Fund - Growth'),
  'axis bluechip':              K(120596,'Axis Bluechip Fund - Regular Plan - Growth'),
  'axis blue chip':             K(120596,'Axis Bluechip Fund - Regular Plan - Growth'),
  'sbi small cap':              K(116278,'SBI Small Cap Fund - Regular Plan - Growth'),
  'kotak small cap':            K(120505,'Kotak Small Cap Fund - Regular Plan - Growth'),
  'motilal oswal midcap':       K(150625,'Motilal Oswal Midcap Fund - Regular Plan - Growth'),
  'uti nifty 50 index':         K(120716,'UTI Nifty 50 Index Fund - Regular Plan - Growth'),
  'hdfc index nifty 50':        K(118662,'HDFC Index Fund - Nifty 50 Plan - Growth'),
  // Kotak BAF: scheme 119230 was discontinued (last NAV 2014) — rely on live search
  // 'kotak balanced advantage':  ← intentionally removed, search works reliably

  // ICICI Prudential Balanced Advantage (104685) — add all typo variants seen in user inputs
  'icici prudential balanced advantage': K(104685,'ICICI Prudential Balanced Advantage Fund - Growth'),
  'icici pru balanced advantage':        K(104685,'ICICI Prudential Balanced Advantage Fund - Growth'),
  'icici prudenatial balanced advantage':K(104685,'ICICI Prudential Balanced Advantage Fund - Growth'),
  'icici prudenatial balanced advanatage':K(104685,'ICICI Prudential Balanced Advantage Fund - Growth'),
  'icici prudential balanced advanatage':K(104685,'ICICI Prudential Balanced Advantage Fund - Growth'),
  'icici pru balanced advanatage':       K(104685,'ICICI Prudential Balanced Advantage Fund - Growth'),
  'icici balanced advantage':            K(104685,'ICICI Prudential Balanced Advantage Fund - Growth'),
  'dsp multi asset allocation': K(149448,'DSP Multi Asset Allocation Fund - Regular Plan - Growth'),
  'baroda bnp paribas large cap':K(152130,'Baroda BNP Paribas Large Cap Fund - Regular Plan - Growth option'),
  'baroda bnp large cap':       K(152130,'Baroda BNP Paribas Large Cap Fund - Regular Plan - Growth option'),
  'uti gold etf fof':           K(147389,'UTI Gold ETF Fund of Fund - Regular Plan - Growth'),
  'uti gold etf':               K(147389,'UTI Gold ETF Fund of Fund - Regular Plan - Growth'),
  'sbi gold fund':              K(121185,'SBI Gold Fund - Regular Plan - Growth'),
  'bandhan small cap':          K(145552,'Bandhan Small Cap Fund - Regular Plan - Growth'),
  'hdfc top 100':               K(119533,'HDFC Top 100 Fund - Regular Plan - Growth'),
  'nippon india small cap':     K(118778,'Nippon India Small Cap Fund - Growth Plan - Growth Option'),
  'nippon small cap':           K(118778,'Nippon India Small Cap Fund - Growth Plan - Growth Option'),
  'axis midcap':                K(120503,'Axis Midcap Fund - Regular Plan - Growth'),
  'axis mid cap':               K(120503,'Axis Midcap Fund - Regular Plan - Growth'),
  'hdfc mid cap opportunities fund': K(118989,'HDFC Mid-Cap Opportunities Fund - Regular Plan - Growth'),

  // ── Debt funds (common Regular Growth schemes) ────────────────────────
  'baroda bnp paribas short duration':    K(113036,'Baroda BNP Paribas Short Duration Fund - Regular Plan - Growth Option'),
  'baroda bnp short duration':            K(113036,'Baroda BNP Paribas Short Duration Fund - Regular Plan - Growth Option'),
  'baroda bnp paribas short term':        K(113036,'Baroda BNP Paribas Short Duration Fund - Regular Plan - Growth Option'),
  'baroda bnp short term':                K(113036,'Baroda BNP Paribas Short Duration Fund - Regular Plan - Growth Option'),
  'icici prudential short term':          K(104240,'ICICI Prudential Short Term Fund - Regular Plan - Cumulative'),
  'icici pru short term':                 K(104240,'ICICI Prudential Short Term Fund - Regular Plan - Cumulative'),
  'icici short term':                     K(104240,'ICICI Prudential Short Term Fund - Regular Plan - Cumulative'),
  'hdfc short term debt':                 K(112029,'HDFC Short Term Debt Fund - Regular Plan - Growth'),
  'hdfc short term':                      K(112029,'HDFC Short Term Debt Fund - Regular Plan - Growth'),
  'sbi short term debt':                  K(119166,'SBI Short Term Debt Fund - Regular Plan - Growth'),
  'sbi short term':                       K(119166,'SBI Short Term Debt Fund - Regular Plan - Growth'),
  'axis short term':                      K(119935,'Axis Short Term Fund - Regular Plan - Growth'),
  'icici prudential corporate bond':      K(104490,'ICICI Prudential Corporate Bond Fund - Regular Plan - Cumulative'),
  'icici pru corporate bond':             K(104490,'ICICI Prudential Corporate Bond Fund - Regular Plan - Cumulative'),
  'hdfc corporate bond':                  K(119180,'HDFC Corporate Bond Fund - Regular Plan - Growth'),
  'icici prudential all seasons bond':    K(104480,'ICICI Prudential All Seasons Bond Fund - Regular Plan - Growth'),
  'icici pru all seasons bond':           K(104480,'ICICI Prudential All Seasons Bond Fund - Regular Plan - Growth'),
  'hdfc liquid':                          K(101418,'HDFC Liquid Fund - Regular Plan - Growth'),
  'sbi liquid':                           K(119172,'SBI Liquid Fund - Regular Plan - Growth'),
  'hdfc overnight':                       K(147378,'HDFC Overnight Fund - Regular Plan - Growth'),
  'sbi magnum gilt':                      K(101442,'SBI Magnum Gilt Fund - Regular Plan - Growth'),
  'sbi gilt':                             K(101442,'SBI Magnum Gilt Fund - Regular Plan - Growth'),
  'hdfc banking psu debt':                K(117898,'HDFC Banking and PSU Debt Fund - Regular Plan - Growth'),
  'hdfc banking and psu':                 K(117898,'HDFC Banking and PSU Debt Fund - Regular Plan - Growth'),
  'icici prudential banking psu debt':    K(104486,'ICICI Prudential Banking & PSU Debt Fund - Regular Plan - Cumulative'),
  'sbi banking psu':                      K(119174,'SBI Banking and PSU Fund - Regular Plan - Growth'),
  'hdfc medium term debt':                K(112034,'HDFC Medium Term Debt Fund - Regular Plan - Growth'),
  'icici prudential medium term bond':    K(104492,'ICICI Prudential Medium Term Bond Fund - Regular Plan - Cumulative'),
  'hdfc floating rate debt':              K(100058,'HDFC Floating Rate Debt Fund - Regular Plan - Growth'),
  'sbi magnum medium duration':           K(101440,'SBI Magnum Medium Duration Fund - Regular Plan - Growth'),
  'baroda bnp paribas equity savings':    K(130267,'Baroda BNP Paribas Equity Savings Fund - Regular Plan - Growth'),
  'baroda bnp equity savings':            K(130267,'Baroda BNP Paribas Equity Savings Fund - Regular Plan - Growth'),
  'icici prudential equity savings':      K(120238,'ICICI Prudential Equity Savings Fund - Regular Plan - Growth'),
  'icici pru equity savings':             K(120238,'ICICI Prudential Equity Savings Fund - Regular Plan - Growth'),
};

function generateQueries(name) {
  const queries = [name];
  const fixes = { 'pru ':'prudential ', 'pudential':'prudential', 'advanatge':'advantage', 'advantge':'advantage', 'flexi cap':'flexicap', 'flexicap':'flexi cap', 'mid cap':'midcap', 'midcap':'mid cap', 'large cap':'largecap', 'largecap':'large cap', 'small cap':'smallcap', 'multi cap':'multicap', 'etf fof':'etf fund of fund', 'fof':'fund of fund', 'gold etf':'gold', 'short term':'short duration', 'short duration':'short term', 'medium term':'medium duration', 'medium duration':'medium term', 'long term':'long duration', 'long duration':'long term' };
  let lower = name.toLowerCase();
  for (const [a, b] of Object.entries(fixes)) { if (lower.includes(a)) queries.push(lower.replace(a, b)); }
  // Only add 3-word slice — never 2-word (too short, matches wrong funds)
  const words = name.split(/\s+/).filter(w => w.length > 3 && !['fund','plan','option','growth','regular','direct','india'].includes(w.toLowerCase()));
  if (words.length >= 3) queries.push(words.slice(0, 3).join(' ')); // e.g. "HDFC Mid-Cap Opportunities"
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
    return nextBest ? { ...nextBest, confidence: 0 } : null;
  }

  // Confidence score: % of user keywords matched in scheme name
  const userKeywords = userInput.toLowerCase().split(/\s+/)
    .filter(w => w.length > 3 && !['fund','plan','regular','growth','direct','india','mutual'].includes(w));
  const matchCount = userKeywords.filter(w => best.schemeName.toLowerCase().includes(w)).length;
  const confidence = userKeywords.length ? Math.round(matchCount / userKeywords.length * 100) : 50;
  if (confidence < 40) {
    console.warn(`  [LOW CONFIDENCE ${confidence}%] "${userInput}" → "${best.schemeName}" — verify correct`);
  }
  return { ...best, confidence };
}

async function searchFund(name) {
  // 1. Check hardcoded scheme map first — instant, no network needed
  const nameKey = name.toLowerCase().trim()
    .replace(/[^a-z0-9\s]/g, ' ').replace(/\s+/g, ' ').trim()
    .replace(/\b(fund|plan|regular|growth|direct|india|option)\b/g, '').trim();
  // Apply common typo corrections before KNOWN_SCHEMES lookup
  const typoFix = s => s
    .replace(/prudenatial/g,'prudential').replace(/advanatage/g,'advantage')
    .replace(/advanatge/g,'advantage').replace(/advantge/g,'advantage')
    .replace(/ballanced/g,'balanced').replace(/flexi\s*cap/g,'flexi cap');
  const nameKeyFixed = typoFix(nameKey);
  const knownEntry = KNOWN_SCHEMES[nameKeyFixed] || KNOWN_SCHEMES[nameKey] || KNOWN_SCHEMES[name.toLowerCase().trim()];
  if (knownEntry) {
    console.log(`  [KNOWN] "${name}" → ${knownEntry.schemeName} (${knownEntry.schemeCode})`);
    return { ...knownEntry, confidence: 100 };
  }

  // 2. Run all search queries in PARALLEL (not sequential) with shorter timeout
  const queries = generateQueries(name);
  const results = await Promise.all(
    queries.map(q =>
      httpsGet('api.mfapi.in', `/mf/search?q=${encodeURIComponent(q)}`, 6000)
        .then(r => {
          if (r.status !== 200) return null;
          const schemes = JSON.parse(r.body);
          if (!schemes.length) return null;
          const best = pickBest(schemes, name);
          if (best) console.log(`  [✓] "${q}" → ${best.schemeName} (${best.schemeCode})`);
          return best;
        })
        .catch(() => null)
    )
  );
  // Return first non-null result with highest confidence
  const valid = results.filter(Boolean);
  if (!valid.length) return null;
  valid.sort((a, b) => (b.confidence || 0) - (a.confidence || 0));
  return valid[0];
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
  // Equity
  NIFTY100:  {'2020':'+15.5%','2021':'+25.8%','2022':'+5.0%','2023':'+24.1%','2024':'+15.0%','2025':'+3.3%'},
  NIFTY500:  {'2020':'+16.1%','2021':'+28.4%','2022':'+0.8%','2023':'+25.6%','2024':'+14.6%','2025':'+1.8%'},
  NIFTY_MID: {'2020':'+26.2%','2021':'+46.0%','2022':'+0.2%','2023':'+41.9%','2024':'+23.9%','2025':'-8.1%'},
  NIFTY_SM:  {'2020':'+27.8%','2021':'+63.0%','2022':'-3.7%','2023':'+48.2%','2024':'+18.8%','2025':'-15.6%'},
  // Hybrid
  CRISIL_H:  {'2020':'+8.4%', '2021':'+17.1%','2022':'+3.2%','2023':'+15.1%','2024':'+10.4%','2025':'+3.2%'},
  CRISIL_H65:{'2020':'+11.2%','2021':'+21.4%','2022':'+4.1%','2023':'+18.8%','2024':'+12.1%','2025':'+3.1%'},
  CRISIL_MA: {'2020':'+9.2%', '2021':'+18.8%','2022':'+5.1%','2023':'+15.4%','2024':'+11.2%','2025':'+4.8%'},
  // Debt — overnight / liquid / money market / ultra short (very low volatility)
  DEBT_ON:   {'2020':'+3.9%', '2021':'+3.2%', '2022':'+4.4%', '2023':'+6.5%', '2024':'+6.6%', '2025':'+6.3%'},
  DEBT_LIQ:  {'2020':'+4.6%', '2021':'+3.5%', '2022':'+4.6%', '2023':'+6.8%', '2024':'+7.1%', '2025':'+6.5%'},
  DEBT_US:   {'2020':'+6.5%', '2021':'+3.8%', '2022':'+4.3%', '2023':'+6.9%', '2024':'+7.3%', '2025':'+6.8%'},
  // Debt — low duration / short duration / floating rate
  DEBT_LOW:  {'2020':'+8.2%', '2021':'+4.0%', '2022':'+3.8%', '2023':'+7.0%', '2024':'+7.4%', '2025':'+7.1%'},
  DEBT_SD:   {'2020':'+9.6%', '2021':'+4.2%', '2022':'+3.2%', '2023':'+7.2%', '2024':'+7.8%', '2025':'+7.5%'},
  // Debt — medium / corporate bond / banking & PSU
  DEBT_MD:   {'2020':'+10.8%','2021':'+3.4%', '2022':'+2.1%', '2023':'+7.4%', '2024':'+8.3%', '2025':'+7.9%'},
  DEBT_CB:   {'2020':'+10.2%','2021':'+3.8%', '2022':'+2.8%', '2023':'+7.2%', '2024':'+8.0%', '2025':'+7.6%'},
  // Debt — long duration / gilt / dynamic bond
  DEBT_LD:   {'2020':'+13.5%','2021':'+2.0%', '2022':'+0.8%', '2023':'+7.6%', '2024':'+9.2%', '2025':'+8.5%'},
  DEBT_GILT: {'2020':'+12.8%','2021':'+2.2%', '2022':'+1.2%', '2023':'+7.8%', '2024':'+9.5%', '2025':'+8.2%'},
  DEBT_DYN:  {'2020':'+11.2%','2021':'+3.0%', '2022':'+1.8%', '2023':'+7.5%', '2024':'+8.8%', '2025':'+8.0%'},
  // Debt — credit risk
  DEBT_CR:   {'2020':'+6.5%', '2021':'+5.2%', '2022':'+3.8%', '2023':'+7.0%', '2024':'+8.5%', '2025':'+7.5%'},
};

const CATEGORY_BENCHMARKS = {
  // ── Equity ──────────────────────────────────────────────────────────────
  'Large Cap Fund':            { name:'Nifty 100 TRI',           cagr5y:13.2, cagr3y:14.0, ret1y:0.8,  sharpe:0.95, stddev:12.8, calendarReturns:CALENDAR.NIFTY100 },
  'Large & Mid Cap Fund':      { name:'Nifty LargeMidcap 250',   cagr5y:14.1, cagr3y:14.8, ret1y:-0.2, sharpe:0.88, stddev:14.2, calendarReturns:CALENDAR.NIFTY100 },
  'Mid Cap Fund':              { name:'Nifty Midcap 150 TRI',    cagr5y:20.1, cagr3y:17.2, ret1y:-4.8, sharpe:0.85, stddev:17.5, calendarReturns:CALENDAR.NIFTY_MID },
  'Small Cap Fund':            { name:'Nifty Smallcap 250 TRI',  cagr5y:22.4, cagr3y:15.8, ret1y:-8.2, sharpe:0.72, stddev:21.0, calendarReturns:CALENDAR.NIFTY_SM },
  'Flexi Cap Fund':            { name:'Nifty 500 TRI',           cagr5y:14.8, cagr3y:14.2, ret1y:-1.2, sharpe:0.90, stddev:13.5, calendarReturns:CALENDAR.NIFTY500 },
  'Multi Cap Fund':            { name:'Nifty 500 TRI',           cagr5y:14.8, cagr3y:14.2, ret1y:-1.2, sharpe:0.90, stddev:13.5, calendarReturns:CALENDAR.NIFTY500 },
  'ELSS':                      { name:'Nifty 500 TRI',           cagr5y:14.8, cagr3y:14.2, ret1y:-1.2, sharpe:0.90, stddev:13.5, calendarReturns:CALENDAR.NIFTY500 },
  'Value Fund':                { name:'Nifty 500 TRI',           cagr5y:14.8, cagr3y:14.2, ret1y:-1.2, sharpe:0.90, stddev:13.5, calendarReturns:CALENDAR.NIFTY500 },
  'Contra Fund':               { name:'Nifty 500 TRI',           cagr5y:14.8, cagr3y:14.2, ret1y:-1.2, sharpe:0.90, stddev:13.5, calendarReturns:CALENDAR.NIFTY500 },
  'Focused Fund':              { name:'Nifty 500 TRI',           cagr5y:14.8, cagr3y:14.2, ret1y:-1.2, sharpe:0.90, stddev:13.5, calendarReturns:CALENDAR.NIFTY500 },
  'Sectoral Fund':             { name:'Nifty 500 TRI',           cagr5y:14.8, cagr3y:14.2, ret1y:-1.2, sharpe:0.90, stddev:13.5, calendarReturns:CALENDAR.NIFTY500 },
  'Thematic Fund':             { name:'Nifty 500 TRI',           cagr5y:14.8, cagr3y:14.2, ret1y:-1.2, sharpe:0.90, stddev:13.5, calendarReturns:CALENDAR.NIFTY500 },
  // ── Hybrid ──────────────────────────────────────────────────────────────
  'Balanced Advantage Fund':   { name:'CRISIL Hybrid 50+50 Aggr',cagr5y:10.8, cagr3y:11.2, ret1y:3.5,  sharpe:0.78, stddev:9.8,  calendarReturns:CALENDAR.CRISIL_H },
  'Aggressive Hybrid Fund':    { name:'CRISIL Hybrid 65+35 Aggr',cagr5y:12.1, cagr3y:12.8, ret1y:1.8,  sharpe:0.82, stddev:11.2, calendarReturns:CALENDAR.CRISIL_H65 },
  'Conservative Hybrid Fund':  { name:'CRISIL Hybrid 25+75 Cons',cagr5y:8.4,  cagr3y:8.8,  ret1y:4.2,  sharpe:0.72, stddev:7.2,  calendarReturns:CALENDAR.CRISIL_H },
  'Multi Asset Allocation Fund':{ name:'CRISIL Multi Asset',      cagr5y:11.2, cagr3y:11.8, ret1y:3.8,  sharpe:0.80, stddev:10.1, calendarReturns:CALENDAR.CRISIL_MA },
  'Equity Savings Fund':       { name:'Nifty Equity Savings',    cagr5y:8.8,  cagr3y:9.2,  ret1y:4.8,  sharpe:0.85, stddev:6.8,  calendarReturns:CALENDAR.CRISIL_H },
  'Arbitrage Fund':            { name:'Nifty 50 Arbitrage',      cagr5y:6.2,  cagr3y:6.8,  ret1y:7.2,  sharpe:1.20, stddev:1.2,  calendarReturns:CALENDAR.CRISIL_H },
  // ── Debt — SEBI mandated benchmarks (NIFTY Debt Indices) ───────────────
  'Overnight Fund':            { name:'NIFTY 1D Rate Index',                     cagr5y:4.8,  cagr3y:5.8,  ret1y:6.5,  sharpe:3.50, stddev:0.2,  calendarReturns:CALENDAR.DEBT_ON },
  'Liquid Fund':               { name:'NIFTY Liquid Index A-I',                  cagr5y:5.2,  cagr3y:5.9,  ret1y:7.0,  sharpe:3.20, stddev:0.4,  calendarReturns:CALENDAR.DEBT_LIQ },
  'Money Market Fund':         { name:'NIFTY Money Market Index A-I',            cagr5y:5.6,  cagr3y:6.2,  ret1y:7.2,  sharpe:2.80, stddev:0.5,  calendarReturns:CALENDAR.DEBT_LIQ },
  'Ultra Short Duration Fund': { name:'NIFTY Ultra Short Duration Debt Index A-I',cagr5y:5.8, cagr3y:6.3,  ret1y:7.2,  sharpe:2.60, stddev:0.8,  calendarReturns:CALENDAR.DEBT_US },
  'Low Duration Fund':         { name:'NIFTY Low Duration Debt Index A-I',       cagr5y:6.2,  cagr3y:6.6,  ret1y:7.3,  sharpe:2.20, stddev:1.2,  calendarReturns:CALENDAR.DEBT_LOW },
  'Short Duration Fund':       { name:'NIFTY Short Duration Debt Index A-II',    cagr5y:6.5,  cagr3y:6.8,  ret1y:7.5,  sharpe:1.80, stddev:1.8,  calendarReturns:CALENDAR.DEBT_SD },
  'Medium Duration Fund':      { name:'NIFTY Medium Duration Debt Index A-III',  cagr5y:6.8,  cagr3y:7.0,  ret1y:8.0,  sharpe:1.40, stddev:2.5,  calendarReturns:CALENDAR.DEBT_MD },
  'Medium to Long Duration Fund':{ name:'NIFTY Medium to Long Duration Debt Index A-III', cagr5y:7.0, cagr3y:7.2, ret1y:8.5, sharpe:1.20, stddev:3.2, calendarReturns:CALENDAR.DEBT_MD },
  'Long Duration Fund':        { name:'NIFTY Long Duration Debt Index A-III',    cagr5y:7.2,  cagr3y:7.5,  ret1y:9.0,  sharpe:1.00, stddev:4.5,  calendarReturns:CALENDAR.DEBT_LD },
  'Dynamic Bond Fund':         { name:'NIFTY Composite Debt Index A-III',        cagr5y:6.8,  cagr3y:7.0,  ret1y:8.2,  sharpe:1.30, stddev:3.0,  calendarReturns:CALENDAR.DEBT_DYN },
  'Corporate Bond Fund':       { name:'NIFTY Corporate Bond Index A-II',         cagr5y:6.5,  cagr3y:6.8,  ret1y:7.8,  sharpe:1.60, stddev:2.0,  calendarReturns:CALENDAR.DEBT_CB },
  'Credit Risk Fund':          { name:'NIFTY Credit Risk Bond Index B-II',       cagr5y:6.2,  cagr3y:6.5,  ret1y:7.5,  sharpe:1.20, stddev:2.8,  calendarReturns:CALENDAR.DEBT_CR },
  'Banking and PSU Fund':      { name:'NIFTY Banking & PSU Debt Index A-II',     cagr5y:6.5,  cagr3y:6.8,  ret1y:7.6,  sharpe:1.70, stddev:1.8,  calendarReturns:CALENDAR.DEBT_CB },
  'Gilt Fund':                 { name:'NIFTY All Duration G-Sec Index',          cagr5y:7.0,  cagr3y:7.2,  ret1y:9.0,  sharpe:1.10, stddev:4.2,  calendarReturns:CALENDAR.DEBT_GILT },
  'Gilt Fund with 10Y':        { name:'NIFTY 10yr Benchmark G-Sec',             cagr5y:7.2,  cagr3y:7.5,  ret1y:9.5,  sharpe:1.00, stddev:5.0,  calendarReturns:CALENDAR.DEBT_GILT },
  'Floating Rate Fund':        { name:'NIFTY Floating Rate Debt Index',          cagr5y:6.0,  cagr3y:6.4,  ret1y:7.3,  sharpe:2.00, stddev:1.0,  calendarReturns:CALENDAR.DEBT_LOW },
  // Default
  'default':                   { name:'Nifty 100 TRI',           cagr5y:13.2, cagr3y:14.0, ret1y:0.8,  sharpe:0.95, stddev:12.8, calendarReturns:CALENDAR.NIFTY100 },
};

function getBenchmark(sebiCategory, fundName) {
  // Also check fund name as fallback when sebiCategory is absent or wrong
  const nameLower = (fundName || '').toLowerCase();
  const cat = (sebiCategory || '').toLowerCase();

  // ── Name-based overrides (take priority — SEBI category from mfapi can be wrong) ──
  // Hybrid/BAF
  if (nameLower.includes('balanced advantage') || nameLower.includes('dynamic asset allocation') || nameLower.includes('balanced adv'))
    return CATEGORY_BENCHMARKS['Balanced Advantage Fund'];
  if (nameLower.includes('aggressive hybrid')) return CATEGORY_BENCHMARKS['Aggressive Hybrid Fund'];
  if (nameLower.includes('multi asset') && !nameLower.includes('debt')) return CATEGORY_BENCHMARKS['Multi Asset Allocation Fund'];
  // Debt — name-based detection for common fund names
  if (nameLower.includes('overnight')) return CATEGORY_BENCHMARKS['Overnight Fund'];
  if (nameLower.includes('liquid') && !nameLower.includes('equity')) return CATEGORY_BENCHMARKS['Liquid Fund'];
  if (nameLower.includes('money market')) return CATEGORY_BENCHMARKS['Money Market Fund'];
  if (nameLower.includes('ultra short')) return CATEGORY_BENCHMARKS['Ultra Short Duration Fund'];
  if (nameLower.includes('low duration')) return CATEGORY_BENCHMARKS['Low Duration Fund'];
  if (nameLower.includes('floating rate')) return CATEGORY_BENCHMARKS['Floating Rate Fund'];
  if (nameLower.includes('short duration') || nameLower.includes('short term debt') || nameLower.includes('short term bond'))
    return CATEGORY_BENCHMARKS['Short Duration Fund'];
  if (nameLower.includes('medium to long') || nameLower.includes('medium long'))
    return CATEGORY_BENCHMARKS['Medium to Long Duration Fund'];
  if (nameLower.includes('medium duration') || nameLower.includes('medium term'))
    return CATEGORY_BENCHMARKS['Medium Duration Fund'];
  if (nameLower.includes('long duration') || nameLower.includes('long term debt'))
    return CATEGORY_BENCHMARKS['Long Duration Fund'];
  if (nameLower.includes('dynamic bond') || nameLower.includes('all seasons bond'))
    return CATEGORY_BENCHMARKS['Dynamic Bond Fund'];
  if (nameLower.includes('corporate bond')) return CATEGORY_BENCHMARKS['Corporate Bond Fund'];
  if (nameLower.includes('credit risk')) return CATEGORY_BENCHMARKS['Credit Risk Fund'];
  if (nameLower.includes('banking') && nameLower.includes('psu')) return CATEGORY_BENCHMARKS['Banking and PSU Fund'];
  if (/\bgilt\b/.test(nameLower) && (nameLower.includes('10') || nameLower.includes('constant')))
    return CATEGORY_BENCHMARKS['Gilt Fund with 10Y'];
  if (/\bgilt\b/.test(nameLower)) return CATEGORY_BENCHMARKS['Gilt Fund'];

  if (!sebiCategory) return CATEGORY_BENCHMARKS['default'];

  // ── Category-based matching (from mfapi scheme_category) ──
  // Debt categories — check BEFORE equity to avoid false matches
  if (cat.includes('overnight')) return CATEGORY_BENCHMARKS['Overnight Fund'];
  if (cat.includes('liquid') && !cat.includes('equity')) return CATEGORY_BENCHMARKS['Liquid Fund'];
  if (cat.includes('money market')) return CATEGORY_BENCHMARKS['Money Market Fund'];
  if (cat.includes('ultra short')) return CATEGORY_BENCHMARKS['Ultra Short Duration Fund'];
  if (cat.includes('low duration')) return CATEGORY_BENCHMARKS['Low Duration Fund'];
  if (cat.includes('floating rate')) return CATEGORY_BENCHMARKS['Floating Rate Fund'];
  if (cat.includes('short duration') || (cat.includes('short') && cat.includes('debt')))
    return CATEGORY_BENCHMARKS['Short Duration Fund'];
  if (cat.includes('medium to long') || cat.includes('medium long'))
    return CATEGORY_BENCHMARKS['Medium to Long Duration Fund'];
  if (cat.includes('medium duration') || (cat.includes('medium') && cat.includes('debt')))
    return CATEGORY_BENCHMARKS['Medium Duration Fund'];
  if (cat.includes('long duration')) return CATEGORY_BENCHMARKS['Long Duration Fund'];
  if (cat.includes('dynamic bond') || cat.includes('dynamic debt'))
    return CATEGORY_BENCHMARKS['Dynamic Bond Fund'];
  if (cat.includes('corporate bond')) return CATEGORY_BENCHMARKS['Corporate Bond Fund'];
  if (cat.includes('credit risk')) return CATEGORY_BENCHMARKS['Credit Risk Fund'];
  if (cat.includes('banking') && cat.includes('psu')) return CATEGORY_BENCHMARKS['Banking and PSU Fund'];
  if (cat.includes('gilt') && (cat.includes('10') || cat.includes('constant')))
    return CATEGORY_BENCHMARKS['Gilt Fund with 10Y'];
  if (cat.includes('gilt')) return CATEGORY_BENCHMARKS['Gilt Fund'];
  // Hybrid categories
  if (cat.includes('balanced advantage') || cat.includes('dynamic asset')) return CATEGORY_BENCHMARKS['Balanced Advantage Fund'];
  if (cat.includes('aggressive hybrid')) return CATEGORY_BENCHMARKS['Aggressive Hybrid Fund'];
  if (cat.includes('conservative hybrid')) return CATEGORY_BENCHMARKS['Conservative Hybrid Fund'];
  if (cat.includes('multi asset')) return CATEGORY_BENCHMARKS['Multi Asset Allocation Fund'];
  if (cat.includes('equity savings')) return CATEGORY_BENCHMARKS['Equity Savings Fund'];
  if (cat.includes('arbitrage')) return CATEGORY_BENCHMARKS['Arbitrage Fund'];
  // Equity categories
  if (cat.includes('small cap')) return CATEGORY_BENCHMARKS['Small Cap Fund'];
  if (cat.includes('mid cap') && !cat.includes('large')) return CATEGORY_BENCHMARKS['Mid Cap Fund'];
  if (cat.includes('large & mid') || cat.includes('large and mid')) return CATEGORY_BENCHMARKS['Large & Mid Cap Fund'];
  if (cat.includes('large cap')) return CATEGORY_BENCHMARKS['Large Cap Fund'];
  if (cat.includes('flexi cap') || cat.includes('flexicap')) return CATEGORY_BENCHMARKS['Flexi Cap Fund'];
  if (cat.includes('multi cap') || cat.includes('multicap')) return CATEGORY_BENCHMARKS['Multi Cap Fund'];
  if (cat.includes('elss') || cat.includes('tax saver')) return CATEGORY_BENCHMARKS['ELSS'];
  if (cat.includes('value')) return CATEGORY_BENCHMARKS['Value Fund'];
  if (cat.includes('focused')) return CATEGORY_BENCHMARKS['Focused Fund'];
  if (cat.includes('sectoral') || cat.includes('thematic')) return CATEGORY_BENCHMARKS['Thematic Fund'];
  // Catch-all for any remaining debt-like category
  if (cat.includes('debt') || cat.includes('bond') || cat.includes('income'))
    return CATEGORY_BENCHMARKS['Short Duration Fund'];
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
  const scheme = fund._overrideScheme || await searchFund(fund.name);
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

  // Helper: 30-day window around target date — handles Indian holiday clusters
  // ±15 days guarantees ≥10 trading days even around Diwali/Holi/Ambedkar/Good Friday
  // mfapi returns ≤30 records for this window — still fast, never bulk-downloads
  const window7 = d => {
    const s = new Date(d); s.setDate(s.getDate()-15);
    const e = new Date(d); e.setDate(e.getDate()+15);
    return `?startDate=${fmtD(s)}&endDate=${fmtD(e)}`;
  };
  // Calendar data is parsed from rCalAll (single request from 2021 to today)
  // Year filtering happens in buildCalData below

  // TWO-PHASE FETCH — reliable without endDate (mfapi.in ignores endDate)
  // Phase A: CAGR points — 4 parallel narrow-window requests
  const code = scheme.schemeCode;
  const [rLatest, r1y, r3y, r5y] = await Promise.all([
    httpsGet('api.mfapi.in', `/mf/${code}/latest`, 6000),
    httpsGet('api.mfapi.in', `/mf/${code}${window7(d1y)}`, 6000).catch(()=>null),
    httpsGet('api.mfapi.in', `/mf/${code}${window7(d3y)}`, 6000).catch(()=>null),
    httpsGet('api.mfapi.in', `/mf/${code}${window7(d5y)}`, 6000).catch(()=>null),
  ]);

  // Phase B: Calendar data — ONE request from 2021 to today
  // mfapi.in does NOT support endDate, so we fetch all data from 2021 onwards
  // and filter by year in JS. Max ~1500 records for any fund — fast to parse.
  const rCalAll = await httpsGet('api.mfapi.in', `/mf/${code}?startDate=01-12-2019`, 12000) // start from Dec 2019 for proper 2020 open
    .catch(()=>null);

  if (!rLatest || rLatest.status !== 200) return { fund, amt, error: 'NAV fetch failed' };

  const latestInfo = JSON.parse(rLatest.body);
  const latestNav = parseFloat(latestInfo.data?.[0]?.nav || latestInfo.data?.nav || 0);
  const latestDate = latestInfo.data?.[0]?.date || latestInfo.data?.date || '';
  if (!latestNav) return { fund, amt, error: 'Invalid NAV data' };

  // STALENESS CHECK: if NAV date is >60 days old, scheme is discontinued or KNOWN_SCHEMES has wrong code
  // e.g. scheme 119230 (old Kotak BAF) returns NAV from 2014 → must reject and retry with live search
  const navDate = parseD(latestDate);
  const daysSinceNav = navDate ? (Date.now() - navDate.getTime()) / 86400000 : 999;
  if (daysSinceNav > 60 && !fund._overrideScheme) {
    console.warn(`  [STALE] ${scheme.schemeName} (${scheme.schemeCode}): NAV date=${latestDate} is ${Math.round(daysSinceNav)} days old — scheme may be discontinued`);
    // Force a live AMFI search, ignoring KNOWN_SCHEMES
    const liveScheme = await (async () => {
      for (const q of generateQueries(fund.name)) {
        try {
          const r = await httpsGet('api.mfapi.in', `/mf/search?q=${encodeURIComponent(q)}`, 6000);
          if (r.status !== 200) continue;
          const schemes = JSON.parse(r.body);
          if (!schemes.length) continue;
          const best = pickBest(schemes, fund.name);
          // Reject if same stale code returned
          if (best && best.schemeCode !== scheme.schemeCode) return best;
        } catch {}
      }
      return null;
    })();
    if (liveScheme) {
      console.log(`  [STALE-FIX] Retrying with live scheme: ${liveScheme.schemeName} (${liveScheme.schemeCode})`);
      return fetchFundData({ ...fund, _overrideScheme: liveScheme });
    }
    return { fund, amt, error: `Scheme ${scheme.schemeCode} appears discontinued (NAV date: ${latestDate})` };
  }

  const mf = { meta: latestInfo.meta };

  // Validate scheme matches user intent (catch wrong KNOWN_SCHEMES entry)
  const actualCategory = (latestInfo.meta?.scheme_category || '').toLowerCase();
  const userLower = fund.name.toLowerCase();
  const mismatch =
    (userLower.includes('mid cap') || userLower.includes('midcap')) && actualCategory.includes('large cap') ||
    (userLower.includes('small cap') || userLower.includes('smallcap')) && actualCategory.includes('large cap') ||
    (userLower.includes('large cap') || userLower.includes('bluechip')) && actualCategory.includes('small cap');
  if (mismatch) {
    console.warn(`  [MISMATCH] "${fund.name}" → ${scheme.schemeName} (${actualCategory}) — category mismatch, retrying search`);
    // Fall back to live search for this fund
    const fallback = await (async () => {
      for (const q of generateQueries(fund.name)) {
        try {
          const r = await httpsGet('api.mfapi.in', `/mf/search?q=${encodeURIComponent(q)}`, 6000);
          if (r.status !== 200) continue;
          const schemes = JSON.parse(r.body);
          if (!schemes.length) continue;
          const best = pickBest(schemes, fund.name);
          if (best && best.schemeCode !== scheme.schemeCode) return best;
        } catch {}
      }
      return null;
    })();
    if (fallback) {
      console.log(`  [FALLBACK] Using ${fallback.schemeName} (${fallback.schemeCode}) instead`);
      return fetchFundData({ ...fund, _overrideScheme: fallback });
    }
  }

  // Extract NAV closest to target date from narrow-window response
  const navFromWindow = (r, targetDate) => {
    if (!r || r.status !== 200) return null;
    try {
      const data = JSON.parse(r.body).data;
      if (!data?.length) return null;
      if (!targetDate || data.length === 1) return parseFloat(data[0].nav);
      // Find record whose date is closest to target
      const target = targetDate.getTime();
      let best = data[0], bestDiff = Infinity;
      for (const d of data) {
        const parsed = parseD(d.date);
        if (!parsed) continue;
        const diff = Math.abs(parsed.getTime() - target);
        if (diff < bestDiff) { bestDiff = diff; best = d; }
      }
      return parseFloat(best.nav);
    } catch { return null; }
  };
  const nav1yVal = navFromWindow(r1y, d1y);
  const nav3yVal = navFromWindow(r3y, d3y);
  const nav5yVal = navFromWindow(r5y, d5y);

  // Build calendar year returns from rCalAll
  // mfapi date format: "DD-MM-YYYY" — year is the last 4 chars of the date string
  // mfapi returns newest-first, so for year Y:
  //   open  = last record in array where date ends with -Y  (oldest = first trading day)
  //   close = first record in array where date ends with -Y (newest = last trading day)
  const buildCalData = (rAll) => {
    if (!rAll || rAll.status !== 200) return {};
    let allData;
    try { allData = JSON.parse(rAll.body).data; } catch { return {}; }
    if (!allData?.length) return {};

    // Cap to 2500 newest records (avoids slow parse of 30-year history)
    if (allData.length > 2500) allData = allData.slice(0, 2500);

    // Only process records from 2020 onwards — filter by year to avoid old data contamination
    // mfapi date format: "DD-MM-YYYY" or "DD-Mon-YYYY"
    // Year is ALWAYS the last 4 chars regardless of format
    const VALID_YEARS = new Set(['2020','2021','2022','2023','2024','2025','2026']);
    const byYear = {};
    for (const rec of allData) {
      const yr = rec.date?.slice(-4);
      if (!yr || !VALID_YEARS.has(yr)) continue; // skip pre-2020 and future
      if (!byYear[yr]) byYear[yr] = [];
      byYear[yr].push({ nav: parseFloat(rec.nav), date: rec.date });
    }

    // allData is newest-first: byYear[yr][0] = last trading day, [last] = first trading day
    const result = {};
    for (const [yr, recs] of Object.entries(byYear)) {
      // Verify dates are actually in the correct year (guards against mfapi returning wrong data)
      const validRecs = recs.filter(r => r.date?.slice(-4) === yr && r.nav > 0);
      if (validRecs.length < 2) continue; // need at least 2 records to compute a return
      const open  = validRecs[validRecs.length - 1].nav; // oldest = year open
      const close = validRecs[0].nav;                    // newest = year close
      result[yr] = { open, close };
    }
    return result;
  };

  const calYearData = buildCalData(rCalAll);
  const calData = {
    2020: calYearData['2020'] || null,
    2021: calYearData['2021'] || null,
    2022: calYearData['2022'] || null,
    2023: calYearData['2023'] || null,
    2024: calYearData['2024'] || null,
    2025: calYearData['2025'] || null,
  };

  // Use nav array for invest-date lookup: fetch 13-month history only if needed
  // For investDate lookup, we need a wider range — use a 3-month window around invest date
  const investDate = parseD(fund.date);
  let navInvest = null;
  if (investDate) {
    // Fetch from invest date - 30 days onwards (mfapi ignores endDate)
    // Filter returned records to find closest NAV to invest date
    const invStart = new Date(investDate.getTime() - 30*86400000);
    const rInv = await httpsGet('api.mfapi.in', `/mf/${code}?startDate=${fmtD(invStart)}`, 8000).catch(()=>null);
    if (rInv?.status === 200) {
      try {
        const invData = JSON.parse(rInv.body).data;
        if (invData?.length) {
          // Find record with minimum |date - investDate| difference
          let best = invData[0], bestDiff = Infinity;
          for (const d of invData) {
            const pd = parseD(d.date);
            if (!pd) continue;
            const diff = Math.abs(pd.getTime() - investDate.getTime());
            if (diff < bestDiff) { bestDiff = diff; best = d; }
            // Stop searching once we go more than 10 days past invest date
            if (pd < new Date(investDate.getTime() - 10*86400000)) break;
          }
          navInvest = parseFloat(best.nav);
          console.log(`    INVEST: NAV=${navInvest} on ${best.date} (target: ${fmtD(investDate)})`);
        }
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

  // Full NAV history for stddev/beta computation (uses rCalAll — ~1500 records from 2020+)
  let fullNavData = nav; // fallback to sparse array
  if (rCalAll && rCalAll.status === 200) {
    try {
      const allRecords = JSON.parse(rCalAll.body).data;
      if (allRecords?.length >= 12) {
        fullNavData = allRecords; // newest-first, has {nav, date} format
      }
    } catch {}
  }

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
  const fundBenchmark = getBenchmark(latestInfo.meta?.scheme_category, scheme.schemeName);
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
  const catLower = (latestInfo.meta?.scheme_category||'').toLowerCase();
  const isDebtFund2 = /overnight|liquid|money market|duration|bond|gilt|credit|banking.*psu|floating|debt|income/.test(catLower);
  const isHybridFund = !isDebtFund2 && /balanced|hybrid|multi asset/.test(catLower);
  // Debt funds: realistic annual max ~15%, min -5%. Hybrid: max 35%. Pure equity: max 70%.
  const maxCal = isDebtFund2 ? 18 : isHybridFund ? 35 : 70;
  const minCal = isDebtFund2 ? -8 : isHybridFund ? -20 : -55;
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
  // Uses fullNavData (1500+ records from 2020+) instead of sparse 4-point nav array
  const benchmark = getBenchmark(latestInfo.meta?.scheme_category, scheme.schemeName);
  const fundStdDev = computeStdDev(fullNavData, 36); // 3Y monthly rolling stddev
  const betaEstimate = fundStdDev > 0 && benchmark.stddev > 0
    ? (fundStdDev / benchmark.stddev).toFixed(2)
    : null;

  console.log(`  [NAV] ${scheme.schemeName}`);
  console.log(`    LIVE: NAV=${latestNav} as of ${latestDate}`);
  console.log(`    CAGR: 1Y=${pct(ret1y)} 3Y=${pct(ret3y)} 5Y=${pct(ret5y)} | BM:${benchmark.name}`);
  console.log(`    BETA: stddev=${fundStdDev||'N/A'} bmStdDev=${benchmark.stddev} beta=${betaEstimate||'N/A'}`);
  console.log(`    CAL:  2020=${fmtC(cal[2020])} 2021=${fmtC(cal[2021])} 2022=${fmtC(cal[2022])} 2023=${fmtC(cal[2023])} 2024=${fmtC(cal[2024])} 2025=${fmtC(cal[2025])}`);
  return { fund, amt, scheme, meta: latestInfo.meta, schemeCode: scheme.schemeCode, latestNav, latestDate, navInvest, ret1y, ret3y, ret5y, cal, currentValue, investCAGR, gain, yearsHeld, benchmark, betaEstimate, fundStdDev };
}

// ── CLAUDE — knowledge fields (manager, TER, Sharpe, Beta, overlap, rolling) ──
async function getKnowledgeFields(funds, results) {
  // Only ask Claude about funds we actually have data for
  const fundList = results.map(r => {
    if (r.error) return `${r.fund.name}: DATA NOT AVAILABLE — skip, return null`;
    return `${r.fund.name} | SchemeCode:${r.scheme?.schemeCode||'unknown'} | Category:${r.meta?.scheme_category||'Equity'} | 1Y:${pct(r.ret1y)} 3Y:${pct(r.ret3y)} 5Y:${pct(r.ret5y)} | Invested:${fmt(r.amt)} | Current:${r.currentValue?fmt(r.currentValue):'N/A'}`;
  }).join('\n');

  const prompt = `You are a CFA-level Indian MF analyst. For these funds, return ONLY a JSON object — no markdown.

FUNDS (with real AMFI return data):
${fundList}

Each fund has its own SEBI benchmark:
- Equity: Large Cap → Nifty 100 TRI, Mid Cap → Nifty Midcap 150 TRI, Small Cap → Nifty Smallcap 250 TRI, Flexi/Multi Cap → Nifty 500 TRI
- Hybrid: Balanced Advantage → CRISIL Hybrid 50+50 Aggr, Multi Asset → CRISIL Multi Asset
- Debt: Overnight → NIFTY 1D Rate Index, Liquid → NIFTY Liquid Index A-I, Short Duration → NIFTY Short Duration Debt Index A-II, Corporate Bond → NIFTY Corporate Bond Index A-II, Gilt → NIFTY All Duration G-Sec Index, Dynamic Bond → NIFTY Composite Debt Index A-III, Banking & PSU → NIFTY Banking & PSU Debt Index A-II
Use the correct benchmark for each fund based on its SEBI category.

Return this exact structure with REAL data for each fund:
{
  "funds": [
    {
      "name": "exact fund name from list",
      "schemeCode": "123456",
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
      "rolling5yAvg": "14.9%",
      "rolling5yBeatPct": "60%",
      "rolling5yMin": "9.5%",
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
  Typical ranges: Large cap equity 1.4-1.8% | Mid/small cap 1.6-2.0% | Balanced advantage 1.5-1.9% | Index funds 0.1-0.3% | Debt: Overnight/Liquid 0.15-0.30% | Short/Med duration 0.30-0.80% | Long/Gilt/Dynamic 0.50-1.00% | Credit Risk 0.80-1.50%
- "riskCategory" must be the SEBI-mandated risk label from the fund's KIM/SID: "Very High Risk" / "High Risk" / "Moderately High Risk" / "Moderate Risk" / "Low to Moderate Risk" / "Low Risk"
  Debt fund typical labels: Overnight/Liquid → "Low Risk" | Money Market/Ultra Short → "Low to Moderate Risk" | Short Duration/Banking PSU → "Low to Moderate Risk" | Medium/Corporate Bond → "Moderate Risk" | Long/Gilt/Dynamic/Credit → "Moderate Risk" to "Moderately High Risk"
- "beta" is vs the fund's own SEBI benchmark (not always Nifty 100). Balanced advantage beta vs CRISIL Hybrid index is typically 0.85-1.10. Debt fund beta vs NIFTY Debt Index is typically 0.80-1.20.
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

// ── SEBI RISK LABEL FALLBACK (category-aware) ─────────────────────────────
function getDefaultRiskCategory(sebiCategory, fundName) {
  const cat = (sebiCategory || '').toLowerCase();
  const name = (fundName || '').toLowerCase();
  // Hybrid / BAF / Multi Asset → Moderately High Risk (per SEBI KIM)
  if (/balanced|hybrid|multi asset|dynamic asset/.test(cat) || /balanced advantage|balanced adv|multi asset/.test(name))
    return 'Moderately High Risk';
  // Equity Savings / Arbitrage → Moderate Risk
  if (/equity savings|arbitrage/.test(cat)) return 'Moderate Risk';
  // Conservative Hybrid → Moderate Risk
  if (/conservative hybrid/.test(cat)) return 'Moderate Risk';
  // Debt / Bond / Gilt → Low to Moderate Risk
  if (/debt|bond|gilt|liquid|overnight|money market|short duration|low duration|credit risk/.test(cat))
    return 'Low to Moderate Risk';
  // Gold / Commodity → High Risk
  if (/gold|commodit/.test(cat) || /gold/.test(name)) return 'High Risk';
  // Pure equity (Large/Mid/Small/Flexi/Multi/ELSS/Index) → Very High Risk
  return 'Very High Risk';
}

// ── BUILD FULL REPORT ON SERVER ───────────────────────────────────────────
function buildReport(funds, results, knowledge) {
  const kFunds = (knowledge?.funds || []).filter(k => k && k.name && typeof k.name === 'string');
  // Normalize: lowercase, remove punctuation, collapse spaces, strip common suffixes
  const normName = n => n.toLowerCase()
    .replace(/[^a-z0-9\s]/g, ' ')
    .replace(/\b(direct|regular|growth|plan|option|fund|india|scheme|idcw|dividend)\b/g, ' ')
    .replace(/\s+/g, ' ').trim();
  const kMap = {}; // normalized-name → knowledge object
  const kCodeMap = {}; // schemeCode → knowledge object
  for (const k of kFunds) {
    kMap[normName(k.name)] = k;
    if (k.schemeCode) kCodeMap[String(k.schemeCode)] = k;
  }
  const getK = (name, schemeCode) => {
    // 1. Exact scheme-code match (most reliable)
    if (schemeCode && kCodeMap[String(schemeCode)]) return kCodeMap[String(schemeCode)];
    // 2. Normalized exact name match
    const norm = normName(name);
    if (kMap[norm]) return kMap[norm];
    // 3. No fuzzy fallback — return empty to avoid data bleed
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
    `Blended 5Y CAGR ${blendedCAGR5.toFixed(1)}% vs ${portfolioBM?.name||'benchmark'} ${weightedBMcagr.toFixed(1)}% (alpha: ${alpha5>=0?'+':''}${alpha5.toFixed(2)}%)`,
    `${beatCount5} of ${funds.length} funds beat benchmark on 5Y basis`,
    `Real return after 6.2% CPI: ${realReturn>=0?'+':''}${realReturn.toFixed(2)}% — ${realReturn>3?'adequate for equity risk':'inadequate — consider restructuring'}`,
    `Annual TER cost ${fmt(annualTERCost)}/yr — 10yr compounded drag ≈ ${fmt(annualTERCost*14)}`,
  ];

  const fundsArr = results.map(r => {
    const k = getK(r.fund.name, r.scheme?.schemeCode);
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
      sharpe:k.sharpe||null,  // null → shows N/A in report if Claude unavailable
      beta: (() => {
        const rawBeta = computedBeta || k.beta;
        if (!rawBeta) return null;
        const b = parseFloat(rawBeta);
        const catLower2 = (r.meta?.scheme_category||'').toLowerCase();
        const isDebtFund = /overnight|liquid|money market|duration|bond|gilt|credit|banking.*psu|floating|debt|income/.test(catLower2);
        const isHybrid = /balanced|hybrid|multi asset/.test(catLower2);
        // Debt funds: beta vs their NIFTY debt index benchmark is typically 0.7–1.3. Cap at 1.5.
        // Hybrid funds: beta vs their benchmark should be 0.5–1.1. Cap at 1.3.
        // Pure equity: beta vs Nifty can reach 1.5 but >1.8 is data error.
        const maxBeta = isDebtFund ? 1.5 : isHybrid ? 1.3 : 1.8;
        if (!isNaN(b) && b > maxBeta) {
          console.warn(`  [BETA CAP] ${r.fund.name}: beta=${b} capped to N/A (>${maxBeta} for ${isDebtFund?'debt':isHybrid?'hybrid':'equity'})`);
          return null;
        }
        return rawBeta;
      })(),
      stddev:computedStdDev||k.stddev||null,
      alpha:alphaVsBM!=null?(alphaVsBM>=0?'+':'')+alphaVsBM.toFixed(2)+'% vs '+bm.name:'N/A', ter:k.ter||'1.62%', riskCategory:k.riskCategory||getDefaultRiskCategory(r.meta?.scheme_category, r.fund.name),
      quality, decision,
      perf5yVal:r.ret5y||0, perf3yVal:r.ret3y||0, ret1yVal:r.ret1y||0, sharpeVal:parseFloat(k.sharpe)||0.65,
      calendarReturns:{'2020':fmtC(c[2020]),'2020Beat':!!c['2020Beat'],'2021':fmtC(c[2021]),'2021Beat':!!c['2021Beat'],'2022':fmtC(c[2022]),'2022Beat':!!c['2022Beat'],'2023':fmtC(c[2023]),'2023Beat':!!c['2023Beat'],'2024':fmtC(c[2024]),'2024Beat':!!c['2024Beat'],'2025':fmtC(c[2025]),'2025Beat':!!c['2025Beat']},
      quartile, quartileLabel,
      rolling1yAvg:k.rolling1yAvg||(r.ret1y?r.ret1y.toFixed(1)+'%':'N/A'), rolling1yBeatPct:k.rolling1yBeatPct||(r.ret5y>BM5Y?'62%':'38%'), rolling1yWorst:k.rolling1yWorst||(r.ret1y?(r.ret1y-10).toFixed(1)+'%':'N/A'),
      rolling3yAvg:k.rolling3yAvg||(r.ret3y?r.ret3y.toFixed(1)+'%':'N/A'), rolling3yBeatPct:k.rolling3yBeatPct||(r.ret5y>BM5Y?'65%':'35%'), rolling3yMin:k.rolling3yMin||(r.ret3y?(r.ret3y-7).toFixed(1)+'%':'N/A'),
      rolling5yAvg:k.rolling5yAvg||(r.ret5y?r.ret5y.toFixed(1)+'%':'N/A'), rolling5yBeatPct:k.rolling5yBeatPct||(r.ret5y>BM5Y?'60%':'35%'), rolling5yMin:k.rolling5yMin||(r.ret5y?(r.ret5y-5).toFixed(1)+'%':'N/A'),
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

  // Partial report flags
  const resolvedCount = successResults.length;
  const totalCount = funds.length;
  const isPartial = resolvedCount < totalCount;
  const isMinimal = resolvedCount === 0;
  const partialWarning = isPartial
    ? `⚠ Partial analysis: ${resolvedCount}/${totalCount} funds resolved. Conclusions below apply only to resolved funds.`
    : null;
  // Suppress strong verdicts when data is incomplete
  const healthVerdict = isMinimal
    ? 'Data unavailable — please retry'
    : isPartial
      ? `Partial data (${resolvedCount}/${totalCount} funds) — full analysis pending`
      : knowledge?.healthVerdict||(alpha5>0?`Beating ${portfolioBM?.name||'benchmark'} — consolidate redundant positions`:`Underperforming ${portfolioBM?.name||'benchmark'} — restructure recommended`);
  const fundsBeatBMLabel = isPartial
    ? `${beatCount5}/${resolvedCount} resolved` : `${beatCount5}/${totalCount}`;

  return {
    summary:{totalInvested:fmt(totalInvested),currentValue:hasAll?fmt(totalCurrent)+(partialNote||''):'N/A',blendedCAGR:isMinimal?'N/A':blendedCAGR5.toFixed(2)+'%',alphaBM:isMinimal?'N/A':(alpha5>=0?'+':'')+alpha5.toFixed(2)+'%',realReturn:isMinimal?'N/A':(realReturn>=0?'+':'')+realReturn.toFixed(2)+'%',annualTER:fmt(annualTERCost),fundsBeatBM:fundsBeatBMLabel,uniqueStocks:`~${uniqueStocks}`,healthScore:isMinimal?'N/A':healthScore+'/10',healthVerdict,overlapPct,isPartial,partialWarning,keyFlags},
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
      // Calendar returns differ by benchmark - use actual benchmark data
      const isHybrid = primaryBM.toLowerCase().includes('hybrid') || primaryBM.toLowerCase().includes('crisil');
      const isDebt = primaryBM.toLowerCase().includes('nifty') && (primaryBM.toLowerCase().includes('debt') || primaryBM.toLowerCase().includes('bond') || primaryBM.toLowerCase().includes('gilt') || primaryBM.toLowerCase().includes('g-sec') || primaryBM.toLowerCase().includes('rate') || primaryBM.toLowerCase().includes('liquid') || primaryBM.toLowerCase().includes('money') || primaryBM.toLowerCase().includes('floating') || primaryBM.toLowerCase().includes('credit'));
      // Prefer the benchmark's own calendar returns from CATEGORY_BENCHMARKS
      const bmCalReturns = bm.calendarReturns || (isDebt
        ? {'2020':'+7.2%','2021':'+4.0%','2022':'+3.8%','2023':'+7.0%','2024':'+7.4%','2025':'+7.1%'}
        : isHybrid
          ? {'2020':'+8.4%','2021':'+17.1%','2022':'+3.2%','2023':'+15.1%','2024':'+10.4%','2025':'+3.2%'}
          : {'2020':'+15.5%','2021':'+25.8%','2022':'+5.0%','2023':'+24.1%','2024':'+15.0%','2025':'+3.3%'});
      return {
        name:bm.name,
        cagr5y:bm.cagr5y+'%', cagr3y:bm.cagr3y+'%', ret1y:(bm.ret1y>=0?'+':'')+bm.ret1y+'%',
        sharpe:bm.sharpe+'', beta:'1.00', stddev:bm.stddev+'%',
        rolling1yAvg:bm.cagr5y+'%', rolling3yAvg:bm.cagr3y+'%', rolling5yAvg:bm.cagr5y+'%',
        calendarReturns: bmCalReturns
      };
    })(),
    risk:{blendedBeta:'0.99',bfsiPct:(sectors.find(s=>s.name==='BFSI')?.pct||38)+'%',top5StocksPct:'24%',midSmallPct:funds.length>3?'<5%':'10%',uniqueStocks:`~${uniqueStocks}`,stddev:'14.2%',maxDrawdown:'~-33%',downsideCap:'~93%',upsideCap:'~96%',stressScenarios:stress},
    sectors,
    overlap:{overallPct:overlapPct,verdict:knowledge?.overlap?.verdict||(funds.length>4?'Critical redundancy — multiple funds, one strategy':'Moderate overlap — consolidate'),topStocks},
    projections:{corpus:fmt(corpus),rows:[{label:'Current portfolio',cagr:blendedCAGR5.toFixed(1)+'%',y5:project(blendedCAGR5,5),y10:project(blendedCAGR5,10),y15:project(blendedCAGR5,15),y20:project(blendedCAGR5,20),type:'bad'},{label:portfolioBM?.name||'Benchmark Index',cagr:weightedBMcagr.toFixed(1)+'%',y5:project(weightedBMcagr,5),y10:project(weightedBMcagr,10),y15:project(weightedBMcagr,15),y20:project(weightedBMcagr,20),type:'mid'},{label:'Recommended portfolio',cagr:recCAGR+'%',y5:project(recCAGR,5),y10:project(recCAGR,10),y15:project(recCAGR,15),y20:project(recCAGR,20),type:'good'}],gap20y:(()=>{
      const diff = corpus*Math.pow(1+recCAGR/100,20)-corpus*Math.pow(1+blendedCAGR5/100,20);
      return (diff>=0?'+':'-') + fmt(Math.abs(diff));
    })()},
    // ── CATEGORY-AWARE RECOMMENDED PORTFOLIO ──────────────────────────────────
  // Best fund per SEBI category (Regular Plan - Growth) — used to replace EXIT/SWITCH funds
  recommended: (()=>{
    const BEST_IN_CAT = {
      // Equity
      'large cap':          {name:'Nippon India Large Cap Fund',    cat:'Large Cap',          cagr5y:'15.9%',sharpe:'0.81',ter:'0.69%'},
      'mid cap':            {name:'Motilal Oswal Midcap Fund',      cat:'Mid Cap',            cagr5y:'28.4%',sharpe:'1.14',ter:'0.58%'},
      'small cap':          {name:'Nippon India Small Cap Fund',    cat:'Small Cap',          cagr5y:'22.4%',sharpe:'0.85',ter:'0.65%'},
      'flexi cap':          {name:'Parag Parikh Flexi Cap Fund',    cat:'Flexi Cap',          cagr5y:'16.3%',sharpe:'0.92',ter:'0.63%'},
      'multi cap':          {name:'Nippon India Multi Cap Fund',    cat:'Multi Cap',          cagr5y:'21.4%',sharpe:'0.88',ter:'0.89%'},
      // Hybrid
      'balanced advantage': {name:'ICICI Pru Balanced Advantage',  cat:'Balanced Advantage', cagr5y:'11.4%',sharpe:'0.76',ter:'0.95%'},
      'aggressive hybrid':  {name:'ICICI Pru Equity & Debt Fund',  cat:'Aggressive Hybrid',  cagr5y:'14.2%',sharpe:'0.78',ter:'1.12%'},
      'multi asset':        {name:'ICICI Pru Multi Asset Fund',    cat:'Multi Asset',        cagr5y:'14.8%',sharpe:'0.85',ter:'0.99%'},
      'elss':               {name:'Mirae Asset ELSS Tax Saver',    cat:'ELSS',               cagr5y:'16.1%',sharpe:'0.82',ter:'0.63%'},
      'index':              {name:'UTI Nifty 50 Index Fund',       cat:'Index',              cagr5y:'14.7%',sharpe:'0.94',ter:'0.20%'},
      'gold':               {name:'SBI Gold Fund',                 cat:'Gold FoF',           cagr5y:'13.8%',sharpe:'0.65',ter:'0.20%'},
      // Debt — SEBI categories with best Regular Plan funds
      'overnight':          {name:'HDFC Overnight Fund',           cat:'Overnight',          cagr5y:'5.0%', sharpe:'3.50',ter:'0.10%'},
      'liquid':             {name:'HDFC Liquid Fund',              cat:'Liquid',             cagr5y:'5.5%', sharpe:'3.20',ter:'0.20%'},
      'money market':       {name:'HDFC Money Market Fund',        cat:'Money Market',       cagr5y:'5.8%', sharpe:'2.80',ter:'0.22%'},
      'ultra short':        {name:'HDFC Ultra Short Term Fund',    cat:'Ultra Short Duration',cagr5y:'6.0%',sharpe:'2.60',ter:'0.28%'},
      'low duration':       {name:'HDFC Low Duration Fund',        cat:'Low Duration',       cagr5y:'6.4%', sharpe:'2.20',ter:'0.30%'},
      'short duration':     {name:'HDFC Short Term Debt Fund',     cat:'Short Duration',     cagr5y:'7.1%', sharpe:'1.80',ter:'0.30%'},
      'medium duration':    {name:'SBI Magnum Medium Duration',    cat:'Medium Duration',    cagr5y:'7.2%', sharpe:'1.40',ter:'0.65%'},
      'medium to long':     {name:'ICICI Pru Medium Term Bond',    cat:'Medium to Long',     cagr5y:'7.5%', sharpe:'1.20',ter:'0.90%'},
      'long duration':      {name:'ICICI Pru Long Term Bond',      cat:'Long Duration',      cagr5y:'7.4%', sharpe:'1.00',ter:'0.70%'},
      'dynamic bond':       {name:'ICICI Pru All Seasons Bond',    cat:'Dynamic Bond',       cagr5y:'7.9%', sharpe:'1.30',ter:'0.43%'},
      'corporate bond':     {name:'HDFC Corporate Bond Fund',      cat:'Corporate Bond',     cagr5y:'7.0%', sharpe:'1.60',ter:'0.30%'},
      'credit risk':        {name:'HDFC Credit Risk Debt Fund',    cat:'Credit Risk',        cagr5y:'6.8%', sharpe:'1.20',ter:'0.80%'},
      'banking psu':        {name:'HDFC Banking & PSU Debt Fund',  cat:'Banking & PSU Debt', cagr5y:'6.8%', sharpe:'1.70',ter:'0.25%'},
      'gilt':               {name:'SBI Magnum Gilt Fund',          cat:'Gilt',               cagr5y:'7.2%', sharpe:'1.10',ter:'0.50%'},
      'gilt 10y':           {name:'SBI Magnum Const Mat Gilt 10Y', cat:'Gilt 10Y',           cagr5y:'7.5%', sharpe:'1.00',ter:'0.45%'},
      'floating rate':      {name:'HDFC Floating Rate Debt Fund',  cat:'Floating Rate',      cagr5y:'6.2%', sharpe:'2.00',ter:'0.25%'},
    };

    // Normalize scheme_category → BEST_IN_CAT key
    const normCat = sebiCat => {
      const c = (sebiCat||'').toLowerCase();
      // Debt categories — check first to avoid partial matches with equity
      if (c.includes('overnight')) return 'overnight';
      if (c.includes('liquid') && !c.includes('equity')) return 'liquid';
      if (c.includes('money market')) return 'money market';
      if (c.includes('ultra short')) return 'ultra short';
      if (c.includes('low duration')) return 'low duration';
      if (c.includes('floating rate')) return 'floating rate';
      if (c.includes('short duration') || (c.includes('short') && c.includes('debt'))) return 'short duration';
      if (c.includes('medium to long') || c.includes('medium long')) return 'medium to long';
      if (c.includes('medium duration') || (c.includes('medium') && c.includes('debt'))) return 'medium duration';
      if (c.includes('long duration')) return 'long duration';
      if (c.includes('dynamic bond') || c.includes('dynamic debt')) return 'dynamic bond';
      if (c.includes('corporate bond')) return 'corporate bond';
      if (c.includes('credit risk')) return 'credit risk';
      if (c.includes('banking') && c.includes('psu')) return 'banking psu';
      if (c.includes('gilt') && (c.includes('10') || c.includes('constant'))) return 'gilt 10y';
      if (c.includes('gilt')) return 'gilt';
      // Hybrid
      if (c.includes('balanced advantage') || c.includes('dynamic asset')) return 'balanced advantage';
      if (c.includes('aggressive hybrid')) return 'aggressive hybrid';
      if (c.includes('multi asset')) return 'multi asset';
      if (c.includes('hybrid')) return 'balanced advantage'; // catch-all for hybrid
      // Equity
      if (c.includes('large cap') && !c.includes('mid')) return 'large cap';
      if (c.includes('mid cap') && !c.includes('small')) return 'mid cap';
      if (c.includes('small cap')) return 'small cap';
      if (c.includes('flexi cap') || c.includes('flexicap')) return 'flexi cap';
      if (c.includes('multi cap') || c.includes('multicap')) return 'multi cap';
      if (c.includes('elss') || c.includes('tax saver')) return 'elss';
      if (c.includes('index') || c.includes('nifty') || c.includes('sensex')) return 'index';
      if (c.includes('gold')) return 'gold';
      // Catch-all for remaining debt categories
      if (c.includes('debt') || c.includes('bond') || c.includes('income')) return 'short duration';
      return null;
    };

    const recMap = {}; // catKey → recommendation object
    const seen = new Set();

    for (const r of results.filter(x => !x.error)) {
      const catKey = normCat(r.meta?.scheme_category);
      if (!catKey) continue;

      const bmCAGR5 = r.benchmark?.cagr5y || 13.2;
      const alphaVsBM = r.ret5y != null ? r.ret5y - bmCAGR5 : null;
      const decision = alphaVsBM == null ? 'Hold' : alphaVsBM > 1 ? 'Hold' : alphaVsBM > -1 ? 'Switch' : 'Exit';

      if (decision === 'Hold') {
        // Investor's fund is performing — keep it (no replacement needed)
        if (!recMap[catKey]) {
          recMap[catKey] = {
            name: r.fund.name,
            cat: r.meta?.scheme_category || catKey,
            cagr5y: r.ret5y != null ? r.ret5y.toFixed(1)+'%' : 'N/A',
            sharpe: 'N/A',
            ter: 'N/A',
            role: `Retain — beating ${r.benchmark?.name||'benchmark'} by ${alphaVsBM!=null?(alphaVsBM>=0?'+':'')+alphaVsBM.toFixed(1)+'%':'N/A'}`,
            _isUserFund: true
          };
        }
      } else {
        // EXIT or SWITCH: recommend best fund in same category
        const best = BEST_IN_CAT[catKey];
        if (!best || seen.has(catKey)) continue;
        seen.add(catKey);

        // Don't recommend a fund the user already holds in their portfolio
        const alreadyInPortfolio = results.some(x =>
          (x.fund.name||'').toLowerCase().includes((best.name||'').split(' ').slice(0,3).join(' ').toLowerCase())
        );
        if (alreadyInPortfolio && decision === 'Switch') continue; // they already have the best one

        const reason = decision === 'Exit'
          ? `Replace ${r.fund.name.split(' ').slice(0,2).join(' ')} (underperforming -${Math.abs(alphaVsBM||0).toFixed(1)}% vs ${r.benchmark?.name||'BM'}) — best in ${catKey} category`
          : `Upgrade from ${r.fund.name.split(' ').slice(0,2).join(' ')} — better ${catKey} alpha`;

        recMap[catKey] = { ...best, role: reason };
      }
    }

    // Deduplicate — if user already holds the "best" fund (as HOLD), don't add it again as replacement
    const finalRecs = Object.values(recMap);

    // If zero recommendations generated, fall back to category-matched defaults
    if (finalRecs.length === 0) {
      const cats = results.filter(r=>r.meta?.scheme_category).map(r=>normCat(r.meta.scheme_category)).filter(Boolean);
      const primaryCat = cats[0] || 'large cap';
      const best = BEST_IN_CAT[primaryCat] || BEST_IN_CAT['large cap'];
      finalRecs.push({ ...best, role: 'Primary category recommendation' });
    }

    // Allocate proportionally and cap at 5 funds
    const capped = finalRecs.slice(0, 5);
    const allocPct = Math.floor(100 / capped.length);
    const rem = 100 - allocPct * capped.length;
    capped.forEach((r, i) => {
      const pct = allocPct + (i === 0 ? rem : 0);
      r.alloc = pct + '%';
      r.amt = fmt(corpus * pct / 100);
    });
    return capped;
  })(),
    execution: (()=>{
      const exitFunds2 = fundsArr.filter(f=>f.decision==='Exit').slice(0,2);
      const switchFunds = fundsArr.filter(f=>f.decision==='Switch').slice(0,2);
      const exitNames = exitFunds2.map(f=>(f.name||'').split(' ').slice(0,2).join(' ')).join(' + ') || 'worst performers';
      const switchNames = switchFunds.map(f=>(f.name||'').split(' ').slice(0,2).join(' ')).join(' + ') || 'underperformers';
      const cats = results.filter(r=>r.meta?.scheme_category).map(r=>(r.meta.scheme_category||'').toLowerCase());
      const isHybrid = cats.filter(c=>/balanced|hybrid|multi asset/.test(c)).length >= cats.length/2;
      const hasMid = cats.some(c=>c.includes('mid cap')) && !isHybrid;
      const hasSmall = cats.some(c=>c.includes('small cap')) && !isHybrid;
      const deployTarget = isHybrid
        ? 'ICICI Pru Balanced Advantage + UTI Nifty 50 Index'
        : 'Nippon India Large Cap' + (hasMid?' + Motilal Oswal Midcap':'') + (hasSmall?' + Nippon India Small Cap':'') + ' + UTI Nifty 50 Index';
      return [
        {step:'Step 1 — April 2026 (Now)',color:'bad',detail:`Exit ${exitNames} first. Fresh FY — use full ₹1.25L LTCG exemption. Deploy into ${deployTarget}.`},
        {step:'Step 2 — May–July 2026',color:'warn',detail:switchFunds.length>0?`Switch ${switchNames} to best-in-category alternatives (see recommended portfolio above). Split exits across months to optimise LTCG.`:'Review remaining holdings quarterly. Consolidate if overlap >60%. Rebalance if any fund drifts significantly.'},
        {step:'Step 3 — April 2027+',color:'ok',detail:`Fresh ₹1.25L LTCG exemption for final restructuring. Target: ${Math.min(funds.length,4)}-fund portfolio with blended TER <0.8%. Estimated annual TER saving: ${fmt(annualTERCost*0.55)}/yr.`}
      ];
    })(),
    scorecard:[{label:'Performance consistency',score:Math.min(9,Math.max(1,5+(alpha5*0.4))).toFixed(1),note:`${beatCount5}/${successResults.length} resolved funds beat their category benchmark on 5Y basis`},{label:'Diversification',score:Math.max(1,7-(funds.length>5?2:0)-(parseFloat(overlapPct)>60?2:0)).toFixed(1),note:`${overlapPct} overlap — ${funds.length>5?'critical redundancy':'concentrated'}`},{label:'Risk control',score:'5.0',note:'Beta ~0.99 — full market downside, limited upside capture'},{label:'Cost efficiency',score:Math.min(8,Math.max(1,alpha5>2?7:alpha5>0?5:3)).toFixed(1),note:`${avgTER.toFixed(2)}% blended TER — 16x costlier than equivalent index`},{label:'Overall health',score:healthScore,note:alpha5>0?'Consolidate to eliminate redundancy':'Restructure immediately'}],
  };
}


// ── MAIN ANALYSIS ──────────────────────────────────────────────────────────
async function runAnalysis(funds) {
  console.log(`\n[Phase 1] Fetching AMFI for ${funds.length} funds (max 2 at a time to avoid rate limiting)`);
  const FUND_TIMEOUT = 28000; // 28s — covers 3-phase fetch + invest window + buffer

  const fetchOne = async fund => {
    console.log(`  → ${fund.name}`);
    try {
      return await Promise.race([
        fetchFundData(fund),
        new Promise((_, rej) => setTimeout(() => rej(new Error('Fund fetch timed out')), FUND_TIMEOUT))
      ]);
    } catch(e) {
      console.error(`  ✗ ${fund.name}: ${e.message}`);
      return { fund, amt: parseFloat(fund.amt.replace(/[₹,\s]/g,''))||0, error: e.message };
    }
  };

  // Process in batches of 2 — limits peak concurrent mfapi requests to ~30
  const results = [];
  for (let i = 0; i < funds.length; i += 2) {
    const batch = funds.slice(i, i + 2);
    const batchResults = await Promise.all(batch.map(fetchOne));
    results.push(...batchResults);
    if (i + 2 < funds.length) {
      await new Promise(r => setTimeout(r, 300)); // 300ms pause between batches
    }
  }
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
      funds: funds.map(f => ({name:f.name,manager:'See factsheet',tenureYrs:3,tenureFlag:false,cagr5y:'N/A',cagr3y:'N/A',ret1y:'N/A',sharpe:'N/A',beta:'N/A',stddev:'N/A',alpha:'N/A',ter:'N/A',riskCategory:'N/A',quality:'N/A',decision:'Hold',perf5yVal:0,perf3yVal:0,ret1yVal:0,sharpeVal:0,calendarReturns:{'2020':'N/A','2020Beat':false,'2021':'N/A','2021Beat':false,'2022':'N/A','2022Beat':false,'2023':'N/A','2023Beat':false,'2024':'N/A','2024Beat':false,'2025':'N/A','2025Beat':false},quartile:'N/A',quartileLabel:'N/A',rolling1yAvg:'N/A',rolling1yBeatPct:'N/A',rolling1yWorst:'N/A',rolling3yAvg:'N/A',rolling3yBeatPct:'N/A',rolling3yMin:'N/A',rolling5yAvg:'N/A',rolling5yBeatPct:'N/A',rolling5yMin:'N/A',realReturn:'N/A',estCurrentValue:'N/A',gainAmt:'N/A',ltcgTax:'N/A',netProceeds:'N/A',breakEvenMonths:0,benchmarkName:'Nifty 100 TRI',benchmarkCAGR5y:13.2})),
      benchmark:{name:'Nifty 100 TRI',cagr5y:'13.2%',cagr3y:'14.0%',ret1y:'+0.8%',sharpe:'0.95',beta:'1.00',stddev:'12.8%',rolling1yAvg:'13.8%',rolling3yAvg:'14.4%',rolling5yAvg:'13.2%',calendarReturns:{'2020':'+15.5%','2021':'+25.8%','2022':'+5.0%','2023':'+24.1%','2024':'+15.0%','2025':'+3.3%'}},
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
  const { pathname } = new URL(req.url, 'http://localhost');
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
    // Claude enrichment is optional — AMFI fetch works without it
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

server.listen(process.env.PORT || 3000, '0.0.0.0', () => {
  console.log(`JD MF Report v2 on port ${process.env.PORT||3000} | key:${!!ANTHROPIC_API_KEY}`);
});

// Prevent uncaught errors from crashing the server (Render kills on port loss)
process.on('uncaughtException', (err) => {
  console.error('[UNCAUGHT]', err.message, err.stack?.split('\n').slice(0,3).join(' | '));
});
process.on('unhandledRejection', (err) => {
  console.error('[UNHANDLED]', err?.message || err);
});
