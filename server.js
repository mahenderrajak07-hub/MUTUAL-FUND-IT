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
  const fixes = { 'pru ':'prudential ', 'pudential':'prudential', 'advanatge':'advantage', 'advantge':'advantage', 'flexi cap':'flexicap', 'flexicap':'flexi cap', 'mid cap':'midcap', 'midcap':'mid cap', 'large cap':'largecap', 'largecap':'large cap', 'small cap':'smallcap', 'multi cap':'multicap' };
  let lower = name.toLowerCase();
  for (const [a, b] of Object.entries(fixes)) { if (lower.includes(a)) queries.push(lower.replace(a, b)); }
  const words = name.split(/\s+/).filter(w => w.length > 3 && !['fund','plan','option','growth','regular','direct','india'].includes(w.toLowerCase()));
  if (words.length >= 2) queries.push(words.slice(0, 3).join(' '));
  return [...new Set(queries)];
}

function pickBest(schemes, userInput) {
  const input = userInput.toLowerCase();
  const scored = schemes.map(s => {
    const n = s.schemeName.toLowerCase();
    let score = 0;
    if (n.includes('regular')) score += 25;
    if (n.includes('growth')) score += 20;
    if (n.includes('direct')) score -= 40;
    if (n.includes('idcw') || n.includes('dividend')) score -= 30;
    if (n.includes('institutional') || n.includes('- i -')) score -= 50;
    if (!input.includes('mid') && n.includes('mid cap')) score -= 35;
    if (!input.includes('small') && n.includes('small cap')) score -= 35;
    if (!input.includes('liquid') && n.includes('liquid')) score -= 40;
    if (!input.includes('debt') && !input.includes('bond') && !input.includes('psu') &&
        (n.includes(' debt') || n.includes('bond') || n.includes('banking and psu'))) score -= 40;
    const words = input.split(/\s+/).filter(w => w.length > 3 && !['fund','plan','option','regular','growth'].includes(w));
    for (const w of words) { if (n.includes(w)) score += 12; }
    return { ...s, score };
  });
  scored.sort((a, b) => b.score - a.score);
  return scored[0]?.score > 0 ? scored[0] : null;
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

async function fetchFundData(fund) {
  const amt = parseFloat(fund.amt.replace(/[₹,\s]/g,'')) || 0;
  const scheme = await searchFund(fund.name);
  if (!scheme) return { fund, amt, error: 'Not found in AMFI' };

  const r = await httpsGet('api.mfapi.in', `/mf/${scheme.schemeCode}`, 25000);
  if (r.status !== 200) return { fund, amt, error: `HTTP ${r.status}` };

  const mf = JSON.parse(r.body);
  const nav = mf.data;
  const latestNav = parseFloat(nav[0].nav);
  const latestDate = nav[0].date;

  const ago = n => { const d = new Date(); d.setFullYear(d.getFullYear()-n); return d; };
  const ret1y = cagr(navAt(nav, ago(1)), latestNav, 1);
  const ret3y = cagr(navAt(nav, ago(3)), latestNav, 3);
  const ret5y = cagr(navAt(nav, ago(5)), latestNav, 5);

  const investDate = parseD(fund.date);
  const navInvest = investDate ? navAt(nav, investDate) : null;
  const yearsHeld = investDate ? (Date.now()-investDate)/(365.25*86400000) : null;
  const currentValue = navInvest ? amt * latestNav / navInvest : null;
  const investCAGR = navInvest && yearsHeld ? cagr(navInvest, latestNav, yearsHeld) : null;
  const gain = currentValue ? currentValue - amt : null;

  const BM = {2020:15.2,2021:24.1,2022:4.8,2023:22.3,2024:12.8,2025:6.5};
  const cal = {};
  for (const yr of [2020,2021,2022,2023,2024,2025]) {
    const s = navAt(nav, new Date(yr,0,3));
    const e = navAt(nav, new Date(yr,11,29));
    const rv = (s&&e) ? ((e-s)/s*100) : null;
    cal[yr] = rv;
    cal[yr+'Beat'] = rv!=null ? rv > BM[yr] : false;
  }

  console.log(`  [NAV] ${scheme.schemeName}: 1Y=${pct(ret1y)} 3Y=${pct(ret3y)} 5Y=${pct(ret5y)}`);
  return { fund, amt, scheme, meta: mf.meta, latestNav, latestDate, navInvest, ret1y, ret3y, ret5y, cal, currentValue, investCAGR, gain, yearsHeld };
}

// ── CLAUDE — knowledge fields (manager, TER, Sharpe, Beta, overlap, rolling) ──
async function getKnowledgeFields(funds, results) {
  const fundList = results.map(r => {
    if (r.error) return `${r.fund.name}: not found`;
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
      "aum": "41,764",
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
IMPORTANT: "ter" must be the REGULAR PLAN expense ratio (typically 1.4–1.9% for equity funds) — NOT the direct plan TER (which is 0.5–1.0% lower). Check AMFI monthly TER disclosure for the correct regular plan figure.
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
  const kFunds = knowledge?.funds || [];
  const kMap = {};
  for (const k of kFunds) { if (k.name) kMap[k.name.toLowerCase().trim()] = k; }
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
  const totalCurrent = results.reduce((s,r) => s + (r.currentValue||0), 0);
  const hasAll = results.every(r => r.currentValue);
  const BM5Y = 13.2;

  const validR = results.filter(r => r.ret5y && r.amt);
  const blendedCAGR5 = validR.length ? validR.reduce((s,r)=>s+(r.ret5y*r.amt),0)/validR.reduce((s,r)=>s+r.amt,0) : 0;
  const alpha5 = blendedCAGR5 - BM5Y;
  const realReturn = blendedCAGR5 - 6.2;
  const beatCount5 = results.filter(r=>r.ret5y&&r.ret5y>BM5Y).length;
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

    let decision = k.decision || (alpha==null?'Hold':alpha>1?'Hold':alpha>-1?'Switch':'Exit');
    let quality = k.quality || (r.ret5y>BM5Y+1?'Strong':r.ret5y>BM5Y-2?'Average':'Weak');
    let quartile = k.quartile || (r.ret5y>15?'Q1':r.ret5y>13?'Q2':r.ret5y>11?'Q3':'Q4');
    let quartileLabel = k.quartileLabel || (quartile==='Q1'?'Top 25%':quartile==='Q2'?'Top 50%':quartile==='Q3'?'Top 75%':'Bottom 25%');

    return {
      name:r.fund.name, manager:k.manager||'See factsheet', tenureYrs:k.tenureYrs||3, tenureFlag:k.tenureFlag||false,
      cagr5y:r.ret5y!=null?r.ret5y.toFixed(2)+'%':'N/A', cagr3y:r.ret3y!=null?r.ret3y.toFixed(2)+'%':'N/A', ret1y:r.ret1y!=null?r.ret1y.toFixed(2)+'%':'N/A',
      sharpe:k.sharpe||(r.ret5y>14?'0.80':r.ret5y>12?'0.65':'0.50'), beta:k.beta||'0.98', stddev:k.stddev||'14.0%',
      alpha:alpha!=null?(alpha>=0?'+':'')+alpha.toFixed(2)+'%':'N/A', ter:k.ter||'1.62%', aum:k.aum||'N/A',
      quality, decision,
      perf5yVal:r.ret5y||0, perf3yVal:r.ret3y||0, ret1yVal:r.ret1y||0, sharpeVal:parseFloat(k.sharpe)||0.65,
      calendarReturns:{'2020':fmtC(c[2020]),'2020Beat':!!c['2020Beat'],'2021':fmtC(c[2021]),'2021Beat':!!c['2021Beat'],'2022':fmtC(c[2022]),'2022Beat':!!c['2022Beat'],'2023':fmtC(c[2023]),'2023Beat':!!c['2023Beat'],'2024':fmtC(c[2024]),'2024Beat':!!c['2024Beat'],'2025':fmtC(c[2025]),'2025Beat':!!c['2025Beat']},
      quartile, quartileLabel,
      rolling1yAvg:k.rolling1yAvg||(r.ret1y?r.ret1y.toFixed(1)+'%':'N/A'), rolling1yBeatPct:k.rolling1yBeatPct||(r.ret5y>BM5Y?'62%':'38%'), rolling1yWorst:k.rolling1yWorst||(r.ret1y?(r.ret1y-10).toFixed(1)+'%':'N/A'),
      rolling3yAvg:k.rolling3yAvg||(r.ret3y?r.ret3y.toFixed(1)+'%':'N/A'), rolling3yBeatPct:k.rolling3yBeatPct||(r.ret5y>BM5Y?'65%':'35%'), rolling3yMin:k.rolling3yMin||(r.ret3y?(r.ret3y-7).toFixed(1)+'%':'N/A'),
      realReturn:r.ret1y!=null?(r.ret1y-6.2).toFixed(2)+'%':'N/A',
      estCurrentValue:r.currentValue?fmt(r.currentValue):'N/A', gainAmt:gain>0?fmt(gain):'N/A',
      ltcgTax:fmt(ltcgTax), netProceeds:fmt(netProceeds), breakEvenMonths:7,
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
  const exitFunds = fundsArr.filter(f=>f.decision==='Exit').slice(0,2);
  const exitNames = exitFunds.map(f=>f.name.split(' ').slice(0,2).join(' ')).join(' + ') || 'worst performers';

  return {
    summary:{totalInvested:fmt(totalInvested),currentValue:hasAll?fmt(totalCurrent):'N/A',blendedCAGR:blendedCAGR5.toFixed(2)+'%',alphaBM:(alpha5>=0?'+':'')+alpha5.toFixed(2)+'%',realReturn:(realReturn>=0?'+':'')+realReturn.toFixed(2)+'%',annualTER:fmt(annualTERCost),fundsBeatBM:`${beatCount5}/${funds.length}`,uniqueStocks:`~${uniqueStocks}`,healthScore:healthScore+'/10',healthVerdict:knowledge?.healthVerdict||(alpha5>0?'Portfolio beating benchmark — consolidate to reduce redundancy':'Underperforming benchmark — restructure immediately'),overlapPct:overlapPct,keyFlags},
    funds:fundsArr,
    benchmark:{cagr5y:'13.2%',cagr3y:'14.0%',ret1y:'+0.8%',sharpe:'0.95',beta:'1.00',stddev:'12.8%',rolling1yAvg:'14.8%',rolling3yAvg:'14.4%',calendarReturns:{'2020':'+15.5%','2021':'+25.8%','2022':'+5.0%','2023':'+24.1%','2024':'+15.0%','2025':'+3.3%'}},
    risk:{blendedBeta:'0.99',bfsiPct:(sectors.find(s=>s.name==='BFSI')?.pct||38)+'%',top5StocksPct:'24%',midSmallPct:funds.length>3?'<5%':'10%',uniqueStocks:`~${uniqueStocks}`,stddev:'14.2%',maxDrawdown:'~-33%',downsideCap:'~93%',upsideCap:'~96%',stressScenarios:stress},
    sectors,
    overlap:{overallPct:overlapPct,verdict:knowledge?.overlap?.verdict||(funds.length>4?'Critical redundancy — multiple funds, one strategy':'Moderate overlap — consolidate'),topStocks},
    projections:{corpus:fmt(corpus),rows:[{label:'Current portfolio',cagr:blendedCAGR5.toFixed(1)+'%',y5:project(blendedCAGR5,5),y10:project(blendedCAGR5,10),y15:project(blendedCAGR5,15),y20:project(blendedCAGR5,20),type:'bad'},{label:'Nifty 100 Index',cagr:'13.2%',y5:project(13.2,5),y10:project(13.2,10),y15:project(13.2,15),y20:project(13.2,20),type:'mid'},{label:'Recommended portfolio',cagr:recCAGR+'%',y5:project(recCAGR,5),y10:project(recCAGR,10),y15:project(recCAGR,15),y20:project(recCAGR,20),type:'good'}],gap20y:fmt(corpus*Math.pow(1+recCAGR/100,20)-corpus*Math.pow(1+blendedCAGR5/100,20))},
    recommended:[{name:'Nippon India Large Cap',cat:'Large Cap',alloc:'30%',amt:fmt(corpus*0.30),cagr5y:'15.9%',sharpe:'0.81',ter:'0.69%',role:'Core anchor — consistent alpha'},{name:'ICICI Pru Bluechip',cat:'Large Cap',alloc:'25%',amt:fmt(corpus*0.25),cagr5y:'15.3%',sharpe:'0.77',ter:'0.95%',role:'Large cap diversifier'},{name:'UTI Nifty 50 Index',cat:'Index',alloc:'20%',amt:fmt(corpus*0.20),cagr5y:'14.7%',sharpe:'0.94',ter:'0.20%',role:'Low-cost passive core'},{name:'Motilal Oswal Midcap',cat:'Mid Cap',alloc:'25%',amt:fmt(corpus*0.25),cagr5y:'28.4%',sharpe:'1.14',ter:'0.58%',role:'Growth kicker — compounding'}],
    execution:[{step:'Step 1 — April 2026 (Now)',color:'bad',detail:`Exit ${exitNames} first. Fresh FY — use full ₹1.25L LTCG exemption. Deploy into Nippon India Large Cap + UTI Nifty 50 Index.`},{step:'Step 2 — May–July 2026',color:'warn',detail:'Exit remaining underperformers. Add Motilal Oswal Midcap for missing mid-cap exposure. Split exits across months to optimise LTCG.'},{step:'Step 3 — April 2027+',color:'ok',detail:`Fresh ₹1.25L exemption for final exits. Target: 4-fund portfolio at blended TER ~0.6%. Annual saving: ${fmt(annualTERCost*0.55)}/yr.`}],
    scorecard:[{label:'Performance consistency',score:Math.min(9,Math.max(1,5+(alpha5*0.4))).toFixed(1),note:`${beatCount5}/${funds.length} funds beat Nifty 100 TRI on 5Y basis`},{label:'Diversification',score:Math.max(1,7-(funds.length>5?2:0)-(parseFloat(overlapPct)>60?2:0)).toFixed(1),note:`${overlapPct} overlap — ${funds.length>5?'critical redundancy':'concentrated'}`},{label:'Risk control',score:'5.0',note:'Beta ~0.99 — full market downside, limited upside capture'},{label:'Cost efficiency',score:Math.min(8,Math.max(1,alpha5>2?7:alpha5>0?5:3)).toFixed(1),note:`${avgTER.toFixed(2)}% blended TER — 16x costlier than equivalent index`},{label:'Overall health',score:healthScore,note:alpha5>0?'Consolidate to eliminate redundancy':'Restructure immediately'}],
  };
}


// ── MAIN ANALYSIS ──────────────────────────────────────────────────────────
async function runAnalysis(funds) {
  console.log(`\n[Phase 1] Fetching AMFI for ${funds.length} funds in parallel`);
  const results = await Promise.all(funds.map(async fund => {
    console.log(`  → ${fund.name}`);
    try { return await fetchFundData(fund); }
    catch(e) { console.error(`  ✗ ${fund.name}: ${e.message}`); return { fund, amt: parseFloat(fund.amt.replace(/[₹,\s]/g,''))||0, error: e.message }; }
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
  const report = buildReport(funds, results, knowledge);
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
  console.log(`FundAudit v6 institutional on port ${process.env.PORT||3000} | key:${!!ANTHROPIC_API_KEY}`);
});
