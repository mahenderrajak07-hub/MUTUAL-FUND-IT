const http = require('http');
const https = require('https');
const fs = require('fs');
const path = require('path');
const url = require('url');

const PORT = process.env.PORT || 3000;
const ANTHROPIC_API_KEY = process.env.ANTHROPIC_API_KEY;

const rateLimitMap = new Map();
const RATE_LIMIT_WINDOW = 60 * 60 * 1000;
const RATE_LIMIT_MAX = 20;

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

// Make an HTTPS request and return response body as string
function httpsPost(options, postData) {
  return new Promise((resolve, reject) => {
    const req = https.request(options, (res) => {
      let data = '';
      res.on('data', chunk => data += chunk);
      res.on('end', () => resolve({ status: res.statusCode, body: data }));
    });
    req.on('error', reject);
    req.setTimeout(120000, () => { req.destroy(); reject(new Error('Request timed out')); });
    req.write(postData);
    req.end();
  });
}

// Sleep helper
function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }

// Call Anthropic API with automatic retry on overload
async function callAnthropic(messages, tools, retries = 3) {
  const payload = { model: 'claude-sonnet-4-5', max_tokens: 8000, messages };
  if (tools) payload.tools = tools;

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
    try {
      const result = await httpsPost(opts, postData);
      const parsed = JSON.parse(result.body);

      // Retry on overload (529) or rate limit (429) or server error (500)
      if (result.status === 529 || result.status === 500 ||
          (result.status === 429 && parsed.error?.type === 'overloaded_error')) {
        if (attempt < retries) {
          const wait = attempt * 15000; // 15s, 30s, 45s
          console.log(`[Retry ${attempt}/${retries}] Overloaded, waiting ${wait/1000}s...`);
          await sleep(wait);
          continue;
        }
      }

      if (result.status !== 200) {
        const msg = parsed.error?.message || `Anthropic error ${result.status}`;
        throw new Error(msg);
      }
      return parsed;
    } catch (err) {
      if (attempt < retries && (err.message.includes('overloaded') || err.message.includes('timeout'))) {
        const wait = attempt * 15000;
        console.log(`[Retry ${attempt}/${retries}] Error: ${err.message}, waiting ${wait/1000}s...`);
        await sleep(wait);
        continue;
      }
      throw err;
    }
  }
  throw new Error('Anthropic API is currently overloaded. Please try again in a few minutes.');
}

// Perform a web search via Brave Search API (or fallback to DuckDuckGo scrape)
async function webSearch(query) {
  // Use Claude's built-in web search tool by calling a mini Claude call
  // This is a lightweight search that returns snippets
  // Use callAnthropic so we get retry logic on overload
  const parsed = await callAnthropic(
    [{
      role: 'user',
      content: `Search for "${query}" on Value Research, Moneycontrol, Tickertape or Groww.

Extract and list ONLY these specific numbers:
- 1Y return (trailing): X%
- 3Y return (CAGR): X%  
- 5Y return (CAGR): X%
- Sharpe ratio: X
- Beta: X
- Standard deviation: X%
- Alpha: X%
- Expense ratio / TER: X%
- AUM: ₹X Crore
- Fund manager name: X
- Manager tenure (years on this fund): X years
- Latest NAV: ₹X
- Peer quartile rank: Q1/Q2/Q3/Q4

If a value is not found, write "not found". Do not write N/A for everything — actually search and find the numbers.`
    }],
    [{ type: 'web_search_20250305', name: 'web_search' }]
  );

  const textParts = (parsed.content || [])
    .filter(b => b.type === 'text')
    .map(b => b.text)
    .join('\n');

  return textParts || 'No data found';
}

// Two-phase analysis: 1) fetch live data, 2) generate structured JSON
async function runAnalysis(funds) {
  const fundList = funds.map(f => `${f.name} | ${f.amt} | ${f.date}`).join('\n');

  // Phase 1: Fetch live data for all funds via web search
  console.log(`[Phase 1] Fetching live data for ${funds.length} funds`);
  const searchResults = [];

  for (const fund of funds) {
    const query = `${fund.name} mutual fund NAV returns 2026 Sharpe ratio TER AUM expense ratio Value Research Moneycontrol`;
    try {
      const data = await webSearch(query);
      searchResults.push(`=== ${fund.name} ===\n${data}`);
      console.log(`  Fetched data for: ${fund.name}`);
    } catch (e) {
      console.warn(`  Failed to fetch ${fund.name}: ${e.message}`);
      searchResults.push(`=== ${fund.name} ===\nData unavailable - use best known data`);
    }
  }

  const liveData = searchResults.join('\n\n');
  console.log(`[Phase 1] Done. Live data length: ${liveData.length} chars`);

  // Phase 2: Generate structured JSON using live data
  console.log(`[Phase 2] Generating analysis JSON`);

  const total = funds.reduce((s, f) => {
    const n = parseFloat(f.amt.replace(/[₹,\s]/g, ''));
    return s + (isNaN(n) ? 0 : n);
  }, 0);
  const fmt = v => new Intl.NumberFormat('en-IN', { style: 'currency', currency: 'INR', maximumFractionDigits: 0 }).format(v);
  const fmtTotal = fmt(total);
  const estCorpus = fmt(total * 1.72);

  const ft = funds.map(f =>
    `{"name":"${f.name}","manager":"X","tenureYrs":0,"tenureFlag":false,"cagr5y":"X%","cagr3y":"X%","ret1y":"X%","sharpe":"X","beta":"X","stddev":"X%","alpha":"X%","ter":"X%","aum":"X","quality":"Average","decision":"Hold","perf5yVal":0,"perf3yVal":0,"ret1yVal":0,"sharpeVal":0,"calendarReturns":{"2020":"X%","2020Beat":true,"2021":"X%","2021Beat":true,"2022":"X%","2022Beat":false,"2023":"X%","2023Beat":true,"2024":"X%","2024Beat":true,"2025":"X%","2025Beat":false},"quartile":"Q2","quartileLabel":"Top 40%","rolling1yAvg":"X%","rolling1yBeatPct":"X%","rolling1yWorst":"X%","rolling3yAvg":"X%","rolling3yBeatPct":"X%","rolling3yMin":"X%","realReturn":"X%","estCurrentValue":"X","gainAmt":"X","ltcgTax":"X","netProceeds":"X","breakEvenMonths":0}`
  ).join(',');

  const prompt = `You are a CFA-level Indian mutual fund analyst. I have fetched LIVE data from the web for each fund. Use ONLY the data provided below — do not hallucinate or use training data. If a value is not in the live data, mark it as "N/A".

LIVE DATA FETCHED FROM WEB (April 2026):
${liveData}

PORTFOLIO (${funds.length} funds, ${fmtTotal} invested, Regular plans):
${fundList}

Using ONLY the live data above, return a single valid JSON object. No markdown, no text outside JSON. Keep all strings under 60 chars.

{"summary":{"totalInvested":"${fmtTotal}","currentValue":"X","blendedCAGR":"X%","alphaBM":"X%","realReturn":"X%","annualTER":"X","fundsBeatBM":"X/${funds.length}","uniqueStocks":"~X","healthScore":"X/10","healthVerdict":"X","overlapPct":"X%","keyFlags":["X","X","X","X"]},"funds":[${ft}],"benchmark":{"cagr5y":"13.2%","cagr3y":"14.0%","ret1y":"+0.8%","sharpe":"0.95","beta":"1.00","stddev":"12.8%","rolling1yAvg":"13.8%","rolling3yAvg":"14.4%","calendarReturns":{"2020":"+15.2%","2021":"+24.1%","2022":"+4.8%","2023":"+22.3%","2024":"+12.8%","2025":"+6.5%"}},"risk":{"blendedBeta":"X","bfsiPct":"X%","top5StocksPct":"X%","midSmallPct":"X%","uniqueStocks":"~X","stddev":"X%","maxDrawdown":"X%","downsideCap":"X%","upsideCap":"X%","stressScenarios":[{"label":"Bull +15%","impact":"X","pct":"X%"},{"label":"Flat 3Y","impact":"X","pct":"X%"},{"label":"Correction -20%","impact":"X","pct":"X%"},{"label":"Crash -30%","impact":"X","pct":"X%"}]},"sectors":[{"name":"BFSI","pct":35,"flag":true},{"name":"IT","pct":14,"flag":false},{"name":"Energy","pct":11,"flag":false},{"name":"Industrials","pct":10,"flag":false},{"name":"Consumer","pct":9,"flag":false},{"name":"Others","pct":21,"flag":false}],"overlap":{"overallPct":"X%","verdict":"X","topStocks":[{"stock":"HDFC Bank","funds":"X","avgWt":"X%","risk":"Very High"},{"stock":"ICICI Bank","funds":"X","avgWt":"X%","risk":"Very High"},{"stock":"Reliance","funds":"X","avgWt":"X%","risk":"High"},{"stock":"Infosys","funds":"X","avgWt":"X%","risk":"Moderate"},{"stock":"L&T","funds":"X","avgWt":"X%","risk":"Moderate"}]},"projections":{"corpus":"${estCorpus}","rows":[{"label":"Current portfolio","cagr":"X%","y5":"X","y10":"X","y15":"X","y20":"X","type":"bad"},{"label":"Nifty 100 Index","cagr":"13.2%","y5":"X","y10":"X","y15":"X","y20":"X","type":"mid"},{"label":"Recommended portfolio","cagr":"X%","y5":"X","y10":"X","y15":"X","y20":"X","type":"good"}],"gap20y":"X"},"recommended":[{"name":"Nippon India Large Cap","cat":"Large Cap","alloc":"25%","amt":"X","cagr5y":"15.98%","sharpe":"0.89","ter":"0.65%","role":"Core anchor"},{"name":"HDFC Mid-Cap Opp.","cat":"Mid Cap","alloc":"30%","amt":"X","cagr5y":"18.7%","sharpe":"0.82","ter":"0.75%","role":"Growth kicker"},{"name":"PPFAS Flexicap","cat":"Flexi Cap","alloc":"25%","amt":"X","cagr5y":"17.3%","sharpe":"0.88","ter":"0.59%","role":"Intl diversifier"},{"name":"Motilal Nifty 50 Index","cat":"Index","alloc":"20%","amt":"X","cagr5y":"13.5%","sharpe":"0.94","ter":"0.11%","role":"Passive core"}],"execution":[{"step":"Step 1 — Now","color":"bad","detail":"Exit worst performer. Deploy into better funds. Use ₹1.25L LTCG exemption this FY."},{"step":"Step 2 — April 2027","color":"warn","detail":"Exit second underperformer with fresh ₹1.25L exemption. Deploy into mid-cap and flexi."},{"step":"Step 3 — Oct 2027+","color":"ok","detail":"Annual rebalance. Exit any Q3/Q4 fund for 2 years running. Monitor managers."}],"scorecard":[{"label":"Performance consistency","score":3.5,"note":"Check rolling window beat rates above"},{"label":"Diversification","score":2.0,"note":"Overlap and category concentration"},{"label":"Risk control","score":5.0,"note":"Downside vs upside capture ratio"},{"label":"Cost efficiency","score":2.5,"note":"TER vs benchmark alpha delivered"},{"label":"Overall health","score":3.8,"note":"Based on live data analysis"}]}

CRITICAL RULES:
1. Use the exact numbers from the LIVE DATA above
2. If a specific metric was not found in live data, use your training knowledge for that fund as a fallback — do NOT write N/A
3. All numeric fields (perf5yVal, perf3yVal, ret1yVal, sharpeVal) MUST be actual numbers, never 0 unless the real value is 0
4. Return ONLY the JSON object, nothing else`;

  const response = await callAnthropic([{ role: 'user', content: prompt }]);

  const text = (response.content || [])
    .filter(b => b.type === 'text')
    .map(b => b.text)
    .join('');

  console.log(`[Phase 2] Done. Response length: ${text.length} chars`);
  return text;
}

const server = http.createServer((req, res) => {
  const parsedUrl = url.parse(req.url, true);
  const pathname = parsedUrl.pathname;

  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type');

  if (req.method === 'OPTIONS') { res.writeHead(204); res.end(); return; }

  if (pathname === '/health') {
    sendJSON(res, 200, { ok: true, key: !!ANTHROPIC_API_KEY, mode: 'live-search' });
    return;
  }

  if (pathname === '/api/analyse' && req.method === 'POST') {
    const ip = getClientIP(req);
    const rl = getRateLimit(ip);

    if (rl.count > rl.limit) {
      sendJSON(res, 429, { error: `Rate limit: ${rl.limit} analyses/hour. Try again later.` });
      return;
    }
    if (!ANTHROPIC_API_KEY) {
      sendJSON(res, 500, { error: 'API key not configured. Add ANTHROPIC_API_KEY in Render environment.' });
      return;
    }

    let body = '';
    req.on('data', chunk => { body += chunk; });
    req.on('end', async () => {
      let payload;
      try { payload = JSON.parse(body); } catch {
        sendJSON(res, 400, { error: 'Invalid request' }); return;
      }

      if (!payload.funds || !Array.isArray(payload.funds)) {
        sendJSON(res, 400, { error: 'Missing funds array in request' }); return;
      }

      try {
        console.log(`[${new Date().toISOString()}] Analysis request: ${payload.funds.length} funds from ${ip}`);
        const result = await runAnalysis(payload.funds);
        // Return in same format as before so frontend works unchanged
        sendJSON(res, 200, {
          content: [{ type: 'text', text: result }]
        });
      } catch (err) {
        console.error('Analysis error:', err.message);
        const isOverload = err.message && (
          err.message.toLowerCase().includes('overloaded') ||
          err.message.toLowerCase().includes('529')
        );
        const userMsg = isOverload
          ? 'Anthropic servers are busy right now. We tried 3 times. Please wait 2-3 minutes and try again.'
          : (err.message || 'Analysis failed. Please retry.');
        sendJSON(res, isOverload ? 503 : 500, { error: userMsg });
      }
    });
    return;
  }

  // Serve static files
  let filePath = path.join(__dirname, pathname === '/' ? 'index.html' : pathname.replace(/^\//, ''));
  if (!filePath.startsWith(__dirname)) { res.writeHead(403); res.end('Forbidden'); return; }

  const ext = path.extname(filePath);
  const mime = MIME[ext] || 'text/html; charset=utf-8';

  fs.readFile(filePath, (err, data) => {
    if (err) {
      fs.readFile(path.join(__dirname, 'index.html'), (err2, data2) => {
        if (err2) { res.writeHead(404); res.end('Not found'); }
        else { res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8' }); res.end(data2); }
      });
    } else {
      res.writeHead(200, { 'Content-Type': mime }); res.end(data);
    }
  });
});

server.listen(PORT, () => {
  console.log(`FundAudit (live search mode) running on port ${PORT}`);
  console.log(`API key: ${ANTHROPIC_API_KEY ? 'configured' : 'MISSING'}`);
});
