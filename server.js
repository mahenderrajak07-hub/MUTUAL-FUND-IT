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
  res.writeHead(status, { 'Content-Type': 'application/json' });
  res.end(JSON.stringify(obj));
}

const server = http.createServer((req, res) => {
  const parsedUrl = url.parse(req.url, true);
  const pathname = parsedUrl.pathname;

  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type');

  if (req.method === 'OPTIONS') { res.writeHead(204); res.end(); return; }

  // Health check
  if (pathname === '/health') {
    sendJSON(res, 200, { ok: true, key: !!ANTHROPIC_API_KEY });
    return;
  }

  // API proxy
  if (pathname === '/api/analyse' && req.method === 'POST') {
    const ip = getClientIP(req);
    const rl = getRateLimit(ip);

    if (rl.count > rl.limit) {
      sendJSON(res, 429, { error: `Rate limit: ${rl.limit} analyses/hour. Try again later.` });
      return;
    }

    if (!ANTHROPIC_API_KEY) {
      sendJSON(res, 500, { error: 'API key not configured on server. Add ANTHROPIC_API_KEY in Render environment variables.' });
      return;
    }

    let body = '';
    req.on('data', chunk => { body += chunk; if (body.length > 500000) { res.writeHead(413); res.end('Too large'); } });
    req.on('end', () => {
      let payload;
      try { payload = JSON.parse(body); } catch {
        sendJSON(res, 400, { error: 'Invalid request format' });
        return;
      }

      if (!payload.messages || !Array.isArray(payload.messages)) {
        sendJSON(res, 400, { error: 'Missing messages array in request' });
        return;
      }

      const postData = JSON.stringify({
        model: 'claude-sonnet-4-5',
        max_tokens: 8000,
        messages: payload.messages
      });

      console.log(`[${new Date().toISOString()}] Analyse request from ${ip}, body ${body.length} bytes`);

      const options = {
        hostname: 'api.anthropic.com',
        port: 443,
        path: '/v1/messages',
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'x-api-key': ANTHROPIC_API_KEY,
          'anthropic-version': '2023-06-01',
          'Content-Length': Buffer.byteLength(postData)
        }
      };

      const apiReq = https.request(options, (apiRes) => {
        let data = '';
        apiRes.on('data', chunk => data += chunk);
        apiRes.on('end', () => {
          console.log(`[${new Date().toISOString()}] Anthropic responded: ${apiRes.statusCode}, ${data.length} bytes`);
          // Always forward Anthropic's response as-is
          res.writeHead(apiRes.statusCode, { 'Content-Type': 'application/json' });
          res.end(data);
        });
      });

      apiReq.on('error', (e) => {
        console.error('Anthropic request error:', e.message);
        sendJSON(res, 500, { error: 'Could not reach AI service: ' + e.message });
      });

      apiReq.setTimeout(120000, () => {
        apiReq.destroy();
        sendJSON(res, 504, { error: 'Analysis timed out after 2 minutes. Please try again.' });
      });

      apiReq.write(postData);
      apiReq.end();
    });
    return;
  }

  // Serve static files from root (index.html is at root, not in public/)
  let filePath = path.join(__dirname, pathname === '/' ? 'index.html' : pathname.replace(/^\//, ''));
  // Security: prevent directory traversal
  if (!filePath.startsWith(__dirname)) {
    res.writeHead(403); res.end('Forbidden'); return;
  }

  const ext = path.extname(filePath);
  const mime = MIME[ext] || 'text/html; charset=utf-8';

  fs.readFile(filePath, (err, data) => {
    if (err) {
      fs.readFile(path.join(__dirname, 'index.html'), (err2, data2) => {
        if (err2) { res.writeHead(404); res.end('Not found'); }
        else { res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8' }); res.end(data2); }
      });
    } else {
      res.writeHead(200, { 'Content-Type': mime });
      res.end(data);
    }
  });
});

server.listen(PORT, () => {
  console.log(`FundAudit running on port ${PORT}`);
  console.log(`API key configured: ${!!ANTHROPIC_API_KEY}`);
});
