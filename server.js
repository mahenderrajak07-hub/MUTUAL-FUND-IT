function buildReport(funds, results, knowledge) {
  const kFunds = (knowledge?.funds || []).filter(k => k && k.name && typeof k.name === 'string');

  const normName = n => String(n || '')
    .toLowerCase()
    .replace(/[^a-z0-9\s]/g, ' ')
    .replace(/\b(direct|regular|growth|plan|option|fund|india|scheme|idcw|dividend)\b/g, ' ')
    .replace(/\s+/g, ' ')
    .trim();

  const kMap = {};
  const kCodeMap = {};
  for (const k of kFunds) {
    kMap[normName(k.name)] = k;
    if (k.schemeCode) kCodeMap[String(k.schemeCode)] = k;
  }

  const getK = (name, schemeCode) => {
    if (schemeCode && kCodeMap[String(schemeCode)]) return kCodeMap[String(schemeCode)];
    const norm = normName(name);
    return kMap[norm] || {};
  };

  const na = v => (v == null || v === '' || Number.isNaN(v) ? 'N/A' : v);
  const numOrNull = v => (v == null || Number.isNaN(Number(v)) ? null : Number(v));
  const pctStr = v => (v == null || Number.isNaN(Number(v)) ? 'N/A' : `${v > 0 ? '+' : ''}${Number(v).toFixed(2)}%`);
  const safeFmt = v => (v == null || Number.isNaN(Number(v)) ? 'N/A' : fmt(Number(v)));

  const totalInvested = results.reduce((s, r) => s + (r.amt || 0), 0);

  // IMPORTANT: separate fetched vs valued funds
  const fetchedResults = results.filter(r => !r.error && r.latestNav != null);
  const valuedResults = results.filter(r => !r.error && r.currentValue != null);

  const resolvedCount = fetchedResults.length;
  const valuedCount = valuedResults.length;
  const totalCount = funds.length;

  const isPartial = resolvedCount < totalCount;
  const isMinimal = resolvedCount === 0;

  const totalCurrent = valuedResults.reduce((s, r) => s + (r.currentValue || 0), 0);

  // Use fetched results for performance analytics
  const validPerf = fetchedResults.filter(r => r.ret5y != null && r.amt > 0);

  const blendedCAGR5 = validPerf.length
    ? validPerf.reduce((s, r) => s + (r.ret5y * r.amt), 0) / validPerf.reduce((s, r) => s + r.amt, 0)
    : null;

  const weightedBMcagr = validPerf.length
    ? validPerf.reduce((s, r) => s + ((r.benchmark?.cagr5y || 13.2) * r.amt), 0) / validPerf.reduce((s, r) => s + r.amt, 0)
    : null;

  const alpha5 = (blendedCAGR5 != null && weightedBMcagr != null) ? (blendedCAGR5 - weightedBMcagr) : null;
  const realReturn = blendedCAGR5 != null ? (blendedCAGR5 - 6.2) : null;

  const beatCount5 = fetchedResults.filter(r => {
    if (r.ret5y == null) return false;
    return r.ret5y > (r.benchmark?.cagr5y || 13.2);
  }).length;

  const avgTER = kFunds.length
    ? kFunds.reduce((s, k) => s + (parseFloat(k.ter) || 1.62), 0) / kFunds.length
    : 1.62;

  const annualTERCost = totalInvested * avgTER / 100;

  // NO fake fallback corpus
  const corpus = valuedCount === totalCount ? totalCurrent : null;

  const healthScore = alpha5 == null
    ? null
    : Math.min(9.5, Math.max(1, 5 + alpha5 * 0.4 - (funds.length > 5 ? 1 : 0))).toFixed(1);

  const benchmarkRows = (() => {
    const bmMap = {};
    for (const r of fetchedResults) {
      const bm = r.benchmark;
      if (!bm?.name) continue;
      if (!bmMap[bm.name]) bmMap[bm.name] = { ...bm, fundCount: 0 };
      bmMap[bm.name].fundCount++;
    }
    return Object.values(bmMap);
  })();

  const primaryBenchmark = (() => {
    if (!benchmarkRows.length) return null;
    return benchmarkRows.sort((a, b) => (b.fundCount || 0) - (a.fundCount || 0))[0];
  })();

  const fundsArr = results.map(r => {
    const k = getK(r.fund.name, r.schemeCode || r.scheme?.schemeCode);
    const bm = r.benchmark || null;
    const bmCAGR5 = bm?.cagr5y ?? null;
    const alphaVsBM = (r.ret5y != null && bmCAGR5 != null) ? (r.ret5y - bmCAGR5) : null;

    const gain = r.gain || 0;
    const ltcgTax = r.currentValue != null ? Math.max(0, gain - 125000) * 0.125 : null;
    const netProceeds = r.currentValue != null ? (r.currentValue - (ltcgTax || 0)) : null;

    let decision = k.decision || (alphaVsBM == null ? 'N/A' : alphaVsBM > 1 ? 'Hold' : alphaVsBM > -1 ? 'Switch' : 'Exit');
    let quality = k.quality || (r.ret5y == null || bmCAGR5 == null ? 'N/A' : r.ret5y > bmCAGR5 + 1 ? 'Strong' : r.ret5y > bmCAGR5 - 2 ? 'Average' : 'Weak');
    let quartile = k.quartile || (r.ret5y == null || bmCAGR5 == null ? 'N/A' : r.ret5y > bmCAGR5 + 2 ? 'Q1' : r.ret5y > bmCAGR5 ? 'Q2' : r.ret5y > bmCAGR5 - 2 ? 'Q3' : 'Q4');
    let quartileLabel = k.quartileLabel || (
      quartile === 'Q1' ? 'Top 25%' :
      quartile === 'Q2' ? 'Top 50%' :
      quartile === 'Q3' ? 'Top 75%' :
      quartile === 'Q4' ? 'Bottom 25%' :
      'N/A'
    );

    return {
      name: r.fund.name,
      manager: k.manager || 'See factsheet',
      tenureYrs: k.tenureYrs || null,
      tenureFlag: !!k.tenureFlag,
      cagr5y: r.ret5y != null ? `${r.ret5y.toFixed(2)}%` : 'N/A',
      cagr3y: r.ret3y != null ? `${r.ret3y.toFixed(2)}%` : 'N/A',
      ret1y: r.ret1y != null ? `${r.ret1y.toFixed(2)}%` : 'N/A',
      sharpe: k.sharpe || 'N/A',
      beta: r.betaEstimate || k.beta || 'N/A',
      stddev: r.fundStdDev ? `${r.fundStdDev}%` : (k.stddev || 'N/A'),
      alpha: alphaVsBM != null ? `${alphaVsBM >= 0 ? '+' : ''}${alphaVsBM.toFixed(2)}% vs ${bm?.name || 'Benchmark'}` : 'N/A',
      ter: k.ter || 'N/A',
      riskCategory: k.riskCategory || 'N/A',
      quality,
      decision,
      perf5yVal: r.ret5y ?? null,
      perf3yVal: r.ret3y ?? null,
      ret1yVal: r.ret1y ?? null,
      sharpeVal: parseFloat(k.sharpe) || null,
      calendarReturns: {
        '2020': fmtC(r.cal?.[2020]),
        '2020Beat': !!r.cal?.['2020Beat'],
        '2021': fmtC(r.cal?.[2021]),
        '2021Beat': !!r.cal?.['2021Beat'],
        '2022': fmtC(r.cal?.[2022]),
        '2022Beat': !!r.cal?.['2022Beat'],
        '2023': fmtC(r.cal?.[2023]),
        '2023Beat': !!r.cal?.['2023Beat'],
        '2024': fmtC(r.cal?.[2024]),
        '2024Beat': !!r.cal?.['2024Beat'],
        '2025': fmtC(r.cal?.[2025]),
        '2025Beat': !!r.cal?.['2025Beat'],
      },
      quartile,
      quartileLabel,
      rolling1yAvg: k.rolling1yAvg || (r.ret1y != null ? `${r.ret1y.toFixed(1)}%` : 'N/A'),
      rolling1yBeatPct: k.rolling1yBeatPct || 'N/A',
      rolling1yWorst: k.rolling1yWorst || 'N/A',
      rolling3yAvg: k.rolling3yAvg || (r.ret3y != null ? `${r.ret3y.toFixed(1)}%` : 'N/A'),
      rolling3yBeatPct: k.rolling3yBeatPct || 'N/A',
      rolling3yMin: k.rolling3yMin || 'N/A',
      realReturn: r.ret1y != null ? `${(r.ret1y - 6.2).toFixed(2)}%` : 'N/A',
      estCurrentValue: safeFmt(r.currentValue),
      gainAmt: r.currentValue != null ? safeFmt(gain) : 'N/A',
      ltcgTax: safeFmt(ltcgTax),
      netProceeds: safeFmt(netProceeds),
      breakEvenMonths: r.currentValue != null ? 7 : 'N/A',
      benchmarkName: bm?.name || 'N/A',
      benchmarkCAGR5y: bm?.cagr5y ?? null,
      resolved: !r.error && r.latestNav != null
    };
  });

  const partialWarning = isPartial
    ? `Partial analysis: ${resolvedCount}/${totalCount} funds fetched successfully. Conclusions apply only to resolved funds. Portfolio-level recommendations are withheld.`
    : null;

  const keyFlags = (() => {
    if (isMinimal) {
      return [
        'No funds could be fully fetched from AMFI.',
        'Please retry analysis or verify the entered fund names.',
        'Do not use this report for portfolio decisions.',
        'No benchmark or return conclusion is reliable yet.'
      ];
    }

    const arr = [];
    if (blendedCAGR5 != null && weightedBMcagr != null) {
      arr.push(`Resolved-fund 5Y CAGR ${blendedCAGR5.toFixed(1)}% vs weighted benchmark ${weightedBMcagr.toFixed(1)}% (alpha ${alpha5 >= 0 ? '+' : ''}${alpha5.toFixed(2)}%).`);
    }
    arr.push(`${beatCount5}/${resolvedCount} resolved funds beat their benchmark on a 5Y basis.`);
    if (realReturn != null) {
      arr.push(`Real return after 6.2% CPI is ${realReturn >= 0 ? '+' : ''}${realReturn.toFixed(2)}% for resolved funds.`);
    }
    arr.push(`Annual TER cost on invested amount is approximately ${fmt(annualTERCost)}.`);
    return arr.slice(0, 4);
  })();

  return {
    summary: {
      totalInvested: fmt(totalInvested),
      currentValue: valuedCount === totalCount ? fmt(totalCurrent) : 'N/A',
      blendedCAGR: blendedCAGR5 != null ? `${blendedCAGR5.toFixed(2)}%` : 'N/A',
      alphaBM: alpha5 != null ? `${alpha5 >= 0 ? '+' : ''}${alpha5.toFixed(2)}%` : 'N/A',
      realReturn: realReturn != null ? `${realReturn >= 0 ? '+' : ''}${realReturn.toFixed(2)}%` : 'N/A',
      annualTER: fmt(annualTERCost),
      fundsBeatBM: `${beatCount5}/${resolvedCount || totalCount}${isPartial ? ' resolved' : ''}`,
      uniqueStocks: isPartial ? 'N/A' : `~${Math.min(funds.length * 22, 150)}`,
      healthScore: healthScore ? `${healthScore}/10` : 'N/A',
      healthVerdict: isMinimal
        ? 'Data unavailable — please retry.'
        : isPartial
          ? `Partial data (${resolvedCount}/${totalCount} funds) — full portfolio verdict withheld.`
          : (alpha5 != null && alpha5 > 0 ? 'Portfolio is beating benchmark.' : 'Portfolio is lagging benchmark.'),
      overlapPct: isPartial ? 'N/A' : (knowledge?.overlap?.overallPct || 'N/A'),
      isPartial,
      partialWarning,
      keyFlags
    },

    funds: fundsArr,

    benchmarkRows,

    benchmark: primaryBenchmark ? {
      name: primaryBenchmark.name,
      cagr5y: `${primaryBenchmark.cagr5y}%`,
      cagr3y: `${primaryBenchmark.cagr3y}%`,
      ret1y: `${primaryBenchmark.ret1y >= 0 ? '+' : ''}${primaryBenchmark.ret1y}%`,
      sharpe: `${primaryBenchmark.sharpe}`,
      beta: '1.00',
      stddev: `${primaryBenchmark.stddev}%`,
      rolling1yAvg: `${primaryBenchmark.cagr5y}%`,
      rolling3yAvg: `${primaryBenchmark.cagr3y}%`,
      calendarReturns: primaryBenchmark.calendarReturns || {}
    } : null,

    // Hide portfolio-wide synthetic sections in partial mode
    risk: (!isPartial && corpus != null) ? {
      blendedBeta: 'N/A',
      bfsiPct: 'N/A',
      top5StocksPct: 'N/A',
      midSmallPct: 'N/A',
      uniqueStocks: `~${Math.min(funds.length * 22, 150)}`,
      stddev: 'N/A',
      maxDrawdown: 'N/A',
      downsideCap: 'N/A',
      upsideCap: 'N/A',
      stressScenarios: [
        { label: 'Bull +15%', impact: '+' + fmt(corpus * 0.15), pct: '+15%' },
        { label: 'Flat 3Y', impact: '-' + fmt(corpus * 0.08), pct: '-8%' },
        { label: 'Correction -20%', impact: '-' + fmt(corpus * 0.20), pct: '-20%' },
        { label: 'Crash -30%', impact: '-' + fmt(corpus * 0.30), pct: '-30%' }
      ]
    } : null,

    sectors: !isPartial ? (knowledge?.sectors || []) : [],

    overlap: !isPartial && resolvedCount >= 2 ? {
      overallPct: knowledge?.overlap?.overallPct || 'N/A',
      verdict: knowledge?.overlap?.verdict || 'N/A',
      topStocks: knowledge?.overlap?.topStocks || []
    } : null,

    projections: (!isPartial && corpus != null && blendedCAGR5 != null) ? {
      corpus: fmt(corpus),
      rows: [
        {
          label: 'Current portfolio',
          cagr: `${blendedCAGR5.toFixed(1)}%`,
          y5: fmt(corpus * Math.pow(1 + blendedCAGR5 / 100, 5)),
          y10: fmt(corpus * Math.pow(1 + blendedCAGR5 / 100, 10)),
          y15: fmt(corpus * Math.pow(1 + blendedCAGR5 / 100, 15)),
          y20: fmt(corpus * Math.pow(1 + blendedCAGR5 / 100, 20)),
          type: 'bad'
        }
      ],
      gap20y: 'N/A'
    } : null,

    recommended: isPartial ? [] : [
      { name: 'Nippon India Large Cap', cat: 'Large Cap', alloc: '30%', amt: corpus ? fmt(corpus * 0.30) : 'N/A', cagr5y: '15.9%', sharpe: '0.81', ter: '0.69%', role: 'Core anchor — consistent alpha' },
      { name: 'ICICI Pru Bluechip', cat: 'Large Cap', alloc: '25%', amt: corpus ? fmt(corpus * 0.25) : 'N/A', cagr5y: '15.3%', sharpe: '0.77%', ter: '0.95%', role: 'Large cap diversifier' }
    ],

    execution: isPartial ? [] : [
      { step: 'Step 1', color: 'warn', detail: 'Execution plan available only after full portfolio resolution.' }
    ],

    scorecard: [
      {
        label: 'Performance consistency',
        score: healthScore || 'N/A',
        note: resolvedCount ? `${beatCount5}/${resolvedCount} resolved funds beat their benchmark on 5Y basis.` : 'No resolved funds.'
      },
      {
        label: 'Diversification',
        score: isPartial ? 'N/A' : 'N/A',
        note: isPartial ? 'Withheld due to incomplete portfolio coverage.' : 'Available only with full data.'
      },
      {
        label: 'Overall health',
        score: healthScore || 'N/A',
        note: isPartial ? 'Portfolio verdict withheld due to partial data.' : 'Based on fully resolved portfolio.'
      }
    ]
  };
}async function runAnalysis(funds) {
  console.log(`\n[Phase 1] Fetching AMFI for ${funds.length} funds in parallel`);
  const FUND_TIMEOUT = 22000;

  const results = await Promise.all(funds.map(async fund => {
    console.log(`  → ${fund.name}`);
    try {
      return await Promise.race([
        fetchFundData(fund),
        new Promise((_, rej) => setTimeout(() => rej(new Error('Fund fetch timed out')), FUND_TIMEOUT))
      ]);
    } catch (e) {
      console.error(`  ✗ ${fund.name}: ${e.message}`);
      return {
        fund,
        amt: parseFloat(fund.amt.replace(/[₹,\s]/g, '')) || 0,
        error: e.message
      };
    }
  }));

  const fetchedCount = results.filter(r => !r.error && r.latestNav != null).length;
  console.log(`[Phase 1] Done: ${fetchedCount}/${funds.length} fetched`);

  let knowledge = null;
  console.log(`[Phase 2] Claude — manager/TER/Sharpe/Beta/overlap`);
  try {
    if (ANTHROPIC_API_KEY) {
      knowledge = await getKnowledgeFields(funds, results);
      console.log(`[Phase 2] Got ${knowledge?.funds?.length || 0} fund records`);
    } else {
      console.warn('[Phase 2] Claude skipped (no API key) — using computed values');
    }
  } catch (e) {
    console.warn(`[Phase 2] Claude skipped (${e.message}) — using computed values`);
  }

  console.log(`[Phase 3] Building report`);
  const report = buildReport(funds, results, knowledge);
  console.log(`[Phase 3] Done`);
  return JSON.stringify(report);
}
