export const name = "polymarket_scorer";
export const description = "Compute whale score (0-100) and tier from Polymarket trading history. Returns score, tier, win rate, PnL, volume, and per-category stats.";

export const inputSchema = {
  type: "object",
  properties: {
    trades: { type: "array", items: { type: "object", properties: { tokenId: { type: "string" }, price: { type: "number" }, size: { type: "number" }, side: { type: "string" }, conditionId: { type: "string" } }, required: ["tokenId", "price", "size", "side", "conditionId"] }, description: "List of wallet trades" },
    markets: { type: "array", items: { type: "object", properties: { conditionId: { type: "string" }, resolutionOutcome: { type: "string" }, category: { type: "string" }, outcomeTokens: { type: "array", items: { type: "object", properties: { tokenId: { type: "string" }, outcome: { type: "string" } } } }, tokenOutcomes: { type: "object" } }, required: ["conditionId", "resolutionOutcome", "category"] }, description: "Resolved markets" },
  },
  required: ["trades", "markets"],
};

function estWin(vol: number) { return vol * 0.1; }
function estLoss(vol: number) { return vol * 0.05; }

function isWin(trade: any, market: any): boolean {
  const tokOut = market.tokenOutcomes?.[trade.tokenId] ?? "";
  const side = trade.side?.toUpperCase();
  return (side === "BUY" && tokOut === market.resolutionOutcome) || (side === "SELL" && tokOut !== market.resolutionOutcome);
}

export async function run(input: any): Promise<string> {
  const marketMap = new Map<string, any>();
  for (const m of input.markets) marketMap.set(m.conditionId, m);

  const catData: [string, boolean, number][] = [];
  let totalVol = 0, totalPnl = 0, totalWins = 0;

  for (const trade of input.trades) {
    const market = marketMap.get(trade.conditionId);
    if (!market) continue;
    const vol = trade.price * trade.size;
    totalVol += vol;
    const win = isWin(trade, market);
    catData.push([market.category, win, vol]);
    if (win) { totalWins++; totalPnl += estWin(vol); }
    else { totalPnl -= estLoss(vol); }
  }

  const catStats = new Map<string, { trades: number; wins: number; winRate: number; pnl: number }>();
  for (const [cat, win, vol] of catData) {
    let e = catStats.get(cat);
    if (!e) { e = { trades: 0, wins: 0, winRate: 0, pnl: 0 }; catStats.set(cat, e); }
    e.trades++; if (win) e.wins++; e.pnl += win ? estWin(vol) : -estLoss(vol);
  }
  for (const e of catStats.values()) e.winRate = e.trades > 0 ? Math.round((e.wins / e.trades) * 1000) / 10 : 0;

  let domCat: string | null = null, domWr = 0;
  for (const [cat, s] of catStats) {
    if (s.trades > (catStats.get(domCat!)?.trades ?? 0)) { domCat = cat; domWr = s.winRate; }
  }

  const entries = [...catStats.entries()];
  const wrScore = Math.min(100, domWr) * 0.4;
  const pnlScore = Math.min(Math.max(Math.pow(Math.abs(totalPnl), 0.3) * 2, 0), 25) * (totalPnl > 0 ? 1 : 0.3);
  const volScore = Math.min(Math.max(Math.pow(totalVol, 0.25) / 2, 0), 15);
  const tradesScore = Math.min(10, input.trades.length / 20);
  let specScore = 5;
  if (entries.length >= 2) {
    const wrs = entries.map(([, s]) => s.winRate).sort((a, b) => b - a);
    specScore = Math.min(Math.max((wrs[0] - wrs.slice(1).reduce((s, v) => s + v, 0) / (wrs.length - 1)) / 5, 0), 10);
  }
  const score = Math.round(Math.min(Math.max(wrScore + pnlScore + volScore + tradesScore + specScore, 0), 100));
  const tier = score >= 85 ? "alpha" : score >= 70 ? "beta" : score >= 50 ? "gamma" : "unrated";
  const winRate = input.trades.length > 0 ? Math.round((totalWins / input.trades.length) * 1000) / 10 : 0;

  const catObj: Record<string, any> = {};
  for (const [k, v] of catStats) catObj[k] = v;

  return JSON.stringify({ score, tier, totalTrades: input.trades.length, totalVolume: Math.round(totalVol * 100) / 100, totalPnl: Math.round(totalPnl * 100) / 100, overallWinRate: winRate, dominantCategory: domCat, dominantWinRate: domWr, categoryStats: catObj }, null, 2);
}
