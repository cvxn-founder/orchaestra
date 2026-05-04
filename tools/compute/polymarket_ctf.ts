export const name = "polymarket_ctf";
export const description = "Derive CTF token IDs for Polymarket condition IDs via encoded eth_call data. Returns collection ID and position ID calls per outcome.";

export const inputSchema = {
  type: "object",
  properties: {
    conditionId: { type: "string", description: "Polymarket condition ID (hex, with or without 0x prefix)" },
    outcomeCount: { type: "integer", default: 2, description: "Number of outcomes" },
  },
  required: ["conditionId"],
};

const CTF_CONTRACT = "0x4D97DCd97eC945f40cF65F87097ACe5EA0476045";
const USDC_POLYGON = "0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174";
const SEL_GET_COLLECTION_ID = "0x856296f7";
const SEL_GET_POSITION_ID = "0x39dd7530";

function strip0x(h: string): string { return h.startsWith("0x") ? h.slice(2) : h; }
function pad64(h: string): string { return h.padStart(64, "0"); }

function encodeGetCollectionId(conditionId: string, indexSet: number): string {
  const cond = strip0x(conditionId);
  return `${SEL_GET_COLLECTION_ID}${"0".repeat(64)}${cond}${pad64(indexSet.toString(16))}`;
}

function encodeGetPositionId(collectionId: string): string {
  const usdc = strip0x(USDC_POLYGON);
  return `${SEL_GET_POSITION_ID}${usdc.padStart(64, "0")}${strip0x(collectionId)}`;
}

export async function run(input: any): Promise<string> {
  const { conditionId } = input;
  const outcomeCount = input.outcomeCount ?? 2;
  const tokens = [];
  for (let i = 0; i < outcomeCount; i++) {
    const idx = 1 << i;
    const collCall = encodeGetCollectionId(conditionId, idx);
    const tempCollId = `0x${pad64(strip0x(conditionId))}`;
    const posCall = encodeGetPositionId(tempCollId);
    tokens.push({
      outcomeIndex: i, indexSet: idx,
      collectionIdCall: collCall, positionIdCall: posCall,
      note: "Execute collectionIdCall via eth_call first, use result in positionIdCall, then decodeTokenId on second result",
    });
  }
  return JSON.stringify({ conditionId, outcomeCount, tokens }, null, 2);
}
