export const name = "factsearch_parse";
export const description = "Parse an LLM response into structured facts with confidence scores and source attribution";

export const inputSchema = {
  type: "object",
  properties: {
    llmResponse: { type: "string", description: "Raw LLM output containing factual claims (JSON or bullet list)" },
    sourceUrl: { type: "string", description: "Source URL to attribute facts to" },
  },
  required: ["llmResponse", "sourceUrl"],
};

export async function run(input: { llmResponse: string; sourceUrl: string }): Promise<string> {
  const { llmResponse, sourceUrl } = input;
  const facts: any[] = [];

  // Try JSON parse first
  try {
    const parsed = JSON.parse(llmResponse);
    const items = Array.isArray(parsed) ? parsed : (parsed.facts ?? parsed.claims ?? [parsed]);
    for (const item of items) {
      facts.push({
        text: item.text ?? item.claim ?? item.fact ?? JSON.stringify(item),
        sourceUrl,
        confidence: item.confidence ?? 0.5,
      });
    }
    return JSON.stringify({ facts }, null, 2);
  } catch {}

  // Fallback: parse bullet/markdown list
  const lines = llmResponse.split("\n").filter(l => l.trim());
  for (const line of lines) {
    const cleaned = line.replace(/^[-*•]\s*/, "").replace(/^\d+\.\s*/, "").trim();
    if (cleaned.length > 10) {
      facts.push({ text: cleaned, sourceUrl, confidence: 0.5 });
    }
  }

  return JSON.stringify({ facts }, null, 2);
}
