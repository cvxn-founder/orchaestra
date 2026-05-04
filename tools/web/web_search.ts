export const name = "web_search";
export const description = "Search the web using DuckDuckGo HTML (no API key required). Returns titles + URLs + snippets.";

export const inputSchema = {
  type: "object",
  properties: {
    query: { type: "string", description: "Search query" },
  },
  required: ["query"],
};

export async function run(input: { query: string }): Promise<string> {
  const { query } = input;
  try {
    const url = `https://html.duckduckgo.com/html/?q=${encodeURIComponent(query)}`;
    const resp = await fetch(url, {
      headers: { "User-Agent": "orchaestra/0.2.0" },
      signal: AbortSignal.timeout(10000),
    });
    if (!resp.ok) return `Search error: HTTP ${resp.status}`;
    const html = await resp.text();

    // Extract result snippets
    const results: string[] = [];
    const linkRe = /<a[^>]*class="result__a"[^>]*href="([^"]*)"[^>]*>([^<]*)<\/a>/gi;
    const snippetRe = /<a[^>]*class="result__snippet"[^>]*>([\s\S]*?)<\/a>/gi;

    let match;
    const links: { title: string; href: string }[] = [];
    while ((match = linkRe.exec(html)) !== null) {
      links.push({ href: match[1], title: match[2].replace(/<[^>]*>/g, "").trim() });
    }

    const snippets: string[] = [];
    while ((match = snippetRe.exec(html)) !== null) {
      snippets.push(match[1].replace(/<[^>]*>/g, "").trim());
    }

    for (let i = 0; i < Math.min(links.length, snippets.length, 10); i++) {
      results.push(`${links[i].title}\n  ${links[i].href}\n  ${snippets[i]}`);
    }

    return results.join("\n\n") || "No results found";
  } catch (e: any) {
    return `Search error: ${e.message}`;
  }
}
