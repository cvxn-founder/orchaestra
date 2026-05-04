export const name = "web_fetch";
export const description = "Fetch content from a URL and return the text response";

export const inputSchema = {
  type: "object",
  properties: {
    url: { type: "string", description: "The URL to fetch" },
  },
  required: ["url"],
};

export async function run(input: { url: string }): Promise<string> {
  const { url } = input;
  try {
    const resp = await fetch(url, {
      headers: { "User-Agent": "orchaestra/0.2.0" },
      signal: AbortSignal.timeout(15000),
    });
    if (!resp.ok) return `HTTP ${resp.status}: ${resp.statusText}`;
    const text = await resp.text();
    return text.slice(0, 50000);
  } catch (e: any) {
    return `Fetch error: ${e.message}`;
  }
}
