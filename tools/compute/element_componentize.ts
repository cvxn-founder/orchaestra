export const name = "element_componentize";
export const description = "Find duplicate component candidates from source files by comparing class sets across elements. Returns pairs with similarity scores and class diffs.";

export const inputSchema = {
  type: "object",
  properties: {
    sources: { type: "array", items: { type: "array", items: { type: "string" }, minItems: 2, maxItems: 2 }, description: "Array of [filename, source_content] pairs" },
    maxDiff: { type: "integer", default: 2, description: "Max differing classes for a match" },
    minClasses: { type: "integer", default: 2, description: "Min classes per element" },
  },
  required: ["sources"],
};

interface Element {
  tag: string; file: string; line: number; classes: string[];
}

export async function run(input: any): Promise<string> {
  const { sources, maxDiff = 2, minClasses = 2 } = input;
  const elements: Element[] = [];
  for (const [file, content] of sources) {
    elements.push(...extractElements(content, file));
  }

  const candidates: any[] = [];
  for (let i = 0; i < elements.length; i++) {
    for (let j = i + 1; j < elements.length; j++) {
      const a = elements[i], b = elements[j];
      if (a.file === b.file || a.tag !== b.tag) continue;
      const aSet = new Set(a.classes), bSet = new Set(b.classes);
      if (aSet.size < minClasses && bSet.size < minClasses) continue;

      const diff: string[] = [];
      for (const c of aSet) { if (!bSet.has(c)) diff.push(c); }
      for (const c of bSet) { if (!aSet.has(c)) diff.push(c); }

      if (diff.length <= maxDiff) {
        const union = new Set([...aSet, ...bSet]);
        const intersection = new Set([...aSet].filter(c => bSet.has(c)));
        const similarity = union.size > 0 ? Math.round((intersection.size / union.size) * 100) / 100 : 0;
        candidates.push({ a: { tag: a.tag, file: a.file, line: a.line, classes: a.classes }, b: { tag: b.tag, file: b.file, line: b.line, classes: b.classes }, diff, similarity });
      }
    }
  }

  candidates.sort((a, b) => b.similarity - a.similarity || a.a.tag.localeCompare(b.a.tag));
  return JSON.stringify({ target: "in-memory", totalFiles: sources.length, totalElements: elements.length, candidates }, null, 2);
}

function extractElements(content: string, file: string): Element[] {
  const elements: Element[] = [];
  const tagRe = /<(\w+)[^>]*?\sclass=["']([^"']*)["'][^>]*>/gi;
  let match, lineNum = 1, pos = 0;

  while ((match = tagRe.exec(content)) !== null) {
    const tag = match[1].toLowerCase();
    const classes = match[2].split(/\s+/).filter(Boolean);
    const before = content.slice(pos, match.index);
    lineNum += before.split("\n").length - 1;
    pos = match.index;
    if (classes.length > 0) elements.push({ tag, file, line: lineNum, classes });
  }
  return elements;
}
