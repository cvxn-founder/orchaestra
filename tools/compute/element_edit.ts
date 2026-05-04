export const name = "element_edit";
export const description = "Generate edit request markdown or parse multi-file edits from LLM output. Supports edit, delete, distill, and inspect task types.";

export const inputSchema = {
  type: "object",
  properties: {
    taskType: { type: "string", enum: ["edit", "delete", "distill", "inspect"] },
    tag: { type: "string", description: "HTML tag of the target element" },
    selector: { type: "string", description: "CSS selector" },
    instruction: { type: "string", description: "Natural language edit instruction" },
    classes: { type: "array", items: { type: "string" }, description: "CSS classes on the target" },
    sourceFiles: { type: "array", items: { type: "string" }, description: "Source file paths" },
    parseContent: { type: "string", description: "LLM output to parse into structured multi-file edits" },
  },
  required: ["taskType", "tag"],
};

export async function run(input: any): Promise<string> {
  if (input.parseContent) {
    return JSON.stringify({ edits: parseMultiFileEdits(input.parseContent) }, null, 2);
  }
  return JSON.stringify({ markdown: editRequestMarkdown(input) }, null, 2);
}

function editRequestMarkdown(input: any): string {
  const { taskType, tag, selector, instruction, classes, sourceFiles } = input;
  const verb = taskType === "delete" ? "Delete" : taskType === "distill" ? "Distill" : "Edit";
  let md = `## ${verb} Element: \`<${tag}>\`\n`;
  if (selector) md += `- **Selector:** \`${selector}\`\n`;
  if (instruction) md += `- **Instruction:** ${instruction}\n`;
  if (classes?.length) md += `- **Classes:** ${classes.map((c: string) => "`." + c + "`").join(" ")}\n`;
  if (sourceFiles?.length) md += `\n### Source Files\n${sourceFiles.map((f: string) => `- \`${f}\``).join("\n")}\n`;
  md += `\n### Requested Change\nProvide the exact code replacement (search/replace block) for each file.`;
  return md;
}

function parseMultiFileEdits(content: string): any[] {
  const edits: any[] = [];
  const blocks = content.split(/### File:|#### File:|## File:/i);
  for (const block of blocks) {
    const trimmed = block.trim();
    if (!trimmed) continue;
    const lines = trimmed.split("\n");
    const file = lines[0].trim().replace(/^`|`$/g, "");
    const searchSec = trimmed.match(/(?:SEARCH|search|Search):\s*\n?```[^\n]*\n([\s\S]*?)```/);
    const replaceSec = trimmed.match(/(?:REPLACE|replace|Replace):\s*\n?```[^\n]*\n([\s\S]*?)```/);
    if (file && searchSec && replaceSec) edits.push({ file, search: searchSec[1], replace: replaceSec[1] });
  }
  return edits;
}
