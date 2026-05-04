export const name = "element_inspect";
export const description = "Audit a DOM node for a11y issues (contrast, missing alt/labels, heading skips), detect layout direction/spacing, and classify wrapper nodes. Modes: a11y, layout, wrapper, all.";

export const inputSchema = {
  type: "object",
  properties: {
    node: { type: "object", description: "DomNode with tag, attrs, styles, computed, children" },
    mode: { type: "string", enum: ["a11y", "layout", "wrapper", "all"], default: "all" },
  },
  required: ["node"],
};

interface DomNode {
  tag: string;
  attrs: Record<string, string>;
  styles: Record<string, string>;
  computed: ComputedStyles;
  children: DomNode[];
}

interface ComputedStyles {
  display: string; flexDirection: string; flexWrap: string;
  alignItems: string; justifyContent: string; position: string;
  width: string; height: string; maxWidth: string; minWidth: string;
  paddingTop: string; paddingBottom: string; paddingLeft: string; paddingRight: string;
  marginTop: string; marginBottom: string; marginLeft: string; marginRight: string;
  gap: string; rowGap: string;
}

export async function run(input: any): Promise<string> {
  const node: DomNode = {
    tag: input.node?.tag ?? "div",
    attrs: input.node?.attrs ?? {},
    styles: input.node?.styles ?? {},
    computed: input.node?.computed ?? {} as ComputedStyles,
    children: input.node?.children ?? [],
  };
  const mode = input.mode ?? "all";

  let result: any;
  switch (mode) {
    case "a11y": result = auditA11y(node); break;
    case "layout": result = { direction: detectDirection(node.computed), props: extractProps(node.computed) }; break;
    case "wrapper": result = { isWrapper: isWrapper(node) }; break;
    default: result = { a11y: auditA11y(node), layout: { direction: detectDirection(node.computed), props: extractProps(node.computed) }, isWrapper: isWrapper(node) };
  }
  return JSON.stringify(result, null, 2);
}

// ── A11y ──
function auditA11y(root: DomNode): any {
  const report: any = { contrastIssues: [], missingAlt: [], missingLabels: [], headingSkips: [], score: 100 };
  walk(root, (n, path) => {
    if (n.tag === "img" && !n.attrs.alt) { report.missingAlt.push(path.join(" > ")); report.score -= 5; }
    if ((n.tag === "input" || n.tag === "button") && !n.attrs["aria-label"] && !hasTextContent(n)) { report.missingLabels.push(path.join(" > ")); report.score -= 3; }
    if (/^h[2-6]$/.test(n.tag)) report.headingSkips.push(path.join(" > "));
    const fg = textColor(n);
    if (fg) {
      const bg = parseCssColor(n.styles["background-color"] ?? "white");
      if (bg) {
        const ratio = contrastRatio(fg, bg);
        if (ratio < 4.5) report.contrastIssues.push({ selector: path.join(" > "), foreground: rgbStr(fg), background: rgbStr(bg), ratio: Math.round(ratio * 100) / 100, required: 4.5, level: "AA" });
      }
    }
  });
  report.score = Math.max(0, report.score);
  return report;
}

// ── Layout ──
function detectDirection(c: ComputedStyles): string | null {
  if (c.display === "flex" || c.display === "grid") return c.flexDirection === "column" ? "col" : "row";
  if (c.position === "absolute" || c.position === "fixed") return "absolute";
  return null;
}

function extractProps(c: ComputedStyles): Record<string, string> | null {
  const props: Record<string, string> = {};
  const pad = extractSpacing(c); if (pad) props.p = pad;
  const mar = extractMargin(c); if (mar) props.m = mar;
  if (c.gap && c.gap !== "0px" && c.gap !== "normal") props.gap = c.gap;
  if (c.rowGap && c.rowGap !== "0px" && c.rowGap !== "normal") props["row-gap"] = c.rowGap;
  if (c.width && c.width !== "auto") props.w = c.width;
  if (c.height && c.height !== "auto") props.h = c.height;
  if (c.maxWidth && c.maxWidth !== "none") props["max-w"] = c.maxWidth;
  if (c.minWidth && c.minWidth !== "0px" && c.minWidth !== "auto") props["min-w"] = c.minWidth;
  return Object.keys(props).length > 0 ? props : null;
}

function extractSpacing(c: ComputedStyles): string | null {
  const vals = [pxVal(c.paddingTop), pxVal(c.paddingRight), pxVal(c.paddingBottom), pxVal(c.paddingLeft)];
  return vals.every(v => v === 0) ? null : vals.join("_");
}

function extractMargin(c: ComputedStyles): string | null {
  const vals = [pxVal(c.marginTop), pxVal(c.marginRight), pxVal(c.marginBottom), pxVal(c.marginLeft)];
  return vals.every(v => v === 0) ? null : vals.join("_");
}

// ── Wrapper ──
function isWrapper(node: DomNode): boolean {
  if (node.children.length !== 1) return false;
  const child = node.children[0];
  return child.tag !== "img" && child.tag !== "input" && child.tag !== "button";
}

// ── Color ──
function textColor(n: DomNode): [number, number, number] | null { return n.styles.color ? parseCssColor(n.styles.color) : null; }

function parseCssColor(color: string): [number, number, number] | null {
  if (!color || color === "transparent") return null;
  if (color.startsWith("rgb")) { const m = color.match(/[\d.]+/g); return m && m.length >= 3 ? [parseFloat(m[0]), parseFloat(m[1]), parseFloat(m[2])] : null; }
  if (color.startsWith("#")) { const h = color.slice(1); return h.length === 3 ? [parseInt(h[0]+h[0],16), parseInt(h[1]+h[1],16), parseInt(h[2]+h[2],16)] : h.length === 6 ? [parseInt(h.slice(0,2),16), parseInt(h.slice(2,4),16), parseInt(h.slice(4,6),16)] : null; }
  const named: Record<string, [number, number, number]> = { white: [255,255,255], black: [0,0,0], red: [255,0,0], blue: [0,0,255], green: [0,128,0], gray: [128,128,128], grey: [128,128,128], transparent: [255,255,255] };
  return named[color.toLowerCase()] ?? null;
}

function relativeLuminance([r,g,b]: [number, number, number]): number {
  const c = [r/255, g/255, b/255].map(v => v <= 0.03928 ? v/12.92 : ((v+0.055)/1.055)**2.4);
  return 0.2126*c[0] + 0.7152*c[1] + 0.0722*c[2];
}

function contrastRatio(a: [number, number, number], b: [number, number, number]): number {
  const l1 = relativeLuminance(a), l2 = relativeLuminance(b);
  return (Math.max(l1,l2)+0.05)/(Math.min(l1,l2)+0.05);
}

function rgbStr(c: [number, number, number]): string { return `rgb(${c[0]},${c[1]},${c[2]})`; }
function pxVal(s: string): number { if (!s) return 0; const m = s.match(/^([\d.]+)/); return m ? parseFloat(m[1]) : 0; }

function walk(node: DomNode, fn: (n: DomNode, path: string[]) => void, path: string[] = []): void {
  const cur = [...path, node.tag || node.attrs?.id || node.attrs?.class || "*"];
  fn(node, cur);
  for (const child of node.children) walk(child, fn, cur);
}

function hasTextContent(node: DomNode): boolean {
  return node.children.some(c => c.tag === "#text" || c.tag === "span" || c.tag === "p");
}
