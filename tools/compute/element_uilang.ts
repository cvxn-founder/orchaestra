export const name = "element_uilang";
export const description = "Generate a UILang DSL string from a UI node tree with layout hints (direction, padding, margin, gap, width, wrap, justify, align) and labels.";

export const inputSchema = {
  type: "object",
  properties: {
    root: { type: "object", description: "Root UINode with name, label?, direction?, props?, children?" },
  },
  required: ["root"],
};

interface LayoutProps {
  p?: string; m?: string; gap?: number; "row-gap"?: number;
  w?: number | string; h?: number | string; "max-w"?: number | string; "min-w"?: number | string;
  wrap?: boolean; justify?: string; align?: string;
}

interface UINode {
  name: string; label?: string; direction?: "row" | "col";
  props?: LayoutProps; children?: UINode[];
}

export async function run(input: any): Promise<string> {
  return JSON.stringify({ dsl: genNode(input.root, 0) });
}

function genNode(node: UINode, depth: number): string {
  const indent = depth > 0 ? "  ".repeat(depth) : "";
  let result = depth > 0 ? "\n" : "";
  let line = indent + node.name;
  const hints: string[] = [];
  if (node.direction) hints.push(node.direction);
  if (node.props) {
    if (node.props.p) hints.push(`p:${node.props.p}`);
    if (node.props.m) hints.push(`m:${node.props.m}`);
    if (node.props.gap !== undefined) hints.push(`gap:${node.props.gap}`);
    if (node.props["row-gap"] !== undefined) hints.push(`row-gap:${node.props["row-gap"]}`);
    if (node.props.w !== undefined) hints.push(`w:${node.props.w}`);
    if (node.props.h !== undefined) hints.push(`h:${node.props.h}`);
    if (node.props["max-w"] !== undefined) hints.push(`max-w:${node.props["max-w"]}`);
    if (node.props["min-w"] !== undefined) hints.push(`min-w:${node.props["min-w"]}`);
    if (node.props.wrap) hints.push("wrap");
    if (node.props.justify) hints.push(`justify:${node.props.justify}`);
    if (node.props.align) hints.push(`align:${node.props.align}`);
  }
  if (hints.length > 0) line += `(${hints.join(", ")})`;
  if (node.label) line += ` # ${node.label}`;
  result += line;

  if (node.children && node.children.length > 0) {
    result += "[";
    const isFlat = node.children.every(c => !c.children || c.children.length === 0);
    if (isFlat) {
      result += `\n${indent}  ${node.children.map(c => genLeaf(c)).join(" | ")}\n${indent}`;
    } else {
      for (const child of node.children) result += genNode(child, depth + 1);
      result += "\n" + indent;
    }
    result += "]";
  }
  return result;
}

function genLeaf(node: UINode): string {
  let s = node.name;
  if (node.props?.p) s += `(p:${node.props.p})`;
  if (node.props?.w !== undefined) s += `(w:${node.props.w})`;
  if (node.label) s += ` # ${node.label}`;
  return s;
}
