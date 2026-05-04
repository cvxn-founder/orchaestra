export const name = "element_semantic";
export const description = "Classify an HTML element by tag and attributes — returns semantic name, role, and confidence";

export const inputSchema = {
  type: "object",
  properties: {
    tag: { type: "string", description: "HTML tag name (e.g., button, div, input)" },
    attrs: { type: "object", description: "HTML attributes as key-value pairs" },
  },
  required: ["tag"],
};

export async function run(input: { tag: string; attrs?: Record<string, string> }): Promise<string> {
  const { tag, attrs = {} } = input;
  const name = elementName(tag, attrs);
  const role = detectRole(tag, attrs);
  return JSON.stringify({ name, role });
}

function elementName(tag: string, attrs: Record<string, string>): string {
  if (attrs["aria-label"]) return attrs["aria-label"];
  if (attrs.placeholder) return attrs.placeholder;
  if (attrs.alt) return attrs.alt;
  if (attrs.title) return attrs.title;
  if (attrs.id) return `#${attrs.id}`;
  if (attrs.class) {
    const classes = attrs.class.split(/\s+/).filter(Boolean);
    const meaningful = classes.find((c) => !/^(px|py|pt|pb|pl|pr|mx|my|mt|mb|ml|mr|w-|h-|flex|grid|block|inline|relative|absolute|fixed|hidden|text-|font-|bg-|border|rounded|shadow|opacity|z-|cursor|select)/.test(c));
    if (meaningful) return `.${meaningful}`;
    if (classes[0]) return `.${classes[0]}`;
  }
  return `<${tag}>`;
}

function detectRole(tag: string, attrs: Record<string, string>): string {
  if (attrs.role) return attrs.role;
  switch (tag) {
    case "button": return "Button";
    case "a": return "Link";
    case "input": {
      const type = attrs.type ?? "text";
      return type === "submit" ? "SubmitButton" : type === "checkbox" ? "Checkbox" : type === "radio" ? "Radio" : "TextInput";
    }
    case "textarea": return "TextArea";
    case "select": return "Select";
    case "img": return "Image";
    case "nav": return "Navigation";
    case "main": return "Main";
    case "header": return "Header";
    case "footer": return "Footer";
    case "aside": return "Aside";
    case "section": return "Section";
    case "form": return "Form";
    case "h1": case "h2": case "h3": case "h4": case "h5": case "h6": return "Heading";
    case "p": return "Paragraph";
    case "ul": case "ol": case "dl": return "List";
    case "table": return "Table";
    case "video": return "Video";
    default: return "Generic";
  }
}
