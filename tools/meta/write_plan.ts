import { writeFileSync } from "fs";

export const name = "write_plan";
export const description = "Create or update a structured task plan. Saves to a plan file for tracking progress.";

export const inputSchema = {
  type: "object",
  properties: {
    tasks: { type: "string", description: "JSON array of tasks: [{ id, subject, status }]" },
  },
  required: ["tasks"],
};

export async function run(input: { tasks: string }): Promise<string> {
  try {
    const parsed = JSON.parse(input.tasks);
    if (!Array.isArray(parsed)) return "tasks must be a JSON array";
    const plan = {
      updated: new Date().toISOString(),
      tasks: parsed,
    };
    writeFileSync("/tmp/orchaestra_plan.json", JSON.stringify(plan, null, 2), "utf8");
    return `Plan saved with ${parsed.length} tasks`;
  } catch (e: any) {
    return `Error: ${e.message}`;
  }
}
