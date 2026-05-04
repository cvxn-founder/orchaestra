export const name = "ask_user";
export const description = "Ask the user a question and wait for their response. Use for clarifications, decisions, or confirmations.";

export const inputSchema = {
  type: "object",
  properties: {
    question: { type: "string", description: "The question to ask the user" },
  },
  required: ["question"],
};

export async function run(input: { question: string }): Promise<string> {
  return `[QUESTION] ${input.question}\n\nUser must respond in the chat. This tool returns the question text so the user sees it.`;
}
