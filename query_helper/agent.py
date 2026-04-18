import asyncio
import os
import json
from groq import Groq
from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client

# ── Config ────────────────────────────────────────────────────────────────────

GROQ_API_KEY = os.environ.get("GROQ_API_KEY")
if not GROQ_API_KEY:
    raise ValueError("GROQ_API_KEY not set in environment")
client = Groq(api_key=GROQ_API_KEY)
MODEL = "llama-3.3-70b-versatile"

SYSTEM_PROMPT = """You are a financial analysis assistant with access to NSE stock market data.

IMPORTANT RULES:
- NEVER use fetch_candles or fetch_latest directly for analysis tasks.
- Always prefer analysis tools: summarize_price_action, detect_trend, analyze_volume.
- Only use fetch_candles if the user explicitly asks for raw candle data,
  and in that case limit the date range to 5 days maximum.
- For any question about trends, volume, price behavior — use the analysis tools.
- Use get_all_symbols only to verify a symbol exists.
"""

# ── Tool conversion ───────────────────────────────────────────────────────────

def clean_schema(schema: dict) -> dict:
    """Recursively remove fields Groq/OpenAI don't support in function schemas."""
    unsupported = {"title", "$schema", "additionalProperties"}
    cleaned = {}
    for key, value in schema.items():
        if key in unsupported:
            continue
        if isinstance(value, dict):
            cleaned[key] = clean_schema(value)
        elif isinstance(value, list):
            cleaned[key] = [
                clean_schema(i) if isinstance(i, dict) else i for i in value
            ]
        else:
            cleaned[key] = value
    return cleaned


def mcp_tool_to_groq(tool) -> dict:
    """Convert an MCP tool definition to Groq's function calling format."""
    return {
        "type": "function",
        "function": {
            "name": tool.name,
            "description": tool.description,
            "parameters": clean_schema(dict(tool.inputSchema)),
        }
    }

# ── Agent loop ────────────────────────────────────────────────────────────────

async def run_agent(user_question: str):
    server_params = StdioServerParameters(
        command="python",
        args=[os.path.join(os.path.dirname(__file__), "mcp_ser.py")],  # ← point to your MCP server file
    )

    async with stdio_client(server_params) as (read, write):
        async with ClientSession(read, write) as session:

            # 1. Initialize MCP session
            await session.initialize()

            # 2. Fetch tools from MCP server
            tools_response = await session.list_tools()
            mcp_tools = tools_response.tools
            print(f"\n[Tools loaded: {[t.name for t in mcp_tools]}]")

            # 3. Convert to Groq format
            groq_tools = [mcp_tool_to_groq(t) for t in mcp_tools]

            # 4. Build conversation history 
            messages = [
                {"role": "system", "content": SYSTEM_PROMPT},
                {"role": "user", "content": user_question},
            ]

            # 5. Agent loop
            retries = 0
            while True:
                response = client.chat.completions.create(
                    model=MODEL,
                    messages=messages,
                    tools=groq_tools,
                )

                message = response.choices[0].message

                # Guard against empty response
                if message is None:
                    retries += 1
                    if retries >= 3:
                        print("\n[Agent failed after 3 retries. Try rephrasing.]")
                        break
                    print(f"\n[Empty response, retry {retries}/3...]")
                    continue

                retries = 0

                # Append assistant message to history
                messages.append({
                    "role": "assistant",
                    "content": message.content,
                    "tool_calls": [
                        {
                            "id": tc.id,
                            "type": "function",
                            "function": {
                                "name": tc.function.name,
                                "arguments": tc.function.arguments,
                            }
                        }
                        for tc in (message.tool_calls or [])
                    ] or None,
                })

                # No tool calls → final answer
                if not message.tool_calls:
                    print(f"\nAgent: {message.content}")
                    break

                # Process all tool calls in this response
                for tool_call in message.tool_calls:
                    tool_name = tool_call.function.name
                    tool_args = json.loads(tool_call.function.arguments)

                    print(f"\n[Agent calling tool: {tool_name}]")
                    print(f"[Arguments: {tool_args}]")

                    # 6. Execute via MCP
                    mcp_result = await session.call_tool(tool_name, arguments=tool_args)
                    tool_output = mcp_result.content[0].text

                    # Truncate if too large
                    if len(tool_output) > 8000:
                        tool_output = tool_output[:8000] + "\n...[truncated]"

                    print(f"[Tool returned: {tool_output}]")

                    # 7. Append tool result to history
                    messages.append({
                        "role": "tool",
                        "tool_call_id": tool_call.id,
                        "content": tool_output,
                    })

# ── Entry point ───────────────────────────────────────────────────────────────

async def main():
    print("Finance Agent (powered by Groq + MCP)")
    print("=" * 45)
    print("Type 'exit' to quit.")
    while True:
        question = input("\nYou: ")
        if question.strip().lower() in {"exit", "quit", "q"}:
            print("Bye!")
            break
        if not question.strip():
            continue
        try:
            await run_agent(question)
        except Exception as e:
            print(f"\n[Error: {e}. Please try again.]")

if __name__ == "__main__":
    asyncio.run(main())