namespace MachineHealthExplorer.Agent.Serialization;

/// <summary>
/// JSON Schema payloads for OpenAI-compatible <c>response_format.type=json_schema</c> (LM Studio /v1/chat/completions).
/// </summary>
internal static class AgentStructuredOutputJsonSchemas
{
    internal const string CoordinatorDispatchSchemaName = "coordinator_dispatch_plan";

    /// <summary>
    /// Coordinator dispatch JSON: steps with specialist, reason, parallel_group; optional notes and expects_dataset_query_evidence per step.
    /// </summary>
    internal const string CoordinatorDispatch = """
{
  "type": "object",
  "additionalProperties": true,
  "properties": {
    "steps": {
      "type": "array",
      "items": {
        "type": "object",
        "additionalProperties": true,
        "properties": {
          "specialist": { "type": "string" },
          "reason": { "type": "string" },
          "parallel_group": { "type": "integer" },
          "expects_dataset_query_evidence": { "type": "boolean" },
          "required_evidence": {
            "type": "array",
            "items": { "type": "string" }
          }
        }
      }
    },
    "notes": { "type": "string" }
  },
  "required": ["steps"]
}
""";

    internal const string SpecialistToolSelectionSchemaName = "specialist_tool_selection";

    internal const string SpecialistToolSelection = """
{
  "type": "object",
  "additionalProperties": true,
  "properties": {
    "need_tools": { "type": "boolean" },
    "tools": {
      "type": "array",
      "items": { "type": "string" }
    },
    "reason": { "type": "string" }
  },
  "required": ["need_tools", "tools"]
}
""";

    internal const string SpecialistStructuredSynthesisSchemaName = "specialist_structured_synthesis";

    internal const string SpecialistStructuredSynthesis = """
{
  "type": "object",
  "additionalProperties": true,
  "properties": {
    "relevantColumns": { "type": "array", "items": { "type": "string" } },
    "ambiguities": { "type": "array", "items": { "type": "string" } },
    "evidences": {
      "type": "array",
      "items": {
        "type": "object",
        "additionalProperties": true,
        "properties": {
          "sourceTool": { "type": "string" },
          "summary": { "type": "string" },
          "supportingJsonFragment": {}
        }
      }
    },
    "keyMetrics": { "type": "object", "additionalProperties": true },
    "objectiveObservations": { "type": "array", "items": { "type": "string" } },
    "hypothesesOrCaveats": { "type": "array", "items": { "type": "string" } },
    "reportSections": { "type": "array", "items": { "type": "string" } },
    "analystNotes": { "type": "string" }
  },
  "required": ["relevantColumns", "ambiguities", "evidences", "keyMetrics", "objectiveObservations", "hypothesesOrCaveats", "reportSections", "analystNotes"]
}
""";

    internal const string MemoryWorkerSchemaName = "memory_worker_update";

    internal const string MemoryWorker = """
{
  "type": "object",
  "additionalProperties": true,
  "properties": {
    "currentUserIntent": { "type": "string" },
    "pendingQuestions": { "type": "array", "items": { "type": "string" } },
    "confirmedFacts": { "type": "array", "items": { "type": "string" } },
    "toolHighlights": {
      "type": "array",
      "items": {
        "type": "object",
        "additionalProperties": true,
        "properties": {
          "tool": { "type": "string" },
          "summary": { "type": "string" }
        }
      }
    },
    "language": { "type": "string" },
    "rollingSummary": { "type": "string" }
  },
  "required": ["currentUserIntent", "pendingQuestions", "confirmedFacts", "toolHighlights", "language", "rollingSummary"]
}
""";
}
