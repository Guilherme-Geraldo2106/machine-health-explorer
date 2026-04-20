using MachineHealthExplorer.Agent.Models;

namespace MachineHealthExplorer.Agent.MultiAgent;

internal static class MultiAgentDispatchHeuristics
{
    public static AgentDispatchPlan Plan(string userInput, MultiAgentOrchestrationOptions options)
    {
        var text = userInput ?? string.Empty;
        var lower = text.ToLowerInvariant();

        if (options.QueryAnalysis.Enabled && PrefersQueryAnalysisOnly(lower))
        {
            var quantRoles = new HashSet<AgentSpecialistKind> { AgentSpecialistKind.QueryAnalysis };
            if (options.Reporting.Enabled && MatchesReporting(lower))
            {
                quantRoles.Add(AgentSpecialistKind.Reporting);
            }

            return BuildOrderedPlan(quantRoles, "heuristic_dispatch:quant_bins");
        }

        var roles = new HashSet<AgentSpecialistKind>();

        if (options.Reporting.Enabled && MatchesReporting(lower))
        {
            roles.Add(AgentSpecialistKind.Reporting);
        }

        if (options.FailureAnalysis.Enabled && MatchesFailure(lower))
        {
            roles.Add(AgentSpecialistKind.FailureAnalysis);
        }

        if (options.Discovery.Enabled && MatchesDiscovery(lower))
        {
            roles.Add(AgentSpecialistKind.Discovery);
        }

        if (options.QueryAnalysis.Enabled && MatchesQueryAnalysis(lower))
        {
            roles.Add(AgentSpecialistKind.QueryAnalysis);
        }

        if (roles.Count == 0)
        {
            if (LooksTrivialGreeting(lower))
            {
                return new AgentDispatchPlan(Array.Empty<AgentDispatchStep>(), "heuristic_dispatch:trivial_greeting", UsedLlmPlanner: false);
            }

            if (options.Discovery.Enabled && LooksGenericDataQuestion(lower))
            {
                roles.Add(AgentSpecialistKind.Discovery);
            }

            if (options.QueryAnalysis.Enabled && LooksNumericOrAggregationHeavy(lower))
            {
                roles.Add(AgentSpecialistKind.QueryAnalysis);
            }

            if (roles.Count == 0 && options.Discovery.Enabled)
            {
                roles.Add(AgentSpecialistKind.Discovery);
            }
        }

        if (roles.Contains(AgentSpecialistKind.QueryAnalysis) && roles.Contains(AgentSpecialistKind.FailureAnalysis))
        {
            roles.Remove(AgentSpecialistKind.FailureAnalysis);
        }

        return BuildOrderedPlan(roles, "heuristic_dispatch");
    }

    /// <summary>
    /// Normalizes coordinator plans so simple quantitative failure-vs-bins questions do not stack
    /// Discovery + QueryAnalysis + FailureAnalysis, and so FailureAnalysis is not scheduled alongside QueryAnalysis
    /// when both would use the same generic tools.
    /// </summary>
    public static AgentDispatchPlan PostNormalizeDispatchPlan(
        string userInput,
        AgentDispatchPlan plan,
        MultiAgentOrchestrationOptions options)
    {
        if (plan.Steps.Count == 0)
        {
            return plan;
        }

        var lower = (userInput ?? string.Empty).ToLowerInvariant();

        if (options.QueryAnalysis.Enabled && PrefersQueryAnalysisOnly(lower))
        {
            var reporting = plan.Steps
                .Where(step => step.SpecialistKind == AgentSpecialistKind.Reporting && options.Reporting.Enabled)
                .ToArray();

            var qa = plan.Steps.LastOrDefault(step => step.SpecialistKind == AgentSpecialistKind.QueryAnalysis);
            var core = new List<AgentDispatchStep>();
            core.Add(
                qa is null
                    ? new AgentDispatchStep(AgentSpecialistKind.QueryAnalysis, "post_normalize:quant_bins", ParallelGroup: 0)
                    : qa with { ParallelGroup = 0 });

            foreach (var step in reporting)
            {
                core.Add(step with { ParallelGroup = 0 });
            }

            var notes = string.IsNullOrWhiteSpace(plan.CoordinatorNotes)
                ? "post_normalize:quant_bins"
                : $"{plan.CoordinatorNotes};post_normalize:quant_bins";

            return plan with { Steps = core, CoordinatorNotes = notes };
        }

        if (!options.QueryAnalysis.Enabled || !options.FailureAnalysis.Enabled)
        {
            return plan;
        }

        if (!plan.Steps.Any(step => step.SpecialistKind == AgentSpecialistKind.QueryAnalysis)
            || !plan.Steps.Any(step => step.SpecialistKind == AgentSpecialistKind.FailureAnalysis))
        {
            return plan;
        }

        var filtered = plan.Steps
            .Where(step => step.SpecialistKind != AgentSpecialistKind.FailureAnalysis)
            .ToArray();

        var notes2 = string.IsNullOrWhiteSpace(plan.CoordinatorNotes)
            ? "post_normalize:drop_failure_when_query"
            : $"{plan.CoordinatorNotes};post_normalize:drop_failure_when_query";

        return plan with { Steps = filtered, CoordinatorNotes = notes2 };
    }

    private static bool PrefersQueryAnalysisOnly(string lower)
    {
        if (string.IsNullOrWhiteSpace(lower))
        {
            return false;
        }

        var hasTemp = ContainsAny(lower, "temperatura", "temperature");
        var hasFailure = ContainsAny(lower, "falha", "falhas", "falham", "failure", "failures", "failed");
        var hasRate = ContainsAny(
            lower,
            "chance",
            "chances",
            "probabilidade",
            "probabilidades",
            "taxa",
            "risco",
            "risk",
            "likelihood",
            "probability",
            "failure rate",
            "taxa de falha",
            "rates",
            "rate ");
        var hasThresholdQuestion = ContainsAny(
            lower,
            "a partir de qual",
            "a partir de que",
            "a partir de quando",
            "from which temperature",
            "from what temperature");
        var hasBinsWords = ContainsAny(lower, "faixa", "bin", "bins", "histogram", "banda", "intervalo");

        if (hasTemp && (hasFailure || hasRate))
        {
            return true;
        }

        if (hasThresholdQuestion && hasTemp)
        {
            return true;
        }

        if (hasFailure && hasRate && (hasTemp || hasBinsWords))
        {
            return true;
        }

        if (ContainsAny(lower, "quando a chance", "when the probability", "when the chance", "when chances")
            && (hasFailure || hasTemp))
        {
            return true;
        }

        if (hasBinsWords && hasFailure && hasTemp)
        {
            return true;
        }

        return false;
    }

    private static bool LooksNumericOrAggregationHeavy(string lower)
    {
        if (string.IsNullOrWhiteSpace(lower))
        {
            return false;
        }

        return lower.Contains("máximo", StringComparison.Ordinal)
               || lower.Contains("maximo", StringComparison.Ordinal)
               || lower.Contains("mínimo", StringComparison.Ordinal)
               || lower.Contains("minimo", StringComparison.Ordinal)
               || lower.Contains("maior", StringComparison.Ordinal)
               || lower.Contains("menor", StringComparison.Ordinal)
               || lower.Contains("count", StringComparison.Ordinal)
               || lower.Contains("quantos", StringComparison.Ordinal)
               || lower.Contains("quantas", StringComparison.Ordinal)
               || lower.Contains("média", StringComparison.Ordinal)
               || lower.Contains(" mean ", StringComparison.Ordinal)
               || lower.StartsWith("mean ", StringComparison.Ordinal)
               || lower.Contains("mean?", StringComparison.Ordinal)
               || lower.Contains("median", StringComparison.Ordinal)
               || lower.Contains("soma", StringComparison.Ordinal)
               || lower.Contains("agreg", StringComparison.Ordinal)
               || lower.Contains("aggregate", StringComparison.Ordinal)
               || lower.Contains("histogram", StringComparison.Ordinal)
               || lower.Contains("distrib", StringComparison.Ordinal)
               || lower.Contains("temperatura", StringComparison.Ordinal)
               || lower.Contains("temperature", StringComparison.Ordinal)
               || lower.Contains("torque", StringComparison.Ordinal)
               || lower.Contains(" max ", StringComparison.Ordinal)
               || lower.StartsWith("max ", StringComparison.Ordinal)
               || lower.Contains(" max?", StringComparison.Ordinal)
               || lower.Contains(" min ", StringComparison.Ordinal)
               || lower.StartsWith("min ", StringComparison.Ordinal)
               || lower.Contains(" min?", StringComparison.Ordinal)
               || lower.Contains("max:", StringComparison.Ordinal)
               || lower.Contains("min:", StringComparison.Ordinal);
    }

    private static bool LooksTrivialGreeting(string lower)
    {
        var trimmed = lower.Trim();
        if (trimmed.Length <= 24 && ContainsAny(trimmed, "hello", "hi", "hey", "ola", "olá", "bom dia", "boa tarde", "boa noite", "thanks", "thank you", "obrigado", "obrigada"))
        {
            return true;
        }

        return false;
    }

    private static AgentDispatchPlan BuildOrderedPlan(HashSet<AgentSpecialistKind> roles, string reasonPrefix)
    {
        var steps = new List<AgentDispatchStep>();

        if (roles.Contains(AgentSpecialistKind.Discovery))
        {
            steps.Add(new AgentDispatchStep(
                AgentSpecialistKind.Discovery,
                $"{reasonPrefix}:structural_context",
                ParallelGroup: 0,
                ExpectsDatasetQueryEvidence: false));
        }

        var parallelGroup = roles.Contains(AgentSpecialistKind.Discovery) ? 1 : 0;
        foreach (var specialist in new[]
                 {
                     AgentSpecialistKind.QueryAnalysis,
                     AgentSpecialistKind.FailureAnalysis,
                     AgentSpecialistKind.Reporting
                 })
        {
            if (roles.Contains(specialist))
            {
                steps.Add(new AgentDispatchStep(specialist, $"{reasonPrefix}:{specialist}", parallelGroup));
            }
        }

        return new AgentDispatchPlan(steps, string.Empty, UsedLlmPlanner: false);
    }

    private static bool MatchesReporting(string lower)
    {
        return ContainsAny(
            lower,
            "executive",
            "relatório",
            "relatorio",
            "report",
            "síntese executiva",
            "sintese executiva",
            "management summary",
            "board summary",
            "example analytics",
            "analysis examples",
            "exemplos de análise",
            "exemplos de analise");
    }

    private static bool MatchesFailure(string lower)
    {
        return ContainsAny(
            lower,
            "failure",
            "failures",
            "falha",
            "falhas",
            "failed",
            "healthy vs",
            "vs healthy",
            "cohort",
            "coorte",
            "taxa de falha",
            "failure rate",
            "manutenção",
            "manutencao",
            "maintenance pattern");
    }

    private static bool MatchesDiscovery(string lower)
    {
        return ContainsAny(
            lower,
            "schema",
            "coluna",
            "column",
            "columns",
            "campos",
            "field",
            "fields",
            "tipo da coluna",
            "tipos",
            "distinct",
            "distintos",
            "cardinalidade",
            "cardinality",
            "profile",
            "perfil",
            "domínio",
            "dominio",
            "describe dataset",
            "descreva o dataset",
            "dataset structure",
            "estrutura",
            "summarize the dataset",
            "summarize this dataset",
            "summarize dataset",
            "dataset summary",
            "resuma o dataset",
            "resumo do dataset");
    }

    private static bool MatchesQueryAnalysis(string lower)
    {
        return ContainsAny(
            lower,
            "filter",
            "filtro",
            "where ",
            "aggregate",
            "agreg",
            "group by",
            "agrup",
            "histogram",
            "distrib",
            "distribution",
            "compare subsets",
            "compare ",
            "compar",
            "subset",
            "max ",
            " min ",
            "máximo",
            "maximo",
            "mínimo",
            "minimo",
            "média",
            "media",
            "mean",
            "median",
            "median",
            "count",
            "quantos",
            "quantas",
            "soma",
            "total",
            "range",
            "faixa",
            "extrem",
            "query rows",
            "linhas",
            "rows");
    }

    private static bool LooksGenericDataQuestion(string lower)
    {
        return ContainsAny(
            lower,
            "dataset",
            "dados",
            "data",
            "análise",
            "analise",
            "analyze",
            "insight",
            "metric",
            "métrica",
            "metrica");
    }

    private static bool ContainsAny(string haystack, params string[] needles)
    {
        foreach (var needle in needles)
        {
            if (haystack.Contains(needle, StringComparison.Ordinal))
            {
                return true;
            }
        }

        return false;
    }

}
