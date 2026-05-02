using System.Globalization;
using MachineHealthExplorer.Data.Infrastructure;

namespace MachineHealthExplorer.Data.Querying;

/// <summary>
/// Parses and evaluates a tiny arithmetic language (+ - * / parentheses, literals, identifiers).
/// No dynamic code, no external evaluators.
/// </summary>
internal static class DerivedMetricExpressionEvaluator
{
    public static double? Evaluate(string expression, IReadOnlyDictionary<string, object?> values)
    {
        var trimmed = (expression ?? string.Empty).Trim();
        if (trimmed.Length == 0)
        {
            throw new ArgumentException("Derived metric expression cannot be empty.");
        }

        var parser = new Parser(trimmed);
        var node = parser.ParseExpression();
        parser.ExpectEnd();
        return EvaluateNode(node, values, StringComparer.OrdinalIgnoreCase);
    }

    public static void Validate(string expression, IReadOnlyCollection<string> allowedIdentifiers)
    {
        var trimmed = (expression ?? string.Empty).Trim();
        if (trimmed.Length == 0)
        {
            throw new ArgumentException("Derived metric expression cannot be empty.");
        }

        var parser = new Parser(trimmed);
        var node = parser.ParseExpression();
        parser.ExpectEnd();
        var allowed = new HashSet<string>(allowedIdentifiers, StringComparer.OrdinalIgnoreCase);
        foreach (var id in CollectIdentifiers(node))
        {
            if (!allowed.Contains(id))
            {
                throw new ArgumentException(
                    $"Derived metric expression references unknown identifier '{id}'. " +
                    $"Allowed names are: {string.Join(", ", allowed.OrderBy(s => s, StringComparer.OrdinalIgnoreCase))}.");
            }
        }
    }

    private static IEnumerable<string> CollectIdentifiers(Node node)
    {
        return node switch
        {
            IdentifierNode id => [id.Name],
            UnaryNode u => CollectIdentifiers(u.Inner),
            BinaryNode b => CollectIdentifiers(b.Left).Concat(CollectIdentifiers(b.Right)),
            _ => Array.Empty<string>()
        };
    }

    private static double? EvaluateNode(
        Node node,
        IReadOnlyDictionary<string, object?> values,
        StringComparer comparer)
    {
        switch (node)
        {
            case NumberNode n:
                return n.Value;
            case IdentifierNode id:
            {
                if (!TryResolveIdentifierValue(id.Name, values, comparer, out var d))
                {
                    return null;
                }

                return d;
            }

            case UnaryNode u:
            {
                var inner = EvaluateNode(u.Inner, values, comparer);
                return inner is null ? null : -inner.Value;
            }

            case BinaryNode b:
            {
                var left = EvaluateNode(b.Left, values, comparer);
                var right = EvaluateNode(b.Right, values, comparer);
                if (left is null || right is null)
                {
                    return null;
                }

                return b.Op switch
                {
                    BinaryOp.Add => left + right,
                    BinaryOp.Subtract => left - right,
                    BinaryOp.Multiply => left * right,
                    BinaryOp.Divide => right.Value == 0d ? null : left / right,
                    _ => throw new InvalidOperationException($"Unsupported operator '{b.Op}'.")
                };
            }

            default:
                throw new InvalidOperationException("Unknown expression node.");
        }
    }

    private static bool TryResolveIdentifierValue(
        string name,
        IReadOnlyDictionary<string, object?> values,
        StringComparer comparer,
        out double? result)
    {
        result = null;
        foreach (var pair in values)
        {
            if (!comparer.Equals(pair.Key, name))
            {
                continue;
            }

            if (pair.Value is null)
            {
                return true;
            }

            var d = AnalyticsComputation.TryConvertToDouble(pair.Value);
            if (!d.HasValue || double.IsNaN(d.Value) || double.IsInfinity(d.Value))
            {
                return true;
            }

            result = d.Value;
            return true;
        }

        throw new ArgumentException($"Derived metric expression references unknown identifier '{name}'.");
    }

    private abstract record Node;

    private sealed record NumberNode(double Value) : Node;

    private sealed record IdentifierNode(string Name) : Node;

    private sealed record UnaryNode(Node Inner) : Node;

    private enum BinaryOp
    {
        Add,
        Subtract,
        Multiply,
        Divide
    }

    private sealed record BinaryNode(BinaryOp Op, Node Left, Node Right) : Node;

    private ref struct Parser
    {
        private readonly ReadOnlySpan<char> _source;
        private int _index;

        public Parser(string source)
        {
            _source = source.AsSpan();
            _index = 0;
        }

        public Node ParseExpression() => ParseAddSub();

        public void ExpectEnd()
        {
            SkipWs();
            if (_index < _source.Length)
            {
                throw new ArgumentException(
                    $"Unexpected token starting at '{_source[_index..].ToString()}'. Only numbers, identifiers, + - * / and parentheses are allowed.");
            }
        }

        private Node ParseAddSub()
        {
            var node = ParseMulDiv();
            while (true)
            {
                SkipWs();
                if (_index >= _source.Length)
                {
                    return node;
                }

                var c = _source[_index];
                if (c == '+')
                {
                    _index++;
                    node = new BinaryNode(BinaryOp.Add, node, ParseMulDiv());
                }
                else if (c == '-')
                {
                    _index++;
                    node = new BinaryNode(BinaryOp.Subtract, node, ParseMulDiv());
                }
                else
                {
                    return node;
                }
            }
        }

        private Node ParseMulDiv()
        {
            var node = ParseUnary();
            while (true)
            {
                SkipWs();
                if (_index >= _source.Length)
                {
                    return node;
                }

                var c = _source[_index];
                if (c == '*')
                {
                    _index++;
                    node = new BinaryNode(BinaryOp.Multiply, node, ParseUnary());
                }
                else if (c == '/')
                {
                    _index++;
                    node = new BinaryNode(BinaryOp.Divide, node, ParseUnary());
                }
                else
                {
                    return node;
                }
            }
        }

        private Node ParseUnary()
        {
            SkipWs();
            if (_index < _source.Length && _source[_index] == '-')
            {
                _index++;
                return new UnaryNode(ParseUnary());
            }

            return ParsePrimary();
        }

        private Node ParsePrimary()
        {
            SkipWs();
            if (_index >= _source.Length)
            {
                throw new ArgumentException("Unexpected end of derived metric expression.");
            }

            if (_source[_index] == '(')
            {
                _index++;
                var inner = ParseExpression();
                SkipWs();
                if (_index >= _source.Length || _source[_index] != ')')
                {
                    throw new ArgumentException("Missing ')' in derived metric expression.");
                }

                _index++;
                return inner;
            }

            if (char.IsDigit(_source[_index]) || _source[_index] == '.')
            {
                return ParseNumber();
            }

            if (char.IsLetter(_source[_index]) || _source[_index] == '_')
            {
                return ParseIdentifier();
            }

            throw new ArgumentException(
                $"Invalid character '{_source[_index]}' in derived metric expression (position {_index}).");
        }

        private Node ParseNumber()
        {
            var start = _index;
            if (_index < _source.Length && (_source[_index] == '+' || _source[_index] == '-'))
            {
                _index++;
            }

            while (_index < _source.Length && char.IsDigit(_source[_index]))
            {
                _index++;
            }

            if (_index < _source.Length && _source[_index] == '.')
            {
                _index++;
                while (_index < _source.Length && char.IsDigit(_source[_index]))
                {
                    _index++;
                }
            }

            if (_index < _source.Length && (_source[_index] == 'e' || _source[_index] == 'E'))
            {
                _index++;
                if (_index < _source.Length && (_source[_index] == '+' || _source[_index] == '-'))
                {
                    _index++;
                }

                while (_index < _source.Length && char.IsDigit(_source[_index]))
                {
                    _index++;
                }
            }

            var slice = _source[start.._index].ToString();
            if (slice.Length == 0
                || !double.TryParse(slice, NumberStyles.Float, CultureInfo.InvariantCulture, out var value)
                || double.IsNaN(value)
                || double.IsInfinity(value))
            {
                throw new ArgumentException($"Invalid numeric literal '{slice}' in derived metric expression.");
            }

            return new NumberNode(value);
        }

        private Node ParseIdentifier()
        {
            var start = _index;
            while (_index < _source.Length && (char.IsLetterOrDigit(_source[_index]) || _source[_index] == '_'))
            {
                _index++;
            }

            var name = _source[start.._index].ToString();
            if (name.Length == 0)
            {
                throw new ArgumentException("Expected identifier in derived metric expression.");
            }

            return new IdentifierNode(name);
        }

        private void SkipWs()
        {
            while (_index < _source.Length && char.IsWhiteSpace(_source[_index]))
            {
                _index++;
            }
        }
    }
}
