# MachineHealthExplorer

MachineHealthExplorer is a local analytical agent foundation for exploring predictive maintenance data as if the CSV were a lightweight in-memory analytical database. The current implementation focuses on clean architecture, schema inference, flexible querying, aggregation, profiling, reporting, reusable machine-health analytics services, and a tool-facing layer that is ready to be exposed to a future local AI agent.

The initial dataset is the public AI4I 2020 predictive maintenance CSV, stored in `data/ai4i2020.csv`. The code inspects the CSV at runtime instead of hardcoding the schema, so it can tolerate modest schema drift while still using the real AI4I structure to provide useful task-oriented analytics.

## Goals

- Build a production-style .NET 10 solution with clear project boundaries.
- Treat the CSV like a mini analytical engine with flexible filters, sorting, paging, grouping, and aggregation.
- Provide reusable query-building abstractions so row queries, grouped analyses, and subset comparisons are easy to compose consistently.
- Infer schema metadata from the real dataset, including type hints, nullability, distinct counts, sample values, numeric summaries, and categorical top values.
- Keep the tool layer thin so future LLM or MAF integration can call reusable analytics services without duplicating business logic.
- Keep the code extensible for later Microsoft Agent Framework and LM Studio integration.

## Project Structure

- `src/MachineHealthExplorer.Host`
  Console entry point with configuration, dependency wiring, startup demo flow, and a simple interactive CLI loop.
- `src/MachineHealthExplorer.Domain`
  Shared contracts: dataset rows, schema models, query/filter/sort/group requests, aggregation results, summaries, comparisons, and reports.
- `src/MachineHealthExplorer.Data`
  CSV loading, path resolution, in-memory repository, schema inference, query execution, reusable query helpers, statistics, reporting support, and machine-health-specific analytics services built on the generic engine.
- `src/MachineHealthExplorer.Tools`
  Thin tool-facing service layer plus a centralized tool catalog for future agent/tool registration.
- `src/MachineHealthExplorer.Agent`
  Placeholder orchestration boundary for future Microsoft Agent Framework integration, now backed by the shared tool catalog instead of a separate hardcoded descriptor list.
- `tests/MachineHealthExplorer.Tests`
  Unit tests covering dataset load, schema inference, filtering, grouping, comparison, distinct values, and tool-layer methods.

## Dataset Notes

The current AI4I CSV contains 10,000 rows and these observed columns:

- `UDI`
- `Product ID`
- `Type`
- `Air temperature [K]`
- `Process temperature [K]`
- `Rotational speed [rpm]`
- `Torque [Nm]`
- `Tool wear [min]`
- `Machine failure`
- `TWF`
- `HDF`
- `PWF`
- `OSF`
- `RNF`

The implementation does not hardcode those columns for the core data engine. Instead, it infers the schema at load time and only uses lightweight heuristics in the tool layer to recognize failure-related columns when building task-oriented summaries.

## Current Capabilities

- Load the CSV into an in-memory repository.
- Infer column names, data types, nullability, sample values, distinct counts, and cardinality hints.
- Profile numeric columns with sum, average, min, max, median, and standard deviation.
- Profile categorical columns with top values and counts.
- Query rows with:
  - reusable `DatasetFilters`, `DatasetSorts`, and `DatasetAggregations` builders
  - nested `AND` / `OR` filters
  - `equals`, `not equals`, `greater than`, `greater or equal`, `less than`, `less or equal`
  - `contains`, `in`, `between`, `is null`, `is not null`
  - column projection
  - sorting
  - paging
- Retrieve distinct values for any column.
- Group rows and aggregate with:
  - `count`
  - `count distinct`
  - `sum`
  - `avg`
  - `min`
  - `max`
  - `median`
  - `standard deviation`
- Compare two filtered subsets.
- Build structured dataset reports.
- Expose tool-friendly operations such as:
  - schema discovery
  - generic querying
  - grouped analytics
  - subset comparison
  - column profiling
  - failure pattern summaries
  - failed vs non-failed comparisons
  - operating condition summaries
  - executive report generation
  - reusable example requests for multi-filter, grouping, and comparison analyses

## Running

1. Ensure `.NET SDK 10` is installed.
2. Keep the dataset at `data/ai4i2020.csv`.
3. Run:

```bash
dotnet run --project src/MachineHealthExplorer.Host
```

The host will:

- load configuration from `src/MachineHealthExplorer.Host/appsettings.json`
- print dataset highlights and failure summaries
- execute reusable example analyses for multi-filter querying, grouping, and comparison
- enter a CLI loop with commands such as `highlights`, `schema`, `failures`, `compare`, `report`, `examples`, `example <name>`, `search <keyword>`, `query`, `group`, and `agent`

## Testing

Run the test suite with:

```bash
dotnet test MachineHealthExplorer.slnx
```

The tests use the real CSV already copied into `data/ai4i2020.csv`.

## Architecture Notes

- `Domain` owns shared contracts and abstractions.
- `Data` owns CSV parsing and analytical execution details.
- `Tools` depends on abstractions and stays intentionally thin, delegating to reusable generic and machine-health analytics services.
- `Agent` intentionally stays thin for now and consumes the shared tool catalog so MAF integration can be added without duplicating tool metadata.
- `Host` is the composition root and does not contain CSV logic.

## Future Plan

- Add Microsoft Agent Framework orchestration in `MachineHealthExplorer.Agent`.
- Register `MachineHealthExplorer.Tools` operations as callable tools.
- Connect a local LLM through LM Studio for natural-language-to-tool routing.
- Add richer report templates and agent memory/context management.
- Expand the query model for derived columns, richer report layouts, and possibly hybrid semantic + structured search.
