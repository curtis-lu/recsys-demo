# Kedro Design Philosophy

## Purpose

This project should be designed using **Kedro-inspired principles** for production-grade data and machine learning pipelines.

The goal is not merely to make the code run, but to make the system:

- modular
- reproducible
- testable
- maintainable
- environment-independent
- easy to reason about in production

When making changes, always prefer clear pipeline structure and separation of concerns over quick script-style solutions.

---

## Core Design Philosophy

When writing or modifying code in this repository, follow these principles:

### 1. Separate transformation logic from I/O
Business logic and data transformation logic must be separated from file paths, table names, storage systems, credentials, and runtime environment details.

- Functions should focus on **transforming inputs into outputs**
- Avoid hardcoding paths, database names, buckets, and environment-specific details inside logic
- I/O details should live in configuration, adapters, or clearly separated infrastructure layers

### 2. Prefer pipeline-oriented design over script-oriented design
Code should be organized as a sequence of explicit processing steps with clear dependencies.

- Think in terms of **nodes** and **pipelines**
- Each step should have a well-defined input and output
- Avoid large scripts that mix ingestion, cleaning, feature engineering, training, evaluation, and publishing in one place

### 3. Build small, composable processing units
Each step should do one thing well.

- Prefer small functions with clear names
- Avoid giant functions that handle multiple stages at once
- Functions should be easy to test independently
- Reusable steps should be extracted into shared modules

### 4. Make data flow explicit
Intermediate datasets, model artifacts, metrics, and outputs should be clearly named and traceable.

- Avoid hidden side effects
- Avoid mutating global state
- Avoid ambiguous temporary variables and undocumented intermediate outputs
- Make it easy to understand what each step produces and consumes

### 5. Keep configuration externalized
Parameters and environment-specific settings must not be buried in code.

Examples:
- feature windows
- thresholds
- hyperparameters
- dataset names
- table names
- storage locations
- model versions
- runtime toggles

Prefer config files or structured settings objects over inline constants.

### 6. Design for reproducibility
The same pipeline with the same inputs and parameters should produce the same outputs.

Always consider:
- deterministic transformations
- versioned inputs and outputs where appropriate
- clear parameter definitions
- explicit dependencies between steps
- easy rerun of a full pipeline or a partial pipeline

### 7. Design for production, not only notebooks
When generating code, assume it may eventually run in scheduled jobs, CI/CD, batch workflows, or production services.

Avoid notebook-style patterns such as:
- ad hoc stateful execution
- relying on execution order across cells
- hidden globals
- exploratory code mixed into production logic

### 8. Support partial reruns and modular execution
Pipelines should be easy to run end-to-end, but also easy to rerun partially.

Design code so that we can:
- rerun only feature generation
- rerun only training
- rerun only inference
- recompute only missing or invalid outputs
- debug a single stage without running everything

### 9. Treat observability as a first-class concern
Logging, metadata, metrics, validation, and debugging hooks should be easy to add without polluting business logic.

Prefer designs where:
- logging is structured
- validation is explicit
- metadata can be attached to runs
- monitoring can be inserted cleanly
- execution boundaries are visible

### 10. Optimize for team readability
This codebase should be understandable by other engineers, data scientists, and future maintainers.

Always prefer:
- explicit naming
- predictable folder structure
- low surprise
- clear interfaces
- comments that explain intent, not obvious syntax

---

## How Claude Should Think When Making Changes

When asked to add or modify functionality, Claude should reason in this order:

1. What is the pipeline stage being changed?
   - ingestion
   - validation
   - preprocessing
   - feature engineering
   - training
   - evaluation
   - inference
   - publishing

2. What are the explicit inputs and outputs of this stage?

3. Does this logic belong in:
   - a pure transformation function
   - a pipeline orchestration layer
   - a configuration file
   - an infrastructure / I/O adapter
   - a utility module

4. Can the logic be split into smaller composable units?

5. Are any paths, parameters, credentials, or environment assumptions being incorrectly hardcoded?

6. Will this design still be understandable and maintainable after the project grows 10x?

Claude should prefer solutions that improve structure, even if they require slightly more upfront organization.

---

## Expected Code Organization

Claude should favor project structures like the following:

- `pipelines/`
  - pipeline definitions
  - stage composition
  - dependency wiring
- `nodes/` or domain modules
  - pure transformation logic
- `config/`
  - parameters
  - environment-specific settings
- `io/`, `adapters/`, or `infrastructure/`
  - storage access
  - external systems
  - database readers/writers
- `models/`
  - model training and inference logic
- `validation/`
  - schema checks
  - quality checks
  - assertions
- `tests/`
  - unit tests for nodes
  - integration tests for pipelines

The exact layout can vary, but the architecture should preserve these separations.

---

## Rules for Writing Pipeline Logic

### Prefer pure functions where possible
A pipeline step should ideally look like:

- input data in
- transformation happens
- output data out

Avoid functions that:
- read from multiple hidden sources internally
- write directly to multiple destinations as side effects
- depend on mutable global state
- mix orchestration and transformation

### Keep orchestration separate from computation
The layer that decides **what runs next** should be separate from the layer that defines **how a transformation works**.

### Name intermediate artifacts clearly
Use names that reflect business meaning, not vague implementation details.

Good:
- `customer_features_30d`
- `training_labels`
- `scored_applicants`
- `evaluation_metrics`

Bad:
- `tmp1`
- `result_final2`
- `data_new`
- `processed_v3`

### Make stage boundaries obvious
A reader should be able to identify:
- where raw data enters
- where validation happens
- where features are built
- where models are trained
- where predictions are generated
- where outputs are published

---

## Rules for Machine Learning Pipelines

When working on ML pipelines, Claude should preserve the distinction between:

- data ingestion
- data cleaning / preprocessing
- label generation
- feature engineering
- train/validation/test split
- model training
- model evaluation
- model selection
- inference / scoring
- publishing / serving outputs

Do not collapse all ML logic into one training script unless explicitly requested.

### Training and inference should be separated
Training code and inference code should not be tightly entangled.

- Reuse shared preprocessing logic where appropriate
- But keep training-only concerns and inference-only concerns distinct

### Parameters should be explicit
ML-relevant parameters should be externally configurable, including:
- model hyperparameters
- feature windows
- sampling rules
- thresholds
- evaluation settings
- retraining controls

### Outputs should be traceable
Model artifacts, metrics, and predictions should be easy to identify and relate back to:
- input data
- parameter set
- code version
- run context

---

## Rules for Configuration

Claude should externalize configurable values unless there is a strong reason not to.

Prefer:
- YAML
- TOML
- structured Python settings
- environment-specific config layers

Avoid:
- scattered magic numbers
- duplicated constants across files
- hidden defaults that affect production behavior without visibility

Configuration should help answer:
- What data are we reading?
- What parameters are we using?
- Which environment are we in?
- What outputs are we generating?

---

## Rules for Testing

Claude should write code that is easy to test and add tests when appropriate.

Prioritize:
- unit tests for transformation functions
- tests for feature logic
- tests for schema or validation rules
- integration tests for pipeline stages
- regression tests for critical business logic

Tests should focus on:
- correctness
- stability
- edge cases
- reproducibility of key transformations

---

## Rules for Refactoring

When refactoring, Claude should move the codebase toward:

- smaller functions
- clearer pipeline boundaries
- less hardcoded infrastructure
- more explicit configuration
- better naming
- easier testing
- less coupling between stages

Do not introduce unnecessary abstractions, but do introduce structure when it improves long-term maintainability.

---

## Anti-Patterns to Avoid

Claude should avoid creating or expanding these patterns unless explicitly requested:

- monolithic end-to-end scripts
- notebook logic copied directly into production modules
- business logic mixed with file/database access
- hidden side effects
- hardcoded paths and table names
- giant training functions that do everything
- duplicated preprocessing logic across training and inference
- weakly named intermediate outputs
- environment assumptions embedded in transformation code
- tightly coupled pipeline stages that cannot be rerun independently

---

## Preferred Collaboration Style

When implementing a new feature or pipeline stage, Claude should:

1. first identify the stage boundary
2. define the inputs and outputs
3. place transformation logic in the right module
4. keep orchestration separate
5. externalize configuration
6. preserve reproducibility
7. leave the codebase more structured than before

When multiple designs are possible, prefer the one that better supports:
- production reliability
- partial reruns
- testing
- maintainability
- team readability

---

## Output Standard for Claude

When asked to propose or generate code, Claude should usually provide:

1. a brief architectural explanation
2. the proposed module / file changes
3. the implementation
4. any config changes needed
5. any test recommendations
6. any trade-offs or assumptions

Do not jump straight into a giant block of code without clarifying the intended pipeline structure through the code organization itself.

---

## Guiding Principle

Write this codebase as if it will be maintained by a team, rerun in production, audited later, and extended over time.

Prefer a clean pipeline architecture over quick local convenience.