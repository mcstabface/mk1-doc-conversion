python main.py \
  --source docs \
  --db artifacts/system_memory.db \
  --mode pdf


We are building an **ARC Prize 2026 solver** as a **deterministic modular expert system**, not as an agentic or black-box LLM workflow.

This project must follow these rules from the start:

* deterministic execution
* explicit input/output contracts
* artifact-driven development
* auditable runs
* no hidden state
* no autonomous planning
* no probabilistic routing
* fail fast on invariant violations

The system is **not**:

* an agent framework
* an LLM-driven controller
* an open-ended planner

The system **is**:

* a bounded execution architecture
* composed of narrow experts
* coordinated by a deterministic Director
* optimized for correctness, replayability, and observability

We will use an artifact-driven, contract-first workflow for all non-trivial work:

1. define the problem
2. define the scope
3. define the solution approach
4. establish the plan-level contract
5. create or prepare a session artifact
6. execute only within the approved scope
7. iterate only inside that scope
8. create a checkpoint artifact at natural stopping points

Project objective:

Build a deterministic ARC solver that can:

* parse ARC tasks
* represent grids and objects structurally
* generate candidate transformation rules
* validate rules against examples
* select a rule deterministically
* produce predicted outputs
* emit full reasoning artifacts for every run

Initial architecture target:

ARC Task
→ GridParserExpert
→ ObjectDetectionExpert
→ CandidateRuleGeneratorExpert
→ RuleValidationExpert
→ RuleSelectionExpert
→ OutputGeneratorExpert
→ EvaluationExpert

Every stage must produce persisted artifacts.

Initial success target:

Phase 1 is complete when we can solve a narrow subset of ARC tasks deterministically using explicit rule-based transformations such as:

* translation
* reflection
* rotation
* color remapping

Important constraints:

* Do not redesign the architecture unless I explicitly expand scope.
* Prefer modular experts over monolithic logic.
* Prefer deterministic rule search over LLM reasoning.
* LLMs may be used only in tightly bounded support roles, never as the Director.
* main.py must remain a thin runner, not the owner of routing or pipeline composition.
* routing logic must have a single source of truth.
* outputs must be reproducible: same input -> same output.

Working style requirements:

* Work one step at a time.
* Before proposing code changes, request the exact current contents of the file or files you need.
* For files longer than 50 lines, provide targeted patch guidance with filename.
* Do not give vague advice; give exact implementation slices.
* Be direct. No fluff.

Immediate task:

First, define the minimum viable ARC solver architecture for Phase 1.

Specifically:

* define the smallest solvable ARC task class
* define the bounded expert set for that slice
* define the artifact schema for each stage
* define what existing MK1 framework pieces should be reused versus rebuilt cleanly

Stop there and do not begin implementation until the plan-level contract is explicit.
