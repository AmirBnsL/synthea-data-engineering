# LLM Agentic Workflow — Synthea Module Authoring (Phase 1.8)

## Architecture

```
┌─────────────────────────────────────────────────────────────────────────┐
│   INPUT: Clinical Guideline Document (PDF/txt) + Target Disease Name     │
│   e.g., "Parkinson's Disease Progression, 2023 AAN Guidelines"           │
│   (User-provided or retrieved from PubMed/NIH)                           │
└──────────────────────────┬──────────────────────────────────────────────┘
                           │
┌──────────────────────────▼──────────────────────────────────────────────┐
│              STAGE 1 — EXTRACT DISEASE PROGRESSION                       │
│                                                                          │
│  LLM call: "Parse this clinical guideline into a structured state        │
│  machine. Extract: disease stages, typical ages, durations,              │
│  symptoms, ICD-10/SNOMED-CT codes, medications, dosages,                 │
│  transition probabilities, branching conditions (age/gender)."          │
│                                                                          │
│  Output (Pydantic-validated): StructuredDiseaseProgression               │
│  ┌──────────────────────────────────────────────────────────┐           │
│  │ {                                                         │           │
│  │   "disease": "Parkinson's Disease",                       │           │
│  │   "stages": [                                             │           │
│  │     { "name": "Early", "min_age": 50, "max_age": 70,     │           │
│  │       "duration": {"mean": 3, "unit": "years"},          │           │
│  │       "symptoms": ["tremor", "bradykinesia"],            │           │
│  │       "icd10": "G20",                                     │           │
│  │       "next": [{"stage": "Moderate", "prob": 0.85}] },   │           │
│  │     ...                                                   │           │
│  │   ]                                                       │           │
│  │ }                                                         │           │
│  └──────────────────────────────────────────────────────────┘           │
└──────────────────────────┬──────────────────────────────────────────────┘
                           │
┌──────────────────────────▼──────────────────────────────────────────────┐
│              STAGE 2 — GENERATE SYNTHEA MODULE JSON                      │
│                                                                          │
│  Prompt = structured_progression + few_shot_examples + GMF schema        │
│                                                                          │
│  Few-shot examples embedded in prompt:                                  │
│  • modules/acute_myeloid_leukemia.json (complex branching, chemo)       │
│  • modules/congestive_heart_failure.json (exacerbation cycles)          │
│  • modules/sepsis.json (rapid multi-organ progression)                  │
│                                                                          │
│  LLM generates complete Synthea GMF module with:                        │
│  • State types: Initial, Terminal, ConditionOnset, MedicationOrder,    │
│    Observation, Procedure, Encounter, Delay, Death, SetAttribute,       │
│    CarePlanStart, CarePlanEnd                                           │
│  • Transitions: direct_transition, distributed_transition,              │
│    complex_transition (age, gender, attribute, active medication)        │
│  • Valid LOINC, SNOMED-CT, RxNorm, ICD-10 codes                         │
│                                                                          │
│  Uses structured output mode (tool_use / json_schema) for valid JSON.   │
│                                                                          │
│  Output: Complete module JSON (Pydantic-validated)                       │
└──────────────────────────┬──────────────────────────────────────────────┘
                           │
┌──────────────────────────▼──────────────────────────────────────────────┐
│              STAGE 3 — VALIDATE                                          │
│                                                                          │
│  LAYER A — Structural (Pydantic)                                        │
│  ├─ Valid JSON syntax?                                                   │
│  ├─ All state types recognized?                                          │
│  ├─ All transitions reference existing state names?                      │
│  ├─ No orphan states? (states with no path to/from)                      │
│  ├─ All code systems valid? (ICD-10-CM, SNOMED-CT, LOINC, RxNorm)       │
│  ├─ Numeric ranges sensible? (age 0–120, labs in clinical range)        │
│  └─ Terminal state reachable?                                            │
│                                                                          │
│  LAYER B — Runtime (Synthea JAR)                                        │
│  ├─ java -jar synthea.jar -m module.json -p 100                         │
│  ├─ Parse stdout/stderr for ERROR lines                                  │
│  ├─ Detect dead states (states never entered)                            │
│  ├─ Verify patients reach Terminal                                       │
│  └─ Check all expected codes appear in output                            │
│                                                                          │
│  Output: ValidationReport(valid, errors, warnings, dead_states, ...)    │
└──────────────────────────┬──────────────────────────────────────────────┘
                           │
              ┌────────────▼────────────┐
              │  All checks pass?       │
              └──────┬────────┬─────────┘
                     │ YES    │ NO
                     ▼        ▼
┌────────────────┐  ┌───────────────────────────────────────────┐
│ FINAL OUTPUT   │  │       STAGE 4 — REFINE                     │
│ validated      │  │                                             │
│ module JSON    │  │ LLM call with original module + errors:    │
│                │  │ "Fix the following issues:                  │
│                │  │   1. Dead state: Check_Fever (unreachable)  │
│                │  │   2. Invalid RxNorm code: 199885           │
│                │  │   3. Encounter missing before Medication"   │
│                │  │                                             │
│                │  │ LangGraph tracks attempt count.             │
│                │  │ Max 5 refinement rounds, then best effort.  │
│                │  │                                             │
│                │  └──────────┬──────────────────────────────────┘
│                │             │ loop back to Stage 3
│                ▼             ▼
│         ┌──────────────────────────┐
│         │  Final module + report   │
│         │  (human-reviewable)      │
│         └──────────────────────────┘
└────────────────┘
```

## LangGraph Agent Flow

```
from langgraph.graph import StateGraph, END

class AuthoringState(TypedDict):
    guideline_doc:      str
    disease_name:       str
    attempt:            int          # current refinement round
    progression:        dict | None  # Stage 1 output
    module_json:        dict | None  # Stage 2 output
    validation_report:  dict | None  # Stage 3 output
    is_valid:           bool
    final_output:       str | None

graph = StateGraph(AuthoringState)

graph.add_node("extractor",     extract_progression)    # Stage 1
graph.add_node("generator",     generate_module)        # Stage 2
graph.add_node("validator",     validate_module)        # Stage 3
graph.add_node("refiner",       refine_module)          # Stage 4

graph.set_entry_point("extractor")
graph.add_edge("extractor", "generator")
graph.add_edge("generator", "validator")

# Conditional: if valid → save, else → refine (max 5 attempts)
graph.add_conditional_edges(
    "validator",
    decide_next,
    {
        "valid":   "save_output",
        "refine":  "refiner",
        "abort":   "save_output"   # best effort after max attempts
    }
)

graph.add_edge("refiner", "validator")     # loop back

graph.compile()
```

## Tool Stack

| Tool | Purpose |
|------|---------|
| **LangGraph** | Stateful DAG orchestration with loops, conditional branching |
| **Anthropic Claude / OpenAI GPT-4o** | LLM backend — structured output (tool_use / json_schema) |
| **Pydantic v2** | Define Synthea GMF as Python types; validate Stage 1 + 2 output |
| **Synthea JAR** (`bin/synthea-with-dependencies.jar`) | Runtime validation: run module with `-m` flag, check output |
| **LangChain** (optional) | Document loaders: `PyPDFLoader`, `WebBaseLoader` for guidelines |

## Pydantic Schemas

```python
from pydantic import BaseModel, Field
from typing import Literal, Optional

# ── Stage 1: Disease Progression ──

class StageTransition(BaseModel):
    target: str = Field(description="Target stage name")
    probability: float = Field(ge=0.0, le=1.0)

class DiseaseStage(BaseModel):
    name: str
    min_age: Optional[int] = Field(default=None, ge=0, le=120)
    max_age: Optional[int] = Field(default=None, ge=0, le=120)
    duration_mean: Optional[float] = Field(default=None, ge=0)
    duration_unit: Literal["days", "weeks", "months", "years"] = "years"
    icd10_code: Optional[str] = None
    snomed_code: Optional[str] = None
    symptoms: list[str] = Field(default_factory=list)
    transitions: list[StageTransition] = Field(default_factory=list)

class StructuredDiseaseProgression(BaseModel):
    disease_name: str
    stages: list[DiseaseStage]


# ── Stage 2: Synthea GMF Module ──

class StateType(str, Enum):
    INITIAL = "Initial"
    TERMINAL = "Terminal"
    CONDITION_ONSET = "ConditionOnset"
    MEDICATION_ORDER = "MedicationOrder"
    OBSERVATION = "Observation"
    PROCEDURE = "Procedure"
    ENCOUNTER = "Encounter"
    DELAY = "Delay"
    DEATH = "Death"
    SET_ATTRIBUTE = "SetAttribute"
    CAREPLAN_START = "CarePlanStart"
    CAREPLAN_END = "CarePlanEnd"

class Code(BaseModel):
    system: str           # e.g., "SNOMED-CT", "ICD-10-CM", "LOINC", "RxNorm"
    code: str | int
    display: str

class Distribution(BaseModel):
    transition: str
    distribution: float = Field(ge=0.0, le=1.0)

class Condition(BaseModel):
    condition_type: str   # "Gender", "Age", "Active Medication", "Attribute"
    gender: Optional[str] = None
    attribute: Optional[str] = None
    operator: Optional[str] = None

class ComplexTransition(BaseModel):
    condition: Condition
    distributions: list[Distribution]

class SyntheaState(BaseModel):
    type: StateType
    codes: Optional[list[Code]] = None
    direct_transition: Optional[str] = None
    distributed_transition: Optional[list[Distribution]] = None
    complex_transition: Optional[list[ComplexTransition]] = None
    chronic: Optional[bool] = None
    assign_to_attribute: Optional[str] = None
    target_encounter: Optional[str] = None

class SyntheaModule(BaseModel):
    name: str
    states: dict[str, SyntheaState]
```

## Validation Strategy

### Layer A — Structural (Pydantic + custom checks)

| Check | What it catches |
|-------|-----------------|
| JSON parse | Syntax errors |
| Pydantic field validation | Missing/extra fields, wrong types |
| Transition target check | State references non-existent state |
| Reachability analysis | Orphan states (never entered) |
| Dead state detection | Terminal unreachable from any path |
| Code system check | `system` not in allowed list |
| Numeric sanity check | Age > 120, lab ranges outside clinical bounds |

### Layer B — Runtime (Synthea JAR)

```python
import subprocess, json

def validate_runtime(module_json: dict, output_dir: str) -> dict:
    module_path = os.path.join(output_dir, "_test_module.json")
    with open(module_path, "w") as f:
        json.dump(module_json, f)

    result = subprocess.run(
        ["java", "-jar", "/opt/airflow/bin/synthea-with-dependencies.jar",
         "-m", module_path, "-p", "50", "--exporter.baseDirectory", output_dir],
        capture_output=True, text=True, timeout=120
    )

    errors = [l for l in result.stderr.split("\n") if "ERROR" in l]
    dead_states = extract_dead_states(result.stdout)
    patients_generated = count_patients(result.stdout)
    reachable = "Terminal" in result.stdout

    return {
        "errors": errors,
        "dead_states": dead_states,
        "patients_generated": patients_generated,
        "terminal_reachable": reachable,
        "success_rate": patients_generated / 50
    }
```

## Refinement Strategy

The `refiner` node receives the original module + validation report and returns a corrected module:

```
Prompt:
"The following Synthea module has validation issues:

[module JSON]

Fix these specific issues:
1. {error_1}
2. {error_2}
...

Return the complete corrected module JSON."
```

Max 5 refinement rounds. LangGraph tracks `attempt` in state:

```
def decide_next(state: AuthoringState) -> str:
    if state.is_valid:
        return "valid"
    elif state.attempt >= 5:
        return "abort"
    else:
        state.attempt += 1
        return "refine"
```

## Integration with Existing Pipeline

```
┌────────────────────────────┐
│  User provides clinical    │
│  guideline PDF for a new   │
│  disease module            │
└──────────┬─────────────────┘
           ▼
┌────────────────────────────┐          ┌──────────────────────────┐
│  1.8 LLM Agentic Workflow  │ ─────►   │  Generated Synthea       │
│  (run on workstation,      │          │  module.json              │
│   not in Docker)           │          └──────────┬───────────────┘
└────────────────────────────┘                     │
                                                   ▼
                                           ┌────────────────────────┐
                                           │  Place in synthea     │
                                           │  modules/ directory   │
                                           │  (inside the JAR or   │
                                           │  mounted volume)      │
                                           └──────────┬────────────┘
                                                      │
                                                      ▼
                                           ┌────────────────────────┐
                                           │  Synthea generates     │
                                           │  patients with the     │
                                           │  new condition         │
                                           └────────────────────────┘
```

### Proposed new files

```
synthea/
├── llm_module_authoring/
│   ├── agent.py           # LangGraph workflow definition
│   ├── schemas.py         # Pydantic models (GMF spec)
│   ├── prompts.py         # System prompts + few-shot examples
│   ├── extractor.py       # Stage 1: guideline → progression
│   ├── generator.py       # Stage 2: progression → module JSON
│   ├── validator.py       # Stage 3: structural + runtime checks
│   ├── refiner.py         # Stage 4: fix issues
│   ├── examples/          # Few-shot module examples from JAR
│   │   ├── acute_myeloid_leukemia.json
│   │   ├── congestive_heart_failure.json
│   │   └── sepsis.json
│   └── output/            # Generated modules (gitignored)
└── requirements-llm.txt   # langchain, langgraph, anthropic, openai
```

## Key Design Decisions

| Decision | Choice | Rationale |
|----------|--------|-----------|
| Orchestration | LangGraph (not raw LLM calls) | Stateful DAG with loops; easy to add human-in-the-loop later |
| Structured output | Tool_use / JSON schema mode | Synthea JSON is complex — free-form generation creates invalid modules 80%+ of the time |
| Validation | Pydantic + Synthea JAR | Structural checks catch format errors; runtime checks catch logic errors (dead states, missing codes) |
| Few-shot strategy | 3 diverse examples | AML (trial branching), CHF (exacerbations), Sepsis (rapid cascade) cover most transition patterns |
| Refinement limit | 5 rounds | Beyond this, LLM tends to regress rather than improve |
| Execution location | Workstation (not Docker) | LLM API calls need internet; no GPU needed; no Docker dependency for module authoring |
