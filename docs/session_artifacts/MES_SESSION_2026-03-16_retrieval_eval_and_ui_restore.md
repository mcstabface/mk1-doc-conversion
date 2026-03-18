```
\# MES Development Session Artifact
```

```
`Date: 2026-03-16  `

`System: Modular Expert System (MES) document retrieval pipeline  `

`Corpus: Enron Email Dataset (~517k documents)`

`---`

`\# Session Objective`

`Stabilize the retrieval pipeline, add measurable evaluation, and restore the demo UI.`

`Primary goals achieved:`

`- Query rewrite expert integrated`

`- Retrieval evaluation harness created`

`- Retrieval scoring diagnostics exposed`

`- Streamlit UI restored`

`- Deterministic evidence-grounded answer generation improved`

`---`

`\# Major System Changes`

`\#\# 1. Query Rewrite Expert`

`Implemented deterministic query rewriting using a small local model.`

`Model used:`
```

qwen2.5:3b-instruct

```
Rewrite behavior:

`- Preserve important tokens`

`- Remove conversational filler`

`- Produce retrieval-friendly phrase`

`Example:`

`User query`
```

What risks did Enron executives discuss regarding energy trading strategies?

```
Rewrite
```

Enron executives: energy trading strategy risks

```
Rewrite metadata returned:
```

preserved\_terms  
added\_terms  
removed\_terms  
rewrite\_rationale

```
---

`\# 2. Query Expansion Layer`

`Expanded queries now include:`

`1. Original user query  `

`2. Rewritten query  `

`3. heuristic expansion variant`

`Example:`
```

\[  
original,  
rewritten,  
original + "risk\_management hedging"  
\]

```
---

`\# 3. Hybrid Retrieval Ranking`

`Ranking components now include:`

`BM25 core score  `

`intent bonus  `

`expansion weight  `

`MMR diversity filter`

`Example debug output:`
```

SCORE: 4.133208  
BM25 CORE SCORE: 6.423208  
INTENT BONUS: -2.29  
EXPANSION WEIGHT: 1.0

```
---

`\# 4. Retrieval Evaluation Harness`

`New tool:`
```

tools/run\_query\_eval.py

```
Purpose:

`Evaluate retrieval behavior across a labeled query set.`

`Input:`
```

artifacts/enron\_full\_v2/query\_eval\_set.json

```
Features:

`- executes queries automatically`

`- prints rewrite used`

`- prints expected ranking targets`

`- heuristic scoring using pattern matching`

`- limits scoring to TOP 5 retrieval results`

`Example output:`
```

VERDICT TARGET: top1=good | top3=good | rewrite\_changed=False  
HEURISTIC: good\_hits=4 bad\_hits=0

```
---

`\# 5. Evaluation Dataset`

`File:`
```

artifacts/enron\_full\_v2/query\_eval\_set.json

```
Contains labeled queries including:

`Human-style query`
```

What risks did Enron executives discuss regarding energy trading strategies?

```
Corpus-shaped query
```

Enron trading risk management price swings weather hedging

```
Fields include:
```

expected\_theme  
good\_result\_patterns  
bad\_result\_patterns  
top1\_expected  
top3\_expected

```
---

`\# 6. Streamlit Demo UI Restoration`

`UI launched with:`
```

streamlit run ui/demo\_app.py

```
Demo shows:

`- deterministic run id`

`- answer`

`- source chunks`

`- ranking evidence`

`- chunk scores`

`Example:`
```

Run ID: run\_20260316\_185819

```
---

`\# 7. Backend Patch`

`\`demo\_backend.py\` updated to return run\_id:`
```

"run\_id": diagnostics.get("run\_id")

```
This fixed the UI's run signature display.

`---`

`\# 8. Answer Layer Improvement`

`\`SearchContextAnswerExpert\` updated to produce:`

`Evidence-grounded summaries instead of raw chunk dumps.`

`Behavior:`

`- extract relevant lines from top chunks`

`- synthesize short summary`

`- list sources`

`Example summary:`
```

Retrieved evidence highlights trading risk tied to price swings and market volatility.  
It also points to weather-driven demand variability as an important source of exposure.  
The strongest chunks describe hedging approaches such as demand swaps and related tools.

```
---

`\# Current System Status`

`Corpus:`
```

Documents: 517,401  
Chunks: 517,401  
Embeddings:874,078

```
Pipeline:
```

query  
→ rewrite  
→ expansion  
→ hybrid retrieval  
→ evidence selection  
→ deterministic answer synthesis  
→ UI presentation

```
All stages currently operational.

`---`

`\# Observed Retrieval Behavior`

`Broad human query:`

`- rewrite improves recall`

`- ranking still occasionally favors biography text`

`Corpus-shaped query:`

`- retrieval quality strong`

`- top results match expected themes`

`Conclusion:`

`Retrieval core functioning correctly; future work will focus on ranking refinement.`

`---`

`\# Next Development Goals`

`1. Improve ranking signal to suppress biography chunks`

`2. Add adversarial evaluation queries`

`3. Strengthen intent scoring`

`4. Improve answer synthesis quality`

`5. Add retrieval diagnostics to UI`

`6. Reduce shell invocation between backend and retrieval engine`

`---`

`\# Known Working Commands`

`Run retrieval directly`
```

python query\_search\_context.py   
--query "Enron trading risk management price swings weather hedging"   
--chunk-root artifacts/enron\_full\_v2/search\_context\_chunks   
--artifact-root artifacts/enron\_full\_v2   
--ranker hybrid

```
Run evaluation
```

python tools/run\_query\_eval.py

```
Run UI
```

streamlit run ui/demo\_app.py

```
---

`\# Session Outcome`

`System now includes:`

`- deterministic query rewrite`

`- measurable retrieval evaluation`

`- evidence grounded answers`

`- working UI demonstration`

`The project has transitioned from prototype behavior to \*\*measurable retrieval engineering.\*\*`
```

