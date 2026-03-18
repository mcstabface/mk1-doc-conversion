# MK1 UI Demo Kickoff

## Demo Goal
Build a clean, executive-friendly UI demo for the MK1 Deterministic Retrieval Engine.

Audience:
- Senior Director
- VP leadership

Tone:
- confident
- simple
- non-technical
- no buzzwords
- visually clean

## What the demo should prove
The UI should make one thing obvious:

**MK1 turns enterprise documents into reliable, inspectable answers.**

The demo should emphasize:
- deterministic retrieval
- visible evidence
- structured artifacts
- explainable pipeline behavior
- repeatable results

## What not to do
- do not redesign the architecture
- do not pitch this as an agent
- do not overload the UI with technical internals
- do not use dense dashboards
- do not rely on jargon-heavy AI language

## Demo Story
### 1. The problem
Enterprises run on documents.
But useful answers are buried in files people cannot search reliably.

### 2. What MK1 does
MK1 converts documents into structured retrieval artifacts.
Then it retrieves evidence deterministically and assembles an answer with visible source support.

### 3. Why this matters
This is not a black box chatbot.
This is controlled, inspectable retrieval infrastructure.

## Recommended UI Flow
### Screen 1 — Landing / problem framing
Minimal headline and short value statement.

Suggested headline:
**Reliable answers from enterprise documents**

Suggested subtext:
**Deterministic retrieval with visible evidence and inspectable artifacts.**

### Screen 2 — Ingestion summary
Show that documents become structured assets.

Possible elements:
- document count
- chunk count
- embeddings count
- artifact types created
- simple pipeline visual

Pipeline wording:
Documents → Search Context → Chunks → Embeddings → Retrieval

### Screen 3 — Query experience
Primary interaction:
- query box
- run query button
- ranked source results
- short answer panel
- evidence/source panel

The user should be able to ask a question and immediately see:
- returned sources
- top evidence snippets
- answer generated from the assembled context

### Screen 4 — Why this answer
This is the differentiator.
Show:
- returned sources
- chunk IDs or chunk references
- score or rank
- matched evidence text
- retrieval diagnostics in a clean, readable way

Keep it simple. The goal is trust, not technical overload.

### Screen 5 — Determinism / repeatability
Show that the same query against the same corpus returns the same result.

Suggested framing:
**Same corpus. Same query. Same result.**

Possible supporting elements:
- repeated runs
- identical source order
- identical answer/evidence trace

## UI Design Direction
- white or very light background
- dark text
- bold typography
- large spacing
- minimal borders
- few colors
- one accent color only
- simple cards
- no clutter

## Suggested Components
- hero header
- corpus stats cards
- query input bar
- answer card
- ranked results list
- evidence panel
- artifact/diagnostics drawer
- simple pipeline strip

## Must-have demo behaviors
- load a known dataset cleanly
- submit one or two strong demo queries
- display answer + evidence + source list
- show deterministic repeatability
- keep response flow fast and visually obvious

## Strong demo query themes
Use queries that visibly prove retrieval quality.
Example categories:
- named entity query
- event query
- relationship query
- evidence-backed question

## Build Priorities
### Phase 1
- single-page UI
- load corpus summary
- query input
- ranked results
- evidence panel

### Phase 2
- deterministic repeat-run panel
- retrieval diagnostics drawer
- artifact viewer

### Phase 3
- polished presentation mode
- simplified executive framing copy

## Constraints
- preserve deterministic MES architecture
- preserve artifact-driven design
- preserve observability
- do not add agentic behavior
- do not hardcode dataset names in reusable code

## Working Mode for the next chat
In the next chat, focus on building the UI prototype only.
Work in small, safe steps.
Prefer minimal viable implementation first.
Default to a clean demoable single-page experience.

## Immediate ask for the next chat
Design and build an executive demo UI for MK1 that:
- is visually clean
- is easy to demo live
- shows query → answer → evidence
- highlights deterministic retrieval
- can be implemented quickly without redesigning the backend
