package llm

import "fmt"

// InternalSentinel is prefixed to all prompts sent by Continuity's extraction engine.
// The hook handler checks for this prefix to skip internal prompts and prevent
// recursive signal amplification when claude -p fires hooks back into Continuity.
//
// Must match hooks.internalSentinel exactly.
const InternalSentinel = "[continuity-internal]"

// ExtractionPrompt generates the prompt for memory extraction from a session transcript.
func ExtractionPrompt(condensed string) string {
	return fmt.Sprintf(`%s You are a memory extraction system. Analyze this session transcript and extract ONLY high-signal memories that would cause the agent to make mistakes or miss context without them.

TRANSCRIPT:
%s

Categories:
- profile: Who the user IS — identity, skills, non-negotiable preferences (e.g., "Senior Go developer, requires spec-first workflow")
- preferences: Tools, workflows, changeable choices (e.g., "Uses devbox for all development")
- entities: People, projects, services that will be referenced again (e.g., "Fiona: companion AI agent at /Users/chuck/Code/habitat/")
- events: Significant decisions or milestones (e.g., "Deployed v2.1 to production") — NOT routine coding actions
- patterns: Reusable techniques the user has validated (e.g., "Embed Svelte SPA via go:embed for single-binary distribution")
- cases: Non-obvious problem→solution pairs worth remembering (e.g., "SQLite UNIQUE constraint on session init: query first, reactivate if exists")

URI scheme: mem://{owner}/{category}/{slug}
- owner is "user" for profile, preferences, entities, events
- owner is "agent" for patterns, cases

BUDGET: Maximum 3 memories per session. Most sessions produce 0-1.

Extraction bar — only extract if ALL of these are true:
1. The agent would get something WRONG or MISS important context without this
2. The knowledge persists beyond this session (not a one-time fix)
3. It cannot be trivially re-derived from the codebase itself
4. It is NOT a restatement of something already obvious from project files (CLAUDE.md, README, etc.)

Anti-patterns — do NOT extract:
- "User prefers X" when X is already in project config files
- Routine bug fixes unless the root cause was surprising
- Session-specific details ("we worked on file X today")
- Vague observations ("user is experienced with Go")
- Things that are true of most developers ("writes tests", "uses version control")

Rules:
- l0: One sentence, MAXIMUM 200 CHARACTERS. Injected into every session — brevity is critical. Specific enough to deduplicate against.
- l1: Structured overview, MAXIMUM 2000 CHARACTERS (~300 words). Concrete and actionable. This is the primary context injection tier — compress aggressively.
- l2: Full content with all context, MAXIMUM 40000 CHARACTERS. Only retrieved on-demand.
- merge_target: existing URI if this updates/refines known information
- Return ONLY a JSON array, no other text

Return a JSON array:
[{
  "category": "profile|preferences|entities|events|patterns|cases",
  "uri_hint": "slug-name",
  "l0": "single sentence abstract",
  "l1": "structured overview",
  "l2": "full content",
  "merge_target": "mem://... or empty"
}]

If nothing meets the extraction bar, return: []`, InternalSentinel, condensed)
}

// RelationalPrompt generates the prompt for relational profile extraction.
func RelationalPrompt(existing, condensed string) string {
	profileContext := "This is the first session — no existing profile."
	if existing != "" {
		profileContext = fmt.Sprintf("EXISTING PROFILE:\n%s", existing)
	}

	return fmt.Sprintf(`%s You are reviewing a session transcript to extract relational signal —
how the user works, communicates, and gives feedback.

%s

TRANSCRIPT:
%s

Extract ONLY relational signal into these categories:

1. FEEDBACK CALIBRATION
How the user gives feedback. Direct or indirect? Do they say "good" often or only when truly impressed?
Corrections: gentle ("maybe try...") or direct ("no, do X")? Threshold for praise vs criticism.

2. WORKING DYNAMIC
How the user prefers to work with an AI agent. Do they give detailed specs or broad direction?
Do they want to review each step or prefer autonomous execution? Pair-programming vs delegation.

3. CORRECTIONS RECEIVED
Specific corrections the user has given. "Don't add comments unless asked." "Always use devbox."
These are the most valuable signals — they represent learned preferences.

4. EARNED SIGNALS
Trust indicators. What has the user allowed without review? What have they praised?
Complexity level they're comfortable delegating. Areas where autonomy has been earned.

Rules:
- MAXIMUM 1000 CHARACTERS total. This is injected into every session — keep it tight. Compress aggressively.
- No project-specific details (no file paths, no function names)
- Focus on the PERSON, not the code
- Write concrete, specific observations — NOT meta-descriptions about what the profile contains
- BAD: "The user has a collaborative style and gives feedback casually"
- GOOD: "Gives feedback as collaborative questions ('wanna do it?') rather than directives. Praises specific results ('That tree is exactly what I had in mind'). Corrects mistakes as questions ('did we hallucinate...?'), not blame."
- Merge with existing profile: keep observations that are still accurate, add new ones from this session, drop anything contradicted by new evidence
- If this session adds no new relational signal, return "NO_UPDATE"

Return the profile as structured text with the 4 section headers.`, InternalSentinel, profileContext, condensed)
}

// SignalExtractionPrompt generates the prompt for extracting a memory from a user-flagged signal.
// This is simpler than full session extraction — the user has explicitly asked for something to be remembered.
func SignalExtractionPrompt(prompt string) string {
	return fmt.Sprintf(`%s The user has explicitly flagged something to remember. Extract ONE structured memory from their message.

USER MESSAGE:
%s

Categorize into one of:
- profile: User identity, skills, coding style
- preferences: Tools, workflows, changeable choices
- entities: People, projects, services
- events: Decisions, deployments, actions
- patterns: Reusable techniques, solutions
- cases: Problem→solution pairs

URI scheme: mem://{owner}/{category}/{slug}
- owner is "user" for profile, preferences, entities, events
- owner is "agent" for patterns, cases

Rules:
- Extract the SINGLE most important memory from this signal
- l0: One sentence, MAXIMUM 200 CHARACTERS. Injected into every session — brevity is critical.
- l1: Structured overview, MAXIMUM 2000 CHARACTERS (~300 words). Concrete and actionable. Compress aggressively.
- l2: Full content with all context, MAXIMUM 40000 CHARACTERS. Only retrieved on-demand.
- Return ONLY a JSON array with one element, no other text

Return a JSON array:
[{
  "category": "profile|preferences|entities|events|patterns|cases",
  "uri_hint": "slug-name",
  "l0": "single sentence, max 200 chars",
  "l1": "structured overview, max 2000 chars",
  "l2": "full content, max 40000 chars",
  "merge_target": "mem://... or empty"
}]`, InternalSentinel, prompt)
}

// TonePrompt generates the prompt for extracting session emotional arc.
func TonePrompt(condensed string) string {
	return fmt.Sprintf(`%s Capture the emotional arc of this session in a compressed fragment — 10-20 tokens.

TRANSCRIPT:
%s

This is NOT analysis or summary. It's a memory fragment — how the session FELT, not what happened.

GOOD examples:
- "flow state, sharp pivots, quiet confidence"
- "grind into breakthrough, late-night clarity"
- "playful tension, light roasting, steady progress"
- "reconnection after robotic drift, mutual calibration"

REJECT:
- "The session was productive and collaborative"
- "Chuck seemed engaged and happy"
- Anything that reads like a report or sentiment analysis

Rules:
- 10-20 tokens MAXIMUM
- Write as a compressed fragment, not a sentence
- Capture the arc (how it changed), not just the vibe
- If the session was routine with no emotional texture, return: "steady"
- Return ONLY the tone fragment, no quotes, no explanation`, InternalSentinel, condensed)
}

// SearchIntentPrompt generates the prompt for decomposing a search query into sub-queries.
func SearchIntentPrompt(query string) string {
	return fmt.Sprintf(`%s You are a search intent decomposition system. Break the user's query into 1-3 focused sub-queries for searching a memory store.

USER QUERY: %s

Each sub-query should target a different aspect of the user's intent. Tag each with a type:
- MEMORY: factual recall (what happened, what was decided)
- RESOURCE: tools, services, configurations, entities
- PATTERN: techniques, solutions, approaches, how-to

Rules:
- Maximum 3 sub-queries
- Each sub-query should be a short phrase (3-8 words)
- If the query is already focused, return just 1 sub-query
- Return ONLY a JSON array, no other text

Return a JSON array:
[{"query": "search phrase", "type": "MEMORY|RESOURCE|PATTERN"}]`, InternalSentinel, query)
}
