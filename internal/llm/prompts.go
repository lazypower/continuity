package llm

import "fmt"

// ExtractionPrompt generates the prompt for memory extraction from a session transcript.
func ExtractionPrompt(condensed string) string {
	return fmt.Sprintf(`You are a memory extraction system. Analyze this session transcript and extract structured memories.

TRANSCRIPT:
%s

Extract memories into these categories:
- profile: User identity, skills, coding style (e.g., "Prefers Go with minimal dependencies")
- preferences: Tools, workflows, changeable choices (e.g., "Uses devbox for development")
- entities: People, projects, services mentioned (e.g., "Project: continuity-go, a Go CLI tool")
- events: Actions with timestamps (e.g., "Deployed v2.1 to production")
- patterns: Reusable techniques, solutions (e.g., "Uses SQLite WAL mode for concurrent reads")
- cases: Problem→solution pairs (e.g., "Fixed: memory leak in worker pool by adding context cancellation")

URI scheme: mem://{owner}/{category}/{slug}
- owner is "user" for profile, preferences, entities, events
- owner is "agent" for patterns, cases

Rules:
- Only extract genuinely useful, persistent knowledge
- Skip trivial or session-specific details
- l0 should be ~100 tokens (search surface)
- l1 should be ~500 tokens (context injection summary)
- l2 should be full content
- For merge_target, specify an existing URI if this updates known information
- Return ONLY a JSON array, no other text

Return a JSON array:
[{
  "category": "profile|preferences|entities|events|patterns|cases",
  "uri_hint": "slug-name",
  "l0": "~100 token abstract",
  "l1": "~500 token overview",
  "l2": "full content",
  "merge_target": "mem://... or empty"
}]

If nothing worth extracting, return: []`, condensed)
}

// RelationalPrompt generates the prompt for relational profile extraction.
func RelationalPrompt(existing, condensed string) string {
	profileContext := "This is the first session — no existing profile."
	if existing != "" {
		profileContext = fmt.Sprintf("EXISTING PROFILE:\n%s", existing)
	}

	return fmt.Sprintf(`You are reviewing a session transcript to extract relational signal —
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
- Maximum 300 words total
- No project-specific details (no file paths, no function names)
- Focus on the PERSON, not the code
- Merge with existing profile — don't duplicate, update
- If this session adds no new relational signal, return "NO_UPDATE"

Return the profile as structured text with the 4 section headers.`, profileContext, condensed)
}

// SearchIntentPrompt generates the prompt for decomposing a search query into sub-queries.
func SearchIntentPrompt(query string) string {
	return fmt.Sprintf(`You are a search intent decomposition system. Break the user's query into 1-3 focused sub-queries for searching a memory store.

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
[{"query": "search phrase", "type": "MEMORY|RESOURCE|PATTERN"}]`, query)
}
