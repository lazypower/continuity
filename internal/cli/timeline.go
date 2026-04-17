package cli

import (
	"encoding/json"
	"fmt"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/lazypower/continuity/internal/hooks"
	"github.com/spf13/cobra"
)

var (
	timelineDays    int
	timelineProject string
)

var timelineCmd = &cobra.Command{
	Use:   "timeline",
	Short: "Show temporal view of session history",
	Long: `Display session clusters, gaps, and project rhythm over time.

Shows when you've been active, how sessions cluster, and where the gaps are.
Useful for orienting on projects you haven't touched in a while.`,
	RunE: runTimeline,
}

func init() {
	timelineCmd.Flags().IntVar(&timelineDays, "days", 90, "Number of days to look back")
	timelineCmd.Flags().StringVar(&timelineProject, "project", "", "Filter to a specific project")
}

type timelineSession struct {
	Project   string `json:"project"`
	StartedAt int64  `json:"started_at"`
	ToolCount int    `json:"tool_count"`
	Tone      string `json:"tone"`
}

func (s timelineSession) startTime() time.Time {
	return time.UnixMilli(s.StartedAt)
}

type sessionCluster struct {
	Start    time.Time
	End      time.Time
	Sessions int
	Tools    int
}

func runTimeline(cmd *cobra.Command, args []string) error {
	client := hooks.NewClient()
	if !client.Healthy() {
		return fmt.Errorf("continuity server is not running — start it with: continuity serve")
	}

	sinceMs := time.Now().AddDate(0, 0, -timelineDays).UnixMilli()
	data, err := client.Get(fmt.Sprintf("/api/timeline?since=%d", sinceMs))
	if err != nil {
		return fmt.Errorf("timeline: %w", err)
	}

	var sessions []timelineSession
	if err := json.Unmarshal(data, &sessions); err != nil {
		return fmt.Errorf("parse timeline: %w", err)
	}

	if len(sessions) == 0 {
		fmt.Printf("No sessions in the last %d days.\n", timelineDays)
		return nil
	}

	// Group by project, filtering out empty sessions (import artifacts, extraction calls)
	byProject := map[string][]timelineSession{}
	for _, s := range sessions {
		if s.ToolCount == 0 {
			continue
		}
		if timelineProject != "" && !strings.HasSuffix(s.Project, timelineProject) {
			continue
		}
		name := s.Project
		if name == "" {
			name = "unknown"
		} else {
			name = filepath.Base(name)
		}
		byProject[name] = append(byProject[name], s)
	}

	if len(byProject) == 0 {
		fmt.Printf("No sessions matching project %q in the last %d days.\n", timelineProject, timelineDays)
		return nil
	}

	// Sort projects by most recent session
	type projectSummary struct {
		Name     string
		Sessions []timelineSession
		LastAt   int64
	}
	var projects []projectSummary
	for name, sess := range byProject {
		last := sess[len(sess)-1].StartedAt
		projects = append(projects, projectSummary{name, sess, last})
	}
	sort.Slice(projects, func(i, j int) bool {
		return projects[i].LastAt > projects[j].LastAt
	})

	now := time.Now()

	for _, proj := range projects {
		clusters := clusterSessions(proj.Sessions)
		totalTools := 0
		for _, s := range proj.Sessions {
			totalTools += s.ToolCount
		}

		daySpan := int(now.Sub(proj.Sessions[0].startTime()).Hours()/24) + 1
		fmt.Printf("\n%s (%d sessions, %d tools, %d days)\n", proj.Name, len(proj.Sessions), totalTools, daySpan)

		for i, c := range clusters {
			bar := renderBar(c.Sessions)
			dateRange := c.Start.Format("Jan 02")
			if c.Start.Format("Jan 02") != c.End.Format("Jan 02") {
				dateRange = c.Start.Format("Jan 02") + "-" + c.End.Format("02")
			}

			fmt.Printf("  %-12s %s  %d sessions, %d tools\n", dateRange, bar, c.Sessions, c.Tools)

			// Show gap to next cluster
			if i < len(clusters)-1 {
				gap := clusters[i+1].Start.Sub(c.End)
				if gap.Hours() > 48 {
					fmt.Printf("  %s gap: %d days\n", strings.Repeat(" ", 12), int(gap.Hours()/24))
				}
			}
		}

		// Gap from last cluster to now
		lastCluster := clusters[len(clusters)-1]
		gapToNow := now.Sub(lastCluster.End)
		if gapToNow.Hours() > 48 {
			fmt.Printf("  %s last active: %d days ago\n", strings.Repeat(" ", 12), int(gapToNow.Hours()/24))
		}
	}

	fmt.Println()
	return nil
}

// clusterSessions groups sessions that are within 24 hours of each other.
func clusterSessions(sessions []timelineSession) []sessionCluster {
	if len(sessions) == 0 {
		return nil
	}

	var clusters []sessionCluster
	current := sessionCluster{
		Start:    sessions[0].startTime(),
		End:      sessions[0].startTime(),
		Sessions: 1,
		Tools:    sessions[0].ToolCount,
	}

	for _, s := range sessions[1:] {
		t := s.startTime()
		if t.Sub(current.End).Hours() <= 24 {
			// Same cluster
			current.End = t
			current.Sessions++
			current.Tools += s.ToolCount
		} else {
			// New cluster
			clusters = append(clusters, current)
			current = sessionCluster{
				Start:    t,
				End:      t,
				Sessions: 1,
				Tools:    s.ToolCount,
			}
		}
	}
	clusters = append(clusters, current)
	return clusters
}

// renderBar creates a simple ASCII density bar.
func renderBar(sessions int) string {
	blocks := sessions
	if blocks > 8 {
		blocks = 8
	}
	return strings.Repeat("█", blocks) + strings.Repeat("░", 8-blocks)
}
