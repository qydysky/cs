// SPDX-License-Identifier: MIT

package ranker

import (
	"crypto/sha256"
	"encoding/hex"
	"sort"

	"github.com/boyter/cs/v3/pkg/common"
)

// ComputeMatchHash returns a SHA-256 hex digest of the concatenated matched
// byte regions in fj, sorted by position. Returns "" if there are no match
// locations or no content.
func ComputeMatchHash(fj *common.FileJob) string {
	if len(fj.MatchLocations) == 0 || len(fj.Content) == 0 {
		return ""
	}

	// Collect all [start, end] spans
	var spans [][]int
	for _, locs := range fj.MatchLocations {
		for _, loc := range locs {
			if len(loc) < 2 {
				continue
			}
			spans = append(spans, loc)
		}
	}

	// Sort by start position, then by end position
	sort.Slice(spans, func(i, j int) bool {
		if spans[i][0] == spans[j][0] {
			return spans[i][1] < spans[j][1]
		}
		return spans[i][0] < spans[j][0]
	})

	h := sha256.New()
	for _, span := range spans {
		start, end := span[0], span[1]
		if start < 0 {
			start = 0
		}
		if end > len(fj.Content) {
			end = len(fj.Content)
		}
		if start >= end {
			continue
		}
		h.Write(fj.Content[start:end])
	}

	return hex.EncodeToString(h.Sum(nil))
}

// DeduplicateResults groups results by their MatchHash (computed if not already
// set), keeps the first result in each group (highest-scored, assuming the
// input is already sorted by score descending), and populates DuplicateCount
// and DuplicateLocations on the representative.
func DeduplicateResults(results []*common.FileJob) []*common.FileJob {
	r := NewDeduplicateResultsT()
	for i := 0; i < len(results); i++ {
		r.Add(results[i])
	}
	return r.Fin()
}

type group struct {
	representative *common.FileJob
	locations      []string
}

type DeduplicateResultsT struct {
	seen map[string]*group
}

func NewDeduplicateResultsT() *DeduplicateResultsT {
	return &DeduplicateResultsT{seen: map[string]*group{}}
}

func (t *DeduplicateResultsT) Add(fj *common.FileJob) {
	if fj.MatchHash == "" {
		fj.MatchHash = ComputeMatchHash(fj)
	}
	hash := fj.MatchHash
	if hash == "" {
		// No hash means no match content - keep as-is (unique)
		hash = "[[no-hash]]:" + fj.Location
	}
	if g, ok := t.seen[hash]; ok {
		g.locations = append(g.locations, fj.Location)
		g.representative.DuplicateCount = len(g.locations)
		g.representative.DuplicateLocations = g.locations
	} else {
		t.seen[hash] = &group{representative: fj}
	}
}

func (t *DeduplicateResultsT) Fin() []*common.FileJob {
	out := make([]*common.FileJob, 0, len(t.seen))
	for _, g := range t.seen {
		out = append(out, g.representative)
	}
	return out
}
