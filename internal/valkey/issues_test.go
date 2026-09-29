package valkey

import "testing"

// TestConsistencyIssuesMapping pins the bounded (kind, resolution) mapping
// behind hookrelay_consistency_issues_total, with invalid dedup and
// dead-letter records reported once even though they also hold readiness
// as unhandled; due leases and retries are work, not inconsistencies.
func TestConsistencyIssuesMapping(t *testing.T) {
	rep := ReconcileReport{
		Findings: map[string]int{
			"repaired": 2, "drained": 1, "blocked": 1, "already_blocked": 1, "due_lease": 1, "due_retry": 2,
			"unhandled": 4, "dedup_skipped": 2, "dlq_invalid": 1, "dlq_message_missing": 3, "dlq_orphans_removed": 2, "dlq_restored": 1, "dedup_expired_removed": 4, "dedup_restored": 5,
			"dedup_orphans_removed": 6, "counter_repaired": 1, "counter_unverified": 0,
		},
		BlockReasons: map[string]int{"head_state_missing": 1},
	}
	got := map[[2]string]int{}
	for _, i := range rep.ConsistencyIssues() {
		got[[2]string{i.Kind, i.Resolution}] = i.Count
	}
	want := map[[2]string]int{
		{"derived_index_drift", "repaired"}:       2,
		{"stale_index_entry", "removed"}:          1,
		{"head_state_missing", "blocked"}:         1,
		{"existing_block", "kept"}:                1,
		{"dedup_record_invalid", "held"}:          2,
		{"dead_letter_record_invalid", "held"}:    1,
		{"dead_letter_message_missing", "held"}:   3,
		{"dead_letter_index_orphan", "removed"}:   2,
		{"dead_letter_index_missing", "restored"}: 1,
		{"unhandled_state", "held"}:               1,
		{"dedup_record_expired", "removed"}:       4,
		{"dedup_index_missing", "restored"}:       5,
		{"dedup_index_orphan", "removed"}:         6,
		{"queued_counter_drift", "repaired"}:      1,
	}
	if len(got) != len(want) {
		t.Errorf("issues = %v", got)
	}
	for k, n := range want {
		if got[k] != n {
			t.Errorf("%v = %d, want %d", k, got[k], n)
		}
	}
	if len((ReconcileReport{Findings: map[string]int{}}).ConsistencyIssues()) != 0 {
		t.Error("a clean pass reports issues")
	}
}
