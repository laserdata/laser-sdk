package laser

import (
	"testing"
	"time"
)

// Mirrors sdk/src/agent/consumer.rs's own
// given_each_warm_dedup_policy_when_deciding_refills_then_should_leave_out_only_what_it_names.
func TestGivenEachWarmDedupPolicyWhenDecidingRefillsThenShouldLeaveOutOnlyWhatItNames(t *testing.T) {
	now := time.Now()
	past := now.Add(-time.Hour)
	future := now.Add(time.Hour)
	namedExpired := map[warmDedupPosition]struct{}{{partition: 0, offset: 1}: {}}

	noDeadline := warmDedupSeed{partition: 0, offset: 0}
	unexpired := warmDedupSeed{partition: 0, offset: 2, deadline: &future}
	namedByDeadLetter := warmDedupSeed{partition: 0, offset: 1, deadline: &past}
	expiredButNotNamed := warmDedupSeed{partition: 0, offset: 3, deadline: &past}

	cases := []struct {
		name     string
		policy   WarmDedupPolicy
		expired  map[warmDedupPosition]struct{}
		seed     warmDedupSeed
		wantKept bool
	}{
		{"no deadline always refills, DeadLetters", WarmDedupDeadLetters, namedExpired, noDeadline, true},
		{"unexpired refills, DeadLetters", WarmDedupDeadLetters, namedExpired, unexpired, true},
		{"named by a dead letter is left out, DeadLetters", WarmDedupDeadLetters, namedExpired, namedByDeadLetter, false},
		{"expired but not named still refills, DeadLetters", WarmDedupDeadLetters, namedExpired, expiredButNotNamed, true},
		{"expired with no usable scan falls back to Unexpired", WarmDedupDeadLetters, nil, expiredButNotNamed, false},
		{"unexpired refills, Unexpired", WarmDedupUnexpired, nil, unexpired, true},
		{"expired is left out, Unexpired", WarmDedupUnexpired, nil, expiredButNotNamed, false},
		{"Unexpired ignores a scan it never asked for", WarmDedupUnexpired, namedExpired, expiredButNotNamed, false},
		{"expired still refills, All", WarmDedupAll, nil, expiredButNotNamed, true},
		{"named by a dead letter still refills, All", WarmDedupAll, namedExpired, namedByDeadLetter, true},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			got := warmDedupRefills(testCase.policy, testCase.seed, testCase.expired, now)
			if got != testCase.wantKept {
				t.Errorf("want refills=%v, got %v", testCase.wantKept, got)
			}
		})
	}
}
