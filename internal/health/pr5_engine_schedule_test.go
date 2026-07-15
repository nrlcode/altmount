package health

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type pr5FakeClock struct {
	now time.Time
}

func (c *pr5FakeClock) Now() time.Time { return c.now }
func (c *pr5FakeClock) Advance(d time.Duration) {
	c.now = c.now.Add(d)
}

func TestPR5BackgroundHealthRetryScheduleUsesFourDurableStages(t *testing.T) {
	clock := &pr5FakeClock{now: time.Unix(1_800_000_000, 0).UTC()}
	want := []time.Duration{30 * time.Second, 2 * time.Minute, 10 * time.Minute, time.Hour}

	for attempt, delay := range want {
		due, ok := nextHealthRetryAt(clock.Now(), attempt)
		require.Truef(t, ok, "attempt %d unexpectedly exhausted", attempt)
		assert.Equal(t, clock.Now().Add(delay), due)
	}
	_, ok := nextHealthRetryAt(clock.Now(), len(want))
	assert.False(t, ok, "temporary retry exhaustion must stop this staged series")
}

func TestPR5ImportSecondPassHasSeparateThirtySecondSchedule(t *testing.T) {
	clock := &pr5FakeClock{now: time.Unix(1_800_000_100, 0).UTC()}
	firstPassCompletedAt := clock.Now()
	due := importSecondPassDueAt(firstPassCompletedAt, 30*time.Second)
	assert.Equal(t, firstPassCompletedAt.Add(30*time.Second), due)

	// Import confirmation is a single durable wait between two complete
	// passes. It must not be advanced through the background-health retry
	// sequence after 30 seconds.
	clock.Advance(30 * time.Second)
	assert.True(t, importSecondPassDue(clock.Now(), due))
	assert.Equal(t, firstPassCompletedAt.Add(2*time.Minute), mustHealthRetryDue(t, firstPassCompletedAt, 1))
}

func TestPR5PersistentGapNeedsTwoConfirmationsTenMinutesApart(t *testing.T) {
	clock := &pr5FakeClock{now: time.Unix(1_800_001_000, 0).UTC()}
	first := clock.Now()

	assert.True(t, confirmationEligible(nil, first, 10*time.Minute),
		"the first all-provider conclusive observation starts the lifecycle")
	clock.Advance(10*time.Minute - time.Nanosecond)
	assert.False(t, confirmationEligible(&first, clock.Now(), 10*time.Minute),
		"repeated evidence inside the minimum delay must not increment confirmation")
	clock.Advance(time.Nanosecond)
	assert.True(t, confirmationEligible(&first, clock.Now(), 10*time.Minute),
		"the second independent confirmation is eligible at the exact boundary")
}

func TestPR5GapRevalidationUsesAbsoluteDayOneThreeSevenFourteenMilestones(t *testing.T) {
	confirmedAt := time.Unix(1_800_002_000, 0).UTC()
	wantAges := []time.Duration{24 * time.Hour, 3 * 24 * time.Hour, 7 * 24 * time.Hour, 14 * 24 * time.Hour}

	for milestone, age := range wantAges {
		due, ok := nextGapRevalidationAt(confirmedAt, milestone)
		require.Truef(t, ok, "milestone %d unexpectedly absent", milestone)
		assert.Equal(t, confirmedAt.Add(age), due,
			"milestones must remain anchored to confirmation, not drift from the prior attempt")
	}
	_, ok := nextGapRevalidationAt(confirmedAt, len(wantAges))
	assert.False(t, ok, "a conclusive day-14 result must leave the gap dormant")
}

func TestPR5TemporaryRevalidationDoesNotAdvanceAbsoluteMilestone(t *testing.T) {
	confirmedAt := time.Unix(1_800_003_000, 0).UTC()
	dayThree, ok := nextGapRevalidationAt(confirmedAt, 1)
	require.True(t, ok)

	milestone := advanceGapRevalidationMilestone(1, observationOutcomeTemporary)
	assert.Equal(t, 1, milestone,
		"temporary/incomplete work cannot consume a conclusive aging milestone")
	retriedDue, ok := nextGapRevalidationAt(confirmedAt, milestone)
	require.True(t, ok)
	assert.Equal(t, dayThree, retriedDue)

	milestone = advanceGapRevalidationMilestone(1, observationOutcomeHardAbsent)
	assert.Equal(t, 2, milestone)
	daySeven, ok := nextGapRevalidationAt(confirmedAt, milestone)
	require.True(t, ok)
	assert.Equal(t, confirmedAt.Add(7*24*time.Hour), daySeven)
}

func mustHealthRetryDue(t *testing.T, at time.Time, attempt int) time.Time {
	t.Helper()
	due, ok := nextHealthRetryAt(at, attempt)
	require.True(t, ok)
	return due
}
