package constants

import (
	"regexp"
	"testing"
)

func TestTimeFormat(t *testing.T) {
	// Check that a time zone component exists in the global time format.
	re := regexp.MustCompile("^.*0700$")
	if !re.MatchString(TimeFormatYearSecondsTZ) {
		t.Fatal("Unexpected time format - missing time zone component.")
	}
	// Check that the global regexp can match constant TimeFormatYearSeconds.
	re = regexp.MustCompile(TimeFormatYearSecondsRegex)
	if !re.MatchString(TimeFormatYearSeconds) {
		t.Fatal("Mismatch between TimeFormatYearSeconds and regexp in constant TimeFormatYearSecondsRegex.")
	}
}
