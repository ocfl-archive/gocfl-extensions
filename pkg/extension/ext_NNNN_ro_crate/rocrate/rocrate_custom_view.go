// Custom view/summary objects for consumers of RO-CRATE metadata.
package rocrate

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

const stringSeparator = "; "

// GocflSummary provides a summary compatible with gocfl user's
// expectations for the info.json object.
/*
	-- via RO-CRATE.
	signature       = id
	title           = name
	description     = description
	created         = datePublished
	sets            = @type
	keywords        = keywords
	licenses        = license

	-- output at runtime.
	last_changed    = now()

	-- via GOCFL config.
	organisation_id = user.config
	organisation    = user.config
	user            = user.config
	address         = user.config
*/
type GocflSummary struct {
	// provided by ro-crate.
	Signature   string `json:"signature"`
	Title       string `json:"title"`
	Description string `json:"description"`
	Created     string `json:"created"`
	Sets        string `json:"sets"`
	Keywords    string `json:"keywords"`
	Licenses    string `json:"licenses"`
	// generated at runtime, e.g. time.Now().
	LastChanged string `json:"last_changed"`
	// provided by caller.
	OrganisationID string `json:"organisation_id"`
	Organisation   string `json:"organisation"`
	User           string `json:"user"`
	Address        string `json:"address"`
}

// NewGocflSummary returns an initialized GocflSummary object for maximum safety.
func NewGocflSummary() GocflSummary {
	return GocflSummary{
		Signature:      "",
		Title:          "",
		Description:    "",
		Created:        "",
		Sets:           "",
		Keywords:       "",
		Licenses:       "",
		LastChanged:    "",
		OrganisationID: "",
		Organisation:   "",
		User:           "",
		Address:        "",
	}
}

func newGocflSummary() GocflSummary {
	return NewGocflSummary()
}

// ToJSON serializes GocflSummary to formatted JSON.
func (gocflSummary GocflSummary) ToJSON() ([]byte, error) {
	return json.MarshalIndent(gocflSummary, "", " ")
}

// String provides stringer functions for the gocfl summary object.
func (gocflSummary GocflSummary) String() string {
	b, err := gocflSummary.ToJSON()
	if err != nil {
		return fmt.Sprintf("GocflSummary{Signature: %q, Title: %q}", gocflSummary.Signature, gocflSummary.Title)
	}
	return string(b)
}

// joinStrings simplifies string join functions for us so that we can
// exchange string separator values easily.
func joinStrings(inputString []string) string {
	return strings.Join(inputString, stringSeparator)
}

// currentTime provides a module level approach to replacing the time
// functions, e.g. for testing.
var currentTime = getTime

// getTime provides a helper to return the current time formatted as
// a string.
func getTime(utc bool) string {
	if utc {
		t := time.Now().UTC()
		return t.Format("2006-01-02T15:04:05Z")
	}
	t := time.Now()
	return t.Format("2006-01-02T15:04:05")
}

// GOCFLSummary maps RO-CRATE metadata safely into a GOCFL compatible
// summary object before returning it to the caller.
func (c *Crate) GOCFLSummary() (GocflSummary, error) {
	return c.GOCFLSummaryWithTime(currentTime(true))
}

// GOCFLSummaryWithTime maps RO-CRATE metadata with a specified timestamp.
func (c *Crate) GOCFLSummaryWithTime(timestamp string) (GocflSummary, error) {
	if len(c.Graph) == 0 {
		return GocflSummary{}, fmt.Errorf("ro-crate-metadata.json is empty")
	}
	rootEntity, err := c.RootEntity()
	if err != nil {
		return GocflSummary{}, err
	}
	summary := NewGocflSummary()
	summary.Signature = rootEntity.ID
	if rootEntity.Name != nil {
		name := rootEntity.Name.Value()
		if len(name) > 0 {
			summary.Title = rootEntity.Name.Value()[0]
		}
	}
	if rootEntity.Type != nil {
		summary.Sets = joinStrings(rootEntity.Type.Value())
	}
	if rootEntity.Description != nil {
		summary.Description = joinStrings(rootEntity.Description.Value())
	}
	summary.Created = rootEntity.DatePublished
	if rootEntity.License != nil {
		summary.Licenses = joinStrings(rootEntity.License.StringSlice())
	}
	if rootEntity.Keywords != nil {
		summary.Keywords = joinStrings(rootEntity.Keywords.Value())
	}
	summary.LastChanged = timestamp
	return summary, nil
}
