package rocrate

import (
	"encoding/json"
	"fmt"
	"io"
)

// StringerError provides a standard error for our Stringer functions.
const StringerError = "cannot convert to string"

// stringSlice represents a slice of strings or an individual string in
// json. In unmarshalling if we encounter an individual string then we
// convert this to a slice containing a single string.
type stringSlice []string

// Value allows us to access the stringSlice data.
func (sSlice stringSlice) Value() []string {
	return []string(sSlice)
}

// isBlank indicates if a stringSlice has content.
func (sSlice stringSlice) isBlank() bool {
	return len(sSlice.Value()) == 0
}

// String provides a stringer interface for stringSlice.
func (sSlice stringSlice) String() string {
	ret, err := json.Marshal(sSlice.Value())
	if err != nil {
		return fmt.Sprintf("%s: %s", StringerError, err)
	}
	return string(ret)
}

// UnmarshalJSON unmarshals a stringSlice from an RO-CRATE JSON-LD file.
func (sSlice *stringSlice) UnmarshalJSON(data []byte) error {
	var singleString string
	if err := json.Unmarshal(data, &singleString); err == nil {
		*sSlice = []string{singleString}
		return nil
	}
	var multipleStrings []string
	if err := json.Unmarshal(data, &multipleStrings); err == nil {
		*sSlice = multipleStrings
		return nil
	}
	return fmt.Errorf("cannot unmarshal into []string: %s", string(data))
}

// nodeIdentifier provides a structure to unmarshal RO-CRATE nodes.
type nodeIdentifier struct {
	ID string `json:"@id"`
}

// isBlank returns true if a node identifier has no ID value.
func (nIdentifier nodeIdentifier) isBlank() bool {
	return nIdentifier.ID == ""
}

// String provides a stringer interface for nodeIdentifier.
func (nIdentifier nodeIdentifier) String() string {
	ret, err := json.Marshal(nIdentifier)
	if err != nil {
		return fmt.Sprintf("%s: %s", StringerError, err)
	}
	return string(ret)
}

// nodeSlice represents a slice of nodes in an RO-CRATE JSON-LD file.
// In unmarshalling if we encounter an individual node then we convert
// this to a slice containing a single node.
type nodeSlice []nodeIdentifier

// Value allows us to access nodeSlice data.
func (nSlice nodeSlice) Value() []nodeIdentifier {
	return []nodeIdentifier(nSlice)
}

// String provides a stringer interface for nodeSlice.
func (nSlice nodeSlice) String() string {
	ret, err := json.Marshal(nSlice.Value())
	if err != nil {
		return fmt.Sprintf("%s: %s", StringerError, err)
	}
	return string(ret)
}

// StringSlice returns a string slice of the IDs in the nodeSlice.
func (nSlice nodeSlice) StringSlice() []string {
	ret := []string{}
	for _, node := range nSlice.Value() {
		ret = append(ret, node.ID)
	}
	return ret
}

// UnmarshalJSON unmarshals a nodeSlice from an RO-CRATE JSON-LD file.
func (nSlice *nodeSlice) UnmarshalJSON(data []byte) error {
	var singleNode nodeIdentifier
	if err := json.Unmarshal(data, &singleNode); err == nil {
		*nSlice = []nodeIdentifier{singleNode}
		return nil
	}
	var multipleNodes []nodeIdentifier
	if err := json.Unmarshal(data, &multipleNodes); err == nil {
		*nSlice = multipleNodes
		return nil
	}
	return fmt.Errorf("cannot unmarshal into []node: %s", string(data))
}

// graph is our representation of a node in the RO-CRATE JSON-LD graph.
type graph struct {
	ID            string         `json:"@id"`
	Type          *stringSlice   `json:"@type,omitempty"`
	About         *nodeSlice     `json:"about,omitempty"`
	Author        *nodeSlice     `json:"author,omitempty"`
	ContentURL    *nodeSlice     `json:"contentUrl,omitempty"`
	DatePublished string         `json:"datePublished,omitempty"`
	Description   *stringSlice   `json:"description,omitempty"`
	HasPart       *nodeSlice     `json:"hasPart,omitempty"`
	Keywords      *stringSlice   `json:"keywords,omitempty"`
	License       *nodeSlice     `json:"license,omitempty"`
	Name          *stringSlice   `json:"name,omitempty"`
	Publisher     *nodeSlice     `json:"publisher,omitempty"`
	ConformsTo    nodeIdentifier `json:"conformsTo,omitempty"`
}

// String provides a stringer interface for the graph object.
func (g graph) String() string {
	ret, err := json.MarshalIndent(g, " ", " ")
	if err != nil {
		return fmt.Sprintf("%s: %s", StringerError, err)
	}
	return string(ret)
}

// rocrateMeta represents an RO-CRATE object read from a JSON-LD file.
type rocrateMeta struct {
	RocrateContext any     `json:"@context"`
	Graph          []graph `json:"@graph"`
}

// Context gets the context from the RO-CRATE JSON-LD file as a string.
func (rcMeta rocrateMeta) Context() string {
	switch context := rcMeta.RocrateContext.(type) {
	case string:
		return context
	case []any:
		return fmt.Sprintf("%s", context[0])
	}
	return ""
}

// String provides a stringer interface for the rocrateMeta object.
func (rcMeta rocrateMeta) String() string {
	ret, err := json.MarshalIndent(rcMeta, " ", " ")
	if err != nil {
		return fmt.Sprintf("%s: %s", StringerError, err)
	}
	return string(ret)
}

// RocrateSummary is a summary of the RO-CRATE metadata.
type RocrateSummary struct {
	ID            string   `json:"id"`
	Name          []string `json:"name"`
	Type          []string `json:"type"`
	Description   []string `json:"description"`
	DatePublished string   `json:"datePublished"`
	Author        []string `json:"author"`
	License       string   `json:"license"`
	HasPart       []string `json:"hasPart"`
	ContentURL    []string `json:"contentUrl"`
	Keywords      []string `json:"keywords"`
	Publisher     []string `json:"publisher"`
	About         string   `json:"about"`
}

// newSummary returns an initialized RocrateSummary.
func newSummary() RocrateSummary {
	return RocrateSummary{
		ID:            "",
		Name:          []string{},
		Type:          []string{},
		Description:   []string{},
		DatePublished: "",
		Author:        []string{},
		License:       "",
		HasPart:       []string{},
		ContentURL:    []string{},
		Keywords:      []string{},
		Publisher:     []string{},
		About:         "",
	}
}

// String provides a stringer interface for the RocrateSummary object.
func (rSummary RocrateSummary) String() string {
	ret, err := json.MarshalIndent(rSummary, " ", " ")
	if err != nil {
		return fmt.Sprintf("%s: %s", StringerError, err)
	}
	return string(ret)
}

// findRootEntity performs a walk over the graph to find the root entity.
func (rcMeta rocrateMeta) findRootEntity() (graph, error) {
	if len(rcMeta.Graph) == 0 {
		return graph{}, fmt.Errorf("ro-crate-metadata.json is empty")
	}
	var descriptor graph
	for _, g := range rcMeta.Graph {
		if g.ID == "ro-crate-metadata.json" {
			descriptor = g
			break
		}
	}
	if descriptor.ID == "" {
		return graph{}, fmt.Errorf("ro-crate-metadata.json is non-conformant")
	}
	if descriptor.About == nil || len(descriptor.About.Value()) == 0 {
		return graph{}, fmt.Errorf("ro-crate-metadata.json descriptor lacks about entity")
	}
	about := descriptor.About.Value()[0].ID
	for _, g := range rcMeta.Graph {
		if g.ID == about {
			return g, nil
		}
	}
	return graph{}, fmt.Errorf("ro-crate-metadata.json root entity not found")
}

// Summary maps RO-CRATE metadata to a summary object.
func (rcMeta rocrateMeta) Summary() (RocrateSummary, error) {
	summary := newSummary()
	if len(rcMeta.Graph) == 0 {
		return summary, nil
	}
	g, err := rcMeta.findRootEntity()
	if err != nil {
		return summary, err
	}
	summary.ID = g.ID
	if g.Name != nil {
		summary.Name = g.Name.Value()
	}
	if g.Type != nil {
		summary.Type = g.Type.Value()
	}
	if g.Description != nil {
		summary.Description = g.Description.Value()
	}
	summary.DatePublished = g.DatePublished
	if g.Author != nil {
		summary.Author = g.Author.StringSlice()
	}
	if g.License != nil && len(g.License.Value()) > 0 {
		summary.License = g.License.Value()[0].ID
	}
	if g.HasPart != nil {
		summary.HasPart = g.HasPart.StringSlice()
	}
	if g.ContentURL != nil {
		summary.ContentURL = g.ContentURL.StringSlice()
	}
	if g.Keywords != nil {
		summary.Keywords = g.Keywords.Value()
	}
	if g.Publisher != nil {
		summary.Publisher = g.Publisher.StringSlice()
	}
	summary.About = g.ID
	return summary, nil
}

// ProcessMetadataStream processes an RO-CRATE metadata stream from an io.Reader.
func ProcessMetadataStream(reader io.Reader) (rocrateMeta, error) {
	data, err := io.ReadAll(reader)
	if err != nil {
		return rocrateMeta{}, fmt.Errorf("error reading metadata stream: %w", err)
	}
	var rcMeta rocrateMeta
	if err := json.Unmarshal(data, &rcMeta); err != nil {
		return rocrateMeta{}, fmt.Errorf("error unmarshalling rocrate metadata: %w", err)
	}
	return rcMeta, nil
}
