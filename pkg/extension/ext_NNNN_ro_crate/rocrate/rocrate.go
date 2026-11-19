package rocrate

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
)

// FlexSlice represents a slice of elements that can be unmarshalled from either
// a single JSON element or a JSON array of elements.
type FlexSlice[T any] []T

// Value returns the underlying slice of T.
func (f FlexSlice[T]) Value() []T {
	return []T(f)
}

// IsBlank indicates if a FlexSlice is empty.
func (f FlexSlice[T]) IsBlank() bool {
	return len(f) == 0
}

// UnmarshalJSON unmarshals either a single T or a slice []T from JSON.
func (f *FlexSlice[T]) UnmarshalJSON(data []byte) error {
	trimmed := bytes.TrimSpace(data)
	if len(trimmed) == 0 || bytes.Equal(trimmed, []byte("null")) {
		*f = nil
		return nil
	}
	if trimmed[0] == '[' {
		var list []T
		if err := json.Unmarshal(trimmed, &list); err != nil {
			return err
		}
		*f = list
		return nil
	}
	var single T
	if err := json.Unmarshal(trimmed, &single); err != nil {
		return err
	}
	*f = []T{single}
	return nil
}

// StringSlice represents a slice of strings with backward-compatible helper methods.
type StringSlice FlexSlice[string]

// Value allows accessing StringSlice data.
func (sSlice StringSlice) Value() []string {
	return []string(sSlice)
}

func (sSlice StringSlice) isBlank() bool {
	return len(sSlice) == 0
}

func (sSlice *StringSlice) UnmarshalJSON(data []byte) error {
	var fs FlexSlice[string]
	if err := json.Unmarshal(data, &fs); err != nil {
		return fmt.Errorf("cannot unmarshal into []string: %w", err)
	}
	*sSlice = StringSlice(fs)
	return nil
}

// stringSlice alias for internal/legacy compatibility.
type stringSlice = StringSlice

// NodeRef represents a reference to another entity node in RO-Crate.
type NodeRef struct {
	ID string `json:"@id"`
}

// isBlank returns true if a node identifier has no ID value.
func (nr NodeRef) isBlank() bool {
	return nr.ID == ""
}

// nodeIdentifier alias for backward compatibility.
type nodeIdentifier = NodeRef

// NodeRefSlice represents a slice of node references.
type NodeRefSlice FlexSlice[NodeRef]

// Value allows accessing NodeRefSlice data.
func (nSlice NodeRefSlice) Value() []NodeRef {
	return []NodeRef(nSlice)
}

// StringSlice returns a string slice of the IDs in the NodeRefSlice.
func (nSlice NodeRefSlice) StringSlice() []string {
	ret := make([]string, len(nSlice))
	for i, node := range nSlice {
		ret[i] = node.ID
	}
	return ret
}

func (nSlice *NodeRefSlice) UnmarshalJSON(data []byte) error {
	var fs FlexSlice[NodeRef]
	if err := json.Unmarshal(data, &fs); err != nil {
		return fmt.Errorf("cannot unmarshal into []node: %w", err)
	}
	*nSlice = NodeRefSlice(fs)
	return nil
}

// nodeSlice alias for internal/legacy compatibility.
type nodeSlice = NodeRefSlice

// Entity represents a node in the RO-CRATE JSON-LD graph.
type Entity struct {
	ID            string        `json:"@id"`
	Type          *StringSlice  `json:"@type,omitempty"`
	About         *NodeRefSlice `json:"about,omitempty"`
	Author        *NodeRefSlice `json:"author,omitempty"`
	ContentURL    *NodeRefSlice `json:"contentUrl,omitempty"`
	DatePublished string        `json:"datePublished,omitempty"`
	Description   *StringSlice  `json:"description,omitempty"`
	HasPart       *NodeRefSlice `json:"hasPart,omitempty"`
	Keywords      *StringSlice  `json:"keywords,omitempty"`
	License       *NodeRefSlice `json:"license,omitempty"`
	Name          *StringSlice  `json:"name,omitempty"`
	Publisher     *NodeRefSlice `json:"publisher,omitempty"`
	ConformsTo    NodeRef       `json:"conformsTo,omitempty"`
}

// graph alias for backward compatibility.
type graph = Entity

// Crate represents an RO-CRATE object read from a JSON-LD file.
type Crate struct {
	RocrateContext any      `json:"@context"`
	Graph          []Entity `json:"@graph"`
	index          map[string]*Entity
}

// rocrateMeta alias for backward compatibility.
type rocrateMeta = Crate

// Context gets the context from the RO-CRATE JSON-LD file as a string.
func (c *Crate) Context() string {
	switch context := c.RocrateContext.(type) {
	case string:
		return context
	case []any:
		if len(context) > 0 {
			return fmt.Sprintf("%v", context[0])
		}
	}
	return ""
}

// BuildIndex populates the internal O(1) entity lookup map.
func (c *Crate) BuildIndex() {
	c.index = make(map[string]*Entity, len(c.Graph))
	for i := range c.Graph {
		c.index[c.Graph[i].ID] = &c.Graph[i]
	}
}

// Entity returns the graph entity with the given @id in O(1) time.
func (c *Crate) Entity(id string) (*Entity, bool) {
	if c.index == nil {
		c.BuildIndex()
	}
	e, ok := c.index[id]
	return e, ok
}

// Resolve returns the graph entity with the given @id, if present in the graph.
func (c *Crate) Resolve(id string) (*Entity, bool) {
	return c.Entity(id)
}

// Descriptor finds and returns the root metadata descriptor node.
func (c *Crate) Descriptor() (*Entity, bool) {
	if e, ok := c.Entity("ro-crate-metadata.json"); ok {
		return e, true
	}
	if e, ok := c.Entity("ro-crate-metadata.jsonld"); ok {
		return e, true
	}
	for i := range c.Graph {
		g := &c.Graph[i]
		if !g.ConformsTo.isBlank() {
			return g, true
		}
	}
	return nil, false
}

// RootEntity finds and returns the root dataset entity conforming to RO-Crate specification.
func (c *Crate) RootEntity() (*Entity, error) {
	if len(c.Graph) == 0 {
		return nil, fmt.Errorf("ro-crate-metadata.json is empty")
	}

	// 1. Resolve via descriptor "about" property
	if descriptor, ok := c.Descriptor(); ok {
		if descriptor.About != nil && len(descriptor.About.Value()) > 0 {
			aboutID := descriptor.About.Value()[0].ID
			if root, ok := c.Entity(aboutID); ok {
				return root, nil
			}
		}
	}

	// 2. Fallback: Lookup standard root dataset IDs
	if root, ok := c.Entity("./"); ok {
		return root, nil
	}
	if root, ok := c.Entity("."); ok {
		return root, nil
	}

	// 3. Fallback: Search for an entity with @type "Dataset"
	for i := range c.Graph {
		g := &c.Graph[i]
		if g.Type != nil {
			for _, t := range g.Type.Value() {
				if t == "Dataset" {
					return g, nil
				}
			}
		}
	}

	// 4. Fallback: If only one non-descriptor entity exists
	if len(c.Graph) == 1 {
		return &c.Graph[0], nil
	}

	return nil, fmt.Errorf("ro-crate root entity not found")
}

func (c *Crate) findRootEntity() (Entity, error) {
	root, err := c.RootEntity()
	if err != nil {
		return Entity{}, err
	}
	return *root, nil
}

// GetEntityNames resolves the IDs in nodeRefs and returns their names if found, or the ID as fallback.
func (c *Crate) GetEntityNames(refs []NodeRef) []string {
	names := make([]string, 0, len(refs))
	for _, ref := range refs {
		if entity, ok := c.Resolve(ref.ID); ok && entity.Name != nil && len(entity.Name.Value()) > 0 {
			names = append(names, entity.Name.Value()[0])
		} else if ref.ID != "" {
			names = append(names, ref.ID)
		}
	}
	return names
}

// GetAuthorNames returns the author names of an entity, resolving referenced author nodes when available.
func (c *Crate) GetAuthorNames(entity *Entity) []string {
	if entity == nil || entity.Author == nil {
		return nil
	}
	return c.GetEntityNames(entity.Author.Value())
}

// GetPublisherNames returns the publisher names of an entity, resolving referenced publisher nodes when available.
func (c *Crate) GetPublisherNames(entity *Entity) []string {
	if entity == nil || entity.Publisher == nil {
		return nil
	}
	return c.GetEntityNames(entity.Publisher.Value())
}

// ToJSON serializes the Crate to JSON.
func (c *Crate) ToJSON() ([]byte, error) {
	return json.MarshalIndent(c, "", " ")
}

// String provides a string representation of Crate.
func (c *Crate) String() string {
	b, err := c.ToJSON()
	if err != nil {
		return fmt.Sprintf("Crate{Graph: %d entities}", len(c.Graph))
	}
	return string(b)
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

// NewSummary returns an initialized RocrateSummary.
func NewSummary() RocrateSummary {
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

func newSummary() RocrateSummary {
	return NewSummary()
}

// ToJSON serializes RocrateSummary to formatted JSON.
func (rSummary RocrateSummary) ToJSON() ([]byte, error) {
	return json.MarshalIndent(rSummary, "", " ")
}

// String provides a string representation of RocrateSummary.
func (rSummary RocrateSummary) String() string {
	b, err := rSummary.ToJSON()
	if err != nil {
		return fmt.Sprintf("RocrateSummary{ID: %q}", rSummary.ID)
	}
	return string(b)
}

// Summary maps RO-CRATE metadata to a summary object.
func (c *Crate) Summary() (RocrateSummary, error) {
	summary := NewSummary()
	if len(c.Graph) == 0 {
		return summary, nil
	}
	g, err := c.RootEntity()
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
func ProcessMetadataStream(reader io.Reader) (*Crate, error) {
	data, err := io.ReadAll(reader)
	if err != nil {
		return nil, fmt.Errorf("error reading metadata stream: %w", err)
	}
	var crate Crate
	if err := json.Unmarshal(data, &crate); err != nil {
		return nil, fmt.Errorf("error unmarshalling rocrate metadata: %w", err)
	}
	crate.BuildIndex()
	return &crate, nil
}
