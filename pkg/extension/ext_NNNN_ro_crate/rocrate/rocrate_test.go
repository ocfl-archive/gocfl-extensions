package rocrate

import (
	"bytes"
	"fmt"
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestContexts ensures that we can open an empty JSON-LD as expected
// and reason about it.
func TestContexts(t *testing.T) {
	expectedContext := "https://w3id.org/ro/crate/1.1/context"

	simpleContextRes := bytes.NewBuffer(simpleContext)
	res, err := ProcessMetadataStream(simpleContextRes)

	require.NoError(t, err, "error processing simpleContext")
	assert.Equal(t, expectedContext, res.Context())
	assert.Empty(t, res.Graph)

	complexContextRes := bytes.NewBuffer(complexContext)
	res, err = ProcessMetadataStream(complexContextRes)

	require.NoError(t, err, "error processing complexContext")
	assert.Equal(t, expectedContext, res.Context())
	assert.Empty(t, res.Graph)
}

type stringTest struct {
	label    string
	testData []byte
	compare1 []string
	compare2 []string
}

var stringTests = []stringTest{
	{
		"name",
		nameTest,
		[]string{"name one", "name two"},
		[]string{"name one"},
	},
	{
		"keyword",
		keywordTest,
		[]string{"kw1", "kw2"},
		[]string{"kw1"},
	},
	{
		"type",
		typeTest,
		[]string{"Person", "Artist"},
		[]string{"Person"},
	},
}

// getStringSliceValue gives us some way of dynamically accessing
// attributes to avoid a decent amount of code replication.
func getStringSliceValue(data graph, label string) []string {
	switch label {
	case "name":
		return data.Name.Value()
	case "keyword":
		return data.Keywords.Value()
	case "type":
		return data.Type.Value()
	}
	return []string{""}
}

// TestStringVariants tests our ability to decode single-value strings
// or slices. We convert the single-value to a slice so we expect a
// slice array at all times.
func TestStringVariants(t *testing.T) {
	for _, test := range stringTests {
		variantTest := bytes.NewBuffer(test.testData)
		res, err := ProcessMetadataStream(variantTest)

		require.NoError(t, err, "%s: cannot process input", test.label)
		assert.Len(t, res.Graph, 2, "%s: test data is incorrect length", test.label)
		assert.Equal(t, "test1", res.Graph[0].ID)

		value := getStringSliceValue(res.Graph[0], test.label)
		assert.Equal(t, test.compare1, value, "%s test 1", test.label)

		assert.Equal(t, "test2", res.Graph[1].ID)
		value = getStringSliceValue(res.Graph[1], test.label)
		assert.Equal(t, test.compare2, value, "%s test 2", test.label)
	}
}

type nodeTest struct {
	label    string
	testData []byte
	compare1 []nodeIdentifier
	compare2 []nodeIdentifier
}

var nodeTests = []nodeTest{
	{
		"author",
		authorTest,
		[]nodeIdentifier{
			{ID: "https://orcid.org/1234-0003-1974-0000"},
			{ID: "#Yann"},
			{ID: "#Organization-SMUC"},
		},
		[]nodeIdentifier{
			{ID: "https://orcid.org/0000-0003-1974-1234"},
		},
	},
	{
		"hasPart",
		hasPartTest,
		[]nodeIdentifier{
			{ID: "part1"},
		},
		[]nodeIdentifier{
			{ID: "part1"},
			{ID: "part2"},
			{ID: "part3"},
		},
	},
	{
		"license",
		licenseTest,
		[]nodeIdentifier{
			{ID: "https://spdx.org/licenses/license1"},
		},
		[]nodeIdentifier{
			{ID: "http://spdx.org/licenses/license2"},
		},
	},
}

// getNodeIdentifierSliceValue allows us to get values more dynamically
// from the nodeIdentifier tests.
func getNodeIdentifierSliceValue(data graph, label string) []nodeIdentifier {
	switch label {
	case "author":
		return data.Author.Value()
	case "license":
		return data.License.Value()
	case "hasPart":
		return data.HasPart.Value()
	}
	return []nodeIdentifier{}
}

// TestNodeIdentifierVariants tests the conversion of single-values to
// a slice of nodeIdentifiers.
func TestNodeIdentifierVariants(t *testing.T) {
	for _, test := range nodeTests {
		variantTest := bytes.NewBuffer(test.testData)
		res, err := ProcessMetadataStream(variantTest)

		require.NoError(t, err, "%s: cannot process input", test.label)
		assert.Len(t, res.Graph, 2, "%s: test data is incorrect length", test.label)
		assert.Equal(t, "test1", res.Graph[0].ID)

		value := getNodeIdentifierSliceValue(res.Graph[0], test.label)
		for idx, v := range value {
			assert.Equal(t, test.compare1[idx].ID, v.ID, "%s test 1", test.label)
		}

		assert.Equal(t, "test2", res.Graph[1].ID)
		value = getNodeIdentifierSliceValue(res.Graph[1], test.label)
		for idx, v := range value {
			assert.Equal(t, test.compare2[idx].ID, v.ID, "%s test 2", test.label)
		}
	}
}

// TestNewSummary ensures new summary is as safe as possible.
func TestNewSummary(t *testing.T) {
	summary := newSummary()
	structType := reflect.TypeOf(summary)
	structVal := reflect.ValueOf(summary)
	fieldNum := structVal.NumField()
	for i := 0; i < fieldNum; i++ {
		field := structVal.Field(i)
		if fmt.Sprintf("%s", field.Type()) == "string" {
			continue
		}
		fieldName := structType.Field(i).Name
		isSet := field.IsValid() && !field.IsZero()
		assert.True(t, isSet, "summary constructor isn't setting: %s", fieldName)
	}
}

// TestNewGocflSummary ensures new gocfl summary is as safe as possible.
func TestNewGocflSummary(t *testing.T) {
	summary := newGocflSummary()
	structType := reflect.TypeOf(summary)
	structVal := reflect.ValueOf(summary)
	fieldNum := structVal.NumField()
	for i := 0; i < fieldNum; i++ {
		field := structVal.Field(i)
		if fmt.Sprintf("%s", field.Type()) == "string" {
			continue
		}
		fieldName := structType.Field(i).Name
		isSet := field.IsValid() && !field.IsZero()
		assert.True(t, isSet, "summary constructor isn't setting: %s", fieldName)
	}
}

type metadataTest struct {
	label        string
	testData     []byte
	summary      RocrateSummary
	gocflSummary GocflSummary
}

var metadataTests = []metadataTest{
	{
		label:    "empty",
		testData: emptyCrate,
		summary: RocrateSummary{
			ID:            "./",
			Name:          []string{},
			Type:          []string{"Dataset"},
			Description:   []string{},
			DatePublished: "",
			Author:        []string{},
			License:       "",
			HasPart:       []string{},
			ContentURL:    []string{},
			Keywords:      []string{},
			Publisher:     []string{},
			About:         "./",
		},
		gocflSummary: GocflSummary{
			Signature:      "./",
			Title:          "",
			Description:    "",
			Created:        "",
			Sets:           "Dataset",
			Keywords:       "",
			Licenses:       "",
			LastChanged:    "",
			OrganisationID: "",
			Organisation:   "",
			User:           "",
			Address:        "",
		},
	},
	{
		label:    "afternoon drinks",
		testData: afternoonDrinks,
		summary: RocrateSummary{
			ID:   "./RC0E772B3021E7E40C2BBDE657",
			Name: []string{"A study of my afternoon drinks"},
			Type: []string{"Dataset"},
			Description: []string{
				"A study of my afternoon consumption one week in 2018",
				"Exported from Dataverse",
			},
			DatePublished: "2018",
			Author:        []string{"#Ross_Spencer-1"},
			License:       "https://spdx.org/licenses/CC0-1.0.html",
			HasPart: []string{
				"metadata/agents.json",
				"metadata/dataset.json",
				"Drinkscitation-endnote.xml",
				"Drinks.tab",
				"Drinks.csv",
				"Drinkscitation-ris.ris",
				"Drinkscitation-bib.bib",
				"Drinks.RData",
				"Drinks-ddi.xml",
			},
			ContentURL: []string{},
			Keywords:   []string{},
			Publisher:  []string{},
			About:      "./RC0E772B3021E7E40C2BBDE657",
		},
		gocflSummary: GocflSummary{
			Signature:      "./RC0E772B3021E7E40C2BBDE657",
			Title:          "A study of my afternoon drinks",
			Description:    "A study of my afternoon consumption one week in 2018; Exported from Dataverse",
			Created:        "2018",
			Sets:           "Dataset",
			Keywords:       "",
			Licenses:       "https://spdx.org/licenses/CC0-1.0.html",
			LastChanged:    "",
			OrganisationID: "",
			Organisation:   "",
			User:           "",
			Address:        "",
		},
	},
	{
		label:    "tape artifacts",
		testData: tapeArtifacts,
		summary: RocrateSummary{
			ID:   "https://doi.org/10.5281/zenodo.10057813",
			Name: []string{"Tape Artifacts (Tape Transfer Artifacts from an Investigation of Digital Preservation Storage Strategies)"},
			Type: []string{"Dataset"},
			Description: []string{
				"Digital preservation is not without impact on the environment. At the same time, cultural heritage organizations and memory institutions operate under severe financial constraints. In this paper we consider how digital preservation processes can be made cheaper and more ecologically sustainable by reducing data accessions and storage volumes.",
			},
			DatePublished: "2023-10-31",
			Author: []string{
				"https://orcid.org/0000-0003-1974-1234",
				"#Yann",
				"#Organization-SMUC",
			},
			License: "https://creativecommons.org/licenses/by/4.0/legalcode",
			HasPart: []string{
				"A_Slice_of_PI_Sustainability_Final.pdf",
				"Tape_Artifacts.zip",
			},
			ContentURL: []string{
				"https://zenodo.org/api/records/10057813/files-archive",
			},
			Keywords: []string{
				"digital preservation",
				"environmental sustainability",
				"carbon footprint",
				"tape storage",
				"green digital preservation",
			},
			Publisher: []string{
				"https://ror.org/010000000",
			},
			About: "https://doi.org/10.5281/zenodo.10057813",
		},
		gocflSummary: GocflSummary{
			Signature:      "https://doi.org/10.5281/zenodo.10057813",
			Title:          "Tape Artifacts (Tape Transfer Artifacts from an Investigation of Digital Preservation Storage Strategies)",
			Description:    "Digital preservation is not without impact on the environment. At the same time, cultural heritage organizations and memory institutions operate under severe financial constraints. In this paper we consider how digital preservation processes can be made cheaper and more ecologically sustainable by reducing data accessions and storage volumes.",
			Created:        "2023-10-31",
			Sets:           "Dataset",
			Keywords:       "digital preservation; environmental sustainability; carbon footprint; tape storage; green digital preservation",
			Licenses:       "https://creativecommons.org/licenses/by/4.0/legalcode",
			LastChanged:    "",
			OrganisationID: "",
			Organisation:   "",
			User:           "",
			Address:        "",
		},
	},
	{
		label:    "simple",
		testData: simpleCrate,
		summary: RocrateSummary{
			ID:            "./",
			Name:          []string{"My Research Object"},
			Type:          []string{"Dataset"},
			Description:   []string{"The RO-Crate Root Data Entity"},
			DatePublished: "2023-05-22",
			Author:        []string{},
			License:       "https://creativecommons.org/licenses/by-nc-sa/4.0/",
			HasPart:       []string{},
			ContentURL:    []string{},
			Keywords:      []string{},
			Publisher:     []string{},
			About:         "./",
		},
		gocflSummary: GocflSummary{
			Signature:      "./",
			Title:          "My Research Object",
			Description:    "The RO-Crate Root Data Entity",
			Created:        "2023-05-22",
			Sets:           "Dataset",
			Keywords:       "",
			Licenses:       "https://creativecommons.org/licenses/by-nc-sa/4.0/",
			LastChanged:    "",
			OrganisationID: "",
			Organisation:   "",
			User:           "",
			Address:        "",
		},
	},
	{
		label:    "sample",
		testData: sampleCrate,
		summary: RocrateSummary{
			ID:            "./",
			Name:          []string{"Introduction to Text Analysis in Python"},
			Type:          []string{"Dataset"},
			Description:   []string{"The RO-Crate Root Data Entity"},
			DatePublished: "",
			Author:        []string{},
			License:       "",
			HasPart:       []string{},
			ContentURL:    []string{},
			Keywords:      []string{},
			Publisher:     []string{},
			About:         "./",
		},
		gocflSummary: GocflSummary{
			Signature:      "./",
			Title:          "Introduction to Text Analysis in Python",
			Description:    "The RO-Crate Root Data Entity",
			Created:        "",
			Sets:           "Dataset",
			Keywords:       "",
			Licenses:       "",
			LastChanged:    "",
			OrganisationID: "",
			Organisation:   "",
			User:           "",
			Address:        "",
		},
	},
	{
		label:    "galaxy",
		testData: galaxyCrate,
		summary: RocrateSummary{
			ID: "./",
			Name: []string{
				"Demo Crate",
			},
			Type: []string{
				"Dataset",
				"LearningResource",
			},
			Description: []string{
				"a demo crate for Galaxy training",
			},
			DatePublished: "2024-03-08",
			Author: []string{
				"https://orcid.org/0000-0000-0000-0000",
			},
			License: "https://spdx.org/licenses/CC-BY-NC-SA-4.0.html",
			HasPart: []string{
				"data.csv",
			},
			ContentURL: []string{
				"http://example.com/resource/data",
			},
			Keywords:  []string{},
			Publisher: []string{"https://ror.org/0abcdef00"},
			About:     "./",
		},
		gocflSummary: GocflSummary{
			Signature:      "./",
			Title:          "Demo Crate",
			Description:    "a demo crate for Galaxy training",
			Created:        "2024-03-08",
			Sets:           "Dataset; LearningResource",
			Keywords:       "",
			Licenses:       "https://spdx.org/licenses/CC-BY-NC-SA-4.0.html",
			LastChanged:    "",
			OrganisationID: "",
			Organisation:   "",
			User:           "",
			Address:        "",
		},
	},
}

// TestMetadata provides more generic testing of the test data within
// this package.
func TestMetadata(t *testing.T) {
	for _, test := range metadataTests {
		variantTest := bytes.NewBuffer(test.testData)
		processed, err := ProcessMetadataStream(variantTest)
		require.NoError(t, err, "%s: cannot process input", test.label)
		res, err := processed.Summary()
		require.NoError(t, err, "%s summary error", test.label)
		assert.Equal(t, test.summary.ID, res.ID, "%s ID", test.label)
		assert.Equal(t, test.summary.Name, res.Name, "%s Name", test.label)
		assert.Equal(t, test.summary.Type, res.Type, "%s Type", test.label)
		assert.Equal(t, test.summary.Description, res.Description, "%s Description", test.label)
		assert.Equal(t, test.summary.DatePublished, res.DatePublished, "%s DatePublished", test.label)
		assert.Equal(t, test.summary.Author, res.Author, "%s Author", test.label)
		assert.Equal(t, test.summary.License, res.License, "%s License", test.label)
		assert.Equal(t, test.summary.HasPart, res.HasPart, "%s HasPart", test.label)
		assert.Equal(t, test.summary.ContentURL, res.ContentURL, "%s ContentURL", test.label)
		assert.Equal(t, test.summary.Keywords, res.Keywords, "%s Keywords", test.label)
		assert.Equal(t, test.summary.Publisher, res.Publisher, "%s Publisher", test.label)
		assert.Equal(t, test.summary.About, res.About, "%s About", test.label)
	}
}

// TestGOCFLMetadata provides more generic testing the summary output
// of the GOCFL struct.
func TestGOCFLMetadata(t *testing.T) {
	oldTime := currentTime
	defer func() { currentTime = oldTime }()
	currentTime = func(bool) string {
		return ""
	}

	for _, test := range metadataTests {
		variantTest := bytes.NewBuffer(test.testData)
		processed, err := ProcessMetadataStream(variantTest)
		require.NoError(t, err, "%s: cannot process input", test.label)
		res, err := processed.GOCFLSummary()
		require.NoError(t, err, "%s gocfl summary error", test.label)
		assert.Equal(t, test.gocflSummary, res, "%s gocfl metadata", test.label)
	}
}

func TestCrate_IndexingAndResolution(t *testing.T) {
	crate, err := ProcessMetadataStream(bytes.NewBuffer(afternoonDrinks))
	require.NoError(t, err)

	// O(1) Index lookup
	descriptor, ok := crate.Descriptor()
	require.True(t, ok)
	assert.Equal(t, "ro-crate-metadata.json", descriptor.ID)

	root, err := crate.RootEntity()
	require.NoError(t, err)
	assert.Equal(t, "./RC0E772B3021E7E40C2BBDE657", root.ID)

	// Resolving referenced author entity
	authorEntity, ok := crate.Resolve("#Ross_Spencer-1")
	require.True(t, ok)
	require.NotNil(t, authorEntity.Name)
	assert.Equal(t, []string{"Ross Spencer"}, authorEntity.Name.Value())

	// Resolving author names via helper
	authorNames := crate.GetAuthorNames(root)
	assert.Equal(t, []string{"Ross Spencer"}, authorNames)

	// Non-existent entity
	_, ok = crate.Resolve("#does-not-exist")
	assert.False(t, ok)
}

func TestFlexSlice_Variants(t *testing.T) {
	// Null JSON
	var fsNull FlexSlice[string]
	require.NoError(t, fsNull.UnmarshalJSON([]byte("null")))
	assert.True(t, fsNull.IsBlank())

	// Single value
	var fsSingle FlexSlice[string]
	require.NoError(t, fsSingle.UnmarshalJSON([]byte(`"hello"`)))
	assert.Equal(t, []string{"hello"}, fsSingle.Value())

	// Array
	var fsArray FlexSlice[string]
	require.NoError(t, fsArray.UnmarshalJSON([]byte(`["a", "b", "c"]`)))
	assert.Equal(t, []string{"a", "b", "c"}, fsArray.Value())
}

func TestCrate_Serialization(t *testing.T) {
	crate, err := ProcessMetadataStream(bytes.NewBuffer(simpleCrate))
	require.NoError(t, err)

	crateJSON, err := crate.ToJSON()
	require.NoError(t, err)
	assert.NotEmpty(t, crateJSON)
	assert.NotEmpty(t, crate.String())

	summary, err := crate.Summary()
	require.NoError(t, err)
	summaryJSON, err := summary.ToJSON()
	require.NoError(t, err)
	assert.NotEmpty(t, summaryJSON)
	assert.NotEmpty(t, summary.String())

	gocflSummary, err := crate.GOCFLSummaryWithTime("2026-11-19T12:00:00Z")
	require.NoError(t, err)
	assert.Equal(t, "2026-11-19T12:00:00Z", gocflSummary.LastChanged)
	gocflJSON, err := gocflSummary.ToJSON()
	require.NoError(t, err)
	assert.NotEmpty(t, gocflJSON)
	assert.NotEmpty(t, gocflSummary.String())
}
