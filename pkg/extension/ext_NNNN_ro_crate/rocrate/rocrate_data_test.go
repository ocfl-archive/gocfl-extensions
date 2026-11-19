package rocrate

// simplecontext contains bare minimum context.
var simpleContext []byte = []byte(`
{
  "@context": "https://w3id.org/ro/crate/1.1/context",
  "@graph": [
  ]
}
`)

// complexcontext provides further information about the schema and
// vocabulary.
var complexContext []byte = []byte(`
{
  "@context": [
    "https://w3id.org/ro/crate/1.1/context",
    {
      "@vocab": "http://schema.org/"
    }
  ],
  "@graph": [
  ]
}
`)

// Variants of different values that can be single values or slices.

// nameTest provides a way of testing a single value name or a slice
// of names.
var nameTest []byte = []byte(`
{
    "@context": [
        "https://w3id.org/ro/crate/1.1/context",
        {
            "@vocab": "http://schema.org/"
        }
    ],
    "@graph": [
        {
            "@id": "test1",
            "name": [
                "name one",
                "name two"
            ]
        },
        {
            "@id": "test2",
            "name": "name one"
        }
    ]
}
`)

// keywordTest provides a way to test a single-value keyword or a
// slice of keywords.
var keywordTest []byte = []byte(`
{
    "@context": [
        "https://w3id.org/ro/crate/1.1/context",
        {
            "@vocab": "http://schema.org/"
        }
    ],
    "@graph": [
        {
            "@id": "test1",
            "keywords": [
                "kw1",
                "kw2"
            ]
        },
        {
            "@id": "test2",
            "keywords": "kw1"
        }
    ]
}
`)

// typeTest provides a way to test a single-value type or a slice of
// values for the same key.
var typeTest []byte = []byte(`
{
    "@context": [
        "https://w3id.org/ro/crate/1.1/context",
        {
            "@vocab": "http://schema.org/"
        }
    ],
    "@graph": [
        {
            "@id": "test1",
            "@type": [
                "Person",
                "Artist"
            ]
        },
        {
            "@id": "test2",
            "@type": "Person"
        }
    ]
}
`)

// authorTest provides a way to test a single-value author, or a slice
// of author values.
var authorTest []byte = []byte(`
{
    "@context": [
        "https://w3id.org/ro/crate/1.1/context",
        {
            "@vocab": "http://schema.org/"
        }
    ],
    "@graph": [
        {
            "@id": "test1",
            "author": [
                {
                    "@id": "https://orcid.org/1234-0003-1974-0000"
                },
                {
                    "@id": "#Yann"
                },
                {
                    "@id": "#Organization-SMUC"
                }
            ]
        },
        {
            "@id": "test2",
            "author": {
                "@id": "https://orcid.org/0000-0003-1974-1234"
            }
        }
    ]
}
`)

// hasPartTest provides a way of testing a single-value part, or a
// slice of parts.
var hasPartTest []byte = []byte(`
{
    "@context": [
        "https://w3id.org/ro/crate/1.1/context",
        {
            "@vocab": "http://schema.org/"
        }
    ],
    "@graph": [
        {
            "@id": "test1",
            "hasPart": {
                "@id": "part1"
            }
        },
        {
            "@id": "test2",
            "hasPart": [
                {
                    "@id": "part1"
                },
                {
                    "@id": "part2"
                },
                {
                    "@id": "part3"
                }
            ]
        }
    ]
}
`)

// licenseTest provides a way of testing a single-value license, or a
// slice of licenses.
var licenseTest []byte = []byte(`
{
    "@context": [
        "https://w3id.org/ro/crate/1.1/context",
        {
            "@vocab": "http://schema.org/"
        }
    ],
    "@graph": [
        {
            "@id": "test1",
            "license": {
                "@id": "https://spdx.org/licenses/license1"
            }
        },
        {
            "@id": "test2",
            "license": [
                {
                    "@id": "http://spdx.org/licenses/license2"
                }
            ]
        }
    ]
}
`)

// Real RO-CRATE metadata files.

// emptyCrate represents an empty RO-CRATE.
var emptyCrate []byte = []byte(`
{
  "@context": "https://w3id.org/ro/crate/1.1/context",
  "@graph": [
    {
      "@type": "CreativeWork",
      "@id": "ro-crate-metadata.json",
      "conformsTo": {
        "@id": "https://w3id.org/ro/crate/1.1"
      },
      "about": {
        "@id": "./"
      }
    },
    {
      "@id": "./",
      "@type": "Dataset"
    }
  ]
}
`)

// afternoonDrinks represents a dataset in RO-CRATE format published
// with a Dataverse export plugin.
var afternoonDrinks []byte = []byte(`
{
    "@context": [
        "https://w3id.org/ro/crate/1.1/context",
        {
            "@vocab": "http://schema.org/"
        }
    ],
    "@graph": [
        {
            "@id": "ro-crate-metadata.json",
            "@type": "CreativeWork",
            "about": {
                "@id": "./RC0E772B3021E7E40C2BBDE657"
            },
            "conformsTo": {
                "@id": "https://w3id.org/ro/crate/1.1"
            }
        },
        {
            "@id": "./RC0E772B3021E7E40C2BBDE657",
            "@type": "Dataset",
            "author": [
                {
                    "@id": "#Ross_Spencer-1"
                }
            ],
            "datePublished": "2018",
            "description": [
                "A study of my afternoon consumption one week in 2018",
                "Exported from Dataverse"
            ],
            "hasPart": [
                {
                    "@id": "metadata/agents.json"
                },
                {
                    "@id": "metadata/dataset.json"
                },
                {
                    "@id": "Drinkscitation-endnote.xml"
                },
                {
                    "@id": "Drinks.tab"
                },
                {
                    "@id": "Drinks.csv"
                },
                {
                    "@id": "Drinkscitation-ris.ris"
                },
                {
                    "@id": "Drinkscitation-bib.bib"
                },
                {
                    "@id": "Drinks.RData"
                },
                {
                    "@id": "Drinks-ddi.xml"
                }
            ],
            "license": {
                "@id": "https://spdx.org/licenses/CC0-1.0.html"
            },
            "name": "A study of my afternoon drinks"
        },
        {
            "@id": "metadata/dataset.json",
            "@type": "File",
            "description": "Original Dataverse metadata export in JSON format.",
            "encodingFormat": "application/json",
            "name": "metadata/dataset.json"
        },
        {
            "@id": "metadata/agents.json",
            "@type": "File",
            "description": "Information about agents in Dataverse associated with this dataset.",
            "encodingFormat": "application/json",
            "name": "metadata/agents.json"
        },
        {
            "@id": "Drinkscitation-endnote.xml",
            "@type": "File",
            "description": "Dataverse generated EndNote citation.",
            "encodingFormat": "application/xml",
            "name": "Drinkscitation-endnote.xml"
        },
        {
            "@id": "Drinks.tab",
            "@type": "File",
            "description": "",
            "encodingFormat": "text/tab-separated-values",
            "name": "Drinks.tab"
        },
        {
            "@id": "Drinks.csv",
            "@type": "File",
            "description": "Dataverse generated CSV file.",
            "encodingFormat": "text/csv",
            "name": "Drinks.csv"
        },
        {
            "@id": "Drinkscitation-ris.ris",
            "@type": "File",
            "description": "Dataverse generated RIS citation.",
            "encodingFormat": "application/x-research-info-systems",
            "name": "Drinkscitation-ris.ris"
        },
        {
            "@id": "Drinkscitation-bib.bib",
            "@type": "File",
            "description": "Dataverse generated BibTeX citation.",
            "encodingFormat": "application/x-bibtex",
            "name": "Drinkscitation-bib.bib"
        },
        {
            "@id": "Drinks.RData",
            "@type": "File",
            "description": "Dataverse generated RData file.",
            "encodingFormat": "application/x-rlang-transport",
            "name": "Drinks.RData"
        },
        {
            "@id": "Drinks-ddi.xml",
            "@type": "File",
            "description": "Dataverse generated DDI metadata export.",
            "encodingFormat": "application/xml",
            "name": "Drinks-ddi.xml"
        },
        {
            "@id": "#Ross_Spencer-1",
            "@type": "Person",
            "affiliation": {
                "@id": "#Arkivum"
            },
            "name": "Ross Spencer"
        },
        {
            "@id": "#Arkivum",
            "@type": "Organization",
            "name": "Arkivum"
        }
    ]
}
`)

// tapeArtifacts represents an RO-CRATE metadata file from Zenodo.
var tapeArtifacts []byte = []byte(`
{
    "@context": "https://w3id.org/ro/crate/1.1/context",
    "@graph": [
        {
            "@id": "ro-crate-metadata.json",
            "@type": "CreativeWork",
            "about": {
                "@id": "https://doi.org/10.5281/zenodo.10057813"
            },
            "conformsTo": {
                "@id": "https://w3id.org/ro/crate/1.1"
            }
        },
        {
            "@id": "https://doi.org/10.5281/zenodo.10057813",
            "@type": "Dataset",
            "author": [
                {
                    "@id": "https://orcid.org/0000-0003-1974-1234"
                },
                {
                    "@id": "#Yann"
                },
                {
                    "@id": "#Organization-SMUC"
                }
            ],
            "contentUrl": [
                {
                    "@id": "https://zenodo.org/api/records/10057813/files-archive"
                }
            ],
            "datePublished": "2023-10-31",
            "description": "Digital preservation is not without impact on the environment. At the same time, cultural heritage organizations and memory institutions operate under severe financial constraints. In this paper we consider how digital preservation processes can be made cheaper and more ecologically sustainable by reducing data accessions and storage volumes.",
            "hasPart": [
                {
                    "@id": "A_Slice_of_PI_Sustainability_Final.pdf"
                },
                {
                    "@id": "Tape_Artifacts.zip"
                }
            ],
            "keywords": [
                "digital preservation",
                "environmental sustainability",
                "carbon footprint",
                "tape storage",
                "green digital preservation"
            ],
            "license": {
                "@id": "https://creativecommons.org/licenses/by/4.0/legalcode"
            },
            "name": "Tape Artifacts (Tape Transfer Artifacts from an Investigation of Digital Preservation Storage Strategies)",
            "publisher": {
                "@id": "https://ror.org/010000000"
            }
        },
        {
            "@id": "A_Slice_of_PI_Sustainability_Final.pdf",
            "@type": "File",
            "contentSize": 218968,
            "description": "A slice of PI...",
            "encodingFormat": "application/pdf",
            "name": "A_Slice_of_PI_Sustainability_Final.pdf"
        },
        {
            "@id": "Tape_Artifacts.zip",
            "@type": "File",
            "contentSize": 4652252,
            "description": "Tape artifacts zip...",
            "encodingFormat": "application/zip",
            "name": "Tape_Artifacts.zip"
        },
        {
            "@id": "https://orcid.org/0000-0003-1974-1234",
            "@type": "Person",
            "name": "Ross Spencer"
        },
        {
            "@id": "#Yann",
            "@type": "Person",
            "name": "Yann Nicolas"
        },
        {
            "@id": "#Organization-SMUC",
            "@type": "Organization",
            "name": "SMUC"
        },
        {
            "@id": "https://ror.org/010000000",
            "@type": "Organization",
            "name": "Zenodo"
        }
    ]
}
`)

// simpleCrate is the crate example from the RO-CRATE documentation.
var simpleCrate []byte = []byte(`
{
    "@context": "https://w3id.org/ro/crate/1.1/context",
    "@graph": [
        {
            "@type": "CreativeWork",
            "@id": "ro-crate-metadata.json",
            "conformsTo": {
                "@id": "https://w3id.org/ro/crate/1.1"
            },
            "about": {
                "@id": "./"
            }
        },
        {
            "@id": "./",
            "@type": "Dataset",
            "name": "My Research Object",
            "description": "The RO-Crate Root Data Entity",
            "datePublished": "2023-05-22",
            "license": {
                "@id": "https://creativecommons.org/licenses/by-nc-sa/4.0/"
            }
        }
    ]
}
`)

// sampleCrate is a sample crate from Language Technology and Data
// Analysis Laboratory (LADAL).
var sampleCrate []byte = []byte(`
{
  "@context": "https://w3id.org/ro/crate/1.1/context",
  "@graph": [
    {
      "@id": "ro-crate-metadata.json",
      "@type": "CreativeWork",
      "about": {
        "@id": "./"
      },
      "conformsTo": {
        "@id": "https://w3id.org/ro/crate/1.1"
      }
    },
    {
      "@id": "./",
      "@type": "Dataset",
      "name": "Introduction to Text Analysis in Python",
      "description": "The RO-Crate Root Data Entity"
    }
  ]
}
`)

// galaxyCrate is a sample crate from the Galaxy Project
// https://training.galaxyproject.org/
var galaxyCrate []byte = []byte(`
{
  "@context": "https://w3id.org/ro/crate/1.1/context",
  "@graph": [
    {
      "@type": "CreativeWork",
      "@id": "ro-crate-metadata.json",
      "conformsTo": {
        "@id": "https://w3id.org/ro/crate/1.1"
      },
      "about": {
        "@id": "./"
      }
    },
    {
      "@id": "./",
      "@type": [
        "Dataset",
        "LearningResource"
      ],
      "name": "Demo Crate",
      "description": "a demo crate for Galaxy training",
      "datePublished": "2024-03-08",
      "hasPart": [
        {
          "@id": "data.csv"
        }
      ],
      "author": [
        {
          "@id": "https://orcid.org/0000-0000-0000-0000"
        }
      ],
      "publisher": [
        {
          "@id": "https://ror.org/0abcdef00"
        }
      ],
      "license": {
        "@id": "https://spdx.org/licenses/CC-BY-NC-SA-4.0.html"
      },
      "contentUrl": [
        {
          "@id": "http://example.com/resource/data"
        }
      ]
    },
    {
      "@id": "data.csv",
      "@type": "File",
      "name": "data.csv"
    },
    {
      "@id": "https://orcid.org/0000-0000-0000-0000",
      "@type": "Person",
      "name": "A trainer"
    },
    {
      "@id": "https://ror.org/0abcdef00",
      "@type": "Organization",
      "name": "Galaxy Training Network"
    }
  ]
}
`)
