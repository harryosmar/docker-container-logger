package models

import "fmt"

// ContainerMeta holds container metadata
type ContainerMeta struct {
	ID     string
	Name   string
	Labels map[string]string
}

// LogEntry for output (used internally)
type LogEntry struct {
	ContainerID   string            `json:"container_id"`
	ContainerName string            `json:"container_name"`
	Labels        map[string]string `json:"labels"`
	Timestamp     string            `json:"timestamp"`
	Source        string            `json:"source"`
	Line          string            `json:"line"`
}

// SchemaLogDocument represents a schema-based log document
type SchemaLogDocument struct {
	Schema []string        `json:"schema"`
	Rows   [][]interface{} `json:"rows"`
}

// NewSchemaLogDocumentWithSchema creates a new schema-based log document with the provided schema
func NewSchemaLogDocumentWithSchema(schema []string) *SchemaLogDocument {
	return &SchemaLogDocument{
		Schema: schema,
		Rows:   [][]interface{}{},
	}
}

// AddLogEntry adds a log entry to the schema-based document
func (d *SchemaLogDocument) AddLogEntry(entry LogEntry) {
	row := []interface{}{
		entry.ContainerID,
		entry.ContainerName,
		entry.Labels,
		entry.Timestamp,
		entry.Source,
		entry.Line,
	}
	d.Rows = append(d.Rows, row)
}

// MergeSchemaLogDocuments merges two schema-based log documents
// Both documents must have the same schema
func MergeSchemaLogDocuments(doc1, doc2 *SchemaLogDocument) (*SchemaLogDocument, error) {
	// Verify schemas match
	if len(doc1.Schema) != len(doc2.Schema) {
		return nil, fmt.Errorf("schema length mismatch: %d vs %d", len(doc1.Schema), len(doc2.Schema))
	}

	for i, field := range doc1.Schema {
		if field != doc2.Schema[i] {
			return nil, fmt.Errorf("schema field mismatch at position %d: %s vs %s", i, field, doc2.Schema[i])
		}
	}

	// Create a new document with the same schema
	result := &SchemaLogDocument{
		Schema: make([]string, len(doc1.Schema)),
		Rows:   make([][]interface{}, 0, len(doc1.Rows)+len(doc2.Rows)),
	}

	// Copy schema
	copy(result.Schema, doc1.Schema)

	// Copy all rows from both documents
	result.Rows = append(result.Rows, doc1.Rows...)
	result.Rows = append(result.Rows, doc2.Rows...)

	return result, nil
}
