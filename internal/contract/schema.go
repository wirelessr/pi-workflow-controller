package contract

import (
	"encoding/json"
	"fmt"
	"maps"
	"math"
	"math/big"
	"net/url"
	"strconv"
	"strings"

	"github.com/santhosh-tekuri/jsonschema/v6"
)

// EnvelopeURI identifies the framework-owned resource, not a network endpoint.
const EnvelopeURI = "https://pi-workflow-controller.local/schemas/envelope.v1.json"

const envelopeJSON = `{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "$id": "https://pi-workflow-controller.local/schemas/envelope.v1.json",
  "type": "object",
  "required": ["meta", "data", "files"],
  "additionalProperties": false,
  "properties": {
    "meta": {
      "type": "object",
      "required": ["version", "run_id", "invocation_id", "attempt_id", "dispatch_token", "schema_id"],
      "additionalProperties": false,
      "properties": {
        "version": {"type": "integer", "const": 1},
        "run_id": {"type": "string", "minLength": 1},
        "invocation_id": {"type": "string", "minLength": 1},
        "attempt_id": {"type": "string", "minLength": 1},
        "dispatch_token": {"type": "string", "minLength": 1},
        "schema_id": {"type": "string", "minLength": 1}
      }
    },
    "data": true,
    "files": {
      "type": "array",
      "items": {
        "type": "object",
        "required": ["id", "kind", "path"],
        "additionalProperties": false,
        "properties": {
          "id": {"type": "string", "minLength": 1},
          "kind": {"enum": ["evidence", "artifact"]},
          "path": {"type": "string", "minLength": 1}
        }
      }
    }
  }
}`

type Resource struct {
	URI  string
	JSON json.RawMessage
}

type SchemaDefinition struct {
	ID  string
	URI string
}

// Registry owns its resource bytes and compiled schemas. After construction it
// is immutable and can be shared by concurrent attempts.
type Registry struct {
	resources map[string]json.RawMessage
	schemas   map[string]*jsonschema.Schema
	uris      map[string]string
	envelope  *jsonschema.Schema
}

func NewRegistry(resources []Resource, definitions []SchemaDefinition) (*Registry, error) {
	r := &Registry{
		resources: make(map[string]json.RawMessage),
		schemas:   make(map[string]*jsonschema.Schema),
		uris:      make(map[string]string),
	}
	compiler := jsonschema.NewCompiler()
	compiler.DefaultDraft(jsonschema.Draft2020)
	compiler.AssertFormat()
	compiler.UseLoader(nil) // Never fall back to the host filesystem or network.

	all := make([]Resource, 0, len(resources)+1)
	all = append(all, Resource{URI: EnvelopeURI, JSON: json.RawMessage(envelopeJSON)})
	all = append(all, resources...)
	for _, resource := range all {
		u, err := url.Parse(resource.URI)
		if err != nil || !u.IsAbs() || strings.Contains(resource.URI, "#") {
			return nil, fmt.Errorf("schema resource URI must be absolute and fragment-free: %q", resource.URI)
		}
		if _, exists := r.resources[resource.URI]; exists {
			return nil, fmt.Errorf("duplicate schema resource URI %q", resource.URI)
		}
		r.resources[resource.URI] = append(json.RawMessage(nil), resource.JSON...)
	}

	var nodes []schemaNode
	documents := make(map[string]any)
	visited := make(map[string]bool)
	for _, resource := range all {
		// Resources are binary-owned definitions, not untrusted candidates; the
		// candidate depth policy is applied by Store before validation instead.
		doc, err := parseJSON(r.resources[resource.URI], math.MaxInt)
		if err != nil {
			return nil, fmt.Errorf("schema resource %q: %w", resource.URI, err)
		}
		documents[resource.URI] = doc
		base, _ := url.Parse(resource.URI)
		if err := collectSchemaNodes(doc, base, resource.URI, "", &nodes, visited); err != nil {
			return nil, err
		}
		if err := compiler.AddResource(resource.URI, doc); err != nil {
			return nil, fmt.Errorf("register schema resource %q: %w", resource.URI, err)
		}
	}

	// Include embedded resources, while rejecting ambiguous IDs across files.
	owners := make(map[string]string)
	for uri := range r.resources {
		owners[uri] = uri + "#"
	}
	registerIDs := func(nodes []schemaNode) error {
		for _, node := range nodes {
			if _, hasID := node.object["$id"]; !hasID {
				continue
			}
			id := node.base.String()
			if owner, exists := owners[id]; exists && owner != node.location {
				return fmt.Errorf("duplicate schema resource ID %q", id)
			}
			owners[id] = node.location
		}
		return nil
	}
	if err := registerIDs(nodes); err != nil {
		return nil, err
	}
	for i := 0; i < len(nodes); i++ {
		node := nodes[i]
		for _, keyword := range []string{"$ref", "$dynamicRef"} {
			ref, ok := node.object[keyword].(string)
			if !ok {
				continue // The metaschema reports invalid keyword types.
			}
			u, err := node.base.Parse(ref)
			if err != nil {
				return nil, fmt.Errorf("invalid %s at %q: %w", keyword, node.location, err)
			}
			fragment := u.Fragment
			u.Fragment, u.RawFragment = "", ""
			owner, exists := owners[u.String()]
			if !exists {
				continue // A later pointer target can introduce this resource ID.
			}
			if !strings.HasPrefix(fragment, "/") {
				continue // Named anchors are collected from known schema nodes.
			}
			target, _ := url.Parse(owner)
			pointer := target.Fragment + fragment
			target.Fragment, target.RawFragment = "", ""
			resourceURI := target.String()
			value, err := schemaPointer(documents[resourceURI], pointer)
			if err != nil {
				return nil, fmt.Errorf("invalid schema reference %q: %w", ref, err)
			}
			// A JSON Pointer can turn an otherwise unknown keyword's value into
			// a schema. Check its dialect and references before compilation too.
			base, longest := target, -1
			for _, ancestor := range nodes {
				location, _ := url.Parse(ancestor.location)
				p := location.Fragment
				location.Fragment, location.RawFragment = "", ""
				if location.String() == resourceURI && len(p) > longest && strings.HasPrefix(pointer, p+"/") {
					base, longest = ancestor.base, len(p)
				}
			}
			start := len(nodes)
			if err := collectSchemaNodes(value, base, resourceURI, pointer, &nodes, visited); err != nil {
				return nil, err
			}
			if err := registerIDs(nodes[start:]); err != nil {
				return nil, err
			}
			if len(nodes) > start {
				// Revisit references whose owners were not known on the first pass.
				i = -1
				break
			}
		}
	}
	for _, node := range nodes {
		for _, keyword := range []string{"$ref", "$dynamicRef"} {
			if ref, ok := node.object[keyword].(string); ok {
				u, _ := node.base.Parse(ref) // Syntax was checked during discovery.
				u.Fragment, u.RawFragment = "", ""
				if _, exists := owners[u.String()]; !exists {
					return nil, fmt.Errorf("unregistered schema reference %q at %q", ref, node.location)
				}
			}
		}
	}
	for uri, owner := range owners {
		if _, registered := r.resources[uri]; registered {
			continue
		}
		location, _ := url.Parse(owner)
		pointer := location.Fragment
		location.Fragment, location.RawFragment = "", ""
		value, err := schemaPointer(documents[location.String()], pointer)
		if err != nil {
			return nil, err
		}
		// v6 resolves embedded IDs only within the current document. Register
		// an in-memory view at each ID, without resolving its relative $id a
		// second time or changing the original resource bytes shared with Pi.
		alias := maps.Clone(value.(map[string]any))
		alias["$id"] = uri
		if err := compiler.AddResource(uri, alias); err != nil {
			return nil, fmt.Errorf("register schema alias %q for %q: %w", uri, owner, err)
		}
	}
	for _, node := range nodes {
		// Compile unused $defs too: a dormant external reference must not pass
		// preflight just because this output does not currently exercise it.
		if _, err := compiler.Compile(node.location); err != nil {
			return nil, fmt.Errorf("compile schema %q: %w", node.location, err)
		}
	}

	var err error
	r.envelope, err = compiler.Compile(EnvelopeURI)
	if err != nil {
		return nil, fmt.Errorf("compile envelope: %w", err)
	}
	usedURIs := make(map[string]bool)
	for _, definition := range definitions {
		if definition.ID == "" {
			return nil, fmt.Errorf("schema ID must not be empty")
		}
		if _, exists := r.schemas[definition.ID]; exists {
			return nil, fmt.Errorf("duplicate schema ID %q", definition.ID)
		}
		if usedURIs[definition.URI] {
			return nil, fmt.Errorf("duplicate schema definition URI %q", definition.URI)
		}
		if _, exists := r.resources[definition.URI]; !exists {
			return nil, fmt.Errorf("schema %q uses unregistered resource %q", definition.ID, definition.URI)
		}
		schema, err := compiler.Compile(definition.URI)
		if err != nil {
			return nil, fmt.Errorf("compile schema %q: %w", definition.ID, err)
		}
		r.schemas[definition.ID] = schema
		r.uris[definition.ID] = definition.URI
		usedURIs[definition.URI] = true
	}
	return r, nil
}

func (r *Registry) Has(id string) bool {
	if r == nil {
		return false
	}
	_, ok := r.schemas[id]
	return ok
}

func validateSchema(schema *jsonschema.Schema, value any) (err error) {
	// Candidate numbers can exceed the dependency's big.Rat representation.
	// Keep its panic inside the validator boundary, without losing precision.
	defer func() {
		if recovered := recover(); recovered != nil {
			err = fmt.Errorf("jsonschema validation panic at %q: %v", schema.Location, recovered)
		}
	}()
	return schema.Validate(value)
}

func (r *Registry) validateData(schemaID string, data any) error {
	if !r.Has(schemaID) {
		return fmt.Errorf("unknown schema ID %q", schemaID)
	}
	if err := validateSchema(r.schemas[schemaID], data); err != nil {
		return fmt.Errorf("invalid contract data for schema %q: %w", schemaID, err)
	}
	return nil
}

type schemaNode struct {
	location string
	base     *url.URL
	object   map[string]any
}

func schemaPointer(value any, pointer string) (any, error) {
	if pointer == "" {
		return value, nil
	}
	for _, part := range strings.Split(strings.TrimPrefix(pointer, "/"), "/") {
		part = strings.ReplaceAll(strings.ReplaceAll(part, "~1", "/"), "~0", "~")
		switch container := value.(type) {
		case map[string]any:
			var exists bool
			value, exists = container[part]
			if !exists {
				return nil, fmt.Errorf("schema pointer %q does not exist", pointer)
			}
		case []any:
			index, err := strconv.Atoi(part)
			if err != nil || index < 0 || index >= len(container) {
				return nil, fmt.Errorf("invalid array index in schema pointer %q", pointer)
			}
			value = container[index]
		default:
			return nil, fmt.Errorf("schema pointer %q traverses a scalar", pointer)
		}
	}
	return value, nil
}

func checkLiteralNumbers(value any) error {
	switch value := value.(type) {
	case json.Number:
		if _, ok := new(big.Rat).SetString(value.String()); !ok {
			return fmt.Errorf("unrepresentable literal number %s", value)
		}
	case []any:
		for _, item := range value {
			if err := checkLiteralNumbers(item); err != nil {
				return err
			}
		}
	case map[string]any:
		for _, item := range value {
			if err := checkLiteralNumbers(item); err != nil {
				return err
			}
		}
	}
	return nil
}

func collectSchemaNodes(value any, base *url.URL, resourceURI, pointer string, nodes *[]schemaNode, visited map[string]bool) error {
	location := resourceURI + "#" + (&url.URL{Fragment: pointer}).EscapedFragment()
	if visited[location] {
		return nil
	}
	visited[location] = true
	object, ok := value.(map[string]any)
	if !ok {
		if _, ok := value.(bool); !ok {
			return fmt.Errorf("schema at %q must be an object or boolean", location)
		}
		*nodes = append(*nodes, schemaNode{location: location, base: base})
		return nil
	}
	if dialect, exists := object["$schema"]; exists {
		uri, _ := dialect.(string)
		uri = strings.TrimSuffix(uri, "#")
		if uri != jsonschema.Draft2020.String() && uri != "http://json-schema.org/draft/2020-12/schema" {
			return fmt.Errorf("schema at %q must declare Draft 2020-12, got %v", location, dialect)
		}
	}
	if id, ok := object["$id"].(string); ok {
		u, err := base.Parse(id)
		if err != nil || u.Fragment != "" {
			return fmt.Errorf("invalid schema resource ID %q at %q", id, location)
		}
		u.Fragment, u.RawFragment = "", ""
		base = u
	}
	// v6 drops unrepresentable numeric bounds and truncates count bounds to
	// int. Reject these definitions before its metaschema/compiler can do so.
	for _, keyword := range []string{"minimum", "maximum", "exclusiveMinimum", "exclusiveMaximum", "multipleOf", "minLength", "maxLength", "minItems", "maxItems", "minProperties", "maxProperties", "minContains", "maxContains"} {
		if number, ok := object[keyword].(json.Number); ok {
			rat, ok := new(big.Rat).SetString(number.String())
			if !ok {
				return fmt.Errorf("unrepresentable %s at %q: %s", keyword, location, number)
			}
			switch keyword {
			case "minimum", "maximum", "exclusiveMinimum", "exclusiveMaximum", "multipleOf":
			default:
				if !rat.IsInt() || !rat.Num().IsInt64() || rat.Sign() < 0 || rat.Num().Int64() > int64(math.MaxInt) {
					return fmt.Errorf("unrepresentable %s at %q: %s", keyword, location, number)
				}
			}
		}
	}
	// Equality silently returns false for numbers the dependency cannot represent,
	// which would invert to a false success under not or conditional schemas.
	for _, keyword := range []string{"const", "enum"} {
		if err := checkLiteralNumbers(object[keyword]); err != nil {
			return fmt.Errorf("%s at %q: %w", keyword, location, err)
		}
	}
	*nodes = append(*nodes, schemaNode{location: location, base: base, object: object})
	child := func(value any, parts ...string) error {
		p := pointer
		for _, part := range parts {
			p += "/" + strings.ReplaceAll(strings.ReplaceAll(part, "~", "~0"), "/", "~1")
		}
		return collectSchemaNodes(value, base, resourceURI, p, nodes, visited)
	}
	for _, keyword := range []string{"not", "if", "then", "else", "items", "contains", "additionalProperties", "propertyNames", "unevaluatedProperties", "unevaluatedItems", "contentSchema"} {
		if value, exists := object[keyword]; exists {
			if err := child(value, keyword); err != nil {
				return err
			}
		}
	}
	for _, keyword := range []string{"$defs", "definitions", "properties", "patternProperties", "dependentSchemas", "dependencies"} {
		if values, ok := object[keyword].(map[string]any); ok {
			for key, value := range values {
				if _, array := value.([]any); keyword == "dependencies" && array {
					continue
				}
				if err := child(value, keyword, key); err != nil {
					return err
				}
			}
		}
	}
	for _, keyword := range []string{"allOf", "anyOf", "oneOf", "prefixItems"} {
		if values, ok := object[keyword].([]any); ok {
			for i, value := range values {
				if err := child(value, keyword, fmt.Sprint(i)); err != nil {
					return err
				}
			}
		}
	}
	return nil
}
