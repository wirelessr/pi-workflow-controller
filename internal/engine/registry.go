package engine

import (
	"fmt"
	"sort"
)

// Registry owns validated definitions and is immutable after construction.
type Registry struct {
	definitions map[string]Definition
}

func NewRegistry(definitions []Definition) (*Registry, error) {
	r := &Registry{definitions: make(map[string]Definition, len(definitions))}
	for _, definition := range definitions {
		if !validName(definition.Name) {
			return nil, newFailure(InvalidDefinition, "registry", fmt.Sprintf("invalid workflow name %q", definition.Name))
		}
		if _, exists := r.definitions[definition.Name]; exists {
			return nil, newFailure(InvalidDefinition, "registry", fmt.Sprintf("duplicate workflow name %q", definition.Name))
		}
		if definition.Execute == nil {
			return nil, newFailure(InvalidDefinition, "registry", fmt.Sprintf("workflow %q requires Execute", definition.Name))
		}
		if err := definition.Policy.validate(); err != nil {
			return nil, fmt.Errorf("workflow %q: %w", definition.Name, err)
		}
		r.definitions[definition.Name] = definition
	}
	return r, nil
}

func (r *Registry) Definitions() []Definition {
	definitions := make([]Definition, 0, len(r.definitions))
	for _, definition := range r.definitions {
		definitions = append(definitions, definition)
	}
	sort.Slice(definitions, func(i, j int) bool {
		return definitions[i].Name < definitions[j].Name
	})
	return definitions
}

func (r *Registry) Lookup(name string) (Definition, error) {
	definition, ok := r.definitions[name]
	if !ok {
		return Definition{}, newFailure(InvalidDefinition, "registry", fmt.Sprintf("unknown workflow %q", name))
	}
	return definition, nil
}

func validName(name string) bool {
	if name == "" || name == "." || name == ".." {
		return false
	}
	for i := 0; i < len(name); i++ {
		c := name[i]
		if (c < 'a' || c > 'z') && (c < 'A' || c > 'Z') && (c < '0' || c > '9') && c != '_' && c != '-' && c != '.' {
			return false
		}
	}
	return true
}
