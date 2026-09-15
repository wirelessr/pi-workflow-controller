package engine_test

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"

	"pi-workflow-controller/internal/engine"
)

func registryDefinition(name string) engine.Definition {
	return engine.Definition{
		Name:   name,
		Policy: engine.DefaultRunPolicy(),
		Execute: func(context.Context, *engine.Run, engine.Input) (engine.Result, error) {
			return engine.Result{}, nil
		},
	}
}

func requireInvalidDefinition(t *testing.T, err error) {
	t.Helper()
	var failure *engine.Failure
	if !errors.As(err, &failure) || failure.Code != engine.InvalidDefinition ||
		failure.Origin != engine.OriginDefinition || failure.DispatchAccepted != engine.AcceptedNo {
		t.Fatalf("got %v, want InvalidDefinition with Definition origin and accepted=no", err)
	}
}

func TestRegistryNames(t *testing.T) {
	cases := []struct {
		name  string
		valid bool
	}{
		{"workflow", true},
		{"AZaz09_-.", true},
		{"_", true},
		{"-", true},
		{".hidden", true},
		{"...", true},
		{"a..b", true},
		{"", false},
		{".", false},
		{"..", false},
		{"a/b", false},
		{"../a", false},
		{"a\\b", false},
		{"a b", false},
		{" a", false},
		{"a\n", false},
		{"a\t", false},
		{"a\x00", false},
		{"a:b", false},
		{"a@b", false},
		{"工作", false},
		{"café", false},
		{"a\xff", false},
	}
	for _, tc := range cases {
		t.Run(fmt.Sprintf("%q", tc.name), func(t *testing.T) {
			r, err := engine.NewRegistry([]engine.Definition{registryDefinition(tc.name)})
			if tc.valid {
				if err != nil {
					t.Fatal(err)
				}
				definition, err := r.Lookup(tc.name)
				if err != nil || definition.Name != tc.name {
					t.Fatalf("Lookup(%q) = %+v, %v", tc.name, definition, err)
				}
				return
			}
			requireInvalidDefinition(t, err)
			if r != nil {
				t.Fatal("invalid registry returned a usable value")
			}
		})
	}
}

func TestRegistryRejectsInvalidDefinitions(t *testing.T) {
	missingExecute := registryDefinition("missing-execute")
	missingExecute.Execute = nil
	zeroPolicy := registryDefinition("zero-policy")
	zeroPolicy.Policy = engine.RunPolicy{}
	cases := []struct {
		name        string
		definitions []engine.Definition
	}{
		{"duplicate", []engine.Definition{registryDefinition("same"), registryDefinition("same")}},
		{"missing Execute", []engine.Definition{missingExecute}},
		{"zero policy", []engine.Definition{zeroPolicy}},
		{"invalid after valid", []engine.Definition{registryDefinition("valid"), missingExecute}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			r, err := engine.NewRegistry(tc.definitions)
			requireInvalidDefinition(t, err)
			if r != nil {
				t.Fatal("invalid registry returned a usable value")
			}
		})
	}
}

func TestRegistryRequiresEveryPolicyValuePositive(t *testing.T) {
	policyType := reflect.TypeOf(engine.RunPolicy{})
	var fields []string
	for i := 0; i < policyType.NumField(); i++ {
		field := policyType.Field(i)
		if field.Type.Kind() == reflect.Struct {
			for j := 0; j < field.Type.NumField(); j++ {
				fields = append(fields, field.Name+"."+field.Type.Field(j).Name)
			}
		} else {
			fields = append(fields, field.Name)
		}
	}
	for _, field := range fields {
		for _, value := range []int64{-1, 0, 1} {
			t.Run(fmt.Sprintf("%s/%d", field, value), func(t *testing.T) {
				definition := registryDefinition("policy")
				v := reflect.ValueOf(&definition.Policy).Elem()
				for _, part := range strings.Split(field, ".") {
					v = v.FieldByName(part)
				}
				v.SetInt(value)
				r, err := engine.NewRegistry([]engine.Definition{definition})
				if value > 0 {
					if err != nil {
						t.Fatal(err)
					}
					got, err := r.Lookup(definition.Name)
					if err != nil || got.Policy != definition.Policy {
						t.Fatalf("explicit positive policy changed: %+v, %v", got.Policy, err)
					}
					return
				}
				requireInvalidDefinition(t, err)
				if r != nil || !strings.Contains(err.Error(), field) {
					t.Fatalf("expected rejection identifying %s, got registry=%v error=%v", field, r, err)
				}
			})
		}
	}
}

func TestRegistryOwnsDefinitionsAndListsSorted(t *testing.T) {
	definitions := []engine.Definition{registryDefinition("zeta"), registryDefinition("Alpha"), registryDefinition("alpha")}
	definitions[1].Description = "description"
	definitions[1].Version = "v1"
	executed := false
	definitions[1].Execute = func(context.Context, *engine.Run, engine.Input) (engine.Result, error) {
		executed = true
		return engine.Result{}, nil
	}
	want := definitions[1]
	r, err := engine.NewRegistry(definitions)
	if err != nil {
		t.Fatal(err)
	}
	if definitions[0].Name != "zeta" {
		t.Fatal("constructor reordered caller's definitions")
	}
	definitions[1] = registryDefinition("changed")
	for i := 0; i < 2; i++ {
		listed := r.Definitions()
		var names []string
		for _, definition := range listed {
			names = append(names, definition.Name)
		}
		if !reflect.DeepEqual(names, []string{"Alpha", "alpha", "zeta"}) {
			t.Fatalf("definitions not sorted by exact name: %v", names)
		}
		listed[0] = registryDefinition("changed-list")
		got, err := r.Lookup("Alpha")
		if err != nil || got.Name != want.Name || got.Description != want.Description || got.Version != want.Version || got.Policy != want.Policy {
			t.Fatalf("registry definition changed: %+v, %v", got, err)
		}
		if executed {
			t.Fatal("registry invoked workflow")
		}
		if i == 1 {
			if _, err := got.Execute(context.Background(), nil, engine.Input{}); err != nil || !executed {
				t.Fatalf("registry did not preserve Execute: %v", err)
			}
		}
		got.Name = "changed-lookup"
		got.Policy.MaxLiveSessions = 0
	}
}

func TestRegistryEmptyAndUnknownLookup(t *testing.T) {
	for _, definitions := range [][]engine.Definition{nil, {}, {registryDefinition("known")}} {
		r, err := engine.NewRegistry(definitions)
		if err != nil {
			t.Fatal(err)
		}
		if len(r.Definitions()) != len(definitions) {
			t.Fatal("registry changed definition count")
		}
		for _, name := range []string{"missing", "KNOWN", "", "../known"} {
			got, err := r.Lookup(name)
			requireInvalidDefinition(t, err)
			if !reflect.DeepEqual(got, engine.Definition{}) {
				t.Fatalf("unknown lookup returned a definition: %+v", got)
			}
		}
	}
}
