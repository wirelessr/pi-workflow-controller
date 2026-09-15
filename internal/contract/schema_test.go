package contract

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
)

const schemaTestURI = "https://workflow.example/schemas/output.json"

func schemaTestRegistry(t *testing.T, schema string) *Registry {
	t.Helper()
	r, err := NewRegistry(
		[]Resource{{URI: schemaTestURI, JSON: json.RawMessage(schema)}},
		[]SchemaDefinition{{ID: "test.output.v1", URI: schemaTestURI}},
	)
	if err != nil {
		t.Fatal(err)
	}
	return r
}

func schemaTestEnvelope(t *testing.T, data string) map[string]any {
	t.Helper()
	value, err := parseJSON([]byte(`{
		"meta":{"version":1,"run_id":"run","invocation_id":"invocation","attempt_id":"attempt","dispatch_token":"token","schema_id":"test.output.v1"},
		"data":`+data+`,"files":[]
	}`), 64)
	if err != nil {
		t.Fatal(err)
	}
	return value.(map[string]any)
}

func TestRegistryEnvelope(t *testing.T) {
	r := schemaTestRegistry(t, `true`)
	cases := []struct {
		name   string
		change func(map[string]any) any
		valid  bool
	}{
		{"valid", func(v map[string]any) any { return v }, true},
		{"null envelope", func(v map[string]any) any { return nil }, false},
		{"array envelope", func(v map[string]any) any { return []any{} }, false},
		{"unknown field", func(v map[string]any) any { v["extra"] = true; return v }, false},
		{"missing meta", func(v map[string]any) any { delete(v, "meta"); return v }, false},
		{"missing data", func(v map[string]any) any { delete(v, "data"); return v }, false},
		{"missing files", func(v map[string]any) any { delete(v, "files"); return v }, false},
		{"null meta", func(v map[string]any) any { v["meta"] = nil; return v }, false},
		{"null files", func(v map[string]any) any { v["files"] = nil; return v }, false},
		{"object files", func(v map[string]any) any { v["files"] = map[string]any{}; return v }, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			value := tc.change(schemaTestEnvelope(t, `null`))
			err := validateSchema(r.envelope, value)
			if (err == nil) != tc.valid {
				t.Fatalf("valid=%v, error=%v", tc.valid, err)
			}
		})
	}
	for _, field := range []string{"version", "run_id", "invocation_id", "attempt_id", "dispatch_token", "schema_id"} {
		t.Run("missing meta "+field, func(t *testing.T) {
			value := schemaTestEnvelope(t, `{}`)
			delete(value["meta"].(map[string]any), field)
			if err := validateSchema(r.envelope, value); err == nil {
				t.Fatalf("accepted missing meta.%s", field)
			}
		})
	}
	for _, tc := range []struct {
		field string
		value any
	}{
		{"version", json.Number("2")}, {"version", "1"}, {"version", json.Number("1.5")},
		{"run_id", ""}, {"invocation_id", nil}, {"attempt_id", json.Number("1")},
		{"dispatch_token", ""}, {"schema_id", ""}, {"extra", "unexpected"},
	} {
		t.Run(fmt.Sprintf("invalid meta %s %v", tc.field, tc.value), func(t *testing.T) {
			value := schemaTestEnvelope(t, `{}`)
			value["meta"].(map[string]any)[tc.field] = tc.value
			if err := validateSchema(r.envelope, value); err == nil {
				t.Fatalf("accepted invalid meta.%s", tc.field)
			}
		})
	}
}

func TestRegistryEnvelopeFiles(t *testing.T) {
	r := schemaTestRegistry(t, `true`)
	cases := []struct {
		name  string
		file  string
		valid bool
	}{
		{"evidence", `{"id":"source","kind":"evidence","path":"evidence/source.txt"}`, true},
		{"artifact", `{"id":"report","kind":"artifact","path":"artifacts/report.txt"}`, true},
		{"plural kind", `{"id":"report","kind":"artifacts","path":"artifacts/report.txt"}`, false},
		{"unknown kind", `{"id":"source","kind":"other","path":"evidence/source.txt"}`, false},
		{"missing id", `{"kind":"evidence","path":"evidence/source.txt"}`, false},
		{"missing kind", `{"id":"source","path":"evidence/source.txt"}`, false},
		{"missing path", `{"id":"source","kind":"evidence"}`, false},
		{"extra field", `{"id":"source","kind":"evidence","path":"evidence/source.txt","sha256":"agent-digest"}`, false},
		{"empty id", `{"id":"","kind":"evidence","path":"evidence/source.txt"}`, false},
		{"empty path", `{"id":"source","kind":"evidence","path":""}`, false},
		{"wrong path type", `{"id":"source","kind":"evidence","path":1}`, false},
		{"null item", `null`, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			value := schemaTestEnvelope(t, `{}`)
			file, err := parseJSON([]byte(tc.file), 64)
			if err != nil {
				t.Fatal(err)
			}
			value["files"] = []any{file}
			err = validateSchema(r.envelope, value)
			if (err == nil) != tc.valid {
				t.Fatalf("valid=%v, error=%v", tc.valid, err)
			}
		})
	}
}

func TestRegistryValidatesOnlyDataAgainstWorkflowSchema(t *testing.T) {
	for _, data := range []string{`null`, `true`, `"text"`, `42`, `[]`, `{}`} {
		t.Run(data, func(t *testing.T) {
			r := schemaTestRegistry(t, `{"const":`+data+`}`)
			if err := r.validateData("test.output.v1", schemaTestEnvelope(t, data)["data"]); err != nil {
				t.Fatal(err)
			}
		})
	}
	r := schemaTestRegistry(t, `{"type":"object","required":["answer"],"additionalProperties":false,"properties":{"answer":{"type":"string"}}}`)
	for _, data := range []string{`{}`, `{"answer":42}`, `{"answer":"ok","extra":true}`} {
		if err := r.validateData("test.output.v1", schemaTestEnvelope(t, data)["data"]); err == nil {
			t.Fatalf("accepted invalid data %s", data)
		}
	}
	if err := r.validateData("test.output.v1", schemaTestEnvelope(t, `{"answer":"ok"}`)["data"]); err != nil {
		t.Fatal(err)
	}
	if err := r.validateData("unknown", schemaTestEnvelope(t, `{"answer":"ok"}`)["data"]); err == nil {
		t.Fatal("accepted unknown schema")
	}
}

func TestRegistryExactNumbers(t *testing.T) {
	cases := []struct{ name, schema, valid, invalid string }{
		{"integer const", `{"type":"integer","const":9007199254740993}`, `9007199254740993`, `9007199254740992`},
		{"integer minimum", `{"type":"integer","minimum":9007199254740993}`, `9007199254740993`, `9007199254740992`},
		{"integer maximum", `{"type":"integer","maximum":9007199254740992}`, `9007199254740992`, `9007199254740993`},
		{"decimal const", `{"const":0.123456789012345678901}`, `0.123456789012345678901`, `0.123456789012345678902`},
		{"large exponent", `{"type":"number","const":1e400}`, `1e400`, `1e399`},
		{"nested numeric const", `{"const":{"n":[1e400]}}`, `{"n":[1e400]}`, `{"n":[1e399]}`},
		{"nested numeric enum", `{"enum":[{"n":[1e400]}]}`, `{"n":[1e400]}`, `{"n":[1e399]}`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			r := schemaTestRegistry(t, tc.schema)
			if err := r.validateData("test.output.v1", schemaTestEnvelope(t, tc.valid)["data"]); err != nil {
				t.Fatal(err)
			}
			if err := r.validateData("test.output.v1", schemaTestEnvelope(t, tc.invalid)["data"]); err == nil {
				t.Fatalf("accepted imprecise value %s", tc.invalid)
			}
		})
	}
}

func TestRegistryRejectsUnrepresentableConstraints(t *testing.T) {
	for _, keyword := range []string{"minimum", "maximum", "exclusiveMinimum", "exclusiveMaximum", "multipleOf", "minLength", "maxLength", "minItems", "maxItems", "minProperties", "maxProperties", "minContains", "maxContains"} {
		number := "1e1000001"
		if strings.HasPrefix(keyword, "min") && keyword != "minimum" || strings.HasPrefix(keyword, "max") && keyword != "maximum" {
			number = "9223372036854775808"
		}
		t.Run(keyword, func(t *testing.T) {
			_, err := NewRegistry([]Resource{{URI: schemaTestURI, JSON: json.RawMessage(fmt.Sprintf(`{"%s":%s}`, keyword, number))}}, []SchemaDefinition{{ID: "test.output.v1", URI: schemaTestURI}})
			if err == nil {
				t.Fatal("unsupported constraint compiled without preserving its value")
			}
		})
	}
}

func TestRegistryRejectsUnrepresentableLiterals(t *testing.T) {
	for _, literal := range []string{`1e1000001`, `-1e-1000001`, `{"n":1e1000001}`, `[{"n":[1e1000001]}]`} {
		for _, keyword := range []string{"const", "enum"} {
			t.Run(keyword+"/"+literal, func(t *testing.T) {
				value := literal
				if keyword == "enum" {
					value = "[" + literal + "]"
				}
				schema := fmt.Sprintf(`{"not":{"%s":%s}}`, keyword, value)
				r, err := NewRegistry([]Resource{{URI: schemaTestURI, JSON: json.RawMessage(schema)}}, []SchemaDefinition{{ID: "test.output.v1", URI: schemaTestURI}})
				if err == nil {
					data, parseErr := parseJSON([]byte(literal), 64)
					if parseErr != nil {
						t.Fatal(parseErr)
					}
					t.Fatalf("unsupported literal compiled; validation of prohibited value = %v", r.validateData("test.output.v1", data))
				}
			})
		}
	}
}

func TestRegistryValidatorPanicBoundary(t *testing.T) {
	for _, constraint := range []string{`"minimum":0`, `"maximum":10`, `"exclusiveMinimum":0`, `"exclusiveMaximum":10`, `"multipleOf":1`} {
		for _, number := range []string{"1e1000001", "-1e1000001", "1e-1000001", "-1e-1000001"} {
			t.Run(constraint+"/"+number, func(t *testing.T) {
				r := schemaTestRegistry(t, `{"type":"number",`+constraint+`}`)
				value := schemaTestEnvelope(t, number)
				if err := validateSchema(r.envelope, value); err != nil {
					t.Fatalf("unconstrained data rejected by envelope: %v", err)
				}
				err := r.validateData("test.output.v1", value["data"])
				if err == nil || !strings.Contains(err.Error(), "jsonschema validation panic") || !strings.Contains(err.Error(), schemaTestURI) {
					t.Fatalf("missing diagnostic for dependency panic: %v", err)
				}
				if value["data"] != json.Number(number) {
					t.Fatal("validation changed the exact number")
				}
				if err := r.validateData("test.output.v1", json.Number("1")); err != nil {
					t.Fatalf("registry unusable after panic: %v", err)
				}
			})
		}
	}
	for _, number := range []string{"1e1000001", "1e-1000001"} {
		t.Run("no numeric quota/"+number, func(t *testing.T) {
			r := schemaTestRegistry(t, `{"type":"number"}`)
			if err := r.validateData("test.output.v1", schemaTestEnvelope(t, number)["data"]); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestRegistryDataDoesNotRevalidateEnvelope(t *testing.T) {
	r := schemaTestRegistry(t, `{"type":"string"}`)
	value := schemaTestEnvelope(t, `"ok"`)
	delete(value, "meta")
	if err := validateSchema(r.envelope, value); err == nil {
		t.Fatal("accepted invalid envelope")
	}
	if err := r.validateData("test.output.v1", value["data"]); err != nil {
		t.Fatalf("data validation depends on envelope: %v", err)
	}
	var absent *Registry
	if err := absent.validateData("unknown", nil); err == nil || !strings.Contains(err.Error(), "unknown schema ID") {
		t.Fatalf("missing unknown schema diagnostic: %v", err)
	}
}

func TestRegistryFormatAssertions(t *testing.T) {
	cases := []struct{ format, valid, invalid string }{
		{"date-time", "2026-09-14T12:34:56Z", "2026-02-30T12:34:56Z"},
		{"email", "person@example.com", "not-an-email"},
		{"ipv4", "192.0.2.1", "999.0.0.1"},
		{"uuid", "550e8400-e29b-41d4-a716-446655440000", "invalid-uuid"},
	}
	for _, tc := range cases {
		t.Run(tc.format, func(t *testing.T) {
			r := schemaTestRegistry(t, fmt.Sprintf(`{"$schema":"https://json-schema.org/draft/2020-12/schema","type":"string","format":%q}`, tc.format))
			if err := r.validateData("test.output.v1", schemaTestEnvelope(t, fmt.Sprintf("%q", tc.valid))["data"]); err != nil {
				t.Fatal(err)
			}
			if err := r.validateData("test.output.v1", schemaTestEnvelope(t, fmt.Sprintf("%q", tc.invalid))["data"]); err == nil {
				t.Fatal("format assertion was not applied")
			}
		})
	}
}

func TestRegistryOfflineReferences(t *testing.T) {
	resources := []Resource{
		{URI: schemaTestURI, JSON: json.RawMessage(`{"$schema":"https://json-schema.org/draft/2020-12/schema","$id":"https://workflow.example/schemas/output.json","type":"object","properties":{"answer":{"$ref":"common.json#/$defs/answer"}},"required":["answer"]}`)},
		{URI: "https://workflow.example/schemas/common.json", JSON: json.RawMessage(`{"$defs":{"answer":{"type":"integer","minimum":9007199254740993}}}`)},
	}
	r, err := NewRegistry(resources, []SchemaDefinition{{ID: "test.output.v1", URI: schemaTestURI}})
	if err != nil {
		t.Fatal(err)
	}
	if err := r.validateData("test.output.v1", schemaTestEnvelope(t, `{"answer":9007199254740993}`)["data"]); err != nil {
		t.Fatal(err)
	}
	if err := r.validateData("test.output.v1", schemaTestEnvelope(t, `{"answer":9007199254740992}`)["data"]); err == nil {
		t.Fatal("referenced schema was not applied")
	}
	for _, resource := range resources {
		if !bytes.Equal(r.resources[resource.URI], resource.JSON) {
			t.Fatalf("resource bytes changed: %s", resource.URI)
		}
	}
	if !bytes.Equal(r.resources[EnvelopeURI], []byte(envelopeJSON)) {
		t.Fatal("missing envelope resource bytes")
	}
	if r.uris["test.output.v1"] != schemaTestURI {
		t.Fatal("definition URI unavailable to Store")
	}
}

func TestRegistryCrossBundleEmbeddedIDs(t *testing.T) {
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		_, _ = w.Write([]byte(`false`))
	}))
	defer server.Close()
	base := server.URL + "/schemas/"
	cases := []struct{ name, output, bundle, other, valid, invalid string }{
		{
			"embedded ID",
			`{"$ref":"BASEcommon.json"}`,
			`{"$defs":{"common":{"$id":"common.json","type":"string"}}}`,
			`true`, `"ok"`, `42`,
		},
		{
			"relative inner ref across bundles",
			`{"$ref":"BASEnested/common.json"}`,
			`{"$defs":{"common":{"$id":"nested/common.json","$ref":"value.json"}}}`,
			`{"$defs":{"value":{"$id":"nested/value.json","type":"string"}}}`,
			`"ok"`, `42`,
		},
		{
			"canonical root alias and relative inner ref",
			`{"$ref":"BASEnested/canonical.json"}`,
			`{"$id":"nested/canonical.json","$ref":"value.json"}`,
			`{"$defs":{"value":{"$id":"nested/value.json","type":"string"}}}`,
			`"ok"`, `42`,
		},
		{
			"relative ID beneath canonical root",
			`{"$ref":"BASEnested/common.json"}`,
			`{"$id":"canonical/bundle.json","$defs":{"common":{"$id":"../nested/common.json","$ref":"../value.json"}}}`,
			`{"$id":"value.json","type":"string"}`,
			`"ok"`, `42`,
		},
		{
			"escaped pointer into embedded resource",
			`{"$ref":"BASEcommon.json#/$defs/a~1b~0c%20%25"}`,
			`{"$defs":{"common":{"$id":"common.json","$defs":{"a/b~c %":{"type":"string"}}}}}`,
			`true`, `"ok"`, `42`,
		},
		{
			"pointer crosses nested resource base",
			`{"$ref":"BASEcommon.json#/$defs/inner"}`,
			`{"$defs":{"common":{"$id":"common.json","$defs":{"inner":{"$id":"nested/inner.json","$ref":"value.json"}}}}}`,
			`{"$id":"nested/value.json","type":"string"}`,
			`"ok"`, `42`,
		},
		{
			"anchor scoped to embedded resource",
			`{"$ref":"BASEcommon.json#answer"}`,
			`{"$defs":{"outside":{"$anchor":"answer","type":"integer"},"common":{"$id":"common.json","$defs":{"answer":{"$anchor":"answer","type":"string"}}}}}`,
			`true`, `"ok"`, `42`,
		},
		{
			"backreference to retrieval URI",
			`{"$ref":"BASEnested/common.json"}`,
			`{"$id":"canonical/bundle.json","$defs":{"value":{"type":"string"},"common":{"$id":"../nested/common.json","$ref":"../bundle.json#/$defs/value"}}}`,
			`true`, `"ok"`, `42`,
		},
		{
			"backreference to canonical URI",
			`{"$ref":"BASEnested/common.json"}`,
			`{"$id":"canonical/bundle.json","$defs":{"value":{"$anchor":"value","type":"string"},"common":{"$id":"../nested/common.json","$ref":"../canonical/bundle.json#value"}}}`,
			`true`, `"ok"`, `42`,
		},
		{
			"dynamic recursive extension",
			`{"$dynamicAnchor":"node","$ref":"BASEtree.json","unevaluatedProperties":false}`,
			`{"$defs":{"tree":{"$id":"tree.json","$dynamicAnchor":"node","type":"object","properties":{"value":{"type":"string"},"children":{"type":"array","items":{"$dynamicRef":"#node"}}}}}}`,
			`true`, `{"value":"ok","children":[{"value":"child"}]}`, `{"value":"ok","children":[{"extra":true}]}`,
		},
		{
			"cross bundle dynamic anchor override",
			`{"$dynamicAnchor":"node","$ref":"BASEbranch.json","required":["label"]}`,
			`{"$defs":{"branch":{"$id":"branch.json","type":"object","properties":{"children":{"type":"array","items":{"$dynamicRef":"BASEtree.json#node"}}}}}}`,
			`{"$defs":{"tree":{"$id":"tree.json","$dynamicAnchor":"node","type":"object"}}}`,
			`{"label":"parent","children":[{"label":"child"}]}`, `{"label":"parent","children":[{}]}`,
		},
		{
			"dynamic ref to static anchor does not override",
			`{"$dynamicAnchor":"node","$ref":"BASEbranch.json","required":["label"]}`,
			`{"$defs":{"branch":{"$id":"branch.json","type":"object","properties":{"children":{"type":"array","items":{"$dynamicRef":"BASEtree.json#node"}}}}}}`,
			`{"$defs":{"tree":{"$id":"tree.json","$anchor":"node","type":"object"}}}`,
			`{"label":"parent","children":[{}]}`, `{"label":"parent","children":[42]}`,
		},
		{
			"pointer discovers ID under unknown keyword",
			`{"allOf":[{"$ref":"BASEcustom.json"},{"$ref":"BASEbundle.json#/custom/0"}]}`,
			`{"custom":[{"$id":"custom.json","type":"string"}]}`,
			`true`, `"ok"`, `42`,
		},
	}
	for _, tc := range cases {
		for _, reverse := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/reverse=%v", tc.name, reverse), func(t *testing.T) {
				resources := []Resource{
					{URI: schemaTestURI, JSON: json.RawMessage(strings.ReplaceAll(tc.output, "BASE", base))},
					{URI: base + "bundle.json", JSON: json.RawMessage(strings.ReplaceAll(tc.bundle, "BASE", base))},
					{URI: base + "other.json", JSON: json.RawMessage(strings.ReplaceAll(tc.other, "BASE", base))},
				}
				if reverse {
					resources[0], resources[2] = resources[2], resources[0]
				}
				r, err := NewRegistry(resources, []SchemaDefinition{{ID: "test.output.v1", URI: schemaTestURI}})
				if err != nil {
					t.Fatal(err)
				}
				if len(r.resources) != len(resources)+1 || r.uris["test.output.v1"] != schemaTestURI {
					t.Fatal("compiler aliases leaked into the published resource specification")
				}
				for _, resource := range resources {
					if !bytes.Equal(r.resources[resource.URI], resource.JSON) {
						t.Fatalf("original resource bytes changed: %s", resource.URI)
					}
					original := string(resource.JSON)
					copy(resource.JSON, strings.Repeat(" ", len(resource.JSON)))
					if string(r.resources[resource.URI]) != original {
						t.Fatalf("registry aliases retained caller-owned bytes: %s", resource.URI)
					}
				}
				for _, sample := range []struct {
					data  string
					valid bool
				}{{tc.valid, true}, {tc.invalid, false}} {
					err := r.validateData("test.output.v1", schemaTestEnvelope(t, sample.data)["data"])
					if (err == nil) != sample.valid {
						t.Fatalf("data=%s valid=%v error=%v", sample.data, sample.valid, err)
					}
				}
			})
		}
	}
	if requests.Load() != 0 {
		t.Fatalf("embedded aliases caused %d external HTTP requests", requests.Load())
	}
}

func TestRegistryNeverLoadsExternalReferences(t *testing.T) {
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		_, _ = w.Write([]byte(`true`))
	}))
	defer server.Close()
	path := filepath.Join(t.TempDir(), "external.json")
	if err := os.WriteFile(path, []byte(`true`), 0600); err != nil {
		t.Fatal(err)
	}
	fileURI := (&url.URL{Scheme: "file", Path: path}).String()
	for _, ref := range []string{server.URL + "/schema.json", fileURI, "https://json-schema.org/draft/2020-12/schema"} {
		for _, schema := range []string{
			fmt.Sprintf(`{"$ref":%q}`, ref),
			fmt.Sprintf(`{"$dynamicRef":%q}`, ref),
			fmt.Sprintf(`{"$defs":{"unused":{"$ref":%q}}}`, ref),
			fmt.Sprintf(`{"$ref":"#/custom","custom":{"$ref":%q}}`, ref),
			fmt.Sprintf(`{"$defs":{"bundle":{"$id":"nested/bundle.json","$defs":{"unused":{"$ref":%q}}}}}`, ref),
			fmt.Sprintf(`{"$defs":{"bundle":{"$id":"nested/bundle.json","$defs":{"unused":{"$dynamicRef":%q}}}}}`, ref),
			fmt.Sprintf(`{"allOf":[{"$ref":"custom.json"},{"$ref":"#/custom"}],"custom":{"$id":"custom.json","$defs":{"unused":{"$ref":%q}}}}`, ref),
		} {
			t.Run(schema, func(t *testing.T) {
				_, err := NewRegistry([]Resource{{URI: schemaTestURI, JSON: json.RawMessage(schema)}}, []SchemaDefinition{{ID: "test", URI: schemaTestURI}})
				if err == nil {
					t.Fatal("accepted unregistered external reference")
				}
				if !strings.Contains(err.Error(), "unregistered schema reference") {
					t.Fatalf("reference was not rejected before loading: %v", err)
				}
			})
		}
	}
	for _, ref := range []string{server.URL + "/schema.json", fileURI} {
		t.Run("registered "+ref, func(t *testing.T) {
			r, err := NewRegistry([]Resource{
				{URI: schemaTestURI, JSON: json.RawMessage(fmt.Sprintf(`{"$ref":%q}`, ref))},
				{URI: ref, JSON: json.RawMessage(`{"type":"string"}`)},
			}, []SchemaDefinition{{ID: "test.output.v1", URI: schemaTestURI}})
			if err != nil {
				t.Fatal(err)
			}
			if err := r.validateData("test.output.v1", schemaTestEnvelope(t, `"ok"`)["data"]); err != nil {
				t.Fatal(err)
			}
			if err := r.validateData("test.output.v1", schemaTestEnvelope(t, `42`)["data"]); err == nil {
				t.Fatal("used external bytes instead of the registered resource")
			}
		})
	}
	if requests.Load() != 0 {
		t.Fatalf("external reference caused %d HTTP requests", requests.Load())
	}
}

func TestRegistryRejectsUnresolvedEmbeddedReferences(t *testing.T) {
	for _, ref := range []string{"common.json#missing", "common.json#/$defs/missing", "missing.json"} {
		for _, keyword := range []string{"$ref", "$dynamicRef"} {
			t.Run(keyword+"/"+ref, func(t *testing.T) {
				_, err := NewRegistry([]Resource{
					{URI: schemaTestURI, JSON: json.RawMessage(fmt.Sprintf(`{"$defs":{"unused":{%q:%q}}}`, keyword, ref))},
					{URI: "https://workflow.example/schemas/bundle.json", JSON: json.RawMessage(`{"$defs":{"common":{"$id":"common.json","type":"string"}}}`)},
				}, []SchemaDefinition{{ID: "test.output.v1", URI: schemaTestURI}})
				if err == nil {
					t.Fatal("dormant embedded reference escaped preflight")
				}
			})
		}
	}
}

func TestRegistryRejectsInvalidDefinitions(t *testing.T) {
	resource := Resource{URI: schemaTestURI, JSON: json.RawMessage(`true`)}
	definition := SchemaDefinition{ID: "test", URI: schemaTestURI}
	cases := []struct {
		name        string
		resources   []Resource
		definitions []SchemaDefinition
	}{
		{"duplicate resource URI", []Resource{resource, resource}, []SchemaDefinition{definition}},
		{"reserved envelope URI", []Resource{{URI: EnvelopeURI, JSON: json.RawMessage(`true`)}}, nil},
		{"duplicate schema ID", []Resource{resource}, []SchemaDefinition{definition, definition}},
		{"duplicate embedded ID across resources", []Resource{
			{URI: schemaTestURI, JSON: json.RawMessage(`{"$id":"https://workflow.example/shared"}`)},
			{URI: "https://workflow.example/other", JSON: json.RawMessage(`{"$id":"https://workflow.example/shared"}`)},
		}, nil},
		{"embedded ID collides with resource URI", []Resource{
			resource,
			{URI: "https://workflow.example/other", JSON: json.RawMessage(`{"$id":"https://workflow.example/schemas/output.json"}`)},
		}, nil},
		{"duplicate definition URI", []Resource{resource}, []SchemaDefinition{definition, {ID: "other", URI: schemaTestURI}}},
		{"empty ID", []Resource{resource}, []SchemaDefinition{{URI: schemaTestURI}}},
		{"unregistered definition", nil, []SchemaDefinition{definition}},
		{"relative resource URI", []Resource{{URI: "schema.json", JSON: json.RawMessage(`true`)}}, nil},
		{"fragment resource URI", []Resource{{URI: schemaTestURI + "#/$defs/a", JSON: json.RawMessage(`true`)}}, nil},
		{"empty resource URI", []Resource{{JSON: json.RawMessage(`true`)}}, nil},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := NewRegistry(tc.resources, tc.definitions); err == nil {
				t.Fatal("accepted invalid registry definition")
			}
		})
	}
	for _, schema := range []string{
		`{"type":"not-a-type"}`, `null`, `[]`, `{"type":"string","type":"integer"}`, `true false`,
		`{"$schema":"http://json-schema.org/draft-07/schema#"}`,
		`{"$schema":"https://json-schema.org/draft/2019-09/schema"}`,
		`{"$schema":"https://json-schema.org/schema"}`,
		`{"$schema":42}`,
		`{"$defs":{"old":{"$id":"old.json","$schema":"http://json-schema.org/draft-07/schema#"}}}`,
		`{"properties":{"a":{"$schema":"http://json-schema.org/draft-07/schema#"}}}`,
		`{"$defs":{"a":{"$id":"same.json"},"b":{"$id":"same.json"}}}`,
		`{"$ref":"#/custom","custom":{"$id":"custom.json","$schema":"http://json-schema.org/draft-07/schema#"}}`,
		`{"dependencies":{"a":{"$id":"old.json","$schema":"http://json-schema.org/draft-07/schema#"}}}`,
		`{"$ref":"#/$defs/missing"}`,
	} {
		t.Run(schema, func(t *testing.T) {
			if _, err := NewRegistry([]Resource{{URI: schemaTestURI, JSON: json.RawMessage(schema)}}, nil); err == nil {
				t.Fatal("accepted invalid resource")
			}
		})
	}
}

func TestRegistryDraft2020AndEmbeddedResources(t *testing.T) {
	cases := []struct{ name, schema, valid, invalid string }{
		{"canonical draft with fragment", `{"$schema":"https://json-schema.org/draft/2020-12/schema#","type":"string"}`, `"ok"`, `42`},
		{"HTTP draft URI", `{"$schema":"http://json-schema.org/draft/2020-12/schema","type":"string"}`, `"ok"`, `42`},
		{"custom pointer target", `{"$ref":"#/custom","custom":{"type":"string"}}`, `"ok"`, `42`},
		{"custom array pointer target", `{"$ref":"#/custom/0","custom":[{"type":"string"}]}`, `"ok"`, `42`},
		{"removed keyword annotation", `{"type":"string","additionalItems":17}`, `"ok"`, `42`},
		{"removed keyword reference annotation", `{"type":"string","additionalItems":{"$ref":"https://unregistered.invalid/ignored"}}`, `"ok"`, `42`},
		{"referenced removed keyword", `{"$ref":"#/additionalItems","additionalItems":{"type":"string"}}`, `"ok"`, `42`},
		{"prefixItems", `{"type":"array","prefixItems":[{"type":"string"}],"items":false}`, `["ok"]`, `["ok",1]`},
		{"unevaluatedProperties", `{"allOf":[{"properties":{"answer":{"type":"string"}}}],"unevaluatedProperties":false}`, `{"answer":"ok"}`, `{"extra":true}`},
		{"embedded resource", `{"$defs":{"answer":{"$id":"answer.json","type":"string"}},"$ref":"answer.json"}`, `"ok"`, `42`},
		{"anchor", `{"$defs":{"answer":{"$anchor":"answer","type":"string"}},"$ref":"#answer"}`, `"ok"`, `42`},
		{"escaped pointer", `{"$defs":{"a/b~c":{"type":"string"}},"$ref":"#/$defs/a~1b~0c"}`, `"ok"`, `42`},
		{"literal schema field", `{"const":{"$schema":"not a schema declaration"}}`, `{"$schema":"not a schema declaration"}`, `{}`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			r := schemaTestRegistry(t, tc.schema)
			if err := r.validateData("test.output.v1", schemaTestEnvelope(t, tc.valid)["data"]); err != nil {
				t.Fatal(err)
			}
			if err := r.validateData("test.output.v1", schemaTestEnvelope(t, tc.invalid)["data"]); err == nil {
				t.Fatal("accepted invalid data")
			}
		})
	}
}

func TestRegistryOwnsResourcesAndSupportsConcurrentValidation(t *testing.T) {
	original := `{"type":"string"}`
	resources := []Resource{{URI: schemaTestURI, JSON: json.RawMessage(original)}}
	definitions := []SchemaDefinition{{ID: "test.output.v1", URI: schemaTestURI}}
	r, err := NewRegistry(resources, definitions)
	if err != nil {
		t.Fatal(err)
	}
	copy(resources[0].JSON, strings.Repeat(" ", len(original)))
	resources[0].URI = "https://changed.example/schema"
	definitions[0].ID = "changed"
	definitions[0].URI = "https://changed.example/schema"
	if string(r.resources[schemaTestURI]) != original {
		t.Fatal("caller modified registry resource bytes")
	}
	value := schemaTestEnvelope(t, `"ok"`)
	var wg sync.WaitGroup
	for range 16 {
		wg.Go(func() {
			if err := r.validateData("test.output.v1", value["data"]); err != nil {
				t.Error(err)
			}
		})
	}
	wg.Wait()
	if err := r.validateData("test.output.v1", schemaTestEnvelope(t, `42`)["data"]); err == nil {
		t.Fatal("caller mutation changed compiled schema")
	}
}
