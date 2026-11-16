package wire_test

import (
	"testing"

	"github.com/laserdata/laser-sdk/foreign/go/wire"
)

// document parses a JSON document into the value a patch applies to.
func document(t *testing.T, text string) wire.DynValue {
	t.Helper()
	var value wire.DynValue
	if err := wire.UnmarshalJSON([]byte(text), &value); err != nil {
		t.Fatalf("parse %s: %v", text, err)
	}
	return value
}

func applied(t *testing.T, doc string, patch ...wire.PatchOperation) string {
	t.Helper()
	result, err := wire.ApplyJSONPatch(document(t, doc), patch)
	if err != nil {
		t.Fatalf("apply: %v", err)
	}
	encoded, err := wire.MarshalJSON(result)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	return string(encoded)
}

// A state patch is the agent surface's conditional write, so every SDK must
// apply one the same way: a divergence would leave two agents holding
// different documents at the same revision.
func TestGivenAStatePatchWhenAppliedThenShouldFollowTheOperationRules(t *testing.T) {
	cases := []struct {
		name     string
		document string
		patch    []wire.PatchOperation
		want     string
	}{
		{
			"add a field",
			`{"a":1}`,
			[]wire.PatchOperation{wire.AddOperation("/b", document(t, `2`))},
			`{"a":1,"b":2}`,
		},
		{
			"add replaces an existing field",
			`{"a":1}`,
			[]wire.PatchOperation{wire.AddOperation("/a", document(t, `9`))},
			`{"a":9}`,
		},
		{
			"add into an array at an index",
			`{"a":[1,3]}`,
			[]wire.PatchOperation{wire.AddOperation("/a/1", document(t, `2`))},
			`{"a":[1,2,3]}`,
		},
		{
			"the end marker appends",
			`{"a":[1]}`,
			[]wire.PatchOperation{wire.AddOperation("/a/-", document(t, `2`))},
			`{"a":[1,2]}`,
		},
		{
			"remove a field",
			`{"a":1,"b":2}`,
			[]wire.PatchOperation{wire.RemoveOperation("/b")},
			`{"a":1}`,
		},
		{
			"replace a nested field",
			`{"a":{"b":1}}`,
			[]wire.PatchOperation{wire.ReplaceOperation("/a/b", document(t, `2`))},
			`{"a":{"b":2}}`,
		},
		{
			"move a value",
			`{"a":1,"b":{}}`,
			[]wire.PatchOperation{wire.MoveOperation("/a", "/b/a")},
			`{"b":{"a":1}}`,
		},
		{
			"copy a value",
			`{"a":1,"b":{}}`,
			[]wire.PatchOperation{wire.CopyOperation("/a", "/b/a")},
			`{"a":1,"b":{"a":1}}`,
		},
		{
			"a passing test changes nothing",
			`{"a":1}`,
			[]wire.PatchOperation{wire.TestOperation("/a", document(t, `1`))},
			`{"a":1}`,
		},
		{
			"operations apply in order",
			`{"a":1}`,
			[]wire.PatchOperation{
				wire.AddOperation("/b", document(t, `2`)),
				wire.RemoveOperation("/a"),
			},
			`{"b":2}`,
		},
		{
			"an empty pointer replaces the root",
			`{"a":1}`,
			[]wire.PatchOperation{wire.AddOperation("", document(t, `{"b":2}`))},
			`{"b":2}`,
		},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			if got := applied(t, testCase.document, testCase.patch...); got != testCase.want {
				t.Errorf("got %s, want %s", got, testCase.want)
			}
		})
	}
}

// A patch that cannot apply must fail whole, so a rejected delta never leaves
// a half-written document behind.
func TestGivenAnInapplicablePatchWhenAppliedThenShouldFailWithoutChangingTheDocument(
	t *testing.T,
) {
	original := `{"a":1,"list":[1,2]}`
	cases := map[string][]wire.PatchOperation{
		"a failing test":            {wire.TestOperation("/a", document(t, `2`))},
		"removing what is absent":   {wire.RemoveOperation("/missing")},
		"replacing what is absent":  {wire.ReplaceOperation("/missing", document(t, `1`))},
		"an out-of-bounds index":    {wire.AddOperation("/list/9", document(t, `3`))},
		"a pointer with no slash":   {wire.AddOperation("a", document(t, `1`))},
		"a malformed escape":        {wire.AddOperation("/a~2b", document(t, `1`))},
		"a reserved property":       {wire.AddOperation("/__proto__", document(t, `1`))},
		"descending through a leaf": {wire.AddOperation("/a/b", document(t, `1`))},
		"moving into a child":       {wire.MoveOperation("/list", "/list/0")},
		"a partial patch":           {wire.AddOperation("/b", document(t, `2`)), wire.RemoveOperation("/missing")},
	}
	for name, patch := range cases {
		t.Run(name, func(t *testing.T) {
			source := document(t, original)
			if _, err := wire.ApplyJSONPatch(source, patch); err == nil {
				t.Fatal("the patch must be refused")
			}
			// The document the caller passed is untouched.
			encoded, err := wire.MarshalJSON(source)
			if err != nil {
				t.Fatalf("encode: %v", err)
			}
			if string(encoded) != original {
				t.Errorf("the source document changed to %s", encoded)
			}
		})
	}
}

// A patch longer than the cap is refused before it is applied.
func TestGivenAPatchBeyondTheOperationCapWhenAppliedThenShouldRefuseIt(t *testing.T) {
	patch := make([]wire.PatchOperation, wire.MaxStatePatchOps+1)
	for index := range patch {
		patch[index] = wire.AddOperation("/a", document(t, `1`))
	}
	if _, err := wire.ApplyJSONPatch(document(t, `{}`), patch); err == nil {
		t.Error("a patch beyond the cap must be refused")
	}
}

// Equality ignores field order, because a document that round-tripped through
// another SDK may name its fields in another order and still be the same.
func TestGivenDocumentsWithReorderedFieldsWhenComparedThenShouldBeEqual(t *testing.T) {
	left := document(t, `{"a":1,"b":{"c":[1,2]}}`)
	right := document(t, `{"b":{"c":[1,2]},"a":1}`)
	if !left.Equal(right) {
		t.Error("field order must not change a document's identity")
	}
	if left.Equal(document(t, `{"a":1,"b":{"c":[2,1]}}`)) {
		t.Error("array order is part of a document's identity")
	}
	if left.Equal(document(t, `{"a":1}`)) {
		t.Error("a missing field changes the document")
	}
}
