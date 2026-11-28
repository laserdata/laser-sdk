package laser

import (
	"errors"
	"testing"

	"github.com/dlclark/regexp2/v2"
	"github.com/santhosh-tekuri/jsonschema/v6"
)

func TestGivenANegatedPatternWhenTheEngineExhaustsItsBudgetThenShouldRefuseThePayload(t *testing.T) {
	compiler := jsonschema.NewCompiler()
	compiler.UseRegexpEngine(func(expr string) (jsonschema.Regexp, error) { return exhaustedPattern(expr), nil })
	const id = "urn:laser:test-pattern"
	if err := compiler.AddResource(id, map[string]any{"not": map[string]any{"pattern": "a"}}); err != nil {
		t.Fatal(err)
	}
	validator, err := compiler.Compile(id)
	if err != nil {
		t.Fatal(err)
	}
	schema := &CompiledSchema{validator: validator}
	var value string
	if err := schema.Decode([]byte(`"a"`), &value); !errors.Is(err, regexp2.ErrMatchTimeout) {
		t.Fatalf("wanted the engine error, got %v", err)
	}
	if schema.ValidateValue("a") {
		t.Fatal("negation converted a timeout into acceptance")
	}
}

type exhaustedPattern string

func (p exhaustedPattern) String() string { return string(p) }

func (exhaustedPattern) MatchString(string) bool {
	panic(patternEvaluationError{regexp2.ErrMatchTimeout})
}
