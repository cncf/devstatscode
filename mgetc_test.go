package devstatscode

import (
	"testing"
)

func TestMgetc(t *testing.T) {
	// Environment context parse
	var ctx Ctx
	ctx.Init()
	ctx.TestMode = true

	// Set context's Mgetc manually (don't need to repeat tests from context_test.go)
	ctx.Mgetc = "y"

	expected := "y"
	got := Mgetc(&ctx)
	if got != expected {
		t.Errorf("expected %v, got %v", expected, got)
	}
}
