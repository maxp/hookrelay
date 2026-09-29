package jsonbody

import (
	"errors"
	"strings"
	"testing"
)

// TestDecode pins the strict body discipline and its failure classes.
func TestDecode(t *testing.T) {
	type body struct {
		A string `json:"a"`
		N *int64 `json:"n"`
	}
	var ok body
	if err := Decode(strings.NewReader(`{"a":"x","n":5}`), &ok); err != nil || ok.A != "x" || *ok.N != 5 {
		t.Fatalf("valid body: %+v %v", ok, err)
	}
	for name, tc := range map[string]struct {
		in    string
		class error
	}{
		"too large":     {`{"a":"` + strings.Repeat("x", MaxBytes) + `"}`, ErrTooLarge},
		"array":         {`[1]`, ErrInvalid},
		"unknown field": {`{"b":1}`, ErrInvalid},
		"trailing":      {`{"a":"x"} {}`, ErrInvalid},
		"string number": {`{"n":"5"}`, ErrInvalid},
		"fraction":      {`{"n":1.5}`, ErrInvalid},
		"too deep":      {`{"a":` + strings.Repeat(`[`, MaxDepth) + strings.Repeat(`]`, MaxDepth) + `}`, ErrInvalid},
		"malformed":     {`{"a":`, ErrInvalid},
	} {
		var b body
		if err := Decode(strings.NewReader(tc.in), &b); !errors.Is(err, tc.class) {
			t.Errorf("%s: err = %v, want %v", name, err, tc.class)
		}
	}
	var b body
	if err := Decode(strings.NewReader(`{"b":"secret-value"}`), &b); err == nil || strings.Contains(err.Error(), "secret-value") {
		t.Errorf("error echoes submitted values: %v", err)
	}
}

// TestRequireJSON pins media-type parsing.
func TestRequireJSON(t *testing.T) {
	for ct, want := range map[string]bool{
		"application/json": true, "application/json; charset=UTF-8": true, "Application/JSON": true,
		"": false, "text/plain": false, "application/json; charset=latin1": false, "application/json; x=y": false,
	} {
		if got := RequireJSON(ct) == nil; got != want {
			t.Errorf("%q: accepted = %v, want %v", ct, got, want)
		}
	}
}
