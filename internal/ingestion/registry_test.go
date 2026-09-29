package ingestion

import "testing"

// TestBuiltinTelegram pins the first production adapter registration.
func TestBuiltinTelegram(t *testing.T) {
	r, err := Builtin(BuiltinOptions{})
	if err != nil {
		t.Fatal(err)
	}
	d, ok := r.Lookup("telegram")
	if !ok {
		t.Fatal("telegram not registered")
	}
	if d.Platform != "telegram" || len(d.CredentialKinds) != 1 || d.CredentialKinds[0] != "secret_token" {
		t.Errorf("telegram definition = %+v", d)
	}
	if _, ok := r.Lookup("maxbot"); ok {
		t.Error("maxbot must not be registered before its adapter")
	}
}

// TestRegisterValidation pins duplicate, pattern, and completeness checks.
func TestRegisterValidation(t *testing.T) {
	r := NewRegistry()
	if err := r.Register(Definition{Type: "telegram", Platform: "telegram", CredentialKinds: []string{"secret_token"}, Verifier: telegramVerifier{}, Converter: telegramConverter{}}); err != nil {
		t.Fatal(err)
	}
	if err := r.Register(Definition{Type: "telegram", Platform: "telegram", CredentialKinds: []string{"secret_token"}, Verifier: telegramVerifier{}, Converter: telegramConverter{}}); err == nil {
		t.Error("duplicate registration accepted")
	}
	if err := r.Register(Definition{Type: "BadType", Platform: "x", CredentialKinds: []string{"k"}, Verifier: telegramVerifier{}, Converter: telegramConverter{}}); err == nil {
		t.Error("pattern violation accepted")
	}
	if err := r.Register(Definition{Type: "emptykinds", Platform: "x"}); err == nil {
		t.Error("missing credential kinds accepted")
	}
}
