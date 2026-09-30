package valkey

func validAdminBotID(s string) bool {
	if len(s) < 1 || len(s) > 20 || s[0] < '1' || s[0] > '9' {
		return false
	}
	for i := 1; i < len(s); i++ {
		if s[i] < '0' || s[i] > '9' {
			return false
		}
	}
	return true
}
