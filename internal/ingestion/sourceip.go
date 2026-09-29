package ingestion

import (
	"net"
	"net/http"
	"strings"
)

// sourceIP resolves the client address. Forwarded addresses are trusted only
// when the direct peer is a trusted proxy: X-Forwarded-For is walked right to
// left and the first untrusted address wins. A malformed chain falls back to
// the direct peer and reports malformed=true.
func sourceIP(r *http.Request, trusted []*net.IPNet) (ip net.IP, malformed bool) {
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		host = r.RemoteAddr
	}
	peer := net.ParseIP(host)
	if peer == nil || len(trusted) == 0 || !ipInAny(peer, trusted) {
		return peer, false
	}
	values := r.Header.Values("X-Forwarded-For")
	if len(values) == 0 {
		return peer, false
	}
	hops := strings.Split(strings.Join(values, ","), ",")
	var last net.IP
	for i := len(hops) - 1; i >= 0; i-- {
		hop := net.ParseIP(strings.TrimSpace(hops[i]))
		if hop == nil {
			return peer, true
		}
		if !ipInAny(hop, trusted) {
			return hop, false
		}
		last = hop
	}
	return last, false
}
