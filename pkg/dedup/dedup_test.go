package dedup

import "testing"

func TestAcceptOnce(t *testing.T) {
	var s Store
	if !s.Accept("tx-1") { t.Fatal("first transaction rejected") }
	if s.Accept("tx-1") { t.Fatal("duplicate transaction accepted") }
	if s.Accept("") { t.Fatal("empty transaction id accepted") }
}
