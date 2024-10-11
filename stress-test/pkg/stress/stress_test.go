package stress

import "testing"

func TestPrepareGetRes(t *testing.T) {
	t.Log(prepareGetRes("pods"))
	t.Log(prepareGetRes("nodes"))
}
