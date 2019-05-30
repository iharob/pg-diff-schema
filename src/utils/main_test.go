package utils_test

import (
	"testing"
	"utils"
)

func TestArrayParser(t *testing.T) {
	var arrays []string = []string{
		"{1}", "{1,2,3,4}", "{1, 2}", "   {2,    3,3,1}",
	}
	for _, array := range arrays {
		var content []string
		var err error
		err = utils.ParseArray([]byte(array), &content)
		if err != nil {
			t.Log(err)
			t.Fail()
		}
		t.Logf("%s", content)
	}
}
