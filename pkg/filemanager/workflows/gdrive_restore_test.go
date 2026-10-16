package workflows

import "testing"

func TestRestoreRelPath(t *testing.T) {
	cases := []struct {
		name, entry, selected, want string
		ok                          bool
	}{
		{"whole user keeps full path", "/a/b.txt", "/", "/a/b.txt", true},
		{"selected file keeps its name", "/a/b.txt", "/a/b.txt", "/b.txt", true},
		{"selected folder keeps folder name", "/a/b/c.txt", "/a/b", "/b/c.txt", true},
		{"top level folder", "/a/c.txt", "/a", "/a/c.txt", true},
		{"sibling with same prefix is excluded", "/a/bc.txt", "/a/b", "", false},
		{"unrelated path is excluded", "/x/y.txt", "/a", "", false},
		{"unclean selection is normalized", "/a/b/c.txt", "a/b/", "/b/c.txt", true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, ok := restoreRelPath(c.entry, c.selected)
			if got != c.want || ok != c.ok {
				t.Fatalf("restoreRelPath(%q, %q) = (%q, %v), want (%q, %v)", c.entry, c.selected, got, ok, c.want, c.ok)
			}
		})
	}
}
