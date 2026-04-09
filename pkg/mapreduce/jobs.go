package mapreduce

import (
	"fmt"
	"strconv"
	"strings"
)

// WordCountMapper emits (word, "1") for each word in the input line.
type WordCountMapper struct{}

func (m *WordCountMapper) Map(key, value string, emit func(key, value string)) {
	words := strings.Fields(value)
	for _, word := range words {
		word = strings.ToLower(strings.Trim(word, ".,!?;:\"'()[]{}"))
		if word != "" {
			emit(word, "1")
		}
	}
}

// SumReducer sums integer values for each key.
type SumReducer struct{}

func (r *SumReducer) Reduce(key string, values []string, emit func(key, value string)) {
	total := 0
	for _, v := range values {
		n, err := strconv.Atoi(v)
		if err == nil {
			total += n
		}
	}
	emit(key, strconv.Itoa(total))
}

// SumByKeyMapper emits (key, value) from tab-separated input lines.
type SumByKeyMapper struct{}

func (m *SumByKeyMapper) Map(key, value string, emit func(key, value string)) {
	parts := strings.SplitN(value, "\t", 2)
	if len(parts) == 2 {
		emit(parts[0], parts[1])
	}
}

// IdentityMapper emits the input key-value pair unchanged.
type IdentityMapper struct{}

func (m *IdentityMapper) Map(key, value string, emit func(key, value string)) {
	emit(key, value)
}

// IdentityReducer emits each value unchanged.
type IdentityReducer struct{}

func (r *IdentityReducer) Reduce(key string, values []string, emit func(key, value string)) {
	for _, v := range values {
		emit(key, v)
	}
}

// GetMapper returns a mapper by name.
func GetMapper(name string) (Mapper, error) {
	switch name {
	case "wordcount":
		return &WordCountMapper{}, nil
	case "sumbykey":
		return &SumByKeyMapper{}, nil
	case "identity":
		return &IdentityMapper{}, nil
	default:
		return nil, fmt.Errorf("unknown mapper: %s", name)
	}
}

// GetCombiner returns a combiner by name. Combiners are optional — returns nil, nil for empty name.
func GetCombiner(name string) (Combiner, error) {
	if name == "" {
		return nil, nil
	}
	switch name {
	case "sum", "wordcount", "sumbykey":
		return &SumCombiner{}, nil
	default:
		return nil, fmt.Errorf("unknown combiner: %s", name)
	}
}

// SumCombiner pre-aggregates integer values for each key (same logic as SumReducer).
type SumCombiner struct{}

func (c *SumCombiner) Combine(key string, values []string, emit func(key, value string)) {
	total := 0
	for _, v := range values {
		n, err := strconv.Atoi(v)
		if err == nil {
			total += n
		}
	}
	emit(key, strconv.Itoa(total))
}

// GetReducer returns a reducer by name.
func GetReducer(name string) (Reducer, error) {
	switch name {
	case "sum", "wordcount", "sumbykey":
		return &SumReducer{}, nil
	case "identity":
		return &IdentityReducer{}, nil
	default:
		return nil, fmt.Errorf("unknown reducer: %s", name)
	}
}
