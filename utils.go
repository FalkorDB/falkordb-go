package falkordb

import (
	"crypto/rand"
	"fmt"
	"math"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"
)

// maxNestingDepth bounds the recursion in toString. Cyclic values (a map or
// slice that contains itself) would otherwise recurse until the goroutine stack
// overflows, which is a fatal error that recover cannot catch.
const maxNestingDepth = 100

// quoteCypherString renders s as a Cypher double-quoted string literal.
//
// strconv.Quote is deliberately not used: it escapes control bytes as \xNN and
// non-printable runes as \uXXXX, neither of which FalkorDB decodes, so such
// values would be silently corrupted. Only the escapes the server understands
// are emitted and every other byte, including all multi-byte UTF-8, is passed
// through verbatim.
func quoteCypherString(s string) (string, error) {
	// FalkorDB strings are UTF-8 text, so invalid input has to be rejected
	// rather than passed through to be stored incorrectly. []byte is routed
	// here too, so both follow the same rule.
	if !utf8.ValidString(s) {
		return "", fmt.Errorf("%w: string is not valid UTF-8", ErrUnsupportedType)
	}

	var b strings.Builder
	b.Grow(len(s) + 2)
	b.WriteByte('"')
	for i := 0; i < len(s); i++ {
		switch c := s[i]; c {
		case 0:
			return "", fmt.Errorf("%w: string contains a NUL byte at offset %d", ErrUnsupportedType, i)
		case '"':
			b.WriteString(`\"`)
		case '\\':
			b.WriteString(`\\`)
		case '\n':
			b.WriteString(`\n`)
		case '\r':
			b.WriteString(`\r`)
		case '\t':
			b.WriteString(`\t`)
		default:
			b.WriteByte(c)
		}
	}
	b.WriteByte('"')
	return b.String(), nil
}

// quoteCypherIdentifier renders name as a back-quoted Cypher identifier, which
// is what map keys are. Without the back quotes a key such as
// `x: 1} CREATE (:Pwned) //` would close the map literal and append arbitrary
// clauses to the query.
func quoteCypherIdentifier(name string) (string, error) {
	if name == "" {
		return "", fmt.Errorf("%w: empty identifier", ErrUnsupportedType)
	}
	// FalkorDB has no escape for a back quote inside a back-quoted identifier,
	// so such a name cannot be rendered safely.
	if strings.ContainsAny(name, "`\x00") {
		return "", fmt.Errorf("%w: identifier %q contains a back quote or NUL byte", ErrUnsupportedType, name)
	}
	return "`" + name + "`", nil
}

// sliceToString renders a slice or array as a Cypher list literal.
func sliceToString(rv reflect.Value, depth int) (string, error) {
	elements := make([]string, rv.Len())
	for i := 0; i < rv.Len(); i++ {
		s, err := toStringDepth(rv.Index(i).Interface(), depth+1)
		if err != nil {
			return "", err
		}
		elements[i] = s
	}
	return "[" + strings.Join(elements, ",") + "]", nil
}

// mapValueToString renders a map with string keys as a Cypher map literal. Keys
// are sorted so that the same map always produces the same query text, which
// keeps FalkorDB's query cache effective.
func mapValueToString(rv reflect.Value, depth int) (string, error) {
	if rv.Type().Key().Kind() != reflect.String {
		return "", fmt.Errorf("%w: map key %s, only string keys are supported", ErrUnsupportedType, rv.Type().Key())
	}

	keys := rv.MapKeys()
	sort.Slice(keys, func(i, j int) bool { return keys[i].String() < keys[j].String() })

	pairs := make([]string, 0, len(keys))
	for _, k := range keys {
		key, err := quoteCypherIdentifier(k.String())
		if err != nil {
			return "", err
		}
		s, err := toStringDepth(rv.MapIndex(k).Interface(), depth+1)
		if err != nil {
			return "", err
		}
		pairs = append(pairs, key+": "+s)
	}
	return "{" + strings.Join(pairs, ",") + "}", nil
}

// toString converts a Go value to its Cypher literal representation, reporting
// ErrUnsupportedType for values it cannot represent rather than panicking.
func toString(i interface{}) (string, error) {
	return toStringDepth(i, 0)
}

func toStringDepth(i interface{}, depth int) (string, error) {
	if i == nil {
		return "null", nil
	}
	if depth > maxNestingDepth {
		return "", fmt.Errorf("%w: value nested deeper than %d levels, or self-referential", ErrUnsupportedType, maxNestingDepth)
	}

	// Types whose underlying kind would otherwise be matched by the reflection
	// switch below have to be handled first: time.Duration is an int64 and
	// []byte is a []uint8.
	switch v := i.(type) {
	case string:
		return quoteCypherString(v)
	case bool:
		return strconv.FormatBool(v), nil
	case []byte:
		return quoteCypherString(string(v))
	case time.Time:
		return quoteCypherString(v.Format(time.RFC3339Nano))
	case time.Duration:
		// FalkorDB's DURATION type has second granularity, so sub-second
		// components are truncated.
		return strconv.FormatInt(int64(v/time.Second), 10), nil
	}

	rv := reflect.ValueOf(i)
	switch rv.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return strconv.FormatInt(rv.Int(), 10), nil
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		// FalkorDB integers are signed 64-bit and the server saturates rather
		// than reporting an error, so reject values it cannot represent.
		if u := rv.Uint(); u > math.MaxInt64 {
			return "", fmt.Errorf("%w: uint value %d exceeds the int64 range FalkorDB supports", ErrUnsupportedType, u)
		}
		return strconv.FormatUint(rv.Uint(), 10), nil
	case reflect.Float32, reflect.Float64:
		f := rv.Float()
		if math.IsNaN(f) || math.IsInf(f, 0) {
			return "", fmt.Errorf("%w: %v has no Cypher representation", ErrUnsupportedType, f)
		}
		bitSize := 64
		if rv.Kind() == reflect.Float32 {
			bitSize = 32
		}
		return strconv.FormatFloat(f, 'f', -1, bitSize), nil
	case reflect.Slice, reflect.Array:
		if rv.Kind() == reflect.Slice && rv.IsNil() {
			return "null", nil
		}
		return sliceToString(rv, depth)
	case reflect.Map:
		if rv.IsNil() {
			return "null", nil
		}
		return mapValueToString(rv, depth)
	case reflect.Pointer, reflect.Interface:
		if rv.IsNil() {
			return "null", nil
		}
		return toStringDepth(rv.Elem().Interface(), depth+1)
	}

	return "", fmt.Errorf("%w: %T", ErrUnsupportedType, i)
}

// ToString returns the Cypher literal representation of i.
//
// Values that cannot be represented fall back to a description of their type,
// so that rendering a value never panics. Use query parameters, which report
// ErrUnsupportedType, when the conversion needs to be checked.
func ToString(i interface{}) string {
	s, err := toString(i)
	if err == nil {
		return s
	}

	// Deliberately not fmt.Sprint(i): it walks the value and recurses forever
	// on cyclic input, and a stack overflow is a fatal error that no caller can
	// recover from. %T inspects only the type.
	if stringer, ok := i.(fmt.Stringer); ok {
		if quoted, qerr := quoteCypherString(stringer.String()); qerr == nil {
			return quoted
		}
	}
	if quoted, qerr := quoteCypherString(fmt.Sprintf("<unsupported %T>", i)); qerr == nil {
		return quoted
	}
	return `""`
}

// https://medium.com/@kpbird/golang-generate-fixed-size-random-string-dd6dbd5e63c0
func RandomString(n int) string {
	const letterBytes = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"

	output := make([]byte, n)
	// We will take n bytes, one byte for each character of output.
	randomness := make([]byte, n)
	// read all random
	_, err := rand.Read(randomness)
	if err != nil {
		panic(err)
	}
	l := len(letterBytes)
	// fill output
	for pos := range output {
		// get random item
		random := uint8(randomness[pos])
		// random % 64
		randomPos := random % uint8(l)
		// put into output
		output[pos] = letterBytes[randomPos]
	}
	return string(output)
}

// isValidParamName reports whether name is a plain Cypher identifier. Parameter
// names are interpolated into the query text, so a name such as
// `x=1 CREATE (:Pwned) //` would otherwise append arbitrary clauses.
func isValidParamName(name string) bool {
	if name == "" {
		return false
	}
	for i := 0; i < len(name); i++ {
		c := name[i]
		switch {
		case c >= 'a' && c <= 'z', c >= 'A' && c <= 'Z', c == '_':
		case c >= '0' && c <= '9' && i > 0:
		default:
			return false
		}
	}
	return true
}

// buildParamsHeader renders the CYPHER parameter preamble for a query,
// reporting an error for any parameter that cannot be represented. Parameters
// are emitted in sorted key order so that the same set of parameters always
// produces the same query text, which keeps FalkorDB's query cache effective.
func buildParamsHeader(params map[string]interface{}) (string, error) {
	keys := make([]string, 0, len(params))
	for key := range params {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	var header strings.Builder
	header.WriteString("CYPHER ")
	for _, key := range keys {
		if !isValidParamName(key) {
			return "", fmt.Errorf("%w: parameter name %q is not a valid identifier", ErrUnsupportedType, key)
		}
		value, err := toString(params[key])
		if err != nil {
			return "", fmt.Errorf("parameter %q: %w", key, err)
		}
		header.WriteString(key)
		header.WriteString("=")
		header.WriteString(value)
		header.WriteString(" ")
	}
	return header.String(), nil
}

// BuildParamsHeader renders the CYPHER parameter preamble for a query.
func BuildParamsHeader(params map[string]interface{}) string {
	header := "CYPHER "
	for key, value := range params {
		header += fmt.Sprintf("%s=%v ", key, ToString(value))
	}
	return header
}
