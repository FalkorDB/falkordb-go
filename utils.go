package falkordb

import (
	"crypto/rand"
	"fmt"
	"regexp"
	"strconv"
	"strings"
)

// identifierPattern matches names that Cypher accepts without quoting.
var identifierPattern = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)

// maxNameInError bounds how much of an offending name is echoed in an error.
const maxNameInError = 64

// encodeName renders a parameter name or map key for the CYPHER header.
// Plain identifiers are emitted as is; anything else is wrapped in backticks so
// it cannot close the map or header and append clauses to the query. FalkorDB
// has no escape inside a backtick-quoted name, so empty names and names
// containing a backtick or NUL byte cannot be represented and are rejected.
func encodeName(name string) (string, error) {
	if identifierPattern.MatchString(name) {
		return name, nil
	}
	if name == "" || strings.ContainsAny(name, "`\x00") {
		shown := name
		if len(shown) > maxNameInError {
			shown = shown[:maxNameInError] + "..."
		}
		return "", fmt.Errorf("%w: %q", ErrInvalidName, shown)
	}
	return "`" + name + "`", nil
}

// go array to string is [1 2 3] for [1, 2, 3] array
// cypher expects comma separated array
func arrayToString(arr []interface{}) (string, error) {
	var arrayLength = len(arr)
	strArray := []string{}
	for i := 0; i < arrayLength; i++ {
		s, err := toString(arr[i])
		if err != nil {
			return "", err
		}
		strArray = append(strArray, s)
	}
	return "[" + strings.Join(strArray, ",") + "]", nil
}

// go array to string is [1 2 3] for [1, 2, 3] array
// cypher expects comma separated array
func strArrayToString(arr []string) string {
	var arrayLength = len(arr)
	strArray := []string{}
	for i := 0; i < arrayLength; i++ {
		strArray = append(strArray, ToString(arr[i]))
	}
	return "[" + strings.Join(strArray, ",") + "]"
}

func mapToString(data map[string]interface{}) (string, error) {
	pairsArray := []string{}
	for k, v := range data {
		key, err := encodeName(k)
		if err != nil {
			return "", err
		}
		s, err := toString(v)
		if err != nil {
			return "", err
		}
		pairsArray = append(pairsArray, key+": "+s)
	}
	return "{" + strings.Join(pairsArray, ",") + "}", nil
}

// ToString returns the Cypher literal for i. It panics if i has an unsupported
// type or contains a map key that cannot be represented (see ErrInvalidName).
func ToString(i interface{}) string {
	s, err := toString(i)
	if err != nil {
		panic(err)
	}
	return s
}

func toString(i interface{}) (string, error) {
	if i == nil {
		return "null", nil
	}

	switch i.(type) {
	case string:
		s := i.(string)
		return strconv.Quote(s), nil
	case int:
		return strconv.Itoa(i.(int)), nil
	case int64:
		return strconv.FormatInt(i.(int64), 10), nil
	case float64:
		return strconv.FormatFloat(i.(float64), 'f', -1, 64), nil
	case bool:
		return strconv.FormatBool(i.(bool)), nil
	case []interface{}:
		arr := i.([]interface{})
		return arrayToString(arr)
	case map[string]interface{}:
		data := i.(map[string]interface{})
		return mapToString(data)
	case []string:
		arr := i.([]string)
		return strArrayToString(arr), nil
	default:
		panic("Unrecognized type to convert to string")
	}
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

// BuildParamsHeader returns the CYPHER header that binds params. It panics if a
// parameter name or map key cannot be represented (see ErrInvalidName).
func BuildParamsHeader(params map[string]interface{}) string {
	header, err := buildParamsHeader(params)
	if err != nil {
		panic(err)
	}
	return header
}

func buildParamsHeader(params map[string]interface{}) (string, error) {
	header := "CYPHER "
	for key, value := range params {
		name, err := encodeName(key)
		if err != nil {
			return "", fmt.Errorf("parameter name: %w", err)
		}
		s, err := toString(value)
		if err != nil {
			return "", fmt.Errorf("parameter %s: %w", name, err)
		}
		header += fmt.Sprintf("%s=%v ", name, s)
	}
	return header, nil
}
