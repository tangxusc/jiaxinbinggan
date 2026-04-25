package migrate

import (
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestTransformValue(t *testing.T) {
	parsedDate, err := time.ParseInLocation("2006-01-02", "2026-04-25", time.Local)
	if err != nil {
		t.Fatalf("parse fixture date: %v", err)
	}

	tests := []struct {
		name      string
		transform string
		value     any
		mapping   map[string]any
		want      any
		wantErr   string
	}{
		{name: "empty transform", transform: "", value: "value", want: "value"},
		{name: "nil value", transform: "trimString", value: nil, want: nil},
		{name: "trim string", transform: "trimString", value: "  Alice  ", want: "Alice"},
		{name: "trim non string", transform: "trimString", value: 12, want: 12},
		{name: "empty string to null", transform: "emptyStringToNull", value: "", want: nil},
		{name: "non empty string stays", transform: "emptyStringToNull", value: "x", want: "x"},
		{name: "datetime string", transform: "mysqlDatetimeToPgTimestamp", value: "2026-04-25", want: parsedDate},
		{name: "datetime bytes", transform: "mysqlDatetimeToPgTimestamp", value: []byte("2026-04-25"), want: parsedDate},
		{name: "datetime invalid", transform: "mysqlDatetimeToPgTimestamp", value: "bad", wantErr: "解析"},
		{name: "tinyint true", transform: "tinyintToBoolean", value: int64(1), want: true},
		{name: "tinyint false string", transform: "tinyintToBoolean", value: " no ", want: false},
		{name: "tinyint invalid string", transform: "tinyintToBoolean", value: "maybe", wantErr: "布尔"},
		{name: "json string validates and returns source text", transform: "jsonStringToJsonb", value: `{"a":1}`, want: `{"a":1}`},
		{name: "json bytes validates and returns source text", transform: "jsonStringToJsonb", value: []byte(`[1,2]`), want: `[1,2]`},
		{name: "empty json string becomes null", transform: "jsonStringToJsonb", value: "", want: nil},
		{name: "invalid json", transform: "jsonStringToJsonb", value: "{", wantErr: "unexpected"},
		{name: "enum mapped", transform: "enumMapping", value: 1, mapping: map[string]any{"1": "paid"}, want: "paid"},
		{name: "enum passthrough", transform: "enumMapping", value: 9, mapping: map[string]any{"1": "paid"}, want: 9},
		{name: "unknown transform", transform: "missing", value: "x", wantErr: "未知"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := transformValue(tt.transform, tt.value, tt.mapping)
			if tt.wantErr != "" {
				if err == nil {
					t.Fatalf("transformValue() expected error")
				}
				if !strings.Contains(strings.ToLower(err.Error()), tt.wantErr) && !strings.Contains(err.Error(), tt.wantErr) {
					t.Fatalf("transformValue() error = %q, want substring %q", err.Error(), tt.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("transformValue() error = %v", err)
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Fatalf("transformValue() = %#v (%T), want %#v (%T)", got, got, tt.want, tt.want)
			}
		})
	}
}

func TestAdaptValueToTargetType(t *testing.T) {
	tests := []struct {
		name       string
		targetType string
		value      any
		want       any
		wantErr    string
	}{
		{name: "nil", targetType: "integer", value: nil, want: nil},
		{name: "boolean true", targetType: "boolean", value: "yes", want: true},
		{name: "boolean false", targetType: "boolean", value: []byte("0"), want: false},
		{name: "boolean invalid", targetType: "boolean", value: "maybe", wantErr: "布尔"},
		{name: "integer string", targetType: "integer", value: "42", want: int64(42)},
		{name: "bigint bytes", targetType: "bigint", value: []byte("43"), want: int64(43)},
		{name: "integer float truncates", targetType: "smallint", value: float64(12.8), want: int64(12)},
		{name: "integer invalid", targetType: "integer", value: "abc", wantErr: "invalid"},
		{name: "float string", targetType: "numeric", value: "3.5", want: float64(3.5)},
		{name: "float bytes", targetType: "double precision", value: []byte("4.25"), want: float64(4.25)},
		{name: "float invalid", targetType: "real", value: "abc", wantErr: "invalid"},
		{name: "text bytes", targetType: "text", value: []byte("hello"), want: "hello"},
		{name: "varchar time", targetType: "character varying", value: time.Date(2026, 4, 25, 1, 2, 3, 0, time.UTC), want: "2026-04-25T01:02:03Z"},
		{name: "uuid string", targetType: "uuid", value: "abc", want: "abc"},
		{name: "unknown target passthrough", targetType: "jsonb", value: map[string]any{"a": 1}, want: map[string]any{"a": 1}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := adaptValueToTargetType(tt.value, tt.targetType)
			if tt.wantErr != "" {
				if err == nil || !strings.Contains(strings.ToLower(err.Error()), tt.wantErr) {
					t.Fatalf("adaptValueToTargetType() error = %v, want substring %q", err, tt.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("adaptValueToTargetType() error = %v", err)
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Fatalf("adaptValueToTargetType() = %#v (%T), want %#v (%T)", got, got, tt.want, tt.want)
			}
		})
	}
}

func TestNormalizeAndQuotingHelpers(t *testing.T) {
	if got := normalizeDBValue([]byte("abc")); got != "abc" {
		t.Fatalf("normalizeDBValue([]byte) = %#v", got)
	}
	if got := normalizeDBValue(12); got != 12 {
		t.Fatalf("normalizeDBValue(int) = %#v", got)
	}
	if got := quoteMySQLIdent("a`b"); got != "`a``b`" {
		t.Fatalf("quoteMySQLIdent() = %q", got)
	}
	if got := quotePGIdent(`Mixed"Name`); got != `"Mixed""Name"` {
		t.Fatalf("quotePGIdent() = %q", got)
	}
	if got := joinPGIdents([]string{"id", "select"}); got != `"id", "select"` {
		t.Fatalf("joinPGIdents() = %q", got)
	}
}

func TestPrimitiveConversionBranches(t *testing.T) {
	boolInputs := []any{true, int8(1), int16(1), int32(1), int64(1), int(1), uint8(1), uint16(1), uint32(1), uint64(1), float32(1), float64(1), []byte("true"), "y"}
	for _, input := range boolInputs {
		got, err := tinyintToBoolean(input)
		if err != nil {
			t.Fatalf("tinyintToBoolean(%T) error = %v", input, err)
		}
		if got != true {
			t.Fatalf("tinyintToBoolean(%T) = %v", input, got)
		}
	}
	if _, err := tinyintToBoolean(struct{}{}); err == nil {
		t.Fatalf("tinyintToBoolean(unsupported) expected error")
	}

	intInputs := []any{int(1), int8(1), int16(1), int32(1), int64(1), uint(1), uint8(1), uint16(1), uint32(1), uint64(1), float32(1.9), float64(2.9), []byte("3"), "4"}
	for _, input := range intInputs {
		if _, err := toInteger(input); err != nil {
			t.Fatalf("toInteger(%T) error = %v", input, err)
		}
	}
	unsupported := struct{ A int }{A: 1}
	if got, err := toInteger(unsupported); err != nil || got != unsupported {
		t.Fatalf("toInteger(unsupported) = %#v, %v", got, err)
	}

	floatInputs := []any{float32(1.2), float64(1.2), int(1), int8(1), int16(1), int32(1), int64(1), uint(1), uint8(1), uint16(1), uint32(1), uint64(1), []byte("3.5"), "4.5"}
	for _, input := range floatInputs {
		if _, err := toFloat(input); err != nil {
			t.Fatalf("toFloat(%T) error = %v", input, err)
		}
	}
	if got, err := toFloat(unsupported); err != nil || got != unsupported {
		t.Fatalf("toFloat(unsupported) = %#v, %v", got, err)
	}

	now := time.Now()
	if got, err := normalizeTime(now); err != nil || got != now {
		t.Fatalf("normalizeTime(time) = %#v, %v", got, err)
	}
	if got, err := normalizeTime(12); err != nil || got != 12 {
		t.Fatalf("normalizeTime(default) = %#v, %v", got, err)
	}
}
