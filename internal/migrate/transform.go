package migrate

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"
)

func transformValue(name string, value any, mapping map[string]any) (any, error) {
	if value == nil || name == "" {
		return value, nil
	}
	switch name {
	case "trimString":
		if text, ok := value.(string); ok {
			return strings.TrimSpace(text), nil
		}
		return value, nil
	case "emptyStringToNull":
		if text, ok := value.(string); ok && text == "" {
			return nil, nil
		}
		return value, nil
	case "mysqlDatetimeToPgTimestamp":
		return normalizeTime(value)
	case "tinyintToBoolean":
		return tinyintToBoolean(value)
	case "jsonStringToJsonb":
		return jsonStringToJSON(value)
	case "enumMapping":
		key := fmt.Sprint(value)
		if mapped, ok := mapping[key]; ok {
			return mapped, nil
		}
		return value, nil
	default:
		return nil, fmt.Errorf("未知转换规则: %s", name)
	}
}

func adaptValueToTargetType(value any, targetType string) (any, error) {
	if value == nil {
		return nil, nil
	}
	switch targetType {
	case "boolean":
		return tinyintToBoolean(value)
	case "smallint", "integer", "bigint":
		return toInteger(value)
	case "real", "double precision", "numeric", "decimal":
		return toFloat(value)
	case "text", "character varying", "character", "uuid":
		return toString(value), nil
	default:
		return value, nil
	}
}

func normalizeTime(value any) (any, error) {
	switch typed := value.(type) {
	case time.Time:
		return typed, nil
	case []byte:
		return parseTimeString(string(typed))
	case string:
		return parseTimeString(typed)
	default:
		return value, nil
	}
}

func parseTimeString(value string) (any, error) {
	if value == "" {
		return nil, nil
	}
	layouts := []string{"2006-01-02 15:04:05", time.RFC3339, "2006-01-02"}
	for _, layout := range layouts {
		parsed, err := time.ParseInLocation(layout, value, time.Local)
		if err == nil {
			return parsed, nil
		}
	}
	return nil, fmt.Errorf("无法解析时间: %s", value)
}

func tinyintToBoolean(value any) (any, error) {
	switch typed := value.(type) {
	case bool:
		return typed, nil
	case int8:
		return typed != 0, nil
	case int16:
		return typed != 0, nil
	case int32:
		return typed != 0, nil
	case int64:
		return typed != 0, nil
	case int:
		return typed != 0, nil
	case uint8:
		return typed != 0, nil
	case uint16:
		return typed != 0, nil
	case uint32:
		return typed != 0, nil
	case uint64:
		return typed != 0, nil
	case float32:
		return typed != 0, nil
	case float64:
		return typed != 0, nil
	case []byte:
		return stringToBool(string(typed))
	case string:
		return stringToBool(typed)
	default:
		return nil, fmt.Errorf("无法把 %v 转为布尔值", value)
	}
}

func stringToBool(value string) (bool, error) {
	text := strings.TrimSpace(strings.ToLower(value))
	switch text {
	case "1", "true", "t", "yes", "y":
		return true, nil
	case "0", "false", "f", "no", "n", "":
		return false, nil
	default:
		return false, fmt.Errorf("无法把 %s 转为布尔值", value)
	}
}

func toInteger(value any) (any, error) {
	switch typed := value.(type) {
	case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
		return typed, nil
	case float32:
		return int64(typed), nil
	case float64:
		return int64(typed), nil
	case []byte:
		return strconv.ParseInt(strings.TrimSpace(string(typed)), 10, 64)
	case string:
		return strconv.ParseInt(strings.TrimSpace(typed), 10, 64)
	default:
		return value, nil
	}
}

func toFloat(value any) (any, error) {
	switch typed := value.(type) {
	case float32, float64, int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
		return typed, nil
	case []byte:
		return strconv.ParseFloat(strings.TrimSpace(string(typed)), 64)
	case string:
		return strconv.ParseFloat(strings.TrimSpace(typed), 64)
	default:
		return value, nil
	}
}

func toString(value any) string {
	switch typed := value.(type) {
	case []byte:
		return string(typed)
	case time.Time:
		return typed.Format(time.RFC3339)
	default:
		return fmt.Sprint(value)
	}
}

func jsonStringToJSON(value any) (any, error) {
	var text string
	switch typed := value.(type) {
	case string:
		text = typed
	case []byte:
		text = string(typed)
	default:
		return value, nil
	}
	if text == "" {
		return nil, nil
	}
	var decoded any
	if err := json.Unmarshal([]byte(text), &decoded); err != nil {
		return nil, err
	}
	return text, nil
}
