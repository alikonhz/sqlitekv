package kvstore

import (
	"encoding/binary"
	"errors"
	"math"
)

var (
	ErrTypeNotSupported = errors.New("type not supported")
)

func ConvertToValue(val any) (*KvValue, error) {
	switch v := val.(type) {
	case []byte:
		return KvBinary(v), nil
	case string:
		return KvString(v), nil
	case int32:
		return KvInt32(v), nil
	case int64:
		return KvInt64(v), nil
	case int:
		return KvInt64(int64(v)), nil
	case float32:
		return KvFloat32(v), nil
	case float64:
		return KvFloat64(v), nil
	}

	return nil, ErrTypeNotSupported
}

func ConvertFromValue(val *KvValue) (any, error) {
	switch val.Type {
	case BinaryType:
		return val.Value, nil
	case StringType:
		return string(val.Value), nil
	case Int32Type:
		return toInt32(val.Value), nil
	case Int64Type:
		return toInt64(val.Value), nil
	case Float32Type:
		return toFloat32(val.Value), nil
	case Float64Type:
		return toFloat64(val.Value), nil
	}

	return nil, ErrTypeNotSupported
}

func toInt32(value []byte) any {
	i := binary.BigEndian.Uint32(value)

	return int32(i)
}

func toInt64(value []byte) any {
	i := binary.BigEndian.Uint64(value)

	return int64(i)
}

func toFloat32(value []byte) any {
	i := binary.BigEndian.Uint32(value)

	return math.Float32frombits(i)
}

func toFloat64(value []byte) any {
	i := binary.BigEndian.Uint64(value)

	return math.Float64frombits(i)
}

func KvInt32(val int32) *KvValue {
	data := make([]byte, 4)
	binary.BigEndian.PutUint32(data, uint32(val))

	return &KvValue{
		Value: data,
		Type:  Int32Type,
	}
}

func KvInt64(val int64) *KvValue {
	data := make([]byte, 8)
	binary.BigEndian.PutUint64(data, uint64(val))

	return &KvValue{
		Value: data,
		Type:  Int64Type,
	}
}

func KvFloat64(val float64) *KvValue {
	data := make([]byte, 8)
	binary.BigEndian.PutUint64(data, math.Float64bits(val))

	return &KvValue{
		Value: data,
		Type:  Float64Type,
	}
}

func KvFloat32(val float32) *KvValue {
	data := make([]byte, 4)
	binary.BigEndian.PutUint32(data, math.Float32bits(val))

	return &KvValue{
		Value: data,
		Type:  Float32Type,
	}
}
