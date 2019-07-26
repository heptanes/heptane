package heptane

import (
	"bytes"

	c "github.com/heptanes/heptane/cache"
	r "github.com/heptanes/heptane/row"
)

func marshalField(t r.Table, fn r.FieldName, fv r.FieldValue) ([]byte, error) {
	ft, ok := t.Types[fn]
	if !ok {
		return nil, MissingFieldTypeError{t.Name, fn}
	}
	switch ft {
	case "string":
		if fv == nil {
			return nil, nil
		}
		s, ok := fv.(string)
		if !ok {
			return nil, UnsupportedFieldValueError{ft, fv}
		}
		return []byte(s), nil
	}
	return nil, UnsupportedFieldTypeError{ft}
}

func unmarshalField(t r.Table, fn r.FieldName, q []byte) (r.FieldValue, error) {
	ft, ok := t.Types[fn]
	if !ok {
		return nil, MissingFieldTypeError{t.Name, fn}
	}
	switch ft {
	case "string":
		if q == nil {
			return nil, nil
		}
		return string(q), nil
	}
	return nil, UnsupportedFieldTypeError{ft}
}

type cacheKey [][]byte

func decodeKey(t r.Table, fvn r.FieldValuesByName) (cacheKey, error) {
	ck := make(cacheKey, len(t.PrimaryKeyCachePrefix)+len(t.PrimaryKey), 0)
	for _, k := range t.PrimaryKeyCachePrefix {
		ck = append(ck, []byte(k))
	}
	for _, fn := range t.PrimaryKey {
		fv, ok := fvn[fn]
		if !ok {
			return nil, MissingFieldValueError{t.Name, fn, fvn}
		}
		v, err := marshalField(t, fn, fv)
		if err != nil {
			return nil, err
		}
		ck = append(ck, v)
	}
	return ck, nil
}

func (k cacheKey) key() c.CacheKey {
	b := bytes.Buffer{}
	for _, q := range k {
		b.Write(q)
		// TODO use interface
		b.WriteRune('#')
	}
	return c.CacheKey(b.String())
}

type cacheValue [][]byte

func decodeValue(t r.Table, fvn r.FieldValuesByName) (cacheValue, error) {
	cv := make(cacheValue, len(t.Values), 0)
	for _, fn := range t.Values {
		fv, ok := fvn[fn]
		if !ok {
			cv = append(cv, nil)
			continue
		}
		v, err := marshalField(t, fn, fv)
		if err != nil {
			return nil, err
		}
		cv = append(cv, v)
	}
	return cv, nil
}

func (v cacheValue) value() c.CacheValue {
	if v == nil {
		return nil
	}
	b := bytes.Buffer{}
	for _, q := range v {
		b.Write(q)
		// TODO use interface
		b.WriteRune('#')
	}
	return c.CacheValue(b.Bytes())
}

func split(cv c.CacheValue) (v cacheValue) {
	if cv == nil {
		return nil
	}
	return cacheValue(bytes.Split(cv, []byte("#")))
}

func isMissingSomeValue(t r.Table, fvn r.FieldValuesByName) bool {
	for _, fn := range t.Values {
		if _, ok := fvn[fn]; !ok {
			return true
		}
	}
	return false
}

func unmarshalRow(t r.Table, cv cacheValue) (r.FieldValuesByName, error) {
	if cv == nil {
		return nil, nil
	}
	fvn := make(r.FieldValuesByName, len(t.Values))
	for i, fn := range t.Values {
		if i >= len(cv) {
			continue
		}
		v, err := unmarshalField(t, fn, cv[i])
		if err != nil {
			return nil, err
		}
		fvn[fn] = v
	}
	return fvn, nil
}
