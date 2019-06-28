package heptane

import "bytes"

type cacheKey [][]byte

func (k cacheKey) Key() CacheKey {
	b := bytes.Buffer{}
	for _, q := range k {
		b.Write(q)
		// TODO use interface
		b.WriteRune('#')
	}
	return CacheKey(b.String())
}

type cacheValue [][]byte

func (v cacheValue) Value() CacheValue {
	if v == nil {
		return nil
	}
	b := bytes.Buffer{}
	for _, q := range v {
		b.Write(q)
		// TODO use interface
		b.WriteRune('#')
	}
	return CacheValue(b.Bytes())
}

func (cv CacheValue) value() (v cacheValue) {
	if cv == nil {
		return nil
	}
	// TODO use interface
	for _, q := range bytes.Split(cv, []byte("#")) {
		v = append(v, q)
	}
	return
}
