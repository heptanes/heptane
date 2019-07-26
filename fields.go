package heptane

func (t Table) marshal(fn FieldName, fv FieldValue) ([]byte, error) {
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

func (t Table) unmarshal(fn FieldName, q []byte) (FieldValue, error) {
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

func (t Table) cacheKey(fvn FieldValuesByName) (cacheKey, error) {
	ck := make(cacheKey, len(t.PrimaryKeyCachePrefix)+len(t.PrimaryKey), 0)
	for _, k := range t.PrimaryKeyCachePrefix {
		ck = append(ck, []byte(k))
	}
	for _, fn := range t.PrimaryKey {
		fv, ok := fvn[fn]
		if !ok {
			return nil, MissingFieldValueError{t.Name, fn, fvn}
		}
		v, err := t.marshal(fn, fv)
		if err != nil {
			return nil, err
		}
		ck = append(ck, v)
	}
	return ck, nil
}

func (t Table) cacheValue(fvn FieldValuesByName) (cacheValue, error) {
	cv := make(cacheValue, len(t.Values), 0)
	for _, fn := range t.Values {
		fv, ok := fvn[fn]
		if !ok {
			cv = append(cv, nil)
			continue
		}
		v, err := t.marshal(fn, fv)
		if err != nil {
			return nil, err
		}
		cv = append(cv, v)
	}
	return cv, nil
}

func (t Table) unmarshalRow(cv cacheValue) (FieldValuesByName, error) {
	if cv == nil {
		return nil, nil
	}
	fvn := make(FieldValuesByName, len(t.Values))
	for i, fn := range t.Values {
		if i >= len(cv) {
			continue
		}
		v, err := t.unmarshal(fn, cv[i])
		if err != nil {
			return nil, err
		}
		fvn[fn] = v
	}
	return fvn, nil
}

func (t Table) isMissingSomeValue(fvn FieldValuesByName) bool {
	for _, fn := range t.Values {
		if _, ok := fvn[fn]; !ok {
			return true
		}
	}
	return false
}
