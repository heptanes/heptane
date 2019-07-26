package heptane

import (
	"sync"
)

type info struct {
	Table
	RowProvider
	CacheProvider
}

type heptane struct {
	m sync.Mutex
	f map[TableName]*info
}

// New returns a new instance of Heptane.
func New() Heptane {
	return &heptane{
		sync.Mutex{},
		map[TableName]*info{},
	}
}

func (h *heptane) Register(t Table, rp RowProvider, cp CacheProvider) error {
	if err := t.Validate(); err != nil {
		return err
	}
	if rp == nil {
		return NullRowProviderError{t.Name}
	}
	h.m.Lock()
	defer h.m.Unlock()
	h.f[t.Name] = &info{t, rp, cp}
	return nil
}

func (h *heptane) Unregister(tn TableName) {
	h.m.Lock()
	defer h.m.Unlock()
	delete(h.f, tn)
}

func (h *heptane) TableNames() (tns []TableName) {
	h.m.Lock()
	defer h.m.Unlock()
	for tn := range h.f {
		tns = append(tns, tn)
	}
	return
}

func (h *heptane) info(tn TableName) *info {
	h.m.Lock()
	defer h.m.Unlock()
	return h.f[tn]
}

func (h *heptane) Table(tn TableName) (t Table) {
	if f := h.info(tn); f != nil {
		t = f.Table
	}
	return
}

func (h *heptane) RowProvider(tn TableName) (rp RowProvider) {
	if f := h.info(tn); f != nil {
		rp = f.RowProvider
	}
	return
}

func (h *heptane) CacheProvider(tn TableName) (cp CacheProvider) {
	if f := h.info(tn); f != nil {
		cp = f.CacheProvider
	}
	return
}

func (h *heptane) create(c Create) error {
	tn := c.TableName
	f := h.info(tn)
	if f == nil {
		return UnregisteredTableError{tn}
	}
	key, err := f.Table.cacheKey(c.FieldValues)
	if err != nil {
		return err
	}
	rc := RowCreate{f.Table, c.FieldValues}
	if err := f.RowProvider.Access(rc); err != nil {
		return RowProviderAccessError{rc, err}
	}
	if f.Table.isMissingSomeValue(c.FieldValues) {
		rr := RowRetrieve{f.Table, c.FieldValues, nil}
		if err := f.RowProvider.Access(&rr); err != nil {
			return RowProviderAccessError{rr, err}
		}
		c.FieldValues = rr.RetrievedValues[0]
	}
	if f.CacheProvider == nil || f.Table.PrimaryKeyCachePrefix == nil {
		return nil
	}
	value, err := f.Table.cacheValue(c.FieldValues)
	if err != nil {
		return err
	}
	cs := CacheSet{key.Key(), value.Value()}
	if err := f.CacheProvider.Access(cs); err != nil {
		return CacheProviderAccessError{cs, err}
	}
	return nil
}

func (h *heptane) retrieve(r *Retrieve) error {
	tn := r.TableName
	f := h.info(tn)
	if f == nil {
		return UnregisteredTableError{tn}
	}
	if f.CacheProvider != nil && f.Table.PrimaryKeyCachePrefix != nil {
		key, err := f.Table.cacheKey(r.FieldValues)
		if err == nil {
			cg := CacheGet{key.Key(), nil}
			if err := f.CacheProvider.Access(&cg); err != nil {
				return CacheProviderAccessError{cg, err}
			}
			v, err := f.Table.unmarshalRow(cg.Value.value())
			if err != nil {
				return err
			}
			if v != nil {
				r.RetrievedValues = []FieldValuesByName{v}
				return nil
			}
		}
	}
	rr := RowRetrieve{f.Table, r.FieldValues, nil}
	if err := f.RowProvider.Access(&rr); err != nil {
		return RowProviderAccessError{rr, err}
	}
	r.RetrievedValues = rr.RetrievedValues
	css := make([]CacheAccess, len(rr.RetrievedValues), 0)
	for _, rv := range rr.RetrievedValues {
		key, err := f.Table.cacheKey(rv)
		if err != nil {
			return err
		}
		value, err := f.Table.cacheValue(rv)
		if err != nil {
			return err
		}
		cs := CacheSet{key.Key(), value.Value()}
		css = append(css, cs)
	}
	errs := f.CacheProvider.AccessSlice(css)
	nnerrs := []error(nil)
	for i, err := range errs {
		if err != nil {
			if nnerrs == nil {
				nnerrs = make([]error, len(errs), 0)
			}
			nnerrs = append(nnerrs, CacheProviderAccessError{css[i], err})
		}
	}
	if len(nnerrs) > 0 {
		if len(nnerrs) == 1 {
			return nnerrs[0]
		}
		return MultipleErrors{nnerrs}
	}
	return nil
}

func (h *heptane) update(u Update) error {
	tn := u.TableName
	f := h.info(tn)
	if f == nil {
		return UnregisteredTableError{tn}
	}
	key, err := f.Table.cacheKey(u.FieldValues)
	if err != nil {
		return err
	}
	ru := RowUpdate{f.Table, u.FieldValues}
	if err := f.RowProvider.Access(ru); err != nil {
		return RowProviderAccessError{ru, err}
	}
	if f.Table.isMissingSomeValue(u.FieldValues) {
		rr := RowRetrieve{f.Table, u.FieldValues, nil}
		if err := f.RowProvider.Access(&rr); err != nil {
			return RowProviderAccessError{rr, err}
		}
		u.FieldValues = rr.RetrievedValues[0]
	}
	if f.CacheProvider == nil || f.Table.PrimaryKeyCachePrefix == nil {
		return nil
	}
	value, err := f.Table.cacheValue(u.FieldValues)
	if err != nil {
		return err
	}
	cs := CacheSet{key.Key(), value.Value()}
	if err := f.CacheProvider.Access(cs); err != nil {
		return CacheProviderAccessError{cs, err}
	}
	return nil
}

func (h *heptane) delete(d Delete) error {
	tn := d.TableName
	f := h.info(tn)
	if f == nil {
		return UnregisteredTableError{tn}
	}
	key, err := f.Table.cacheKey(d.FieldValues)
	if err != nil {
		return err
	}
	rd := RowDelete{f.Table, d.FieldValues}
	if err := f.RowProvider.Access(rd); err != nil {
		return RowProviderAccessError{rd, err}
	}
	if f.CacheProvider == nil || f.Table.PrimaryKeyCachePrefix == nil {
		return nil
	}
	cs := CacheSet{key.Key(), make(cacheValue, 0).Value()}
	if err := f.CacheProvider.Access(cs); err != nil {
		return CacheProviderAccessError{cs, err}
	}
	return nil
}

func (h *heptane) Access(a Access) error {
	switch a := a.(type) {
	case Create:
		return h.create(a)
	case *Create:
		return h.create(*a)
	case *Retrieve:
		return h.retrieve(a)
	case Update:
		return h.update(a)
	case *Update:
		return h.update(*a)
	case Delete:
		return h.delete(a)
	case *Delete:
		return h.delete(*a)
	}
	return UnsupportedAccessTypeError{a}
}

func (h *heptane) AccessSlice(aa []Access) (errs []error) {
	for _, a := range aa {
		errs = append(errs, h.Access(a))
	}
	return
}
