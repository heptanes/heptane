package heptane

import (
	"sync"

	c "github.com/heptanes/heptane/cache"
	r "github.com/heptanes/heptane/row"
)

type info struct {
	r.Table
	r.RowProvider
	c.CacheProvider
}

type heptane struct {
	m sync.Mutex
	f map[r.TableName]*info
}

// New returns a new instance of Heptane.
func New() Heptane {
	return &heptane{
		sync.Mutex{},
		map[r.TableName]*info{},
	}
}

func (h *heptane) Register(t r.Table, rp r.RowProvider, cp c.CacheProvider) error {
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

func (h *heptane) Unregister(tn r.TableName) {
	h.m.Lock()
	defer h.m.Unlock()
	delete(h.f, tn)
}

func (h *heptane) TableNames() (tns []r.TableName) {
	h.m.Lock()
	defer h.m.Unlock()
	for tn := range h.f {
		tns = append(tns, tn)
	}
	return
}

func (h *heptane) info(tn r.TableName) *info {
	h.m.Lock()
	defer h.m.Unlock()
	return h.f[tn]
}

func (h *heptane) Table(tn r.TableName) (t r.Table) {
	if f := h.info(tn); f != nil {
		t = f.Table
	}
	return
}

func (h *heptane) RowProvider(tn r.TableName) (rp r.RowProvider) {
	if f := h.info(tn); f != nil {
		rp = f.RowProvider
	}
	return
}

func (h *heptane) CacheProvider(tn r.TableName) (cp c.CacheProvider) {
	if f := h.info(tn); f != nil {
		cp = f.CacheProvider
	}
	return
}

func (h *heptane) create(a Create) error {
	tn := a.TableName
	f := h.info(tn)
	if f == nil {
		return UnregisteredTableError{tn}
	}
	key, err := decodeKey(f.Table, a.FieldValues)
	if err != nil {
		return err
	}
	rc := r.RowCreate{Table: f.Table, FieldValues: a.FieldValues}
	if err := f.RowProvider.Access(rc); err != nil {
		return RowProviderAccessError{rc, err}
	}
	if isMissingSomeValue(f.Table, a.FieldValues) {
		rr := r.RowRetrieve{Table: f.Table, FieldValues: a.FieldValues}
		if err := f.RowProvider.Access(&rr); err != nil {
			return RowProviderAccessError{rr, err}
		}
		a.FieldValues = rr.RetrievedValues[0]
	}
	if f.CacheProvider == nil || f.Table.PrimaryKeyCachePrefix == nil {
		return nil
	}
	value, err := decodeValue(f.Table, a.FieldValues)
	if err != nil {
		return err
	}
	cs := c.CacheSet{Key: key.key(), Value: value.value()}
	if err := f.CacheProvider.Access(cs); err != nil {
		return CacheProviderAccessError{cs, err}
	}
	return nil
}

func (h *heptane) retrieve(a *Retrieve) error {
	tn := a.TableName
	f := h.info(tn)
	if f == nil {
		return UnregisteredTableError{tn}
	}
	if f.CacheProvider != nil && f.Table.PrimaryKeyCachePrefix != nil {
		key, err := decodeKey(f.Table, a.FieldValues)
		if err == nil {
			cg := c.CacheGet{Key: key.key()}
			if err := f.CacheProvider.Access(&cg); err != nil {
				return CacheProviderAccessError{cg, err}
			}
			cv := split(cg.Value)
			v, err := unmarshalRow(f.Table, cv)
			if err != nil {
				return err
			}
			if v != nil {
				a.RetrievedValues = []r.FieldValuesByName{v}
				return nil
			}
		}
	}
	rr := r.RowRetrieve{Table: f.Table, FieldValues: a.FieldValues}
	if err := f.RowProvider.Access(&rr); err != nil {
		return RowProviderAccessError{rr, err}
	}
	a.RetrievedValues = rr.RetrievedValues
	css := make([]c.CacheAccess, len(rr.RetrievedValues), 0)
	for _, rv := range rr.RetrievedValues {
		key, err := decodeKey(f.Table, rv)
		if err != nil {
			return err
		}
		value, err := decodeValue(f.Table, rv)
		if err != nil {
			return err
		}
		cs := c.CacheSet{Key: key.key(), Value: value.value()}
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

func (h *heptane) update(a Update) error {
	tn := a.TableName
	f := h.info(tn)
	if f == nil {
		return UnregisteredTableError{tn}
	}
	key, err := decodeKey(f.Table, a.FieldValues)
	if err != nil {
		return err
	}
	ru := r.RowUpdate{Table: f.Table, FieldValues: a.FieldValues}
	if err := f.RowProvider.Access(ru); err != nil {
		return RowProviderAccessError{ru, err}
	}
	if isMissingSomeValue(f.Table, a.FieldValues) {
		rr := r.RowRetrieve{Table: f.Table, FieldValues: a.FieldValues}
		if err := f.RowProvider.Access(&rr); err != nil {
			return RowProviderAccessError{rr, err}
		}
		a.FieldValues = rr.RetrievedValues[0]
	}
	if f.CacheProvider == nil || f.Table.PrimaryKeyCachePrefix == nil {
		return nil
	}
	value, err := decodeValue(f.Table, a.FieldValues)
	if err != nil {
		return err
	}
	cs := c.CacheSet{Key: key.key(), Value: value.value()}
	if err := f.CacheProvider.Access(cs); err != nil {
		return CacheProviderAccessError{cs, err}
	}
	return nil
}

func (h *heptane) delete(a Delete) error {
	tn := a.TableName
	f := h.info(tn)
	if f == nil {
		return UnregisteredTableError{tn}
	}
	key, err := decodeKey(f.Table, a.FieldValues)
	if err != nil {
		return err
	}
	rd := r.RowDelete{Table: f.Table, FieldValues: a.FieldValues}
	if err := f.RowProvider.Access(rd); err != nil {
		return RowProviderAccessError{rd, err}
	}
	if f.CacheProvider == nil || f.Table.PrimaryKeyCachePrefix == nil {
		return nil
	}
	cs := c.CacheSet{Key: key.key(), Value: cacheValue{}.value()}
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
