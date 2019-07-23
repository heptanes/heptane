package heptane

import "fmt"

// Validate checks there are no inconsistencies in the definition of the Table.
func (t Table) Validate() error {
	if len(t.Name) == 0 {
		return fmt.Errorf("Empty TableName in Table")
	}
	if len(t.PartitionKey) == 0 {
		return fmt.Errorf("Table %v: Missing PartitionKey", t.Name)
	}
	for i, fn := range t.PartitionKey {
		if len(fn) == 0 {
			return fmt.Errorf("Table %v: Empty FieldName in PartitionKey", t.Name)
		}
		for _, fn2 := range t.PartitionKey[:i] {
			if fn2 == fn {
				return fmt.Errorf("Table %v: Repeated FieldName in PartitionKey: %v", t.Name, fn)
			}
		}
	}
	if len(t.PrimaryKey) == 0 {
		return fmt.Errorf("Table %v: Missing PrimaryKey", t.Name)
	}
	for i, fn := range t.PrimaryKey {
		if len(fn) == 0 {
			return fmt.Errorf("Table %v: Empty FieldName in PrimaryKey", t.Name)
		}
		if i < len(t.PartitionKey) {
			if fn2 := t.PartitionKey[i]; fn != fn2 {
				return fmt.Errorf("Table %v: Mismatched PrimaryKey: %v", t.Name, fn)
			}
		}
		for _, fn2 := range t.PrimaryKey[:i] {
			if fn2 == fn {
				return fmt.Errorf("Table %v: Repeated FieldName in PrimaryKey: %v", t.Name, fn)
			}
		}
	}
	for i, fn := range t.Values {
		if len(fn) == 0 {
			return fmt.Errorf("Table %v: Empty FieldName in Values", t.Name)
		}
		for _, fn2 := range t.PrimaryKey {
			if fn2 == fn {
				return fmt.Errorf("Table %v: Repeated FieldName in Values: %v", t.Name, fn)
			}
		}
		for _, fn2 := range t.Values[:i] {
			if fn2 == fn {
				return fmt.Errorf("Table %v: Repeated FieldName in Values: %v", t.Name, fn)
			}
		}
	}
	for _, fn := range t.PrimaryKey {
		ft, ok := t.Types[fn]
		if !ok {
			return fmt.Errorf("Table %v: Missing FieldType for FieldName: %v", t.Name, fn)
		}
		if ft != "string" {
			return fmt.Errorf("Table %v: Invalid FieldType for FieldName %v: %v", t.Name, fn, ft)
		}
	}
	for _, fn := range t.Values {
		ft, ok := t.Types[fn]
		if !ok {
			return fmt.Errorf("Table %v: Missing FieldType for FieldName: %v", t.Name, fn)
		}
		if ft != "string" {
			return fmt.Errorf("Table %v: Invalid FieldType for FieldName %v: %v", t.Name, fn, ft)
		}
	}
	return nil
}
