package heptane

import (
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	r "github.com/heptanes/heptane/row"
)

func TestingTable1() r.Table {
	return r.Table{
		Name:                  "table1",
		PartitionKey:          []r.FieldName{"foo"},
		PrimaryKey:            []r.FieldName{"foo", "bar"},
		Values:                []r.FieldName{"baz"},
		Types:                 r.FieldTypesByName{"foo": "string", "bar": "string", "baz": "string"},
		PrimaryKeyCachePrefix: []string{"table1_pk", "0"},
	}
}

type TestDialect struct{}

func (d TestDialect) WriteTableName(sb *strings.Builder, n r.TableName) {
	sb.WriteString("'")
	sb.WriteString(string(n))
	sb.WriteString("'")
}

func (d TestDialect) WriteFieldName(sb *strings.Builder, n r.FieldName) {
	sb.WriteString("'")
	sb.WriteString(string(n))
	sb.WriteString("'")
}

func (d TestDialect) WritePlaceholder(sb *strings.Builder, i int) {
	sb.WriteString("?")
}

func TestCreate_ValidationError(t *testing.T) {
	b := TestingTable1()
	b.Name = ""
	rp := Row{nil, TestDialect{}}
	a := r.RowCreate{Table: b, FieldValues: r.FieldValuesByName{}}
	if err := rp.Access(a); err == nil {
		t.Error(err)
	} else if s := err.Error(); s != `Empty TableName in Table` {
		t.Error(s)
	}
}

func TestCreate_MissingValue(t *testing.T) {
	t.Parallel()
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Error(err)
	}
	defer db.Close()
	mock.ExpectExec(`INSERT INTO 'table1' \('foo', 'bar'\) VALUES \(\?, \?\)`).
		WithArgs("1", "2").
		WillReturnResult(sqlmock.NewResult(1, 1))
	b := TestingTable1()
	rp := Row{db, TestDialect{}}
	a := r.RowCreate{Table: b, FieldValues: r.FieldValuesByName{"foo": "1", "bar": "2"}}
	if err := rp.Access(a); err != nil {
		t.Error(err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Error(err)
	}
}

func TestCreate_MissingValue_AmongMultipleValues(t *testing.T) {
	t.Parallel()
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Error(err)
	}
	defer db.Close()
	mock.ExpectExec(`INSERT INTO 'table1' \('foo', 'bar', 'qux'\) VALUES \(\?, \?, \?\)`).
		WithArgs("1", "2", "4").
		WillReturnResult(sqlmock.NewResult(1, 1))
	b := TestingTable1()
	b.Values = []r.FieldName{"baz", "qux"}
	b.Types = r.FieldTypesByName{"foo": "string", "bar": "string", "baz": "string", "qux": "string"}
	rp := Row{db, TestDialect{}}
	a := r.RowCreate{Table: b, FieldValues: r.FieldValuesByName{"foo": "1", "bar": "2", "qux": "4"}}
	if err := rp.Access(a); err != nil {
		t.Error(err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Error(err)
	}
}

func TestCreate_Null(t *testing.T) {
	t.Parallel()
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Error(err)
	}
	defer db.Close()
	mock.ExpectExec(`INSERT INTO 'table1' \('foo', 'bar', 'baz'\) VALUES \(\?, \?, \?\)`).
		WithArgs("1", "2", nil).
		WillReturnResult(sqlmock.NewResult(1, 1))
	b := TestingTable1()
	rp := Row{db, TestDialect{}}
	a := r.RowCreate{Table: b, FieldValues: r.FieldValuesByName{"foo": "1", "bar": "2", "baz": nil}}
	if err := rp.Access(a); err != nil {
		t.Error(err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Error(err)
	}
}

func TestCreate_Bool(t *testing.T) {
	t.Parallel()
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Error(err)
	}
	defer db.Close()
	mock.ExpectExec(`INSERT INTO 'table1' \('foo', 'bar', 'baz'\) VALUES \(\?, \?, \?\)`).
		WithArgs(false, true, false).
		WillReturnResult(sqlmock.NewResult(1, 1))
	b := TestingTable1()
	rp := Row{db, TestDialect{}}
	a := r.RowCreate{Table: b, FieldValues: r.FieldValuesByName{"foo": false, "bar": true, "baz": false}}
	if err := rp.Access(a); err != nil {
		t.Error(err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Error(err)
	}
}

func TestCreate_ByRef(t *testing.T) {
	t.Parallel()
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Error(err)
	}
	defer db.Close()
	mock.ExpectExec(`INSERT INTO 'table1' \('foo', 'bar'\) VALUES \(\?, \?\)`).
		WithArgs("1", "2").
		WillReturnResult(sqlmock.NewResult(1, 1))
	b := TestingTable1()
	rp := Row{db, TestDialect{}}
	a := &r.RowCreate{Table: b, FieldValues: r.FieldValuesByName{"foo": "1", "bar": "2"}}
	if err := rp.Access(a); err != nil {
		t.Error(err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Error(err)
	}
}

func TestRetrieve_UnsupportedRowAccessTypeError(t *testing.T) {
	b := TestingTable1()
	rp := Row{nil, TestDialect{}}
	a := r.RowRetrieve{Table: b, FieldValues: r.FieldValuesByName{}}
	if err := rp.Access(a); err == nil {
		t.Error(err)
	} else if s := err.Error(); s != `Unsupported RowAccess Type: heptane.RowRetrieve{Table:heptane.Table{Name:"table1", PartitionKey:[]heptane.FieldName{"foo"}, PrimaryKey:[]heptane.FieldName{"foo", "bar"}, Values:[]heptane.FieldName{"baz"}, Types:heptane.FieldTypesByName{"bar":"string", "baz":"string", "foo":"string"}, PrimaryKeyCachePrefix:[]string{"table1_pk", "0"}}, FieldValues:heptane.FieldValuesByName{}, RetrievedValues:[]heptane.FieldValuesByName(nil)}` {
		t.Error(s)
	}
}

func TestRetrieve_ValidationError(t *testing.T) {
	b := TestingTable1()
	b.Name = ""
	rp := Row{nil, TestDialect{}}
	a := &r.RowRetrieve{Table: b, FieldValues: r.FieldValuesByName{}}
	if err := rp.Access(a); err == nil {
		t.Error(err)
	} else if s := err.Error(); s != `Empty TableName in Table` {
		t.Error(s)
	}
}

func TestRetrieve_SingleSelect(t *testing.T) {
	t.Parallel()
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Error(err)
	}
	defer db.Close()
	mock.ExpectQuery(`SELECT 'baz' FROM 'table1' WHERE 'foo' = \? AND 'bar' = \?`).
		WithArgs("1", "2").
		WillReturnRows(sqlmock.NewRows([]string{"baz"}).AddRow("3"))
	b := TestingTable1()
	rp := Row{db, TestDialect{}}
	a := &r.RowRetrieve{Table: b, FieldValues: r.FieldValuesByName{"foo": "1", "bar": "2"}}
	if err := rp.Access(a); err != nil {
		t.Error(err)
	}
	if s := fmt.Sprintf("%#v", a.RetrievedValues); s != `[]heptane.FieldValuesByName{heptane.FieldValuesByName{"baz":"3"}}` {
		t.Error(s)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Error(err)
	}
}

func TestRetrieve_MultipleSelect(t *testing.T) {
	t.Parallel()
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Error(err)
	}
	defer db.Close()
	mock.ExpectQuery(`SELECT 'baz', 'qux' FROM 'table1' WHERE 'foo' = \? AND 'bar' = \?`).
		WithArgs("1", "2").
		WillReturnRows(sqlmock.NewRows([]string{"baz", "qux"}).AddRow("3", "4"))
	b := TestingTable1()
	b.Values = []r.FieldName{"baz", "qux"}
	b.Types = r.FieldTypesByName{"foo": "string", "bar": "string", "baz": "string", "qux": "string"}
	rp := Row{db, TestDialect{}}
	a := &r.RowRetrieve{Table: b, FieldValues: r.FieldValuesByName{"foo": "1", "bar": "2"}}
	if err := rp.Access(a); err != nil {
		t.Error(err)
	}
	if s := fmt.Sprintf("%#v", a.RetrievedValues); s != `[]heptane.FieldValuesByName{heptane.FieldValuesByName{"baz":"3", "qux":"4"}}` {
		t.Error(s)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Error(err)
	}
}

func TestRetrieve_SinglePrimaryKey(t *testing.T) {
	t.Parallel()
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Error(err)
	}
	defer db.Close()
	mock.ExpectQuery(`SELECT 'baz' FROM 'table1' WHERE 'foo' = \\?`).
		WithArgs("1").
		WillReturnRows(sqlmock.NewRows([]string{"baz"}).AddRow("3"))
	b := TestingTable1()
	b.PrimaryKey = []r.FieldName{"foo"}
	rp := Row{db, TestDialect{}}
	a := &r.RowRetrieve{Table: b, FieldValues: r.FieldValuesByName{"foo": "1"}}
	if err := rp.Access(a); err != nil {
		t.Error(err)
	}
	if s := fmt.Sprintf("%#v", a.RetrievedValues); s != `[]heptane.FieldValuesByName{heptane.FieldValuesByName{"baz":"3"}}` {
		t.Error(s)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Error(err)
	}
}

func TestRetrieve_NullInWhere(t *testing.T) {
	t.Parallel()
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Error(err)
	}
	defer db.Close()
	mock.ExpectQuery(`SELECT 'baz' FROM 'table1' WHERE 'foo' IS NULL AND 'bar' IS NULL`).
		WithArgs().
		WillReturnRows(sqlmock.NewRows([]string{"baz"}).AddRow("3"))
	b := TestingTable1()
	rp := Row{db, TestDialect{}}
	a := &r.RowRetrieve{Table: b, FieldValues: r.FieldValuesByName{"foo": nil, "bar": nil}}
	if err := rp.Access(a); err != nil {
		t.Error(err)
	}
	if s := fmt.Sprintf("%#v", a.RetrievedValues); s != `[]heptane.FieldValuesByName{heptane.FieldValuesByName{"baz":"3"}}` {
		t.Error(s)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Error(err)
	}
}

func TestRetrieve_Bool(t *testing.T) {
	t.Parallel()
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Error(err)
	}
	defer db.Close()
	mock.ExpectQuery(`SELECT 'baz' FROM 'table1' WHERE 'foo' = \? AND 'bar' = \?`).
		WithArgs(false, true).
		WillReturnRows(sqlmock.NewRows([]string{"baz"}).AddRow("false"))
	b := TestingTable1()
	b.Types = r.FieldTypesByName{"foo": "bool", "bar": "bool", "baz": "bool"}
	rp := Row{db, TestDialect{}}
	a := &r.RowRetrieve{Table: b, FieldValues: r.FieldValuesByName{"foo": false, "bar": true}}
	if err := rp.Access(a); err != nil {
		t.Error(err)
	}
	if s := fmt.Sprintf("%#v", a.RetrievedValues); s != `[]heptane.FieldValuesByName{heptane.FieldValuesByName{"baz":false}}` {
		t.Error(s)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Error(err)
	}
}

func TestUpdate_ValidationError(t *testing.T) {
	b := TestingTable1()
	b.Name = ""
	rp := Row{nil, TestDialect{}}
	a := r.RowUpdate{Table: b, FieldValues: r.FieldValuesByName{}}
	if err := rp.Access(a); err == nil {
		t.Error(err)
	} else if s := err.Error(); s != `Empty TableName in Table` {
		t.Error(s)
	}
}

func TestUpdate_SingleSet(t *testing.T) {
	t.Parallel()
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Error(err)
	}
	defer db.Close()
	mock.ExpectExec(`UPDATE 'table1' SET 'baz' = \? WHERE 'foo' = \? AND 'bar' = \?`).
		WithArgs("3", "1", "2").
		WillReturnResult(sqlmock.NewResult(1, 1))
	b := TestingTable1()
	rp := Row{db, TestDialect{}}
	a := r.RowUpdate{Table: b, FieldValues: r.FieldValuesByName{"foo": "1", "bar": "2", "baz": "3"}}
	if err := rp.Access(a); err != nil {
		t.Error(err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Error(err)
	}
}

func TestUpdate_MultipleSet(t *testing.T) {
	t.Parallel()
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Error(err)
	}
	defer db.Close()
	mock.ExpectExec(`UPDATE 'table1' SET 'baz' = \?, 'qux' = \? WHERE 'foo' = \? AND 'bar' = \?`).
		WithArgs("3", "4", "1", "2").
		WillReturnResult(sqlmock.NewResult(1, 1))
	b := TestingTable1()
	b.Values = []r.FieldName{"baz", "qux"}
	b.Types = r.FieldTypesByName{"foo": "string", "bar": "string", "baz": "string", "qux": "string"}
	rp := Row{db, TestDialect{}}
	a := r.RowUpdate{Table: b, FieldValues: r.FieldValuesByName{"foo": "1", "bar": "2", "baz": "3", "qux": "4"}}
	if err := rp.Access(a); err != nil {
		t.Error(err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Error(err)
	}
}

func TestUpdate_SinglePrimaryKey(t *testing.T) {
	t.Parallel()
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Error(err)
	}
	defer db.Close()
	mock.ExpectExec(`UPDATE 'table1' SET 'baz' = \? WHERE 'foo' = \?`).
		WithArgs("3", "1").
		WillReturnResult(sqlmock.NewResult(1, 1))
	b := TestingTable1()
	b.PrimaryKey = []r.FieldName{"foo"}
	rp := Row{db, TestDialect{}}
	a := r.RowUpdate{Table: b, FieldValues: r.FieldValuesByName{"foo": "1", "baz": "3"}}
	if err := rp.Access(a); err != nil {
		t.Error(err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Error(err)
	}
}

func TestUpdate_NullInSet(t *testing.T) {
	t.Parallel()
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Error(err)
	}
	defer db.Close()
	mock.ExpectExec(`UPDATE 'table1' SET 'baz' = \? WHERE 'foo' = \? AND 'bar' = \?`).
		WithArgs(nil, "1", "2").
		WillReturnResult(sqlmock.NewResult(1, 1))
	b := TestingTable1()
	rp := Row{db, TestDialect{}}
	a := r.RowUpdate{Table: b, FieldValues: r.FieldValuesByName{"foo": "1", "bar": "2", "baz": nil}}
	if err := rp.Access(a); err != nil {
		t.Error(err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Error(err)
	}
}

func TestUpdate_NullInWhere(t *testing.T) {
	t.Parallel()
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Error(err)
	}
	defer db.Close()
	mock.ExpectExec(`UPDATE 'table1' SET 'baz' = \? WHERE 'foo' IS NULL AND 'bar' IS NULL`).
		WithArgs("3").
		WillReturnResult(sqlmock.NewResult(1, 1))
	b := TestingTable1()
	rp := Row{db, TestDialect{}}
	a := r.RowUpdate{Table: b, FieldValues: r.FieldValuesByName{"foo": nil, "bar": nil, "baz": "3"}}
	if err := rp.Access(a); err != nil {
		t.Error(err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Error(err)
	}
}

func TestUpdate_Bool(t *testing.T) {
	t.Parallel()
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Error(err)
	}
	defer db.Close()
	mock.ExpectExec(`UPDATE 'table1' SET 'baz' = \? WHERE 'foo' = \? AND 'bar' = \?`).
		WithArgs(false, false, true).
		WillReturnResult(sqlmock.NewResult(1, 1))
	b := TestingTable1()
	rp := Row{db, TestDialect{}}
	a := r.RowUpdate{Table: b, FieldValues: r.FieldValuesByName{"foo": false, "bar": true, "baz": false}}
	if err := rp.Access(a); err != nil {
		t.Error(err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Error(err)
	}
}

func TestUpdate_ByRef(t *testing.T) {
	t.Parallel()
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Error(err)
	}
	defer db.Close()
	mock.ExpectExec(`UPDATE 'table1' SET 'baz' = \? WHERE 'foo' = \? AND 'bar' = \?`).
		WithArgs(false, false, true).
		WillReturnResult(sqlmock.NewResult(1, 1))
	b := TestingTable1()
	rp := Row{db, TestDialect{}}
	a := &r.RowUpdate{Table: b, FieldValues: r.FieldValuesByName{"foo": false, "bar": true, "baz": false}}
	if err := rp.Access(a); err != nil {
		t.Error(err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Error(err)
	}
}

func TestDelete_ValidationError(t *testing.T) {
	b := TestingTable1()
	b.Name = ""
	rp := Row{nil, TestDialect{}}
	a := r.RowDelete{Table: b, FieldValues: r.FieldValuesByName{}}
	if err := rp.Access(a); err == nil {
		t.Error(err)
	} else if s := err.Error(); s != `Empty TableName in Table` {
		t.Error(s)
	}
}

func TestDelete_MultiplePrimaryKey(t *testing.T) {
	t.Parallel()
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Error(err)
	}
	defer db.Close()
	mock.ExpectExec(`DELETE FROM 'table1' WHERE 'foo' = \? AND 'bar' = \?`).
		WithArgs("1", "2").
		WillReturnResult(sqlmock.NewResult(1, 1))
	b := TestingTable1()
	rp := Row{db, TestDialect{}}
	a := r.RowDelete{Table: b, FieldValues: r.FieldValuesByName{"foo": "1", "bar": "2"}}
	if err := rp.Access(a); err != nil {
		t.Error(err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Error(err)
	}
}

func TestDelete_SinglePrimaryKey(t *testing.T) {
	t.Parallel()
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Error(err)
	}
	defer db.Close()
	mock.ExpectExec(`DELETE FROM 'table1' WHERE 'foo' = \?`).
		WithArgs("1").
		WillReturnResult(sqlmock.NewResult(1, 1))
	b := TestingTable1()
	b.PrimaryKey = []r.FieldName{"foo"}
	rp := Row{db, TestDialect{}}
	a := r.RowDelete{Table: b, FieldValues: r.FieldValuesByName{"foo": "1"}}
	if err := rp.Access(a); err != nil {
		t.Error(err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Error(err)
	}
}

func TestDelete_NullInWhere(t *testing.T) {
	t.Parallel()
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Error(err)
	}
	defer db.Close()
	mock.ExpectExec(`DELETE FROM 'table1' WHERE 'foo' IS NULL AND 'bar' IS NULL`).
		WithArgs().
		WillReturnResult(sqlmock.NewResult(1, 1))
	b := TestingTable1()
	rp := Row{db, TestDialect{}}
	a := r.RowDelete{Table: b, FieldValues: r.FieldValuesByName{"foo": nil, "bar": nil}}
	if err := rp.Access(a); err != nil {
		t.Error(err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Error(err)
	}
}

func TestDelete_Bool(t *testing.T) {
	t.Parallel()
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Error(err)
	}
	defer db.Close()
	mock.ExpectExec(`DELETE FROM 'table1' WHERE 'foo' = \? AND 'bar' = \?`).
		WithArgs(false, true).
		WillReturnResult(sqlmock.NewResult(1, 1))
	b := TestingTable1()
	rp := Row{db, TestDialect{}}
	a := r.RowDelete{Table: b, FieldValues: r.FieldValuesByName{"foo": false, "bar": true}}
	if err := rp.Access(a); err != nil {
		t.Error(err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Error(err)
	}
}

func TestDelete_ByRef(t *testing.T) {
	t.Parallel()
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Error(err)
	}
	defer db.Close()
	mock.ExpectExec(`DELETE FROM 'table1' WHERE 'foo' = \? AND 'bar' = \?`).
		WithArgs("1", "2").
		WillReturnResult(sqlmock.NewResult(1, 1))
	b := TestingTable1()
	rp := Row{db, TestDialect{}}
	a := &r.RowDelete{Table: b, FieldValues: r.FieldValuesByName{"foo": "1", "bar": "2"}}
	if err := rp.Access(a); err != nil {
		t.Error(err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Error(err)
	}
}

func TestCreate_QueryError(t *testing.T) {
	t.Parallel()
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Error(err)
	}
	defer db.Close()
	mock.ExpectExec("INSERT INTO 'table1' \\('foo', 'bar'\\) VALUES \\(\\?, \\?\\)").
		WithArgs("1", "2").
		WillReturnError(errors.New("problem"))
	b := TestingTable1()
	rp := Row{db, TestDialect{}}
	a := r.RowCreate{Table: b, FieldValues: r.FieldValuesByName{"foo": "1", "bar": "2"}}
	if err := rp.Access(a); err == nil {
		t.Error(err)
	} else if s := err.Error(); s != `Sql Error: problem` {
		t.Error(s)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Error(err)
	}
}

func TestRetrieve_QueryError(t *testing.T) {
	t.Parallel()
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Error(err)
	}
	defer db.Close()
	mock.ExpectQuery(`SELECT 'baz' FROM 'table1' WHERE 'foo' = \? AND 'bar' = \?`).
		WithArgs("1", "2").
		WillReturnError(errors.New("problem"))
	b := TestingTable1()
	rp := Row{db, TestDialect{}}
	a := &r.RowRetrieve{Table: b, FieldValues: r.FieldValuesByName{"foo": "1", "bar": "2"}}
	if err := rp.Access(a); err == nil {
		t.Error(err)
	} else if s := err.Error(); s != `Sql Error: problem` {
		t.Error(s)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Error(err)
	}
}

func TestRetrieve_ScanError(t *testing.T) {
	t.Parallel()
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Error(err)
	}
	defer db.Close()
	mock.ExpectQuery(`SELECT 'baz' FROM 'table1' WHERE 'foo' = \? AND 'bar' = \?`).
		WithArgs(false, true).
		WillReturnRows(sqlmock.NewRows([]string{"baz"}).AddRow("invalid bool"))
	b := TestingTable1()
	b.Types = r.FieldTypesByName{"foo": "bool", "bar": "bool", "baz": "bool"}
	rp := Row{db, TestDialect{}}
	a := &r.RowRetrieve{Table: b, FieldValues: r.FieldValuesByName{"foo": false, "bar": true}}
	if err := rp.Access(a); err == nil {
		t.Error(err)
	} else if s := err.Error(); s != `Sql Error: sql: Scan error on column index 0, name "baz": sql/driver: couldn't convert "invalid bool" into type bool` {
		t.Error(s)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Error(err)
	}
}

func TestRetrieve_RowError(t *testing.T) {
	t.Parallel()
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Error(err)
	}
	defer db.Close()
	mock.ExpectQuery(`SELECT 'baz' FROM 'table1' WHERE 'foo' = \? AND 'bar' = \?`).
		WithArgs("1", "2").
		WillReturnRows(sqlmock.NewRows([]string{"baz"}).AddRow("3").RowError(0, errors.New("problem")))
	b := TestingTable1()
	rp := Row{db, TestDialect{}}
	a := &r.RowRetrieve{Table: b, FieldValues: r.FieldValuesByName{"foo": "1", "bar": "2"}}
	if err := rp.Access(a); err == nil {
		t.Error(err)
	} else if s := err.Error(); s != `Sql Error: problem` {
		t.Error(s)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Error(err)
	}
}

func TestCreate_BySlice(t *testing.T) {
	t.Parallel()
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Error(err)
	}
	defer db.Close()
	mock.ExpectExec(`INSERT INTO 'table1' \('foo', 'bar'\) VALUES \(\?, \?\)`).
		WithArgs("1", "2").
		WillReturnResult(sqlmock.NewResult(1, 1))
	b := TestingTable1()
	rp := Row{db, TestDialect{}}
	a := r.RowCreate{Table: b, FieldValues: r.FieldValuesByName{"foo": "1", "bar": "2"}}
	errs := rp.AccessSlice([]r.RowAccess{a})
	if errs == nil {
		t.Error(err)
	}
	if l := len(errs); l != 1 {
		t.Fatal(err)
	}
	if err := errs[0]; err != nil {
		t.Error(err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Error(err)
	}
}
