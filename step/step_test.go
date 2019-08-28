package heptane

import (
	"errors"
	"fmt"
	"testing"
)

type testSingleStep struct {
	r StepResult
	c int
}

func (s *testSingleStep) Exec() StepResult {
	s.c++
	return s.r
}

type testBatchStep struct {
	b Batch
}

func (s *testBatchStep) Batch() Batch {
	return s.b
}

type testBatch struct {
	m map[Step]StepResult
	c int
}

func (b *testBatch) Exec(ss []Step) (rr []StepResult) {
	for _, s := range ss {
		rr = append(rr, b.m[s])
	}
	b.c++
	return
}

func TestExec_Single_OK(t *testing.T) {

	s1 := &testSingleStep{}
	s2 := &testSingleStep{}
	s1.r.Next = s2
	s3 := &testSingleStep{r: StepResult{Err: errors.New("err3")}}
	s2.r.Next = s3

	errs := Exec([]Step{s1})
	if l := len(errs); l != 1 {
		t.Error(l)
	} else if s := fmt.Sprintf("%#v", errs[0]); s != `&errors.errorString{s:"err3"}` {
		t.Error(s)
	}
	if c := s1.c; c != 1 {
		t.Error(c)
	}
	if c := s2.c; c != 1 {
		t.Error(c)
	}
	if c := s3.c; c != 1 {
		t.Error(c)
	}
}

func TestExec_Single_Err(t *testing.T) {

	s1 := &testSingleStep{r: StepResult{Err: errors.New("err1")}}
	s2 := &testSingleStep{}
	s1.r.Next = s2
	s3 := &testSingleStep{r: StepResult{Err: errors.New("err3")}}
	s2.r.Next = s3

	errs := Exec([]Step{s1})
	if l := len(errs); l != 1 {
		t.Error(l)
	} else if s := fmt.Sprintf("%#v", errs[0]); s != `&errors.errorString{s:"err1"}` {
		t.Error(s)
	}
	if c := s1.c; c != 1 {
		t.Error(c)
	}
	if c := s2.c; c != 0 {
		t.Error(c)
	}
	if c := s3.c; c != 0 {
		t.Error(c)
	}
}

func TestExec_Batch_OK(t *testing.T) {

	b1 := &testBatch{}
	b2 := &testBatch{}

	s1 := &testBatchStep{b: b1}
	s2 := &testBatchStep{b: b1}
	s3 := &testBatchStep{b: b2}
	s4 := &testBatchStep{b: b2}

	b1.m = map[Step]StepResult{
		s1: {Next: s3},
		s2: {Next: s4},
	}
	b2.m = map[Step]StepResult{
		s3: {Err: errors.New("err3")},
		s4: {Err: errors.New("err4")},
	}

	errs := Exec([]Step{s1, s2})
	if l := len(errs); l != 2 {
		t.Error(l)
	} else if s := fmt.Sprintf("%#v", errs[0]); s != `&errors.errorString{s:"err3"}` {
		t.Error(s)
	} else if s := fmt.Sprintf("%#v", errs[1]); s != `&errors.errorString{s:"err4"}` {
		t.Error(s)
	}
	if c := b1.c; c != 1 {
		t.Error(c)
	}
	if c := b2.c; c != 1 {
		t.Error(c)
	}
}

func TestExec_Batch_Error(t *testing.T) {

	b1 := &testBatch{}
	b2 := &testBatch{}

	s1 := &testBatchStep{b: b1}
	s2 := &testBatchStep{b: b1}
	s3 := &testBatchStep{b: b2}
	s4 := &testBatchStep{b: b2}

	b1.m = map[Step]StepResult{
		s1: {Next: s3, Err: errors.New("err1")},
		s2: {Next: s4, Err: errors.New("err2")},
	}

	errs := Exec([]Step{s1, s2})
	if l := len(errs); l != 2 {
		t.Error(l)
	} else if s := fmt.Sprintf("%#v", errs[0]); s != `&errors.errorString{s:"err1"}` {
		t.Error(s)
	} else if s := fmt.Sprintf("%#v", errs[1]); s != `&errors.errorString{s:"err2"}` {
		t.Error(s)
	}
	if c := b1.c; c != 1 {
		t.Error(c)
	}
	if c := b2.c; c != 0 {
		t.Error(c)
	}
}

func TestExec_UnsupportedStepTypeError(t *testing.T) {

	s := "invalid"
	errs := Exec([]Step{s})
	if l := len(errs); l != 1 {
		t.Error(l)
	} else if s := fmt.Sprintf("%#v", errs[0]); s != `heptane.UnsupportedStepTypeError{Step:"invalid"}` {
		t.Error(s)
	} else if s := errs[0].Error(); s != `Unsupported Step Type: "invalid"` {
		t.Error(s)
	}
}
