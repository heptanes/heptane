package heptane

import "fmt"

// Step represents a single operation in a serial sequence of operations.
type Step interface {
}

// StepResult is the result of the execution of a Step. Err means an error
// produced during the execution of the Step and a non nil value aborts the
// sequence of Steps. Next means the next Step in the sequence and is nil when
// the sequence is over.
type StepResult struct {
	Err  error
	Next Step
}

// SingleStep is a Step executed singly.
type SingleStep interface {
	Step
	Exec() StepResult
}

// BatchStep is a Step executed concurrently with other Steps of the same kind,
// tipically for performace.
type BatchStep interface {
	Step
	Batch() Batch
}

// Batch executes several Steps of the same kind concurrently, tipically for
// performance.
type Batch interface {
	Exec([]Step) []StepResult
}

// UnsupportedStepTypeError is produced when the type of a Step is not
// supported. Current supported types are BatchStep and SingleStep.
type UnsupportedStepTypeError struct {
	Step Step
}

func (e UnsupportedStepTypeError) Error() string {
	return fmt.Sprintf("Unsupported Step Type: %#v", e.Step)
}

// Exec executes the given Steps and all following Steps of their respective
// sequences. Every SingleStep is executed separately. Every BatchStep is
// grouped with others that belong to the same Batch and executed concurrently.
// Batches are used as keys of a built-in map, so the actual types must allow
// it. Errors produced during the execution of a Step abort only the sequence
// of that Step.
func Exec(steps []Step) (errs []error) {
	errs = make([]error, len(steps))
	type step struct {
		i int
		s Step
	}
	steps1 := make([]step, len(steps))
	for i, s := range steps {
		if s != nil {
			steps1[i] = step{i, s}
		}
	}
	steps2 := make([]step, 0, len(steps1))
	processResult := func(i int, r StepResult) {
		if r.Err != nil {
			errs[i] = r.Err
		} else if r.Next != nil {
			steps2 = append(steps2, step{i, r.Next})
		}
	}
	type batch struct {
		ii []int
		ss []Step
	}
	batches := map[Batch]*batch{}
	for len(steps1) > 0 {
		for i, s := range steps1 {
			steps1[i].s = nil
			if bs, ok := s.s.(BatchStep); ok {
				b := bs.Batch()
				h := batches[b]
				if h == nil {
					h = new(batch)
					batches[b] = h
				}
				h.ii = append(h.ii, s.i)
				h.ss = append(h.ss, s.s)
			} else if ss, ok := s.s.(SingleStep); ok {
				processResult(s.i, ss.Exec())
			} else {
				errs[s.i] = UnsupportedStepTypeError{s.s}
			}
		}
		for b, h := range batches {
			delete(batches, b)
			for i, r := range b.Exec(h.ss) {
				processResult(h.ii[i], r)
			}
		}
		steps1, steps2 = steps2, steps1[:0]
	}
	return
}
