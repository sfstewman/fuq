package fuq

import (
	"testing"
)

func TestNewTaskList(t *testing.T) {
	tl := NewTaskList(15, false)
	for i, v := range tl {
		if v != 0 {
			t.Errorf("unfilled task list, expect bits %d-%d to be zero",
				8*i, 8*i+7)
		}
	}

	tl = NewTaskList(8, false)
	for i, v := range tl {
		if v != 0 {
			t.Errorf("unfilled task list, expect bits %d-%d to be zero",
				8*i, 8*i+7)
		}
	}

	tl = NewTaskList(24, true)
	for i, v := range tl {
		if v != 0xff {
			t.Errorf("unfilled task list, expect bits %d-%d to be one",
				8*i, 8*i+7)
		}
	}

	tl = NewTaskList(19, true)
	if tl[0] != 0xff {
		t.Error("unfilled task list, expect bits 0-7 to be one")
	}

	if tl[1] != 0xff {
		t.Error("unfilled task list, expect bits 8-15 to be one")
	}

	if tl[2] != 0x07 {
		t.Logf("bits 16-23 are %0b", tl[2])
		t.Error("unfilled task list, expect bits 16-18 to be one and bits 19-23 to be zero")
	}

}

func TestTaskListHasAndSet(t *testing.T) {
	tl := NewTaskList(27, true)
	for i := 0; i < 27; i++ {
		if !tl.Has(i) {
			t.Errorf("task list Has(%d) should be true\n", i)
		}
	}

	tl.Set(13, false)
	for i := 0; i < 27; i++ {
		if i == 13 && tl.Has(i) {
			t.Errorf("task list Has(%d) should be false\n", i)
		}

		if i != 13 && !tl.Has(i) {
			t.Errorf("task list Has(%d) should be true\n", i)
		}
	}
}

func TestTaskListNext(t *testing.T) {
	tl := NewTaskList(27, true)

	for i := 0; i < 27; i++ {
		n := tl.Next()
		t.Logf("tl.Next() = %d\n", n)
		if i != n {
			t.Errorf("task list Next() should be %d but was %d\n",
				i, n)
		}
	}

	n := tl.Next()
	if n != -1 {
		t.Errorf("task list Next() should be -1 but was %d\n",
			n)
	}

}

func TestTaskListIntervals(t *testing.T) {
	tl := NewTaskList(500, false)
	intervals := tl.Intervals()
	t.Logf("intervals: %v", intervals)
	if len(intervals) != 0 {
		t.Errorf("expected no intervals, but got %#v", intervals)
	}

	tl.Set(32, true)
	intervals = tl.Intervals()
	t.Logf("intervals: %v", intervals)
	if len(intervals) != 1 || intervals[0] != [2]int{32, 32} {
		t.Errorf("expected one interval of 32:32, but got %#v", intervals)
	}

	tl.Set(0, true)
	intervals = tl.Intervals()
	t.Logf("intervals: %v", intervals)
	if len(intervals) != 2 || intervals[0] != [2]int{0, 0} || intervals[1] != [2]int{32, 32} {
		t.Errorf("expected one interval of 32:32, but got %#v", intervals)
	}

	tl = NewTaskList(500, true)
	intervals = tl.Intervals()
	t.Logf("intervals: %v", intervals)
	if len(intervals) != 1 || intervals[0] != [2]int{0, 499} {
		t.Errorf("expected one interval of 0:499, but got %#v", intervals)
	}

	tl.Set(32, false)
	intervals = tl.Intervals()
	t.Logf("intervals: %v", intervals)
	if len(intervals) != 2 || intervals[0] != [2]int{0, 31} || intervals[1] != [2]int{33, 499} {
		t.Errorf("expected intervals 0:31 and 33:499, but got %#v", intervals)
	}

	tl.Set(32, false)
	intervals = tl.Intervals()
	t.Logf("intervals: %v", intervals)
	if len(intervals) != 2 || intervals[0] != [2]int{0, 31} || intervals[1] != [2]int{33, 499} {
		t.Errorf("expected intervals 0:31 and 33:499, but got %#v", intervals)
	}

	tl.Set(0, false)
	tl.Set(8, false)
	tl.Set(15, false)
	intervals = tl.Intervals()
	t.Logf("intervals: %v", intervals)
	if len(intervals) != 4 ||
		intervals[0] != [2]int{1, 7} ||
		intervals[1] != [2]int{9, 14} ||
		intervals[2] != [2]int{16, 31} ||
		intervals[3] != [2]int{33, 499} {
		t.Errorf("expected four intervals, but got %#v", intervals)
	}

	tl.Set(7, false)
	tl.Set(8, true)
	intervals = tl.Intervals()
	t.Logf("intervals: %v", intervals)
	if len(intervals) != 4 ||
		intervals[0] != [2]int{1, 6} ||
		intervals[1] != [2]int{8, 14} ||
		intervals[2] != [2]int{16, 31} ||
		intervals[3] != [2]int{33, 499} {
		t.Errorf("expected four intervals, but got %#v", intervals)
	}

	tl.Set(6, false)
	tl.Set(7, true)
	intervals = tl.Intervals()
	t.Logf("intervals: %v", intervals)
	if len(intervals) != 4 ||
		intervals[0] != [2]int{1, 5} ||
		intervals[1] != [2]int{7, 14} ||
		intervals[2] != [2]int{16, 31} ||
		intervals[3] != [2]int{33, 499} {
		t.Errorf("expected four intervals, but got %#v", intervals)
	}

	for i := 8; i <= 15; i++ {
		tl.Set(i, false)
	}
	intervals = tl.Intervals()
	t.Logf("intervals: %v", intervals)
	if len(intervals) != 4 ||
		intervals[0] != [2]int{1, 5} ||
		intervals[1] != [2]int{7, 7} ||
		intervals[2] != [2]int{16, 31} ||
		intervals[3] != [2]int{33, 499} {
		t.Errorf("expected four intervals, but got %#v", intervals)
	}

	for i := 16; i <= 26; i++ {
		tl.Set(i, false)
	}
	intervals = tl.Intervals()
	t.Logf("intervals: %v", intervals)
	if len(intervals) != 4 ||
		intervals[0] != [2]int{1, 5} ||
		intervals[1] != [2]int{7, 7} ||
		intervals[2] != [2]int{27, 31} ||
		intervals[3] != [2]int{33, 499} {
		t.Errorf("expected four intervals, but got %#v", intervals)
	}

}
