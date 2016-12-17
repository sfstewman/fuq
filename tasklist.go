package fuq

type TaskList []byte

func NewTaskList(num int, fill bool) TaskList {
	ngrp := num / 8
	rem := num % 8
	if rem > 0 {
		ngrp++
	}

	tl := make(TaskList, ngrp, ngrp)

	if fill {
		for i := range tl {
			tl[i] = 0xff
		}

		if rem > 0 {
			tl[ngrp-1] = tl[ngrp-1] >> uint(8-rem)
		}
	}

	return tl
}

func (tl TaskList) Next() int {
	ind := 0
	for i, v := range tl {
		if v == 0 {
			ind += 8
			continue
		}

		m := byte(1)
		for v&m == 0 {
			m = m << 1
			ind++
		}
		tl[i] = v & ^m

		return ind
	}

	return -1
}

func (tl TaskList) Has(i int) bool {
	if i < 0 {
		return false
	}

	grp := i / 8
	if grp >= len(tl) {
		return false
	}

	itm := uint(i % 8)
	return tl[grp]&(1<<itm) != 0
}

func (tl TaskList) Set(i int, v bool) {
	if i < 0 {
		return
	}

	grp := i / 8
	if grp >= len(tl) {
		return
	}

	itm := uint(i % 8)
	if v {
		tl[grp] = tl[grp] | (1 << itm)
	} else {
		tl[grp] = tl[grp] & ^(1 << itm)
	}
}

func (tl TaskList) Intervals() [][2]int {
	i0 := -1
	i1 := -1

	intervals := make([][2]int, 0)

	for i, v := range tl {
		if v == 0 {
			continue
		}

		ind := 8 * i
		for v != 0 {
			if v&1 == 1 {
				if i0 == -1 {
					i0, i1 = ind, ind
				} else if ind != i1+1 {
					intervals = append(intervals, [2]int{i0, i1})
					i0 = ind
				}
				i1 = ind
			}
			v = v >> 1
			ind++
		}
	}

	if i0 != -1 {
		intervals = append(intervals, [2]int{i0, i1})
	}

	return intervals
}
