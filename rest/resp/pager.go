package resp

// Pager represents a response object of pagination
type Pager struct {
	Skip  int `json:"skip"`
	Limit int `json:"limit"`
	Total int `json:"total,omitempty"`
}

// NewPager returns a new Pager
func NewPager(n, skip, limit int) *Pager {
	p := Pager{
		Skip:  skip,
		Limit: limit,
		Total: n,
	}
	if p.Skip >= p.Total {
		p.Limit = 0
		return &p
	}
	if p.Total-p.Skip < p.Limit {
		p.Limit = p.Total - p.Skip
	}
	return &p
}
