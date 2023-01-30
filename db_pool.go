package sqlkite

import (
	"sync/atomic"
	"time"

	"src.goblgobl.com/sqlite"
)

// Every project will have 1 DBPool with a configurable size
type DBPool struct {
	depleted uint64
	list     chan sqlite.Conn
}

func NewDBPool(count uint16, projectId string, setup func(conn sqlite.Conn) error) (*DBPool, error) {
	if count == 0 {
		// this is really bad, and though we guard against it in a number of
		// places, let's guard against here too. There is never a reason to
		// want this at 0
		count = 1
	}

	list := make(chan sqlite.Conn, count)
	p := &DBPool{list: list}
	for i := uint16(0); i < count; i++ {
		db, err := OpenDB(projectId, false)
		if err != nil {
			p.shutdown()
			return nil, err
		}
		list <- db

		if err := setup(db); err != nil {
			p.shutdown()
			return nil, err
		}
	}
	return p, nil
}

func (p *DBPool) Checkout() sqlite.Conn {
	return <-p.list
}

func (p *DBPool) CheckoutTimeout(duration time.Duration) (sqlite.Conn, bool) {
	select {
	case result := <-p.list:
		return result, true
	default:
		atomic.AddUint64(&p.depleted, 1)
		select {
		case result := <-p.list:
			return result, true
		case <-time.After(duration):
			return sqlite.Conn{}, false
		}
	}
}

func (p *DBPool) Len() int {
	return len(p.list)
}

func (p *DBPool) Depleted() uint64 {
	return atomic.SwapUint64(&p.depleted, 0)
}

func (p *DBPool) Release(db sqlite.Conn) {
	p.list <- db
}

// Closes all connections [which are currently in the pool]. Only meant
// to be called in specific instances where we know the pool is no longer
// being used
func (p *DBPool) shutdown() {
	for {
		select {
		case db := <-p.list:
			db.Close()
		default:
			close(p.list)
			return
		}
	}
}
