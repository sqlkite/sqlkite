package sqlkite

import (
	"sync/atomic"
	"time"

	"src.goblgobl.com/sqlite"
	"src.goblgobl.com/utils/log"
)

// Every project will have 1 DBPool with a configurable size
type DBPool struct {
	list     chan sqlite.Conn
	depleted uint64
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
			p.fastShutdown()
			return nil, err
		}
		list <- db

		if err := setup(db); err != nil {
			p.fastShutdown()
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

// Closes all connections. Will wait up to 60 seconds to all connections to be
// returned to the pool. Ideally this is called when there might still be some
// connections checked out of the pool, but these should be promptly returned
// and it should not be possible for new checkouts to happen.
func (p *DBPool) shutdown() {
	list := p.list
	l := cap(list)
	defer close(list)

	timeout := time.After(time.Second * 60)
	for i := 0; i < l; i++ {
		select {
		case db := <-list:
			db.Close()
		case <-timeout:
			log.Error("DBPool.shutdown").Int("i", i).Log()
			return
		}
	}
}

// Closes whatever connections happen to be in the pool when called, potentially
// abandoning already-checked out connections. Meant to be used from NewDBPool
// only (where we're sure no connections have been checked out yet).
func (p *DBPool) fastShutdown() {
	list := p.list
	for {
		select {
		case db := <-list:
			db.Close()
		default:
			return
		}
	}
}
