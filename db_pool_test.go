package sqlkite

import (
	"testing"
	"time"

	"src.goblgobl.com/sqlite"
	"src.goblgobl.com/tests/assert"
	"src.sqlkite.com/sqlkite/tests"
)

func Test_DBPool_New(t *testing.T) {
	called := 0
	pool, err := NewDBPool(2, tests.Factory.StandardId, func(conn sqlite.Conn) error {
		called += 1
		return nil
	})
	assert.Nil(t, err)
	assert.Equal(t, called, 2)
	assert.Equal(t, pool.Len(), 2)
}

func Test_DBPool_Checkout_And_Release(t *testing.T) {
	pool, err := NewDBPool(2, tests.Factory.StandardId, func(conn sqlite.Conn) error {
		return nil
	})
	assert.Nil(t, err)

	defer pool.shutdown()

	var n int
	db := pool.Checkout()
	assert.Nil(t, db.Row("select 1").Scan(&n))
	assert.Equal(t, n, 1)

	assert.Equal(t, pool.Len(), 1)
	pool.Release(db)
	assert.Equal(t, pool.Len(), 2)
}

func Test_DBPool_Checkout_Timeout(t *testing.T) {
	pool, err := NewDBPool(1, tests.Factory.StandardId, func(conn sqlite.Conn) error {
		return nil
	})
	assert.Nil(t, err)
	defer pool.shutdown()

	db, ok := pool.CheckoutTimeout(time.Millisecond)
	defer pool.Release(db)
	assert.True(t, ok)

	db, ok = pool.CheckoutTimeout(time.Millisecond)
	assert.False(t, ok)
}
