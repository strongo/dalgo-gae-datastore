package gaedb

import (
	"context"
	"github.com/pkg/errors"
	"github.com/strongo/dalgo"
	"google.golang.org/appengine/datastore"
	"testing"
)

func TestNewDatabase(t *testing.T) {
	v := NewDatabase()
	if v == nil {
		t.Errorf("v == nil")
	}
	switch v.(type) {
	case gaeDatabase: // OK
	default:
		t.Errorf("unexpected DB type: %T", v)
	}
}

func TestDatabase_RunInTransaction(t *testing.T) {
	dbInstance := gaeDatabase{}
	i, j := 0, 0

	var xg bool

	RunInTransaction = func(c context.Context, f func(c context.Context, tx dalgo.Transaction) error, opts *datastore.TransactionOptions) error {
		if opts == nil {
			if xg {
				t.Errorf("Expected XG==%v", xg)
			}
		} else if opts.XG != xg {
			t.Errorf("Expected XG==%v, got: %v", xg, opts.XG)
		}
		j++
		return f(c, nil)
	}

	t.Run("xg=true", func(t *testing.T) {
		xg = true
		err := dbInstance.RunInTransaction(context.Background(), func(c context.Context, tx dalgo.Transaction) error {
			i++
			return nil
		}, dalgo.WithCrossGroup())

		if err != nil {
			t.Errorf("Got unexpected error: %v", err)
		}

		if i != 1 {
			t.Errorf("Expected 1 exection, got: %d", i)
		}
		if j != 1 {
			t.Errorf("Expected 1 exection, got: %d", i)
		}
	})

	t.Run("xg=false", func(t *testing.T) {
		i, j = 0, 0
		xg = false
		err := dbInstance.RunInTransaction(context.Background(), func(c context.Context, tx dalgo.Transaction) error {
			i++
			return errors.New("Test1")
		})

		if err == nil {
			t.Error("Expected error, got nil")
		} else if err.Error() != "Test1" {
			t.Errorf("Got unexpected error: %v", err)
		}

		if i != 1 {
			t.Errorf("Expected 1 exection, got: %d", i)
		}
		if j != 1 {
			t.Errorf("Expected 1 exection, got: %d", i)
		}
	})

}
