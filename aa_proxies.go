package gaedb

import (
	"github.com/strongo/db/mockdb"
	"google.golang.org/appengine/datastore"
)

var (
	// LoggingEnabled a flag to enable or disable logging inside GAE DAL
	LoggingEnabled = true // TODO: move to Context.WithValue()
	mockDB         *mockdb.MockDB

	// NewIncompleteKey creates new incomplete key.
	NewIncompleteKey = datastore.NewIncompleteKey

	// NewKey creates new key.
	NewKey = datastore.NewKey

	dbRunInTransaction = datastore.RunInTransaction
	dbGet              = datastore.Get
	dbGetMulti         = datastore.GetMulti
	dbPut              = datastore.Put
	dbPutMulti         = datastore.PutMulti
	dbDelete           = datastore.Delete
	dbDeleteMulti      = datastore.DeleteMulti
)
