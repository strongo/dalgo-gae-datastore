package gaedb

import (
	"context"
	"fmt"
	"github.com/pkg/errors"
	"github.com/strongo/dalgo"
	"github.com/strongo/log"
	"google.golang.org/appengine/datastore"
	"strconv"
)

type gaeDatabase struct {
}

func (gaeDb gaeDatabase) Set(ctx context.Context, record dalgo.Record) error {
	panic("implement me")
}

func (gaeDb gaeDatabase) SetMulti(ctx context.Context, records []dalgo.Record) error {
	panic("implement me")
}

func (gaeDb gaeDatabase) Upsert(c context.Context, record dalgo.Record) error {
	panic("implement me")
}

// NewDatabase create database provider to Google Datastore
func NewDatabase() dalgo.Database {
	return gaeDatabase{}
}

func (gaeDatabase) Get(c context.Context, record dalgo.Record) (err error) {
	if record == nil {
		panic("record == nil")
	}
	key, isIncomplete, err := getDatastoreKey(c, record.Key())
	if err != nil {
		return
	}
	if isIncomplete {
		panic("can't get record by incomplete key")
	}
	entity := record.Data()
	if err = Get(c, key, entity); err != nil {
		if err == datastore.ErrNoSuchEntity {
			err = dalgo.NewErrNotFoundByKey(record, err)
		}
		return
	}
	return
}

func (gaeDatabase) Delete(c context.Context, recordKey dalgo.RecordKey) (err error) {
	if recordKey == nil {
		panic("recordKey == nil")
	}
	key, isIncomplete, err := getDatastoreKey(c, recordKey)
	if err != nil {
		return
	}
	if isIncomplete {
		panic("can't delete record by incomplete key")
	}
	if err = Delete(c, key); err != nil {
		return
	}
	return
}

func (gaeDatabase) DeleteMulti(c context.Context, recordKeys []dalgo.RecordKey) (err error) {
	if len(recordKeys) == 0 {
		return
	}
	keys := make([]*datastore.Key, len(recordKeys))
	for i, recordKey := range recordKeys {
		key, isIncomplete, err := getDatastoreKey(c, recordKey)
		if err != nil {
			return errors.WithMessage(err, "i="+strconv.Itoa(i))
		}
		if isIncomplete {
			panic("can't delete record by incomplete key, i=" + strconv.Itoa(i))
		}
		keys[i] = key
	}
	if err = DeleteMulti(c, keys); err != nil {
		return
	}
	return
}

func (gaeDb gaeDatabase) Insert(c context.Context, record dalgo.Record, options dalgo.InsertOptions) (err error) {
	if record == nil {
		panic("record == nil")
	}
	recordKey := record.Key()
	kind := dalgo.GetRecordKind(recordKey)
	log.Debugf(c, "Insert(kind=%v)", kind)
	data := record.Data()
	if data == nil {
		panic("data == nil")
	}
	if generateID := options.IDGenerator(); generateID != nil {
		exists := func(key dalgo.RecordKey) error {
			return gaeDb.exists(c, recordKey)
		}
		insert := func(record dalgo.Record) error {
			return gaeDb.insert(c, record)
		}
		return dalgo.InsertWithRandomID(c, record, generateID, 5, exists, insert)
	}
	return
}

func (gaeDb gaeDatabase) insert(c context.Context, record dalgo.Record) (err error) {
	if record == nil {
		panic("record == nil")
	}
	recordKey := record.Key()
	kind := dalgo.GetRecordKind(recordKey)
	log.Debugf(c, "InsertWithRandomIntID(kind=%v)", kind)
	entity := record.Data()
	if entity == nil {
		panic("record == nil")
	}

	wrapErr := func(err error) error {
		return errors.WithMessage(err, "failed to create record with random str ID for: "+kind)
	}
	key, isIncomplete, err := getDatastoreKey(c, recordKey)
	if err != nil {
		return wrapErr(err)
	}
	if isIncomplete {
		panic(fmt.Sprintf("gaeDatabase.insert() called for key with incomplete ID: %+v", key))
	}

	key, err = Put(c, key, record.Data())
	return err
}

func (gaeDb gaeDatabase) exists(c context.Context, recordKey dalgo.RecordKey) error {
	return gaeDb.Get(c, dalgo.NewRecord(recordKey, dalgo.VoidData()))
}

func (gaeDb gaeDatabase) Update(c context.Context, record dalgo.Record) error {
	data := record.Data()
	log.Debugf(c, "data: %+v", data)
	if data == nil {
		panic("record.Data() == nil")
	}
	if key, isIncomplete, err := getDatastoreKey(c, record.Key()); err != nil {
		return err
	} else if isIncomplete {
		log.Errorf(c, "gaeDatabase.Update() called for incomplete key, will insert.")
		return gaeDb.Insert(c, record, dalgo.NewInsertOptions(dalgo.WithRandomStringID(5)))
	} else if _, err = Put(c, key, data); err != nil {
		return errors.WithMessage(err, "failed to update "+key2str(key))
	}
	return nil
}

func setRecordID(key *datastore.Key, record dalgo.Record) {
	recordKey := record.Key()
	if intID := key.IntID(); intID != 0 {
		recordKey[0].ID = intID
	} else if strID := key.StringID(); strID != "" {
		recordKey[0].ID = strID
	}
}

// ErrKeyHasBothIds indicates record has both string and int ids
var ErrKeyHasBothIds = errors.New("record has both string and int ids")

// ErrEmptyKind indicates record holder returned empty kind
var ErrEmptyKind = errors.New("record holder returned empty kind")

func getDatastoreKey(c context.Context, recordKey dalgo.RecordKey) (key *datastore.Key, isIncomplete bool, err error) {
	if recordKey == nil {
		panic(recordKey == nil)
	}
	if len(recordKey) == 0 {
		panic("len(recordKey) == 0")
	}
	ref := recordKey[0]
	if ref.Kind == "" {
		err = ErrEmptyKind
	} else {
		if ref.ID == nil {
			key = NewIncompleteKey(c, ref.Kind, nil)
		} else {
			switch v := ref.ID.(type) {
			case string:
				key = NewKey(c, ref.Kind, v, 0, nil)
			case int:
				key = NewKey(c, ref.Kind, "", (int64)(v), nil)
			default:
				err = fmt.Errorf("unsupported ID type: %T", ref.ID)
			}
		}
	}
	return
}

func (gaeDatabase) UpdateMulti(c context.Context, records []dalgo.Record) (err error) { // TODO: Rename to PutMulti?

	keys := make([]*datastore.Key, len(records))
	vals := make([]dalgo.Validatable, len(records))

	insertedIndexes := make([]int, 0, len(records))

	for i, record := range records {
		if record == nil {
			panic(fmt.Sprintf("records[%v] is nil: %v", i, record))
		}
		isIncomplete := false
		if keys[i], isIncomplete, err = getDatastoreKey(c, record.Key()); err != nil {
			return
		} else if isIncomplete {
			insertedIndexes = append(insertedIndexes, i)
		}
		if vals[i] = record.Data(); vals[i] == nil {
			return fmt.Errorf("records[%d].Data() == nil", i)
		}
	}

	// logKeys(c, "gaeDatabase.UpdateMulti", keys)

	if keys, err = PutMulti(c, keys, vals); err != nil {
		return
	}

	for _, i := range insertedIndexes {
		setRecordID(keys[i], records[i])
		records[i].SetData(vals[i]) // it seems useless but covers case when .Data() returned newly created object without storing inside record
	}
	return
}

func (gaeDatabase) GetMulti(c context.Context, records []dalgo.Record) error {
	count := len(records)
	keys := make([]*datastore.Key, count)
	vals := make([]dalgo.Validatable, count)
	for i := range records {
		record := records[i]
		recordKey := record.Key()
		kind := recordKey[0].Kind
		var intID int64
		var strID string
		switch v := recordKey[0].ID.(type) {
		case string:
			strID = v
		case int:
			intID = (int64)(v)
		}
		keys[i] = NewKey(c, kind, strID, intID, nil)
		vals[i] = record.Data()
	}
	if err := GetMulti(c, keys, vals); err != nil {
		return err
	}
	for i := range vals {
		records[i].SetData(vals[i])
	}
	return nil
}

var xgTransaction = &datastore.TransactionOptions{XG: true}

var isInTransactionFlag = "is in transaction"
var nonTransactionalContextKey = "non transactional context"

func (gaeDatabase) RunInTransaction(ctx context.Context, f func(ctx context.Context, tx dalgo.Transaction) error, options ...dalgo.TransactionOption) error {
	txOptions := dalgo.NewTransactionOptions(options...)
	var to *datastore.TransactionOptions
	if txOptions.IsCrossGroup() {
		to = xgTransaction
	}
	return RunInTransaction(ctx, f, to)
}

func (gaeDatabase) IsInTransaction(c context.Context) bool {
	if v := c.Value(&isInTransactionFlag); v != nil && v.(bool) {
		return true
	}
	return false
}

func (gaeDatabase) NonTransactionalContext(tc context.Context) context.Context {
	if c := tc.Value(&nonTransactionalContextKey); c != nil {
		return c.(context.Context)
	}
	return tc
}
