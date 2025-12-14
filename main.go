package oddish

import (
	"context"
	"fmt"
	"log"
	"reflect"
	"sync"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// Iter includes a MongoDB cursor and a mutex for thread-safe.
type Iter struct {
	m   sync.Mutex
	cur *mongo.Cursor
	err error
}

// NewIter initializes and returns a new Iter instance.
func NewIter(cur *mongo.Cursor, err error) *Iter {
	return &Iter{
		cur: cur,
		err: err,
	}
}

// Next advances the cursor to the next document
// and decodes it into the provided value.
func (iter *Iter) Next(T any) bool {
	// Lock the mutex to ensure safe concurrent access to the cursor.
	iter.m.Lock()

	// Ensure the mutex is unlocked when the function returns.
	defer iter.m.Unlock()

	// Get the next document for the cursor
	if iter.cur.Next(context.TODO()) {
		// Decode the current document into the provided value T.
		err := iter.cur.Decode(T)
		if err != nil {
			log.Println(err)
			return false
		}
	} else {
		// If there are no more documents to read then return false.
		return false
	}

	// Return true if the document was sucessfully retrieved and decoded.
	return true
}

func Aggregate(col *mongo.Collection, pipe []bson.D, T any) error {
	cur, err := col.Aggregate(context.TODO(), pipe)
	iter := NewIter(cur, err)
	if err != nil {
		log.Println(err)
		return err
	}

	resultv := reflect.ValueOf(T)
	if resultv.Kind() != reflect.Ptr || resultv.Elem().Kind() != reflect.Slice {
		panic("Result argument must be a slice address")
	}
	slicev := resultv.Elem()
	slicev = slicev.Slice(0, slicev.Cap())
	elemt := slicev.Type().Elem()
	i := 0

	for {
		if slicev.Len() == i {
			elemp := reflect.New(elemt)
			if !iter.Next(elemp.Interface()) {
				break
			}
			slicev = reflect.Append(slicev, elemp.Elem())
			slicev = slicev.Slice(0, slicev.Cap())
		} else {
			if !iter.Next(slicev.Index(i).Addr().Interface()) {
				break
			}
		}
		i++
	}

	resultv.Elem().Set(slicev.Slice(0, i))
	return cur.Close(context.TODO())
}

func Count(col *mongo.Collection, filter bson.D) (int64, error) {
	return col.CountDocuments(context.TODO(), filter)
}

// DeleteID deletes a document from the specified MongoDB collection
// based on the provided id.
func DeleteID(col *mongo.Collection, id any) error {
	// Variable to hold the ObjectID to be deleted.
	var objID primitive.ObjectID

	// Use a type switch to determine the actual type of id
	switch value := id.(type) {
	case string:
		objID = ObjID(value)
	case primitive.ObjectID:
		objID = value
	default:
		return fmt.Errorf("unsupported id type: %T", id)
	}

	_, err := col.DeleteOne(context.TODO(), bson.D{primitive.E{Key: "_id", Value: objID}})
	return err
}

// Insert adds a new document to the specified MongoDB collection
// from the provided generic value T.
func Insert(col *mongo.Collection, T any) error {
	_, err := col.InsertOne(context.TODO(), T)
	if err != nil {
		log.Println(err)
	}
	return err
}

func FindID(col *mongo.Collection, id any) *mongo.SingleResult {
	var objID primitive.ObjectID

	switch value := id.(type) {
	case string:
		objID = ObjID(value)
	case primitive.ObjectID:
		objID = value
	case nil:
		return nil
	}
	return col.FindOne(context.TODO(), bson.D{primitive.E{Key: "_id", Value: objID}})
}

// FindMany retrives multiple documents from the specified MongoDB collection
// that match the provided filter and sorts them. The results are stored
// in the provided slice RESULTS. The function returns an error
// if the retrieval fails or if the parameter RESULTS
// is not a pointer to a a slice.
func FindMany(col *mongo.Collection, RESULTS any, filter bson.D, sorts bson.D) error {
	// Create a new find options object
	opts := options.Find()

	// If sorts are provided, set the sort options.
	if len(sorts) > 0 {
		opts.SetSort(sorts)
	}

	// Execute the find query with the given filter and options
	cur, err := col.Find(context.TODO(), filter, opts)
	if err != nil {
		log.Println(err)
		return err
	}

	// Initialize a new iterator with the cursor
	iter := NewIter(cur, err)

	// Use reflection to check if RESULTS is a pointer to a slice
	resultv := reflect.ValueOf(RESULTS)
	// Check the T is a slice address
	if resultv.Kind() != reflect.Ptr || resultv.Elem().Kind() != reflect.Slice {
		panic("Result argument must be a slice address")
	}

	// Initialize the slice value and element type
	slicev := resultv.Elem()
	slicev = slicev.Slice(0, slicev.Cap())
	elemt := slicev.Type().Elem()
	i := 0

	// Interate over the cursor and append results to the slice
	for {
		// Check if the current slice length equals the index
		if slicev.Len() == i {
			// Create a new element pointer
			elemp := reflect.New(elemt)

			// Decode the nexr document into the element pointer
			if !iter.Next(elemp.Interface()) {
				break
			}

			// Append the element to the slice
			slicev = reflect.Append(slicev, elemp.Elem())
			slicev = slicev.Slice(0, slicev.Cap())
		} else {
			// Decode the next document into the existing slice element
			if !iter.Next(slicev.Index(i).Addr().Interface()) {
				break
			}
		}
		i++
	}

	// Set the final slice with the results
	resultv.Elem().Set(slicev.Slice(0, i))

	// Close the cursor and return any error encountered during closing
	return cur.Close(context.TODO())
}

// FindOne retrieves a single document from the specified MongoDB Collection
// that matches the provided filter.
func FindOne(col *mongo.Collection, filter any) *mongo.SingleResult {
	return col.FindOne(context.TODO(), filter)
}

// Return total pages
func FindPage(col *mongo.Collection, T any, filter bson.D, page, limit int, sorts bson.D) (int, error) {
	_page := int64(page)
	_limit := int64(limit)

	opts := options.Find()

	if len(sorts) > 0 {
		opts.SetSort(sorts)
	}

	if _page <= 0 {
		_page = 1
	}

	cnt, err := Count(col, filter)
	if err != nil {
		return 0, err
	}

	totalPages := cnt / _limit
	if cnt%_limit > 0 {
		totalPages += 1
	}
	if totalPages == 0 {
		totalPages = 1
	}

	opts.SetSkip(int64((_page - 1) * _limit)).SetLimit(_limit)

	cur, err := col.Find(context.TODO(), filter, opts)
	if err != nil {
		log.Println(err)
		return 0, err
	}
	iter := NewIter(cur, err)

	resultv := reflect.ValueOf(T)
	if resultv.Kind() != reflect.Ptr || resultv.Elem().Kind() != reflect.Slice {
		panic("Result argument must be a slice address")
	}
	slicev := resultv.Elem()
	slicev = slicev.Slice(0, slicev.Cap())
	elemt := slicev.Type().Elem()
	i := 0

	for {
		if slicev.Len() == i {
			elemp := reflect.New(elemt)
			if !iter.Next(elemp.Interface()) {
				break
			}
			slicev = reflect.Append(slicev, elemp.Elem())
			slicev = slicev.Slice(0, slicev.Cap())
		} else {
			if !iter.Next(slicev.Index(i).Addr().Interface()) {
				break
			}
		}
		i++
	}

	resultv.Elem().Set(slicev.Slice(0, i))

	cur.Close(context.TODO())

	return int(totalPages), nil
}

func Update(col *mongo.Collection, id any, upd any) error {
	var objID primitive.ObjectID

	switch id.(type) {
	case string:
		objID = ObjID(fmt.Sprintf("%v", id))
	case primitive.ObjectID:
		objID = id.(primitive.ObjectID)
	default:
		return nil
	}

	_, err := col.UpdateOne(context.TODO(), bson.D{primitive.E{Key: "_id", Value: objID}}, upd)
	return err
}

// ObjID converts a hexadecimal string to a MongoDB ObjectID
func ObjID(hex string) primitive.ObjectID {
	// Attempt to convert the hex string to an ObjectID
	objID, err := primitive.ObjectIDFromHex(hex)
	if err != nil {
		return primitive.NilObjectID
	}
	return objID
}
