package writer

import (
	"context"
	"encoding/json"
	"fmt"

	sdk "github.com/conduitio/conduit-connector-sdk"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type Writer struct {
	db        *mongo.Collection
	table     string
	updateOpt *options.UpdateOptions
}

// Params is an incoming params for the NewWriter function.
type Params struct {
	DB    *mongo.Collection
	Table string
}

// NewWriter creates new instance of the Writer.
func NewWriter(ctx context.Context, params Params) (*Writer, error) {
	writer := &Writer{
		db:        params.DB,
		table:     params.Table,
		updateOpt: options.Update().SetUpsert(true),
	}

	return writer, nil
}

// InsertRecord inserts a sdk.Record into a Destination.
func (w *Writer) InsertRecord(ctx context.Context, record sdk.Record) error {
	return sdk.Util.Destination.Route(ctx,
		record,
		w.insert,
		w.update,
		w.delete,
		w.insert,
	)
}

func (w *Writer) Close(ctx context.Context) error {
	return w.db.Database().Client().Disconnect(ctx)
}

const (
	idFieldName = "_id"
	setCommand  = "$set"
)

func (w *Writer) insert(ctx context.Context, record sdk.Record) error {
	structuredData := make(sdk.StructuredData)
	if err := json.Unmarshal(record.Payload.After.Bytes(), &structuredData); err != nil {
		return fmt.Errorf("unmarshal data into structured data: %w", err)
	}

	id, data := parseData(structuredData)
	//if id exists, we should return it into map, because we need it as field, not filter
	if id != nil {
		data[idFieldName] = id
	}

	if _, err := w.db.InsertOne(ctx, structuredData); err != nil {
		return fmt.Errorf("insert data into destination: %w", err)
	}

	return nil
}

func (w *Writer) update(ctx context.Context, record sdk.Record) error {
	structuredData := make(sdk.StructuredData)
	if err := json.Unmarshal(record.Payload.After.Bytes(), &structuredData); err != nil {
		return fmt.Errorf("unmarshal data into structured data: %w", err)
	}

	id, data := parseData(structuredData)
	body := generateBsonFromMap(data)

	filter := bson.D{{idFieldName, id}}

	if _, err := w.db.UpdateOne(ctx,
		filter,
		bson.D{{setCommand, body}},
		w.updateOpt,
	); err != nil {
		return fmt.Errorf("upsert data into destination: %w", err)
	}

	return nil
}

func (w *Writer) delete(ctx context.Context, record sdk.Record) error {
	structuredData := make(sdk.StructuredData)
	if err := json.Unmarshal(record.Key.Bytes(), &structuredData); err != nil {
		return fmt.Errorf("unmarshal data into structured data: %w", err)
	}

	//no need to get body, we will delete this record by _id field
	id, _ := parseData(structuredData)

	filter := bson.D{{idFieldName, id}}

	if _, err := w.db.DeleteOne(ctx,
		filter,
	); err != nil {
		return fmt.Errorf("upsert data into destination: %w", err)
	}

	return nil
}

// parseData is checking for _id field in data. If it exists, it splits it for upsert, otherwise leaving raw
func parseData(data sdk.StructuredData) (any, map[string]any) {
	rawID, exist := data[idFieldName]
	if !exist {
		//if no id set, using all fields as data
		return nil, data
	}

	//deleting id from data
	delete(data, idFieldName)

	//trying to assert id into string
	idStr, ok := rawID.(string)
	if ok {
		//if it's string, we could try to parse it into ObjectID
		hex, err := primitive.ObjectIDFromHex(idStr)
		if err == nil {
			return hex, data
		}
	}

	//if id is not an ObjectID, just split it
	return rawID, data
}

// generateBsonFromMap generates bson.D object from map[string]any data
func generateBsonFromMap(m map[string]any) bson.D {
	var res = make(bson.D, 0, len(m))
	for i, v := range m {
		res = append(res, bson.E{
			Key:   i,
			Value: v,
		})
	}
	return res
}
