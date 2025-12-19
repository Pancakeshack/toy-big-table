package main

import (
	"encoding/binary"
	"math"

	"github.com/cockroachdb/pebble"
	pb "github.com/pancakeshack/toy-big-table/proto"
)

const (
	sep = "\x00"
)

type Store struct {
	db *pebble.DB
}

func NewStore(dir string) (*Store, error) {
	db, err := pebble.Open(dir, &pebble.Options{})
	if err != nil {
		return nil, err
	}
	return &Store{db: db}, nil
}

func (s *Store) Close() error {
	return s.db.Close()
}

func EncodeKey(row string, family string, qualifier string, ts int64) []byte {
	invTs := math.MaxInt64 - ts
	tsBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(tsBytes, uint64(invTs))

	key := make([]byte, 0)
	key = append(key, []byte(row)...)
	key = append(key, sep...)
	key = append(key, []byte(family)...)
	key = append(key, sep...)
	key = append(key, []byte(qualifier)...)
	key = append(key, sep...)
	key = append(key, tsBytes...)

	return key
}

func (s *Store) Apply(rowKey string, mutations []*pb.Mutation) error {
	batch := s.db.NewBatch()
	defer batch.Close()

	for _, m := range mutations {
		switch op := m.Mutation.(type) {

		case *pb.Mutation_SetCell:
			sc := op.SetCell
			key := EncodeKey(rowKey, sc.FamilyName, sc.ColumnQualifier, sc.TimestampMicros)
			if err := batch.Set(key, []byte(sc.Value), pebble.Sync); err != nil {
				return err
			}

		case *pb.Mutation_DeleteFromColumn:
			dfc := op.DeleteFromColumn
			minKey := EncodeKey(rowKey, dfc.FamilyName, dfc.ColumnQualifier, math.MaxInt64)
			maxKey := EncodeKey(rowKey, dfc.FamilyName, dfc.ColumnQualifier, 0)
			maxKey = append(maxKey, 0xFF)

			if err := batch.DeleteRange(minKey, maxKey, pebble.Sync); err != nil {
				return err
			}

		case *pb.Mutation_DeleteFromRow:
			startKey := []byte(rowKey)
			endKey := append([]byte(rowKey), 0xFF)
			if err := batch.DeleteRange(startKey, endKey, pebble.Sync); err != nil {
				return err
			}
		}
	}

	return batch.Commit(pebble.Sync)
}

func (s *Store) NewIterator(startRow, endRow string) (*pebble.Iterator, error) {
	o := &pebble.IterOptions{
		LowerBound: []byte(startRow),
		UpperBound: []byte(endRow),
	}
	return s.db.NewIter(o)
}
