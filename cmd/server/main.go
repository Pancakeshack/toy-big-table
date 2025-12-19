package main

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"log"
	"math"
	"net"

	pb "github.com/pancakeshack/toy-big-table/proto"
	"google.golang.org/grpc"
)

const (
	port   = ":50051"
	dbPath = "./tmp/toy_bigtable_data"
)

type server struct {
	pb.UnimplementedTabletServerServer
	store *Store
}

func main() {
	fmt.Printf("Initializing Pebble DB at %s...\n", dbPath)
	store, err := NewStore(dbPath)
	if err != nil {
		log.Fatalf("Failed to open DB: %v", err)
	}
	defer store.Close()

	lis, err := net.Listen("tcp", port)
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	s := grpc.NewServer()
	pb.RegisterTabletServerServer(s, &server{store: store})

	fmt.Printf("Tablet Server listening on %s\n", port)
	if err := s.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}

func (s *server) MutateRow(ctx context.Context, req *pb.MutateRowRequest) (*pb.MutateRowResponse, error) {
	err := s.store.Apply(req.RowKey, req.Mutations)
	if err != nil {
		return nil, err
	}
	return &pb.MutateRowResponse{Success: true}, nil
}

func (s *server) ReadRows(req *pb.ReadRowsRequest, stream pb.TabletServer_ReadRowsServer) error {
	iter, err := s.store.NewIterator(req.StartRowKey, req.EndRowKey)
	if err != nil {
		return err
	}
	defer iter.Close()

	var currentRow *pb.Row

	for iter.First(); iter.Valid(); iter.Next() {
		rowKey, fam, qual, ts := parseKey(iter.Key())

		if currentRow != nil && currentRow.Key != rowKey {
			if err := stream.Send(&pb.ReadRowsResponse{Row: currentRow}); err != nil {
				return err
			}
			currentRow = nil
		}

		if currentRow == nil {
			currentRow = &pb.Row{Key: rowKey}
		}

		valStr := string(iter.Value())
		insertIntoRow(currentRow, fam, qual, ts, valStr)
	}

	if currentRow != nil {
		if err := stream.Send(&pb.ReadRowsResponse{Row: currentRow}); err != nil {
			return err
		}
	}

	return nil
}

func parseKey(fullKey []byte) (row string, fam string, qual string, ts int64) {
	dataLen := len(fullKey)
	if dataLen < 8 {
		return
	}

	tsBytes := fullKey[dataLen-8:]
	invTs := binary.BigEndian.Uint64(tsBytes)
	ts = math.MaxInt64 - int64(invTs)

	parts := bytes.Split(fullKey[:dataLen-9], []byte{0})

	if len(parts) >= 3 {
		row = string(parts[0])
		fam = string(parts[1])
		qual = string(parts[2])
	}
	return
}

func insertIntoRow(r *pb.Row, famName string, qual string, ts int64, val string) {
	var family *pb.Family
	for _, f := range r.Families {
		if f.Name == famName {
			family = f
			break
		}
	}
	if family == nil {
		family = &pb.Family{Name: famName}
		r.Families = append(r.Families, family)
	}

	var col *pb.Column
	for _, c := range family.Columns {
		if c.Qualifier == qual {
			col = c
			break
		}
	}
	if col == nil {
		col = &pb.Column{Qualifier: qual}
		family.Columns = append(family.Columns, col)
	}

	cell := &pb.Cell{
		TimestampMicros: ts,
		Value:           val,
	}
	col.Cells = append(col.Cells, cell)
}
