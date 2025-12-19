package main

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	pb "github.com/pancakeshack/toy-big-table/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const (
	totalRecords = 500_000
	workerCount  = 100
)

var (
	firstNames = []string{"Alice", "Bob", "Charlie", "David", "Eve", "Frank", "Grace", "Heidi", "Ivan", "Judy"}
	lastNames  = []string{"Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller", "Davis"}
	domains    = []string{"gmail.com", "yahoo.com", "outlook.com", "example.org", "tech.io"}
	roles      = []string{"admin", "editor", "viewer", "guest", "banned"}
)

func main() {
	conn, err := grpc.NewClient("localhost:50051", grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("did not create client: %v", err)
	}
	defer conn.Close()

	client := pb.NewTabletServerClient(conn)

	jobs := make(chan int, totalRecords)
	var wg sync.WaitGroup
	var opsProcessed uint64

	start := time.Now()
	fmt.Printf("Starting load test: Inserting %d rows with %d workers...\n", totalRecords, workerCount)

	for w := 0; w < workerCount; w++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for id := range jobs {
				insertRandomRow(client, id)

				current := atomic.AddUint64(&opsProcessed, 1)
				if current%10_000 == 0 {
					fmt.Printf("Processed %d / %d records (%.2f%%)\n", current, totalRecords, float64(current)/float64(totalRecords)*100)
				}
			}
		}(w)
	}

	for i := 1; i <= totalRecords; i++ {
		jobs <- i
	}
	close(jobs)

	wg.Wait()
	duration := time.Since(start)

	fmt.Println("\n--- LOAD TEST COMPLETE ---")
	fmt.Printf("Total Time: %v\n", duration)
	fmt.Printf("Throughput: %.2f req/sec\n", float64(totalRecords)/duration.Seconds())
}

func insertRandomRow(client pb.TabletServerClient, id int) {
	fName := firstNames[rand.Intn(len(firstNames))]
	lName := lastNames[rand.Intn(len(lastNames))]
	fullName := fmt.Sprintf("%s %s", fName, lName)
	email := fmt.Sprintf("%s.%s@%s", fName, lName, domains[rand.Intn(len(domains))])
	role := roles[rand.Intn(len(roles))]

	rowKey := fmt.Sprintf("user#%06d", id)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	mutations := []*pb.Mutation{
		{
			Mutation: &pb.Mutation_SetCell{
				SetCell: &pb.SetCell{
					FamilyName:      "profile",
					ColumnQualifier: "name",
					TimestampMicros: time.Now().UnixMicro(),
					Value:           fullName,
				},
			},
		},
		{
			Mutation: &pb.Mutation_SetCell{
				SetCell: &pb.SetCell{
					FamilyName:      "profile",
					ColumnQualifier: "email",
					TimestampMicros: time.Now().UnixMicro(),
					Value:           email,
				},
			},
		},
		{
			Mutation: &pb.Mutation_SetCell{
				SetCell: &pb.SetCell{
					FamilyName:      "perms",
					ColumnQualifier: "role",
					TimestampMicros: time.Now().UnixMicro(),
					Value:           role,
				},
			},
		},
	}

	_, err := client.MutateRow(ctx, &pb.MutateRowRequest{
		TableName: "users",
		RowKey:    rowKey,
		Mutations: mutations,
	})
	if err != nil {
		log.Printf("Error inserting %s: %v", rowKey, err)
	}
}
