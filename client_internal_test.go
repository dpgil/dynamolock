/*
Copyright 2019 github.com/ucirello & cirello.io

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package dynamolock

import (
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbiface"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
)

type mockDynamoDBClient struct {
	dynamodbiface.DynamoDBAPI
}
//func (m *mockDynamoDBClient) PutItem(input *dynamodb.PutItemInput) (*dynamodb.PutItemOutput, error) {
//	// TODO: Implement mock
//	return &dynamodb.PutItemOutput{}, nil
//}
//func (m *mockDynamoDBClient) GetItem(input *dynamodb.GetItemInput) (*dynamodb.GetItemOutput, error) {
//	// TODO: Implement mock
//	return &dynamodb.GetItemOutput{}, nil
//}
//func (m *mockDynamoDBClient) DeleteItem(input *dynamodb.DeleteItemInput) (*dynamodb.DeleteItemOutput, error) {
//	// TODO: Implement mock
//	return &dynamodb.DeleteItemOutput{}, nil
//}

func TestCloseRace(t *testing.T) {
	mockSvc := &mockDynamoDBClient{}

	// TODO: Most of the input into New isn't relevant since we're mocking
	lockClient, err := New(mockSvc, "locksCloseRace",
		WithLeaseDuration(3*time.Second),
		WithHeartbeatPeriod(100*time.Millisecond),
		WithOwnerName("CloseRace"),
		WithPartitionKeyName("key"),
	)
	if err != nil {
		t.Fatal(err)
	}

	var wg sync.WaitGroup
	n := runtime.NumCPU()

	wg.Add(1)
	go func() {
		defer wg.Done()
		// TODO: Create a ton of goroutines that acquire a lock
		for i := 0; i < n; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				lockClient.AcquireLock(string(i))
			}()
		}
	}()

	// TODO: Close the lock client
	wg.Add(1)
	go func() {
		defer wg.Done()
		lockClient.Close()
	}()

	// TODO: Check for any leaked locks
	wg.Wait()

	for i := 0; i < n; i++ {
		l, _ := lockClient.Get(string(i))
		if l != nil {
			t.Fatal("Leaked lock")
		}
	}
}

func TestBadCreateLockItem(t *testing.T) {
	c := &Client{}
	_, err := c.createLockItem(getLockOptions{}, map[string]*dynamodb.AttributeValue{
		attrLeaseDuration: &dynamodb.AttributeValue{S: aws.String("bad duration")},
	})
	if err == nil {
		t.Fatal("bad duration should prevent the creation of the lock")
	}
}
