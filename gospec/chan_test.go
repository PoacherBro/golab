package gospec_test

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

type User struct {
	ID   int64
	Name string
}

// 为了测试在关闭chan之后，仍然会出现nil pointer dereference panic
func TestSendPointer(t *testing.T) {
	buffer := make(chan *User, 3)
	closeChan := make(chan bool)

	var wg sync.WaitGroup

	// consumer
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			fmt.Println("new loop...")
			select {
			case <-closeChan:
				fmt.Println("consumer received quit")
				return
			case data := <-buffer:
				time.Sleep(time.Second)
				fmt.Println(fmt.Sprintf("%d-%s", data.ID, data.Name)) // panic here
				//			default:
				//				fmt.Println("No data")
			}
		}
	}()

	// producer
	go func() {
		for i := int64(0); i < int64(3); i++ {
			user := &User{
				ID:   i,
				Name: fmt.Sprintf("Leo%d", i),
			}
			buffer <- user
			fmt.Println(fmt.Sprintf("Producer: data[%d]", i))
		}
		closeChan <- true
	}()

	<-closeChan
	close(buffer)
	fmt.Println("closed buffer")
	wg.Wait()

	close(closeChan)
	fmt.Println("closed closeChan")
}
