package test

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
// 解释：在channel close之后如果还有数据，是可以继续读取数据的，知道最后读取空值，即nil
// 所以可以使用两个参数接受， value, ok := <- chan，当ok为false，代表已经关闭了
// 或者可以用 for-range的形式消费channel，这样也是在close之后退出循环
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
			case data, ok := <-buffer:
				if !ok {
					return
				}
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
