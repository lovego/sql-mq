package sqlmq

import "fmt"

func ExampleGetRetryWait() {
	fmt.Println(GetRetryWait(0))
	fmt.Println(GetRetryWait(1))
	fmt.Println(GetRetryWait(2))
	fmt.Println(GetRetryWait(3))
	fmt.Println(GetRetryWait(4))

	// Output:
	// 1s
	// 1m0s
	// 1h0m0s
	// 24h0m0s
	// 24h0m0s
}
