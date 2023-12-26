package main

import (
	"SIMPLEGO/pkg/newbie"
	"fmt"
)

func main() {
	db, err := newbie.Init(20)

	if err != nil{
		fmt.Printf("error while initializing DB")
	}

	db.Set("name",[]byte("abhishek"))

	value, err := db.Get("name")
	if err != nil{
		panic(err)
	}

	fmt.Println("value for key name ",string(value))

}
