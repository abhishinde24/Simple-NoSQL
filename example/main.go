package main

import (
	"SIMPLEGO/pkg/newbie"
	"fmt"
)

func main() {
	db, err := newbie.Init(3)

	if err != nil{
		fmt.Printf("error while initializing DB")
	}

	db.Set("name",[]byte("abhishek"))
	db.Set("a1",[]byte("A1"))
	db.Set("a1",[]byte("Z1"))
	db.Set("b1",[]byte("B1"))
	db.Set("c1",[]byte("C1"))
	db.Set("d1",[]byte("D1"))

	value, err := db.Get("a1")
	if err != nil{
		panic(err)
	}
	fmt.Printf("value for key %s value %s \n","a1",string(value))

	err = db.Delete("a1")
	if err != nil{
		panic(err)
	}
	value, err = db.Get("a1")
	if value == nil{
		fmt.Printf("key %s is deleted \n","a1")
	}

}
