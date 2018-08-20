package main

import (
	"io/ioutil"
	"log"
	"net/http"
	"net/http/cookiejar"
)

var cookieJar, _ = cookiejar.New(nil)

func login() {

}

func main() {
	originURL := "https://time.geekbang.org/column/intro/48"

	client := &http.Client{
		Jar: cookieJar,
	}

	request, err := http.NewRequest("GET", originURL, nil)
	resp, err := client.Do(request)
	if err != nil {
		log.Fatalln("Access remote address failed")
		return
	}

	defer resp.Body.Close()
	entity, err := ioutil.ReadAll(resp.Body)
	body := string(entity)
	log.Printf("Response body: %s", body)
	for _, cookie := range resp.Cookies() {
		log.Println("cookie: ", cookie)
	}
}
