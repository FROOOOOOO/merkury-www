package client

import (
	"crypto/tls"
	"log"
	"net/http"
	"sync"
)

var (
	shortConn []*http.Client
	longConn  []*http.Client
	used      = 0
	lock      = sync.Mutex{}
)

func init() {
	shortConn = make([]*http.Client, 0)
	longConn = make([]*http.Client, 0)
}
func ClientSetWithReuse(num int) []*http.Client {
	if num < 0 {
		panic("conn num is less than 0")
	}
	if num > len(longConn) {
		for i := len(longConn); i < num; i++ {
			longConn = append(longConn, GetClientWithoutReuse(false))
		}
	}
	return longConn[:num]
}
func ClientSetWithOutReuse(num int) []*http.Client {
	if num < 0 {
		panic("conn num is less than 0")
	}
	res := make([]*http.Client, num)
	for i := 0; i < num; i++ {
		res[i] = GetClientWithoutReuse(false)
	}
	return res
}
func ShortClientSetWithReuse(num int) []*http.Client {
	if num < 0 {
		panic("conn num is less than 0")
	}
	if num > len(shortConn) {
		for i := len(shortConn); i < num; i++ {
			shortConn = append(shortConn, GetClientWithoutReuse(true))
		}
	}
	return shortConn[:num]
}
func ShortClientSetWithoutReuse(num int) []*http.Client {
	if num < 0 {
		panic("conn num is less than 0")
	}
	res := make([]*http.Client, num)
	for i := 0; i < num; i++ {
		res[i] = GetClientWithoutReuse(true)
	}
	return res
}

func GetClientWithoutReuse(short bool) *http.Client {
	tr := &http.Transport{
		TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
	}
	if short {
		tr.DisableKeepAlives = true
	}
	return &http.Client{
		Transport: tr,
	}
}
func GetClientWithReuse() *http.Client {
	lock.Lock()
	defer lock.Unlock()
	used++
	log.Println("return client seq is ", used-1)
	return ClientSetWithReuse(used)[used-1]
}
