package stress

import (
	"bytes"
	"context"
	"io"
	"log"
	"math/rand"
	"net/http"
	"stressTest/client"
	"stressTest/config"
	"stressTest/util"
	"strings"
	"sync"
	"time"
)

var (
	lock = sync.Mutex{}
)

func Post(wg *sync.WaitGroup, ctx context.Context, res string, num int) {
	defer wg.Done()
	tick := time.NewTicker(time.Second / time.Duration(num))
	defer tick.Stop()
	wgr := &sync.WaitGroup{}
	defer wgr.Wait()
	httpClient := client.GetClientWithReuse()
	lock.Lock()
	counter := Rescounter[res]
	lock.Unlock()

	defer func() {
		lock.Lock()
		Rescounter[res] = counter
		lock.Unlock()
		log.Println("res", res, "end in", counter)
	}()

	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
			wgr.Add(1)
			post(wgr, counter, res, httpClient)
			counter++
		}
	}
}

func Patch(wg *sync.WaitGroup, ctx context.Context, res string, num int) {
	defer wg.Done()
	tick := time.NewTicker(time.Second / time.Duration(num))
	defer tick.Stop()
	wgr := &sync.WaitGroup{}
	defer wgr.Wait()
	httpClient := client.GetClientWithReuse()
	reslist := Resindex[res]
	if len(reslist) == 0 {
		return
	}
	log.Println("patch end pos is", len(reslist)>>1)
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:

			wgr.Add(1)
			patch(wgr, res, reslist[rand.Intn(len(reslist)>>1)], httpClient)
		}
	}
}

func List(wg *sync.WaitGroup, ctx context.Context, res string, num int) {
	defer wg.Done()
	tick := time.NewTicker(time.Second / time.Duration(num))
	defer tick.Stop()
	wgr := &sync.WaitGroup{}
	defer wgr.Wait()
	httpClient := client.GetClientWithReuse()
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
			wgr.Add(1)
			get(wgr, res, "", httpClient)
		}
	}
}
func Get(wg *sync.WaitGroup, ctx context.Context, res string, num int) {
	defer wg.Done()
	tick := time.NewTicker(time.Second / time.Duration(num))
	defer tick.Stop()
	wgr := &sync.WaitGroup{}
	defer wgr.Wait()
	httpClient := client.GetClientWithReuse()
	reslist := Resindex[res]
	log.Println("get end pos is", len(reslist)>>1)
	if len(reslist) == 0 {
		return
	}
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
			wgr.Add(1)
			get(wgr, res, reslist[rand.Intn(len(reslist)>>1)], httpClient)
		}
	}
}

func Delete(wg *sync.WaitGroup, ctx context.Context, res string, num int) {
	defer wg.Done()
	reslist := Resindex[res]
	pos := len(reslist) >> 1
	log.Println("delete start pos is", pos)
	tick := time.NewTicker(time.Second / time.Duration(num))
	defer tick.Stop()
	wgr := &sync.WaitGroup{}
	defer wgr.Wait()
	httpClient := client.GetClientWithReuse()
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
			if pos < len(reslist) {
				wgr.Add(1)
				delete(wgr, res, reslist[pos], httpClient)
				pos++
			} else {
				log.Println("no", res, "can be deleted")
			}
		}
	}
}

func Put(wg *sync.WaitGroup, ctx context.Context, res string, num int) {
	defer wg.Done()
	tick := time.NewTicker(time.Second / time.Duration(num))
	defer tick.Stop()
	wgr := &sync.WaitGroup{}
	defer wgr.Wait()
	httpClient := client.GetClientWithReuse()
	reslist := Resindex[res]
	log.Println("put end pos is", len(reslist)>>1)
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
			wgr.Add(1)
			put(wgr, res, reslist[rand.Intn(len(reslist)>>1)], httpClient)
		}
	}
}

func post(wg *sync.WaitGroup, num int, res string, httpClient *http.Client) {
	defer wg.Done()

	data, request := util.GetPostDataAndUrl(res, config.GetDefultNameSpace(), rand.Intn(10), num, 0)
	req, err := http.NewRequest("POST", request, bytes.NewBuffer(data))
	// log.Println("POST", req.URL.String())
	if err != nil {
		log.Fatal(err)
	}
	req.Header.Set("Authorization", config.GetDefultAuthor())
	req.Header.Set("Content-Type", "application/json")
	resp, err := httpClient.Do(req)
	if err != nil {
		log.Println("err:", err.Error())
		return
	}
	defer resp.Body.Close()
	if strings.Compare("300", resp.Status) <= 0 {
		log.Println("resp:", resp.Status, resp.Request.Method, resp.Request.URL)
	}
	io.Copy(io.Discard, resp.Body)
}

func delete(wg *sync.WaitGroup, res, resName string, httpClient *http.Client) {
	defer wg.Done()
	_, _, request := util.GetBasic(res, config.GetDefultNameSpace())
	req, err := http.NewRequest("DELETE", request+"/"+resName, nil)
	// log.Println("DELETE", request+"/"+resName)
	if err != nil {
		log.Fatal("new http request err", err)
	}
	req.Header.Set("Authorization", config.GetDefultAuthor())
	req.Header.Set("Content-Type", "application/json")
	resp, err := httpClient.Do(req)
	if err != nil {
		log.Println("do request has err", err.Error())
		return

	}
	defer resp.Body.Close()
	if strings.Compare("300", resp.Status) <= 0 {
		log.Println("resp:", resp.Status, resp.Request.Method, resp.Request.URL)
	}
	io.Copy(io.Discard, resp.Body)
}

func patch(wg *sync.WaitGroup, res, resName string, httpClient *http.Client) {
	defer wg.Done()
	_, _, request := util.GetBasic(res, config.GetDefultNameSpace())
	anno := util.GetPatchAnnotations(rand.Intn(10))
	req, err := http.NewRequest("PATCH", request+"/"+resName, bytes.NewBufferString(anno))
	// log.Println("PATCH", req.URL.String())
	if err != nil {
		log.Fatal("new http request err", err)
	}
	req.Header.Set("Authorization", config.GetDefultAuthor())
	req.Header.Set("Content-Type", "application/strategic-merge-patch+json")
	resp, err := httpClient.Do(req)
	if err != nil {
		log.Println("do request has err", err.Error())
		return

	}
	defer resp.Body.Close()
	if strings.Compare("300", resp.Status) <= 0 {
		log.Println("resp:", resp.Status, resp.Request.Method, resp.Request.URL)
	}
	io.Copy(io.Discard, resp.Body)
}
func get(wg *sync.WaitGroup, res, resName string, httpClient *http.Client) {
	defer wg.Done()
	_, _, request := util.GetBasic(res, config.GetDefultNameSpace())
	var req *http.Request
	var err error
	if resName != "" {
		req, err = http.NewRequest("GET", request+"/"+resName, nil)
	} else {
		req, err = http.NewRequest("GET", request, nil)
	}
	// log.Println("PATCH", req.URL.String())
	if err != nil {
		log.Fatal("new http request err", err)
	}
	req.Header.Set("Authorization", config.GetDefultAuthor())
	req.Header.Set("Content-Type", "application/json")
	resp, err := httpClient.Do(req)
	if err != nil {
		log.Println("do request has err", err.Error())
		return

	}
	defer resp.Body.Close()
	if strings.Compare("300", resp.Status) <= 0 {
		log.Println("resp:", resp.Status, resp.Request.Method, resp.Request.URL)
	}
	io.Copy(io.Discard, resp.Body)
}
func put(wg *sync.WaitGroup, res, resName string, httpClient *http.Client) {
	defer wg.Done()
	data, request := util.GetPutDataAndUrl(res, config.GetDefultNameSpace(), resName, rand.Intn(10))
	req, err := http.NewRequest("PUT", request+"/"+resName, bytes.NewBuffer(data))
	// log.Println("PUT", req.URL.String())
	if err != nil {
		log.Println("new http request err", err.Error())
	}
	req.Header.Set("Authorization", config.GetDefultAuthor())
	req.Header.Set("Content-Type", "application/json")
	resp, err := httpClient.Do(req)
	if err != nil {
		log.Println("do request has err", err.Error())
		return
	}
	defer resp.Body.Close()
	if strings.Compare("300", resp.Status) <= 0 {
		log.Println("resp:", resp.Status, resp.Request.Method, resp.Request.URL)
	}
	io.Copy(io.Discard, resp.Body)
}
