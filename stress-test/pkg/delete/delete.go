package delete

import (
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"strconv"
	"stressTest/client"
	"stressTest/config"
	"stressTest/defs"
	"stressTest/util"
	"strings"
	"sync"
	"time"

	v1 "k8s.io/api/core/v1"
)

func Delete(wg *sync.WaitGroup, id, num int, res string) {
	defer wg.Done()
	log.Println("start delete", res)
	client := client.GetClientWithoutReuse(false)
	for i := 0; i < num; i++ {
		resName := "test-" + strings.ToLower(util.Res2kind(res)) + "-" + strconv.Itoa(i) + "-" + strconv.Itoa(id)
		_, _, request := util.GetBasic(res, config.GetDefultNameSpace())
		req, err := http.NewRequest("DELETE", request+"/"+resName, nil)
		if err != nil {
			log.Fatal("new http request err", err)
		}
		req.Header.Set("Authorization", config.GetDefultAuthor())
		req.Header.Set("Content-Type", "application/json")
		// log.Println("DELETE", req.URL.String())
		resp, err := client.Do(req)
		if err != nil {
			log.Println("do request has err", err)
			return
		}
		defer resp.Body.Close()
		if strings.Compare("300", resp.Status) <= 0 {
			log.Println("resp: ", resp.Status, resp.Request.Method, resp.Request.URL)
		}
		io.Copy(io.Discard, resp.Body)
	}
	log.Println("delete", res, "complete")

}
func ClearAll(ns, labelSelector, auth string) {
	log.Println("in delete ClearAll")
	for _, v := range defs.Reslist {
		ClearPost(v, ns, labelSelector, auth)
	}
	log.Println("complete delete ClearAll")
}
func ClearPost(res, ns, labelSelector, auth string) {
	_, _, request := util.GetBasic(res, ns)
	// req, err := http.NewRequest("DELETE", request+"?labelSelector="+labelSelector, nil)
	req, err := http.NewRequest("DELETE", request, nil)
	log.Println("DELETE", request, nil)
	if err != nil {
		log.Fatal(err)
	}
	req.Header.Set("Authorization", auth)
	client := &http.Client{
		Timeout: time.Minute * 30,
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
		},
	}
	resp, err := client.Do(req)
	if err != nil {
		log.Fatal("req err : ", err)
	}
	defer resp.Body.Close()
	if strings.Compare("300", resp.Status) <= 0 {
		log.Println("resp: ", resp.Status, resp.Request.Method, resp.Request.URL)
	}
	io.Copy(io.Discard, resp.Body)
}
func DeleteNameSpace(res, ns, labelSelector, auth string) {
	_, _, request := util.GetBasic(res, ns)
	req, err := http.NewRequest("GET", request+"?labelSelector="+labelSelector, nil)
	if err != nil {
		panic(err)
	}

	req.Header.Set("Authorization", auth)

	client := &http.Client{
		Timeout: time.Minute * 30,
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{
				InsecureSkipVerify: true,
			},
		},
	}
	resp, err := client.Do(req)
	if err != nil {
		panic(err)
	}
	defer resp.Body.Close()
	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		panic(err)
	}
	var namespaceList v1.NamespaceList
	err = json.Unmarshal(respBody, &namespaceList)
	if err != nil {
		panic(err)
	}
	for _, namespace := range namespaceList.Items {
		req, err := http.NewRequest("DELETE", request+"/"+namespace.Name, nil)
		if err != nil {
			panic(err)
		}
		req.Header.Set("Authorization", auth)
		resp, err := client.Do(req)
		if err != nil {
			panic(err)
		}
		defer resp.Body.Close()

		fmt.Printf("Deleted namespace %s", namespace.Name)
		io.Copy(io.Discard, resp.Body)
	}
}
func DeleteTerminatedNamespace(res, ns, labelSelector, auth string) {
	client := &http.Client{
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
		},
	}
	_, _, request := util.GetBasic(res, ns)
	req, err := http.NewRequest("GET", request+"?labelSelector="+labelSelector, nil)
	if err != nil {
		panic(err)
	}
	req.Header.Set("Authorization", auth)
	resp, err := client.Do(req)
	if err != nil {
		panic(err)
	}
	defer resp.Body.Close()
	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		panic(err)
	}
	var namespaceList v1.NamespaceList
	err = json.Unmarshal(respBody, &namespaceList)
	if err != nil {
		panic(err)
	}
	for _, namespace := range namespaceList.Items {
		if namespace.Status.Phase == "Terminating" {
			payload := strings.NewReader(`{"metadata":{"finalizers":null}}`)
			req, err := http.NewRequest("PATCH", request+"/"+namespace.Name+"/"+"finalize", payload)
			if err != nil {
				panic(err)
			}
			req.Header.Set("Authorization", auth)
			req.Header.Set("Content-Type", "application/strategic-merge-patch+json")
			resp, err := client.Do(req)
			if err != nil {
				panic(err)
			}
			defer resp.Body.Close()
			io.Copy(io.Discard, resp.Body)
			fmt.Printf("Finalized namespace %s", namespace.Name)
		}
	}
}
