package ioinfo

import (
	"fmt"
	"log"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"stressTest/defs"
	"stressTest/util"
	"time"
)

func WriteInfo(start time.Time, s defs.Stress) {
	res, namespace, action, conn, anno, duration, connSend, connRecv, connSendNum := s.Info()
	kind, _, request := util.GetBasic(res, namespace)
	fpath := path.Join("output", action, strconv.Itoa(anno), kind+".txt")
	dir := filepath.Dir(fpath)
	err := os.MkdirAll(dir, 0777)
	if err != nil {
		fmt.Println("dir create err", err)
		return
	}
	f, err := os.OpenFile(fpath, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0777)
	if err != nil {
		log.Fatal("file open fail: ", err)
	}
	defer f.Close()
	fmt.Fprintln(f, time.Now().String())
	fmt.Fprintln(f, "Running "+duration.String()+" "+namespace+" @ "+request)
	fmt.Fprintln(f, strconv.Itoa(conn)+" connections")
	fmt.Fprintln(f, "Requests/bytes (per Connection): ", connSend)
	fmt.Fprintln(f, "Response/bytes (per Connection): ", connRecv)
	fmt.Fprintln(f, "Requests Number (per Connection): ", connSendNum)
	sum := 0
	for i := 0; i < len(connRecv); i++ {
		sum += connSend[i]
	}
	fmt.Fprintln(f, "Requests/sec: ", float64(sum)/duration.Seconds()/1024.0, "KB")
	for i := 0; i < len(connRecv); i++ {
		sum += connRecv[i]
	}
	fmt.Fprintln(f, "Transfer/sec: ", float64(sum)/duration.Seconds()/1024.0, "KB")
	fmt.Fprintln(f)
}
