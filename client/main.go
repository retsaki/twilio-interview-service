package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
)

func main() {
	base := flag.String("base", "http://localhost:8080", "server base URL")
	cmd := flag.String("cmd", "", "record|count|status")
	user := flag.String("user", "", "user id")
	atype := flag.String("type", "", "action type")
	ts := flag.Int64("ts", 0, "timestamp (unix seconds)")
	hours := flag.Int("hours", 0, "hours window")
	threshold := flag.Int("threshold", 0, "threshold")
	flag.Parse()

	switch *cmd {
	case "record":
		body := map[string]any{"userID": *user, "actionType": *atype}
		if *ts > 0 {
			body["timestamp"] = *ts
		}
		buf, _ := json.Marshal(body)
		resp, err := http.Post(*base+"/action", "application/json", bytes.NewReader(buf))
		exitOn(err)
		slurp(resp)
	case "count":
		q := url.Values{}
		q.Set("userID", *user)
		q.Set("actionType", *atype)
		if *hours > 0 {
			q.Set("hours", fmt.Sprint(*hours))
		}
		u := *base + "/action/count?" + q.Encode()
		resp, err := http.Get(u)
		exitOn(err)
		slurp(resp)
	case "status":
		q := url.Values{}
		q.Set("userID", *user)
		if *hours > 0 {
			q.Set("hours", fmt.Sprint(*hours))
		}
		if *threshold > 0 {
			q.Set("threshold", fmt.Sprint(*threshold))
		}
		u := *base + "/user/status?" + q.Encode()
		resp, err := http.Get(u)
		exitOn(err)
		slurp(resp)
	default:
		fmt.Println("go run ./client -cmd record -user u1 -type click")
		fmt.Println("go run ./client -cmd count -user u1 -type click -hours 6")
		fmt.Println("go run ./client -cmd status -user u1 -hours 6 -threshold 10")
	}
}

func slurp(resp *http.Response) {
	defer resp.Body.Close()
	io.Copy(os.Stdout, resp.Body)
}

func exitOn(err error) {
	if err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}
