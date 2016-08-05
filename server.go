package main

import (
  "fmt"
  "io"
  "net/http"
  "sync"
  "time"
)

var c = make(chan []byte, 1024)
var listeners = map[int]http.ResponseWriter{}
var counter = 0
var mutex = &sync.Mutex{}


func hello(w http.ResponseWriter, r *http.Request) {
  io.WriteString(w, "Hello world!")
}

func shareHandler(w http.ResponseWriter, r *http.Request) {
  reader, err := r.MultipartReader()
  if err != nil {
    fmt.Println(err)
    return
  }

  // Goroutine that sprays audio to all listeners
  go spray()

  for {
    part, err_part := reader.NextPart()
    if err_part == io.EOF {
        break
    }
    fmt.Println(part)
    if part.FormName() == "uploadfile" {
        buf := make([]byte, 8192)
        for {
          n, err := part.Read(buf)
          if err == io.EOF {
            break
          }
          c <- buf[:n]
        }
    }
  }
}

func listenHandler(w http.ResponseWriter, r *http.Request) {
  w.Header().Set("Content-type", "audio/mpeg")

  mutex.Lock()
  listeners[counter] = w
  counter++
  fmt.Println(listeners)
  mutex.Unlock()

  // Keep connection alive
  for {
    time.Sleep(time.Second * 10)
  }
}

func spray() {
  for {
    time.Sleep(time.Millisecond)
    if len(listeners) == 0 {
      continue
    }

    audio := <- c
    mutex.Lock()
    for _, w := range listeners {
      w.Write(audio)
      w.(http.Flusher).Flush()
    }
    mutex.Unlock()
  }
}

func main() {
  http.HandleFunc("/", hello)
  http.HandleFunc("/share", shareHandler)
  http.HandleFunc("/listen", listenHandler)
  http.ListenAndServe(":8000", nil)
}