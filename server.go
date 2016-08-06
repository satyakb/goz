package main

import (
  "fmt"
  "io"
  "net/http"
  "os"
  "sync"
  "time"
)

type flushWriter struct {
  f http.Flusher
  w io.Writer
}

func (fw *flushWriter) Write(p []byte) (n int, err error) {
  n, err = fw.w.Write(p)
  if fw.f != nil {
    fw.f.Flush()
  }
  return
}

var c = make(chan []byte, 1024)
var listeners = map[int]flushWriter{}
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
      buf := make([]byte, 1024)
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

  fw := flushWriter{w: w}
  if f, ok := w.(http.Flusher); ok {
    fw.f = f
  }

  mutex.Lock()
  listeners[counter] = fw
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
    for _, fw := range listeners {
      fw.Write(audio)
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