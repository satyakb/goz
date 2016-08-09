package main

import (
	"github.com/Shopify/sarama"
	"github.com/garyburd/redigo/redis"

	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"time"
)

const BufferLen = 1024
const KafkaTopic = "audio"
const RedisKey = "offset"

type flushWriter struct {
	f http.Flusher
	w io.Writer
}

func (fw *flushWriter) Write(p []byte) (n int, err error) {
	n, err = fw.w.Write(p)
	if err != nil {
		return n, err
	}
	if fw.f != nil {
		fw.f.Flush()
	}
	return n, err
}

var producer sarama.SyncProducer
var redisPool *redis.Pool

func hello(w http.ResponseWriter, r *http.Request) {
	io.WriteString(w, "Hello world!")
}

func shareHandler(w http.ResponseWriter, r *http.Request) {
	reader, err := r.MultipartReader()
	if err != nil {
		fmt.Println(err)
		return
	}

	for {
		part, err_part := reader.NextPart()
		if err_part == io.EOF {
			break
		}
		fmt.Println(part)
		if part.FormName() == "uploadfile" {
			for {
				buf := make([]byte, BufferLen)
				n, err := part.Read(buf)
				if err == io.EOF {
					break
				}

				// Write to Kafka
				msg := &sarama.ProducerMessage{Topic: KafkaTopic, Value: sarama.ByteEncoder(buf[:n])}
				_, _, err = producer.SendMessage(msg)
				if err != nil {
					log.Printf("FAILED to send message: %s\n", err)
				}
			}
		}
	}
}

func listenHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-type", "audio/mpeg")
	w.Header().Set("Transfer-Encoding", "chunked")
	w.WriteHeader(200)

	fw := flushWriter{w: w}
	if f, ok := w.(http.Flusher); ok {
		fw.f = f
	}

	spray(fw)

}

func spray(fw flushWriter) {
	consumer, err := sarama.NewConsumer([]string{"localhost:9092"}, nil)
	if err != nil {
		panic(err)
	}
	defer func() {
		if err := consumer.Close(); err != nil {
			log.Fatalln(err)
		}
	}()

	conn := redisPool.Get()
	defer conn.Close()

	offset, err := redis.Int64(conn.Do("GET", RedisKey))
	// Will need these lines later when client is made realtime
	// if err != nil {
	// 	offset = sarama.OffsetNewest
	// }

	partitionConsumer, err := consumer.ConsumePartition(KafkaTopic, 0, offset)
	if err != nil {
		panic(err)
	}

	defer func() {
		if err := partitionConsumer.Close(); err != nil {
			log.Fatalln(err)
		}
	}()

	for {
		time.Sleep(time.Millisecond * 20)

		audio := []byte{}
		select {
		case msg := <-partitionConsumer.Messages():
			audio = msg.Value

			msgOffset := msg.Offset
			offset, _ := redis.Int64(conn.Do("GET", RedisKey))
			if msgOffset > offset {
				conn.Do("SET", RedisKey, msgOffset)
			}
		default:
			fmt.Println("DONE")
		}

		_, err := fw.Write(audio)
		if err != nil {
			fmt.Println("error writing to response")
			break
		}
	}
}

func main() {
	var err error
	producer, err = sarama.NewSyncProducer([]string{"localhost:9092"}, nil)
	if err != nil {
		log.Fatalln(err)
	}
	defer func() {
		if err := producer.Close(); err != nil {
			log.Fatalln(err)
		}
	}()

	redisPool = redis.NewPool(func() (redis.Conn, error) {
		return redis.Dial("tcp", ":6379")
	}, 8)

	http.HandleFunc("/", hello)
	http.HandleFunc("/share", shareHandler)
	http.HandleFunc("/listen", listenHandler)
	http.ListenAndServe(":"+os.Args[1], nil)
}
