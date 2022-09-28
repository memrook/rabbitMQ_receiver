package main

import (
	"encoding/json"
	amqp "github.com/rabbitmq/amqp091-go"
	"io/ioutil"
	"log"
	"time"
)

type Config struct {
	ServerURL string `json:"serverURL"`
	Queue     struct {
		QueueName  string `json:"queueName"`
		Durable    bool   `json:"durable"`
		AutoDelete bool   `json:"autoDelete"`
		Exclusive  bool   `json:"exclusive"`
		NoWait     bool   `json:"noWait"`
	} `json:"queue"`
	Consume struct {
		Consumer  string `json:"consumer"`
		AutoAck   bool   `json:"autoAck"`
		Exclusive bool   `json:"exclusive"`
		NoLocal   bool   `json:"noLocal"`
		NoWait    bool   `json:"noWait"`
	} `json:"consume"`
	Send struct {
		Frequency time.Duration `json:"frequency"`
		Message   string        `json:"message"`
		Exchange  string        `json:"exchange"`
	} `json:"send"`
}

func failOnError(err error, msg string) {
	if err != nil {
		log.Panicf("%s: %s", msg, err)
	}
}

func main() {
	//Read configuration and put to var "c"
	file, _ := ioutil.ReadFile("config.json")
	c := Config{}
	_ = json.Unmarshal([]byte(file), &c)

	conn, err := amqp.Dial(c.ServerURL)
	failOnError(err, "Failed to connect to RabbitMQ")
	defer conn.Close()

	ch, err := conn.Channel()
	failOnError(err, "Failed to open a channel")
	defer ch.Close()

	q, err := ch.QueueDeclare(
		c.Queue.QueueName,  // name
		c.Queue.Durable,    // durable
		c.Queue.AutoDelete, // delete when unused
		c.Queue.Exclusive,  // exclusive
		c.Queue.NoWait,     // no-wait
		nil,                // arguments
	)
	failOnError(err, "Failed to declare a queue")

	msgs, err := ch.Consume(
		q.Name,              // queue
		c.Consume.Consumer,  // consumer
		c.Consume.AutoAck,   // auto-ack
		c.Consume.Exclusive, // exclusive
		c.Consume.NoLocal,   // no-local
		c.Consume.NoWait,    // no-wait
		nil,                 // args
	)
	failOnError(err, "Failed to register a consumer")

	var forever chan struct{}

	go func() {
		for d := range msgs {
			log.Printf("Received a message: %s", d.Body)
		}
	}()

	log.Printf(" [*] Waiting for messages. To exit press CTRL+C")
	<-forever
}
