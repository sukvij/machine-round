package main

import (
	"fmt"
	"sync"
	"time"
)

type Topic string

type Item struct {
	Val        any
	ProducedBy string
	ConsumedBy string
}

type Producer struct {
	Name         string
	SubscribedTo []string
}

type Consumer struct {
	Name         string
	SubscribedTo []string
}

type Kafka struct {
	Topics    []string
	Producers map[string]*Producer
	Consumers map[string]*Consumer
	Channels  map[Topic]chan Item
	Wg        *sync.WaitGroup
}

func NewKafka() *Kafka {
	kafka := &Kafka{Topics: []string{}, Producers: make(map[string]*Producer), Consumers: make(map[string]*Consumer), Channels: make(map[Topic]chan Item)}
	kafka.Wg = &sync.WaitGroup{}
	return kafka
}

func (kafka *Kafka) AddTopic(topic string) {
	kafka.Topics = append(kafka.Topics, topic)
	kafka.Channels[Topic(topic)] = make(chan Item)
}
func (kafka *Kafka) AddProducers(prods []string) {
	for _, prodName := range prods {
		prod := &Producer{Name: prodName}
		kafka.Producers[prodName] = prod
		kafka.Wg.Add(1)
	}
}

func (kafka *Kafka) AddConsumers(conumers []string) {
	for _, consName := range conumers {
		con := &Consumer{Name: consName, SubscribedTo: []string{}}
		kafka.Consumers[consName] = con
		kafka.Wg.Add(1)
	}
}

func (kafka *Kafka) SubscribeConsumers(cons []string, topic []string) {
	for _, conName := range cons {
		kafka.Consumers[conName].SubscribedTo = append(kafka.Consumers[conName].SubscribedTo, topic...)
	}
}

func (kafka *Kafka) SubscribeProducers(prods []string, topic []string) {
	for _, prodName := range prods {
		kafka.Producers[prodName].SubscribedTo = append(kafka.Producers[prodName].SubscribedTo, topic...)
	}
}

func (prod *Producer) ProduceItem(topic string, message string, ch chan Item) {
	ch <- Item{Val: message, ProducedBy: prod.Name}
}

func (kafka *Kafka) PublishMessage(prodName string, topic string, message string) {
	defer kafka.Wg.Done()
	producer := kafka.Producers[prodName]
	go producer.ProduceItem(topic, message, kafka.Channels[Topic(topic)])
}

func (con *Consumer) ConsumeMessage(ch chan Item, wg *sync.WaitGroup) {
	defer wg.Done()
	for {
		select {
		case <-time.After(2 * time.Second):
			return
		case msg, _ := <-ch:
			fmt.Println(msg, "conumed by consumer ", con.Name)
		}
	}
}

// consumes message from channel
func (kafka *Kafka) ConsumeMessage() {
	for _, ch := range kafka.Channels {
		for _, con := range kafka.Consumers {
			go con.ConsumeMessage(ch, kafka.Wg)
		}
	}
}

func main() {
	kafka := NewKafka()

	kafka.AddTopic("topic1")
	kafka.AddTopic("topic2")

	kafka.AddProducers([]string{"producer1", "producer2"})
	kafka.AddConsumers([]string{"consumer1", "consumer2", "consumer3", "consumer4", "consumer5"})
	kafka.SubscribeConsumers([]string{"consumer1", "consumer2", "consumer3", "consumer4", "consumer5"}, []string{"topic1"})
	kafka.SubscribeConsumers([]string{"consumer1", "consumer3", "consumer4"}, []string{"topic2"})
	kafka.PublishMessage("producer1", "topic1", "messages1")
	kafka.PublishMessage("producer1", "topic1", "messages2")
	kafka.PublishMessage("producer2", "topic1", "messages3")
	kafka.PublishMessage("producer1", "topic2", "messages4")
	kafka.PublishMessage("producer2", "topic2", "messages5")
	kafka.ConsumeMessage()

	kafka.Wg.Wait()
}
