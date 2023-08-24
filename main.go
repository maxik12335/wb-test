package main

import (
	"fmt"
	"log"
	"time"

	"database/sql"
	"encoding/json"

	_ "github.com/lib/pq"
	"github.com/nats-io/stan.go"
)

// Структура для данных о доставке
type Delivery struct {
	Name    string `json:"name"`
	Phone   string `json:"phone"`
	Zip     string `json:"zip"`
	City    string `json:"city"`
	Address string `json:"address"`
	Region  string `json:"region"`
	Email   string `json:"email"`
}

// Структура для данных о платеже
type Payment struct {
	Transaction  string `json:"transaction"`
	RequestID    string `json:"request_id"`
	Currency     string `json:"currency"`
	Provider     string `json:"provider"`
	Amount       int    `json:"amount"`
	PaymentDT    int64  `json:"payment_dt"`
	Bank         string `json:"bank"`
	DeliveryCost int    `json:"delivery_cost"`
	GoodsTotal   int    `json:"goods_total"`
	CustomFee    int    `json:"custom_fee"`
}

// Структура для данных о товарах
type Item struct {
	TrackNumber string `json:"track_number"`
	Price       int    `json:"price"`
	RID         string `json:"rid"`
	Name        string `json:"name"`
	Sale        int    `json:"sale"`
	Size        string `json:"size"`
	TotalPrice  int    `json:"total_price"`
	NMID        int    `json:"nm_id"`
	Brand       string `json:"brand"`
	Status      int    `json:"status"`
}


// Структура для данных о заказе
type Order struct {
	OrderUID          string   `json:"order_uid"`
	TrackNumber       string   `json:"track_number"`
	Entry             string   `json:"entry"`
	Delivery          Delivery `json:"delivery"`
	Payment           Payment  `json:"payment"`
	Items             []Item   `json:"items"`
	Locale            string   `json:"locale"`
	InternalSignature string   `json:"internal_signature"`
	CustomerID        string   `json:"customer_id"`
	DeliveryService   string   `json:"delivery_service"`
	ShardKey          string   `json:"shardkey"`
	SMID              int      `json:"sm_id"`
	DateCreated       string   `json:"date_created"`
	OofShard          string   `json:"oof_shard"`
}

// Функция для записи данных о заказе в БД
func insertOrder(dbConn *sql.DB, order Order) error {
	_, err := dbConn.Exec(
		"INSERT INTO delivery (name, phone, zip, city, address, region, email) VALUES ($1, $2, $3, $4, $5, $6, $7)",
		order.Delivery.Name, order.Delivery.Phone, order.Delivery.Zip, order.Delivery.City, order.Delivery.Address, order.Delivery.Region, order.Delivery.Email)
	if err != nil {
		return err
	}
	// ... аналогично выполните запросы для других таблиц ...
	return nil
}

// Функция для записи данных о платеже в БД
func insertPayment(dbConn *sql.DB, payment Payment) error {
	_, err := dbConn.Exec(
		"INSERT INTO payment (transaction, request_id, currency, provider, amount, payment_dt, bank, delivery_cost, goods_total, custom_fee) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)",
		payment.Transaction, payment.RequestID, payment.Currency, payment.Provider, payment.Amount, time.Unix(payment.PaymentDT, 0), payment.Bank, payment.DeliveryCost, payment.GoodsTotal, payment.CustomFee)
	if err != nil {
		return err
	}
	return nil
}

// Функция для записи данных о товарах в БД
func insertItems(dbConn *sql.DB, items []Item) error {
	for _, item := range items {
		_, err := dbConn.Exec(
			"INSERT INTO item (track_number, price, rid, name, sale, size, total_price, nm_id, brand, status) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)",
			item.TrackNumber, item.Price, item.RID, item.Name, item.Sale, item.Size, item.TotalPrice, item.NMID, item.Brand, item.Status)
		if err != nil {
			return err
		}
	}
	return nil
}

func main() {
	// параметры для подключения к кластеру NATS Streaming
	clusterID := "test-cluster"
	clientID := "example-client"

	// подключение к кластеру NATS Streaming
	conn, err := stan.Connect(clusterID, clientID)
	if err != nil {
		log.Fatalf("Error connecting to NATS Streaming: %v", err)
	}
	defer conn.Close()

	// имя канала, на который хотите подписаться
	channel := "order-channel"

	// подписка на канал
	sub, err := conn.Subscribe(channel, func(msg *stan.Msg) {
		// обработка полученного сообщения
		fmt.Printf("Recieved a message: %s\n", msg.Data)

		// Распарсивание JSON
		var orderData Order
		err := json.Unmarshal(msg.Data, &orderData)
		if err != nil {
			log.Printf("Error parsing JSON: %v", err)
			return
		}

		// Подключение к базе данных
		connStr := "user=postgres dbname=orders sslmode=disable port=5433"
		dbConn, err := sql.Open("postgres", connStr)
		if err != nil {
			log.Printf("Error connecting to DB: %v", err)
			return
		}
		defer dbConn.Close()

		if err != nil {
			log.Printf("Error connecting to DB: %v", err)
			return
		}
		defer dbConn.Close()

		// Вызов функции записи данных в БД
		err = insertOrder(dbConn, orderData)
		if err != nil {
			log.Printf("Error inserting order into DB: %v", err)
			return
		}

		// Вызов функции записи данных о платеже в БД
		err = insertPayment(dbConn, orderData.Payment)
		if err != nil {
			log.Printf("Error inserting payment into DB: %v", err)
			return
		}

		// Вызов функции записи данных о товарах в БД
		err = insertItems(dbConn, orderData.Items)
		if err != nil {
			log.Printf("Error inserting items into DB: %v", err)
			return
		}

	})

	if err != nil {
		log.Fatalf("Error subscribing to channel: %v", err)
	}
	defer sub.Unsubscribe()

	// отправка сообщений
	// message := []byte("Hello, NATA Streaming")
	message := []byte(`{
			"order_uid": "b563feb7b2b84b6test",
			"track_number": "WBILMTESTTRACK",
			"entry": "WBIL",
			"delivery": {
				"name": "Test Testov",
				"phone": "+9720000000",
				"zip": "2639809",
				"city": "Kiryat Mozkin",
				"address": "Ploshad Mira 15",
				"region": "Kraiot",
				"email": "test@gmail.com"
			},
			"payment": {
				"transaction": "b563feb7b2b84b6test",
				"request_id": "",
				"currency": "USD",
				"provider": "wbpay",
				"amount": 1817,
				"payment_dt": 1637907727,
				"bank": "alpha",
				"delivery_cost": 1500,
				"goods_total": 317,
				"custom_fee": 0
			},
			"items": [
				{
					"chrt_id": 9934930,
					"track_number": "WBILMTESTTRACK",
					"price": 453,
					"rid": "ab4219087a764ae0btest",
					"name": "Mascaras",
					"sale": 30,
					"size": "0",
					"total_price": 317,
					"nm_id": 2389212,
					"brand": "Vivienne Sabo",
					"status": 202
				}
			],
			"locale": "en",
			"internal_signature": "",
			"customer_id": "test",
			"delivery_service": "meest",
			"shardkey": "9",
			"sm_id": 99,
			"date_created": "2021-11-26T06:22:19Z",
			"oof_shard": "1"
		}`)

	err = conn.Publish(channel, message)
	if err != nil {
		log.Fatalf("Error publishing message: %v", err)
	}

	// ожидание сообщений (можно вставить в цикл)
	time.Sleep(time.Second * 5)
}
