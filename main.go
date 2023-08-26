package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"time"

	"database/sql"

	_ "github.com/lib/pq"
	"github.com/nats-io/stan.go"
)

type Delivery struct {
	Name    string `json:"name"`
	Phone   string `json:"phone"`
	Zip     string `json:"zip"`
	City    string `json:"city"`
	Address string `json:"address"`
	Region  string `json:"region"`
	Email   string `json:"email"`
}

type Payment struct {
	Transaction  string `json:"transaction"`
	RequestID    string `json:"request_id"`
	Currency     string `json:"currency"`
	Provider     string `json:"provider"`
	Amount       int    `json:"amount"`
	PaymentDt    int64  `json:"payment_dt"`
	Bank         string `json:"bank"`
	DeliveryCost int    `json:"delivery_cost"`
	GoodsTotal   int    `json:"goods_total"`
	CustomFee    int    `json:"custom_fee"`
}

type Item struct {
	ChrtID      int    `json:"chrt_id"`
	TrackNumber string `json:"track_number"`
	Price       int    `json:"price"`
	Rid         string `json:"rid"`
	Name        string `json:"name"`
	Sale        int    `json:"sale"`
	Size        string `json:"size"`
	TotalPrice  int    `json:"total_price"`
	NmID        int    `json:"nm_id"`
	Brand       string `json:"brand"`
	Status      int    `json:"status"`
}

type OrderData struct {
	OrderUID        string   `json:"order_uid"`
	TrackNumber     string   `json:"track_number"`
	Entry           string   `json:"entry"`
	Delivery        Delivery `json:"delivery"`
	Payment         Payment  `json:"payment"`
	Items           []Item   `json:"items"`
	Locale          string   `json:"locale"`
	InternalSig     string   `json:"internal_signature"`
	CustomerID      string   `json:"customer_id"`
	DeliveryService string   `json:"delivery_service"`
	ShardKey        string   `json:"shardkey"`
	SmID            int      `json:"sm_id"`
	DateCreated     string   `json:"date_created"`
	OofShard        string   `json:"oof_shard"`
}

func main() {
	// подключение к PostgreSQL
	database, err := sql.Open("postgres", "user=postgres dbname=orders host=localhost port=5433 password=admin sslmode=disable")
	if err != nil {
		fmt.Printf("Ошибка подключения к базе данных: %v\n", err)
	}
	defer database.Close()

	// подключение к NATS Streaming
	clusterID := "test-cluster"
	clientID := "test-client"

	natsStreaming, err := stan.Connect(clusterID, clientID)
	if err != nil {
		fmt.Printf("Ошибка подключения к NATS Streaming: %v/n", err)
	}
	defer natsStreaming.Close()

	// подписка на канал
	channel := "test-channel"

	subscription, err := natsStreaming.Subscribe(channel, func(msg *stan.Msg) {
		fmt.Printf("Получено сообщение: %s\n", msg.Data)

		// Здесь вы можете добавить код для преобразования данных из сообщения в структуру OrderData
		var order OrderData
		err := json.Unmarshal(msg.Data, &order)
		if err != nil {
			fmt.Printf("Ошибка разбора JSON: %v\n", err)
			return
		}

		// Теперь можно вставить данные в базу данных

		// Вставка данных в таблицу orders
		_, err = database.Exec(`
					INSERT INTO orders (order_uid, track_number, entry, locale, internal_signature, customer_id, delivery_service, shardkey, sm_id, date_created, oof_shard)
					VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
			`, order.OrderUID, order.TrackNumber, order.Entry, order.Locale, order.InternalSig, order.CustomerID, order.DeliveryService, order.ShardKey, order.SmID, order.DateCreated, order.OofShard)
		if err != nil {
			fmt.Printf("Ошибка записи в PostgreSQL: %v\n", err)
			return
		}

		// Вставка данных в таблицу delivery
		_, err = database.Exec(`
					INSERT INTO delivery (order_id, name, phone, zip, city, address, region, email)
					VALUES (lastval(), $1, $2, $3, $4, $5, $6, $7)
			`, order.Delivery.Name, order.Delivery.Phone, order.Delivery.Zip, order.Delivery.City, order.Delivery.Address, order.Delivery.Region, order.Delivery.Email)
		if err != nil {
			fmt.Printf("Ошибка записи в PostgreSQL: %v\n", err)
			return
		}

		// Вставка данных в таблицу payment
		_, err = database.Exec(`
					INSERT INTO payment (order_id, transaction, request_id, currency, provider, amount, payment_dt, bank, delivery_cost, goods_total, custom_fee)
					VALUES (lastval(), $1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
			`, order.Payment.Transaction, order.Payment.RequestID, order.Payment.Currency, order.Payment.Provider, order.Payment.Amount, order.Payment.PaymentDt, order.Payment.Bank, order.Payment.DeliveryCost, order.Payment.GoodsTotal, order.Payment.CustomFee)
		if err != nil {
			fmt.Printf("Ошибка записи в PostgreSQL: %v\n", err)
			return
		}

		// Вставка данных в таблицу item
		for _, item := range order.Items {
			_, err = database.Exec(`
							INSERT INTO item (order_id, chrt_id, track_number, price, rid, name, sale, size, total_price, nm_id, brand, status)
							VALUES (lastval(), $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
					`, item.ChrtID, item.TrackNumber, item.Price, item.Rid, item.Name, item.Sale, item.Size, item.TotalPrice, item.NmID, item.Brand, item.Status)
			if err != nil {
				fmt.Printf("Ошибка записи в PostgreSQL: %v\n", err)
				return
			}
		}
	})
	if err != nil {
		fmt.Printf("Ошибка подписки на канал: %v/n", err)
	}
	defer subscription.Close()

	// отправить сообщение
	// получить данные из model.json
	filecontent, err := ioutil.ReadFile("model.json")
	if err != nil {
		fmt.Printf("Ошибка чтения файла: %v\n", err)
		return
	}

	err = natsStreaming.Publish(channel, filecontent)
	if err != nil {
		fmt.Printf("Ошибка отправки сообщения: %v\n", err)
		return
	}

	time.Sleep(2 * time.Second)
}
