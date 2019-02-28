package redis

import (
	"encoding/json"
	"github.com/emeve89/gira/ticket"
	"github.com/go-redis/redis"
)

const table = "tickets"

type ticketDatabase struct {
	connection *redis.Client
}

func (db *ticketDatabase) Create(ticket *ticket.Ticket) error {
	encoded, err := json.Marshal(ticket)

	if err != nil {
		return err
	}

	db.connection.HSet(table, ticket.ID, encoded)
	return nil
}

func (db *ticketDatabase) FindById(id string) (*ticket.Ticket, error) {
	bytes, err := db.connection.HGet(table, id).Bytes()

	if err != nil {
		return nil, err
	}

	ticket := new(ticket.Ticket)
	err = json.Unmarshal(bytes, ticket)

	if err != nil {
		return nil, err
	}

	return ticket, nil
}

func (r *ticketDatabase) FindAll() (tickets []*ticket.Ticket, err error) {
	ts := r.connection.HGetAll(table).Val()
	for key, value := range ts {
		t := new(ticket.Ticket)
		err = json.Unmarshal([]byte(value), t)

		if err != nil {
			return nil, err
		}

		t.ID = key
		tickets = append(tickets, t)
	}
	return tickets, nil
}

func NewRedisTicketDatabase(connection *redis.Client) ticket.TicketRepository {
	return &ticketDatabase{
		connection,
	}
}
