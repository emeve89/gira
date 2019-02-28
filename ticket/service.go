package ticket

import (
	"github.com/google/uuid"
	"time"
)

type TicketService interface {
	CreateTicket(ticket *Ticket) error
	FindTicketById(id string) (*Ticket, error)
	FindAllTickets() ([]*Ticket, error)
}

type ticketService struct {
	repo TicketRepository
}

func (s *ticketService) FindTicketById(id string) (*Ticket, error) {
	return s.repo.FindById(id)
}

func (s *ticketService) FindAllTickets() ([]*Ticket, error) {
	return s.repo.FindAll()
}

func (s *ticketService) CreateTicket(ticket *Ticket) error {
	ticket.ID = uuid.New().String()
	ticket.Created = time.Now()
	ticket.Updated = time.Now()
	ticket.Status = "open"
	return s.repo.Create(ticket)
}

func NewTicketService(repo TicketRepository) TicketService {
	return &ticketService{
		repo,
	}
}
