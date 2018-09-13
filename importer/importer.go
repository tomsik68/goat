package importer

import (
	"github.com/goat-project/goat-proto-go"
	"io"
)

// ClientIdentifierValidator takes a client identifier and returns true if and only if data from this client is accepted
type ClientIdentifierValidator func(identifier string) bool

// AccountingServiceImpl implements goat_grpc.AccountingService server
type AccountingServiceImpl struct {
	clientIdentifierValidator ClientIdentifierValidator
}

// NewAccountingServiceImpl creates a grpc server
func NewAccountingServiceImpl(identifierValidator ClientIdentifierValidator) AccountingServiceImpl {
	return AccountingServiceImpl{
		clientIdentifierValidator: identifierValidator,
	}
}

func (asi *AccountingServiceImpl) ProcessVms(stream goat_grpc.AccountingService_ProcessVmsServer) error {
	for {
		data, err := stream.Recv()
		if !asi.clientIdentifierValidator(data.Identifier) {
			return stream.SendAndClose(&goat_grpc.Confirmation{
				Accepted: false,
				Msg:      "client identifier rejected",
			})
		}

		if err == io.EOF {
			return stream.SendAndClose(&goat_grpc.Confirmation{
				Accepted: true,
				Msg:      "ok",
			})
		}

		if err != nil {
			return err
		}

		// TODO process data
	}
}

func (asi *AccountingServiceImpl) ProcessIps(stream goat_grpc.AccountingService_ProcessIpsServer) error {
	for {
		data, err := stream.Recv()
		if !asi.clientIdentifierValidator(data.Identifier) {
			return stream.SendAndClose(&goat_grpc.Confirmation{
				Accepted: false,
				Msg:      "client identifier rejected",
			})
		}

		if err == io.EOF {
			return stream.SendAndClose(&goat_grpc.Confirmation{
				Accepted: true,
				Msg:      "ok",
			})
		}

		if err != nil {
			return err
		}

		// TODO process data
	}
}

func (asi *AccountingServiceImpl) ProcessStorage(stream goat_grpc.AccountingService_ProcessStorageServer) error {
	for {
		data, err := stream.Recv()
		if !asi.clientIdentifierValidator(data.Identifier) {
			return stream.SendAndClose(&goat_grpc.Confirmation{
				Accepted: false,
				Msg:      "client identifier rejected",
			})
		}

		if err == io.EOF {
			return stream.SendAndClose(&goat_grpc.Confirmation{
				Accepted: true,
				Msg:      "ok",
			})
		}

		if err != nil {
			return err
		}

		// TODO process data
	}
}
