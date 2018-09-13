package importer

import (
	"io"
	"github.com/goat-project/goat-proto-go"
)

// AccountingServiceImpl implements goat_grpc.AccountingService server
type AccountingServiceImpl struct {
}

// NewAccountingServiceImpl creates a grpc server 
func NewAccountingServiceImpl() AccountingServiceImpl {
	return AccountingServiceImpl {}
}

func (asi *AccountingServiceImpl) ProcessVms(stream goat_grpc.AccountingService_ProcessVmsServer) error {
	for {
		_, err := stream.Recv()
		// TODO client identifier validation

		if err == io.EOF {
			return stream.SendAndClose(&goat_grpc.Confirmation{
				Accepted: true,
				Msg: "ok",
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
		_, err := stream.Recv()
		// TODO client identifier validation

		if err == io.EOF {
			return stream.SendAndClose(&goat_grpc.Confirmation{
				Accepted: true,
				Msg: "ok",
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
		_, err := stream.Recv()
		// TODO client identifier validation

		if err == io.EOF {
			return stream.SendAndClose(&goat_grpc.Confirmation{
				Accepted: true,
				Msg: "ok",
			})
		}

		if err != nil {
			return err
		}

		// TODO process data
	}
}
