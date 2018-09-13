package importer

import (
	"github.com/goat-project/goat-proto-go"
	"io"
)

// ClientIdentifierValidator takes a client identifier and returns true if and only if data from this client is accepted
type ClientIdentifierValidator func(identifier string) bool

// AccountingServiceImpl implements goat_grpc.AccountingService server
type AccountingServiceImpl struct {
	vmConsumer                chan<- *goat_grpc.VmRecord
	ipConsumer                chan<- *goat_grpc.IpRecord
	storageConsumer           chan<- *goat_grpc.StorageRecord
	clientIdentifierValidator ClientIdentifierValidator
}

// NewAccountingServiceImpl creates a grpc server that sends received data to given channels and uses clientIdentifierValidator to validate client identifiers
func NewAccountingServiceImpl(vms chan<- *goat_grpc.VmRecord, ips chan<- *goat_grpc.IpRecord, storages chan<- *goat_grpc.StorageRecord, identifierValidator ClientIdentifierValidator) AccountingServiceImpl {
	return AccountingServiceImpl{
		vmConsumer:                vms,
		ipConsumer:                ips,
		storageConsumer:           storages,
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

		vms := data.Vms
		for _, vm := range vms {
			asi.vmConsumer <- vm
		}
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

		ips := data.Ips
		for _, ip := range ips {
			asi.ipConsumer <- ip
		}
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

		storages := data.Storages
		for _, storage := range storages {
			asi.storageConsumer <- storage
		}
	}
}
