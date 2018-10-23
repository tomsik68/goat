package importer

import (
	"context"
	"errors"
	"github.com/goat-project/goat-proto-go"
	"github.com/goat-project/goat/common"
	"github.com/goat-project/goat/consumer"
	"github.com/golang/protobuf/ptypes/empty"
	"io"
)

var (
	// ErrFirstClientIdentifier indicates that the first message of the stream is not client identifier
	ErrFirstClientIdentifier = errors.New("First message in the stream must be client identifier")
	// ErrNonFirstClientIdentifier indicates that client identifier was found as a non-first message of the stream
	ErrNonFirstClientIdentifier = errors.New("Client identifier found as a non-first message of the stream")
	// ErrUnknownMessageType indicates that an unknown type has arrived as part of data stream
	ErrUnknownMessageType = errors.New("Unhandled message type received")
)

// AccountingServiceImpl implements goat_grpc.AccountingService server
type AccountingServiceImpl struct {
	vmConsumer      consumer.VMConsumer
	ipConsumer      consumer.IPConsumer
	storageConsumer consumer.StorageConsumer
}

// NewAccountingServiceImpl creates a grpc server that sends received data to given channels and uses clientIdentifierValidator to validate client identifiers
func NewAccountingServiceImpl(vmConsumer consumer.VMConsumer, ipConsumer consumer.IPConsumer, storageConsumer consumer.StorageConsumer) AccountingServiceImpl {
	return AccountingServiceImpl{
		vmConsumer:      vmConsumer,
		ipConsumer:      ipConsumer,
		storageConsumer: storageConsumer,
	}
}

func (asi AccountingServiceImpl) receiveIdentifier(stream goat_grpc.AccountingService_ProcessServer) (string, error) {
	id, err := stream.Recv()
	if err != nil {
		return "", err
	}

	switch id.Data.(type) {
	case *goat_grpc.AccountingData_Identifier:
		return id.GetIdentifier(), nil
	default:
		return "", ErrFirstClientIdentifier
	}
}

func (asi AccountingServiceImpl) startConsumers(ctx context.Context, id string, vms <-chan goat_grpc.VmRecord, ips <-chan goat_grpc.IpRecord, storages <-chan goat_grpc.StorageRecord) (common.DoneChannel, error) {
	done1, err := asi.vmConsumer.ConsumeVms(ctx, id, vms)
	if err != nil {
		return nil, err
	}

	done2, err := asi.ipConsumer.ConsumeIps(ctx, id, ips)
	if err != nil {
		return nil, err
	}

	done3, err := asi.storageConsumer.ConsumeStorages(ctx, id, storages)
	if err != nil {
		return nil, err
	}

	return common.AndDone(done1, done2, done3), nil
}

// Process is a GRPC call -- do not use!
func (asi AccountingServiceImpl) Process(stream goat_grpc.AccountingService_ProcessServer) error {
	id, err := asi.receiveIdentifier(stream)
	if err != nil {
		return err
	}

	consumerContext, cancelConsumers := context.WithCancel(context.Background())

	// prepare channels for individual data types
	vms := make(chan goat_grpc.VmRecord)
	ips := make(chan goat_grpc.IpRecord)
	storages := make(chan goat_grpc.StorageRecord)

	// close all the channels whenever this method returns
	defer close(vms)
	defer close(ips)
	defer close(storages)
	defer cancelConsumers()

	done, err := asi.startConsumers(consumerContext, id, vms, ips, storages)
	if err != nil {
		return err
	}
	<-done

	for {
		data, err := stream.Recv()

		if err == io.EOF {
			return stream.SendAndClose(&empty.Empty{})
		}

		if err != nil {
			return err
		}

		switch data.Data.(type) {
		case *goat_grpc.AccountingData_Identifier:
			return ErrNonFirstClientIdentifier
		case *goat_grpc.AccountingData_Vm:
			vms <- *data.GetVm()
		case *goat_grpc.AccountingData_Ip:
			ips <- *data.GetIp()
		case *goat_grpc.AccountingData_Storage:
			storages <- *data.GetStorage()
		default:
			return ErrUnknownMessageType
		}
	}
}
