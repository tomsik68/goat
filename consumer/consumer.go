package consumer

import (
	"context"
	"github.com/goat-project/goat-proto-go"
	"github.com/goat-project/goat/common"
)

// IPConsumer processes IpRecord-s
type IPConsumer interface {
	// ConsumeIps processes all ip records from specified channel and specified client id
	ConsumeIps(ctx context.Context, id string, ips <-chan goat_grpc.IpRecord) (common.DoneChannel, error)
}

// VMConsumer processes VmRecords
type VMConsumer interface {
	// ConsumeVMs processes all ip records from specified channel and specified client id
	ConsumeVms(ctx context.Context, id string, vms <-chan goat_grpc.VmRecord) (common.DoneChannel, error)
}

// StorageConsumer processes StorageRecords
type StorageConsumer interface {
	// ConsumeIps processes all ip records from specified channel and specified client id
	ConsumeStorages(ctx context.Context, id string, sts <-chan goat_grpc.StorageRecord) (common.DoneChannel, error)
}
