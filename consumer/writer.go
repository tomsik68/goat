package consumer

import (
	"context"
	"github.com/goat-project/goat-proto-go"
	"github.com/goat-project/goat/common"
	"os"
	"path"
)

// WriterConsumer processes accounting data by transforming them according to supplied templates and subsequently writing to a file
type WriterConsumer struct {
	dir          string
	templatesDir string
}

// NewWriter creates a new WriterConsumer
func NewWriter(dir, templatesDir string) WriterConsumer {
	return WriterConsumer{
		dir:          dir,
		templatesDir: templatesDir,
	}
}

func (wc WriterConsumer) ensureDirectoryExists(id string) error {
	err := os.MkdirAll(path.Join(wc.dir, id), os.ModeDir)
	if err != nil && err != os.ErrExist {
		return err
	}
	return nil
}

// ConsumeIps transforms IpRecord-s into text and writes them to a subdirectory of dir specified by WriterConsumer's dir field. Each IpRecord is written to its own file.
func (wc WriterConsumer) ConsumeIps(ctx context.Context, id string, ips <-chan goat_grpc.IpRecord) (common.DoneChannel, error) {
	done := make(chan struct{})

	if err := wc.ensureDirectoryExists(id); err != nil {
		return nil, err
	}

	go func() {
		defer close(done)

		for {
			select {
			case <-ips:
			case <-ctx.Done():
				return
			}
		}

	}()

	return done, nil
}

// ConsumeVms transforms VmRecord-s into text and writes them to a subdirectory of dir specified by WriterConsumer's dir field. Each VmRecord is written to its own file.
func (wc WriterConsumer) ConsumeVms(ctx context.Context, id string, vms <-chan goat_grpc.VmRecord) (common.DoneChannel, error) {
	done := make(chan struct{})

	if err := wc.ensureDirectoryExists(id); err != nil {
		return nil, err
	}

	go func() {
		defer close(done)

		for {
			select {
			case <-vms:
			case <-ctx.Done():
				return
			}
		}

	}()

	return done, nil
}

// ConsumeStorages transforms StorageRecord-s into text and writes them to a subdirectory of dir specified by WriterConsumer's dir field. Each StorageRecord is written to its own file.
func (wc WriterConsumer) ConsumeStorages(ctx context.Context, id string, sts <-chan goat_grpc.StorageRecord) (common.DoneChannel, error) {
	done := make(chan struct{})

	if err := wc.ensureDirectoryExists(id); err != nil {
		return nil, err
	}

	go func() {
		defer close(done)

		for {
			select {
			case <-sts:
			case <-ctx.Done():
				return
			}
		}

	}()

	return done, nil
}
