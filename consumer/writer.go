package consumer

import (
	"context"
	"github.com/goat-project/goat-proto-go"
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

func ensureDirectoryExists(path string) error {
	err := os.MkdirAll(path, os.ModeDir)
	if err != nil && err != os.ErrExist {
		return err
	}
	return nil
}

// ConsumeStorages transforms StorageRecord-s into text and writes them to a subdirectory of dir specified by WriterConsumer's dir field. Each StorageRecord is written to its own file.
func (wc WriterConsumer) ConsumeIps(ctx context.Context, id string, ips <-chan goat_grpc.IpRecord) (ResultsChannel, error) {
	res := make(chan Result)

	if err := ensureDirectoryExists(path.Join(wc.dir, id)); err != nil {
		return nil, err
	}

	go func() {
		defer close(res)

		for {
			select {
			case <-ips:
				// TODO process read value
			case <-ctx.Done():
				return
			}
		}

	}()

	return res, nil
}

// ConsumeVms transforms VmRecord-s into text and writes them to a subdirectory of dir specified by WriterConsumer's dir field. Each VmRecord is written to its own file.
func (wc WriterConsumer) ConsumeVms(ctx context.Context, id string, vms <-chan goat_grpc.VmRecord) (ResultsChannel, error) {
	res := make(chan Result)

	if err := ensureDirectoryExists(path.Join(wc.dir, id)); err != nil {
		return nil, err
	}

	go func() {
		defer close(res)

		for {
			select {
			case <-vms:
				// TODO process read value
			case <-ctx.Done():
				return
			}
		}

	}()

	return res, nil
}

// ConsumeStorages transforms StorageRecord-s into text and writes them to a subdirectory of dir specified by WriterConsumer's dir field. Each StorageRecord is written to its own file.
func (wc WriterConsumer) ConsumeStorages(ctx context.Context, id string, sts <-chan goat_grpc.StorageRecord) (ResultsChannel, error) {
	res := make(chan Result)

	if err := ensureDirectoryExists(path.Join(wc.dir, id)); err != nil {
		return nil, err
	}
	go func() {
		defer close(res)

		for {
			select {
			case <-sts:
				// TODO process read value
			case <-ctx.Done():
				return
			}
		}

	}()

	return res, nil
}
