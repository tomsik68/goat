package consumer

import (
	"context"
	"github.com/goat-project/goat-proto-go"
	"io"
	"os"
	"path"
	"text/template"
)

// WriterConsumer processes accounting data by transforming them according to supplied templates and subsequently writing to a file
type WriterConsumer struct {
	dir          string
	templatesDir string
	template     *template.Template
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

func (wc *WriterConsumer) initTemplates() error {
	if wc.template != nil {
		return nil
	}
	// TODO check file ext
	template, err := template.ParseGlob(path.Join(wc.templatesDir, "*.tmpl"))
	wc.template = template
	return err
}

func getIPFileName(ip goat_grpc.IpRecord) string {
	// TODO
	return ""
}

func getVMFileName(vm goat_grpc.VmRecord) string {
	return vm.GetVmUuid()
}

func getStorageFileName(storage goat_grpc.StorageRecord) string {
	return storage.GetRecordID()
}

func (wc WriterConsumer) processTo(record interface{}, wr io.Writer) error {
	return wc.template.Execute(wr, record)
}

func (wc WriterConsumer) writeIP(id string, ip goat_grpc.IpRecord) error {

	file, err := os.Open(path.Join(path.Join(wc.dir, id), getIPFileName(ip)))
	if err != nil {
		return err
	}

	return wc.processTo(ip, file)
}

func (wc WriterConsumer) writeVM(id string, vm goat_grpc.VmRecord) error {

	file, err := os.Open(path.Join(path.Join(wc.dir, id), getVMFileName(vm)))
	if err != nil {
		return err
	}

	return wc.processTo(vm, file)
}

func (wc WriterConsumer) writeStorage(id string, storage goat_grpc.StorageRecord) error {

	file, err := os.Open(path.Join(path.Join(wc.dir, id), getStorageFileName(storage)))
	if err != nil {
		return err
	}

	return wc.processTo(storage, file)
}

// ConsumeIps transforms IpRecord-s into text and writes them to a subdirectory of dir specified by WriterConsumer's dir field. Each IpRecord is written to its own file.
func (wc WriterConsumer) ConsumeIps(ctx context.Context, id string, ips <-chan goat_grpc.IpRecord) (ResultsChannel, error) {
	res := make(chan Result)

	if err := ensureDirectoryExists(path.Join(wc.dir, id)); err != nil {
		return nil, err
	}

	if err := wc.initTemplates(); err != nil {
		return nil, err
	}

	go func() {
		defer close(res)

		for {
			select {
			case ip := <-ips:
				r := NewResultFromError(wc.writeIP(id, ip))
				res <- r
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

	if err := wc.initTemplates(); err != nil {
		return nil, err
	}

	go func() {
		defer close(res)

		for {
			select {
			case vm := <-vms:
				r := NewResultFromError(wc.writeVM(id, vm))
				res <- r
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

	if err := wc.initTemplates(); err != nil {
		return nil, err
	}

	go func() {
		defer close(res)

		for {
			select {
			case st := <-sts:
				r := NewResultFromError(wc.writeStorage(id, st))
				res <- r
			case <-ctx.Done():
				return
			}
		}

	}()

	return res, nil
}
