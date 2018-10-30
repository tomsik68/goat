package consumer

import (
	"context"
	"encoding/json"
	"github.com/goat-project/goat-proto-go"
	"io"
	"os"
	"path"
)

// IPJSONConsumer consumes IpRecord-s and writes them to JSON files
type IPJSONConsumer struct {
	dir string
}

// NewIPJSONConsumer creates a new IPJSONConsumer
func NewIPJSONConsumer(dir string) IPJSONConsumer {
	return IPJSONConsumer{
		dir: dir,
	}
}

func (ic IPJSONConsumer) ensureDirectoryExists(id string) error {
	err := os.MkdirAll(path.Join(ic.dir, id), os.ModeDir)
	if err != nil && err != os.ErrExist {
		return err
	}

	return nil
}

func (ic IPJSONConsumer) getIPFileName(ip goat_grpc.IpRecord) string {
	// TODO implement properly
	return "ip.json"
}

func (ic IPJSONConsumer) ipToJSON(ip goat_grpc.IpRecord, w io.Writer) error {
	e := json.NewEncoder(w)
	// TODO maybe change structure of ip
	return e.Encode(ip)
}

func (ic IPJSONConsumer) writeIP(id string, ip goat_grpc.IpRecord) error {
	file, err := os.Open(path.Join(path.Join(ic.dir, id), ic.getIPFileName(ip)))
	defer func() {
		cerr := file.Close()
		if err == nil {
			err = cerr
		}
	}()

	if err != nil {
		return err
	}

	if err = ic.ipToJSON(ip, file); err != nil {
		return err
	}

	return err
}

// ConsumeIps writes all ip records from the channel to json files
func (ic IPJSONConsumer) ConsumeIps(ctx context.Context, id string, ips <-chan goat_grpc.IpRecord) (ResultsChannel, error) {
	res := make(chan Result)

	if err := ic.ensureDirectoryExists(id); err != nil {
		return nil, err
	}

	go func() {
		defer close(res)

		for {
			select {
			case ip := <-ips:
				r := NewResultFromError(ic.writeIP(id, ip))
				res <- r
			case <-ctx.Done():
				return
			}
		}
	}()

	return res, nil
}
