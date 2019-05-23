package consumer

import (
	"context"
	"fmt"
	"github.com/goat-project/goat/consumer/wrapper"
	"os"
	"path"
	"text/template"
)

const (
	templateFileExtension = "tmpl"
	filenameFormat        = "%014d"
)

// TemplateGroupWriter converts each record to template and writes it to file. Multiple records may be written into a single file.
type TemplateGroupWriter struct {
	dir          string
	templatesDir string
	countPerFile uint64
	templateName string
	template     *template.Template
	records      []interface{}
}

// NewTemplateGroupWriter creates a new TemplateGroupWriter.
func NewTemplateGroupWriter(dir, templatesDir, templateName string, countPerFile uint64) TemplateGroupWriter {
	return TemplateGroupWriter{
		dir:          dir,
		templatesDir: templatesDir,
		countPerFile: countPerFile,
		templateName: templateName,
		template:     nil,
		records:      make([]interface{}, countPerFile),
	}
}

func ensureDirectoryExists(path string) error {
	err := os.MkdirAll(path, os.ModeDir)
	if err != nil && err != os.ErrExist {
		return err
	}

	return nil
}

func (tgw *TemplateGroupWriter) initTemplates() error {
	if tgw.template != nil {
		return nil
	}

	template, err := template.ParseGlob(path.Join(tgw.templatesDir, fmt.Sprintf("*.%s", templateFileExtension)))
	tgw.template = template
	return err
}

func trySendError(ctx context.Context, res chan<- Result, err error) {
	for {
		select {
		case res <- NewErrorResult(err):
			// error sent successfully
			return
		case <-ctx.Done():
			// goroutine has been cancelled
			return
		}
	}
}

func writeFile(records []interface{}, recordCount uint64, template *template.Template, filename, templateName string) error {
	// open the file
	file, err := os.Create(filename)
	if err != nil {
		return err
	}

	// write templateData to file
	err = template.ExecuteTemplate(file, templateName, records)
	if err != nil {
		return err
	}

	// close file
	return file.Close()
}

// Consume converts each record to template and writes it to file. Multiple records may be written into a single file.
func (tgw TemplateGroupWriter) Consume(ctx context.Context, id string, records <-chan wrapper.RecordWrapper) (ResultsChannel, error) {
	res := make(chan Result)

	if err := ensureDirectoryExists(path.Join(tgw.dir, id)); err != nil {
		return nil, err
	}

	if err := tgw.initTemplates(); err != nil {
		return nil, err
	}

	go func() {

		defer close(res)

		var countInFile, filenameCounter uint64
		countInFile, filenameCounter = 0, 0
		for {
			select {
			case templateData, ok := <-records:
				if !ok {
					// end of stream
					if countInFile > 0 {
						// but we have something to save!
						err := writeFile(tgw.records, countInFile, tgw.template, path.Join(tgw.dir, path.Join(id, fmt.Sprintf(filenameFormat, filenameCounter))), "VMS")
						if err != nil {
							trySendError(ctx, res, err)
						}
					}
					return
				}

				// convert received record to template
				rec, err := templateData.AsTemplate()
				if err != nil {
					trySendError(ctx, res, err)
				}

				// save it for later
				tgw.records[countInFile] = rec

				countInFile++

				// if we already have this many records in the file
				if countInFile == tgw.countPerFile {

					err = writeFile(tgw.records, countInFile, tgw.template, path.Join(tgw.dir, path.Join(id, fmt.Sprintf(filenameFormat, filenameCounter))), "VMS")
					if err != nil {
						trySendError(ctx, res, err)
					}

					// increase filename counter
					filenameCounter++

					// reset record in file counter
					countInFile = 0
				}
			case <-ctx.Done():
				// goroutine has been cancelled
				return
			}
		}
	}()

	return res, nil
}
