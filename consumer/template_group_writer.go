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
	templateFileName = "vm.tmpl"
	templateName     = "VMS"
	filenameFormat   = "%014d"
)

type vmsTemplateData struct {
	Vms []interface{}
}

// TemplateGroupWriter converts each record to template and writes it to file.
// Multiple records may be written into a single file.
type TemplateGroupWriter struct {
	outputDir    string
	templatesDir string
	countPerFile uint64
	template     *template.Template
	records      []interface{}
}

// NewTemplateGroupWriter creates a new TemplateGroupWriter.
func NewTemplateGroupWriter(outputDir, templatesDir string, countPerFile uint64) TemplateGroupWriter {
	return TemplateGroupWriter{
		outputDir:    outputDir,
		templatesDir: templatesDir,
		countPerFile: countPerFile,
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

	template, err := template.ParseGlob(path.Join(tgw.templatesDir, templateFileName))
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
			// goroutine has been canceled
			return
		}
	}
}

func (tgw TemplateGroupWriter) writeFile(id string, countInFile, filenameCounter uint64) error {
	newRecords := make([]interface{}, countInFile)
	copy(newRecords, tgw.records)
	templateData := vmsTemplateData{Vms: newRecords}
	filename := path.Join(tgw.outputDir, path.Join(id, fmt.Sprintf(filenameFormat, filenameCounter)))
	// open the file
	file, err := os.Create(filename)
	if err != nil {
		return err
	}

	// write templateData to file
	err = tgw.template.ExecuteTemplate(file, templateName, templateData)
	if err != nil {
		return err
	}

	// close file
	return file.Close()
}

// Consume converts each record to template and writes it to file.
// Multiple records may be written into a single file.
func (tgw TemplateGroupWriter) Consume(ctx context.Context, id string, records <-chan wrapper.RecordWrapper) (ResultsChannel, error) {
	res := make(chan Result)

	if err := ensureDirectoryExists(path.Join(tgw.outputDir, id)); err != nil {
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
						err := tgw.writeFile(id, countInFile, filenameCounter)
						// but we have something to save!
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
					err := tgw.writeFile(id, countInFile, filenameCounter)
					if err != nil {
						trySendError(ctx, res, err)
					}

					// increase filename counter
					filenameCounter++

					// reset record in file counter
					countInFile = 0
				}
			case <-ctx.Done():
				// goroutine has been canceled
				return
			}
		}
	}()

	return res, nil
}
