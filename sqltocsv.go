// sqltocsv is a package to make it dead easy to turn arbitrary database query
// results (in the form of database/sql Rows) into CSV output.
//
// Source and README at https://github.com/joho/sqltocsv
package sqltocsv

import (
	"bytes"
	"database/sql"
	"encoding/base64"
	"encoding/csv"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"time"
)

// WriteFile will write a CSV file to the file name specified (with headers)
// based on whatever is in the sql.Rows you pass in. It calls WriteCsvToWriter under
// the hood.
func WriteFile(csvFileName string, rows *sql.Rows) error {
	return New(rows).WriteFile(csvFileName)
}

// WriteString will return a string of the CSV. Don't use this unless you've
// got a small data set or a lot of memory
func WriteString(rows *sql.Rows) (string, error) {
	return New(rows).WriteString()
}

// Write will write a CSV file to the writer passed in (with headers)
// based on whatever is in the sql.Rows you pass in.
func Write(writer io.Writer, rows *sql.Rows) error {
	return New(rows).Write(writer)
}

// CsvPreprocessorFunc is a function type for preprocessing your CSV.
// It takes the columns after they've been munged into strings but
// before they've been passed into the CSV writer.
//
// Return an outputRow of false if you want the row skipped otherwise
// return the processed Row slice as you want it written to the CSV.
type CsvPreProcessorFunc func(row []string, columnNames []string) (outputRow bool, processedRow []string)

type ByteArrayConverter int

const (
	// string([]byte)
	String = iota

	// Standard base64 encoding, as defined in RFC 4648.
	StdBase64

	// Alternate base64 encoding defined in RFC 4648.
	// It is typically used in URLs and file names.
	URLBase64

	// Standard raw, unpadded base64 encoding, as defined in RFC 4648 section 3.2.
	// This is the same as [StdBase64] but omits padding characters.
	RawStdBase64

	// Unpadded alternate base64 encoding defined in RFC 4648.
	// It is typically used in URLs and file names.
	// This is the same as [URLBase64] but omits padding characters.
	RawURLBase64

	// Hexadecimal encoding of src
	Hex
)

// Converter does the actual work of converting the rows to CSV.
// There are a few settings you can override if you want to do
// some fancy stuff to your CSV.
type Converter struct {
	Headers            []string           // Column headers to use (default is rows.Columns())
	WriteHeaders       bool               // Flag to output headers in your CSV (default is true)
	TimeFormat         string             // Format string for any time.Time values (default is time's default)
	FloatFormat        string             // Format string for any float64 and float32 values (default is %v)
	Delimiter          rune               // Delimiter to use in your CSV (default is comma)
	ByteArrayConverter ByteArrayConverter // How to convert []byte. By default string([]byte{})

	rows            *sql.Rows
	rowPreProcessor CsvPreProcessorFunc
}

// SetRowPreProcessor lets you specify a CsvPreprocessorFunc for this conversion
func (c *Converter) SetRowPreProcessor(processor CsvPreProcessorFunc) {
	c.rowPreProcessor = processor
}

// String returns the CSV as a string in an fmt package friendly way
func (c Converter) String() string {
	csv, err := c.WriteString()
	if err != nil {
		return ""
	}
	return csv
}

// WriteString returns the CSV as a string and an error if something goes wrong
func (c Converter) WriteString() (string, error) {
	buffer := bytes.Buffer{}
	err := c.Write(&buffer)
	return buffer.String(), err
}

// WriteFile writes the CSV to the filename specified, return an error if problem
func (c Converter) WriteFile(csvFileName string) error {
	f, err := os.Create(csvFileName)
	if err != nil {
		return err
	}

	err = c.Write(f)
	if err != nil {
		f.Close() // close, but only return/handle the write error
		return err
	}

	return f.Close()
}

// Write writes the CSV to the Writer provided
func (c Converter) Write(writer io.Writer) error {
	rows := c.rows
	csvWriter := csv.NewWriter(writer)
	if c.Delimiter != '\x00' {
		csvWriter.Comma = c.Delimiter
	}

	columnNames, err := rows.Columns()
	if err != nil {
		return err
	}

	if c.WriteHeaders {
		// use Headers if set, otherwise default to
		// query Columns
		var headers []string
		if len(c.Headers) > 0 {
			headers = c.Headers
		} else {
			headers = columnNames
		}
		err = csvWriter.Write(headers)
		if err != nil {
			return fmt.Errorf("failed to write headers: %w", err)
		}
	}

	count := len(columnNames)
	values := make([]any, count)
	valuePtrs := make([]any, count)

	for rows.Next() {
		row := make([]string, count)

		for i := range columnNames {
			valuePtrs[i] = &values[i]
		}

		if err = rows.Scan(valuePtrs...); err != nil {
			return err
		}

		for i := range columnNames {
			row[i] = c.toString(values[i])
		}

		writeRow := true
		if c.rowPreProcessor != nil {
			writeRow, row = c.rowPreProcessor(row, columnNames)
		}
		if writeRow {
			err = csvWriter.Write(row)
			if err != nil {
				return fmt.Errorf("failed to write data row to csv %w", err)
			}
		}
	}
	err = rows.Err()

	csvWriter.Flush()

	return err
}

// New will return a Converter which will write your CSV however you like
// but will allow you to set a bunch of non-default behaivour like overriding
// headers or injecting a pre-processing step into your conversion
func New(rows *sql.Rows) *Converter {
	return &Converter{
		rows:         rows,
		WriteHeaders: true,
		Delimiter:    ',',
	}
}

// toString converts any value to string.
func (c Converter) toString(v any) string {
	if v == nil {
		return ""
	}
	switch val := v.(type) {
	case string:
		return val
	case []byte:
		switch c.ByteArrayConverter {
		case String:
			return string(val)
		case StdBase64:
			return base64.StdEncoding.EncodeToString(val)
		case URLBase64:
			return base64.URLEncoding.EncodeToString(val)
		case RawStdBase64:
			return base64.RawStdEncoding.EncodeToString(val)
		case RawURLBase64:
			return base64.RawURLEncoding.EncodeToString(val)
		case Hex:
			return hex.EncodeToString(val)
		}
		return string(val)
	case bool:
		return strconv.FormatBool(val)
	case int:
		return strconv.Itoa(val)
	case int8:
		return strconv.FormatInt(int64(val), 10)
	case int16:
		return strconv.FormatInt(int64(val), 10)
	case int32:
		return strconv.FormatInt(int64(val), 10)
	case int64:
		return strconv.FormatInt(val, 10)
	case uint:
		return strconv.FormatUint(uint64(val), 10)
	case uint8:
		return strconv.FormatUint(uint64(val), 10)
	case uint16:
		return strconv.FormatUint(uint64(val), 10)
	case uint32:
		return strconv.FormatUint(uint64(val), 10)
	case uint64:
		return strconv.FormatUint(val, 10)
	case time.Time:
		if c.TimeFormat != "" {
			return val.Format(c.TimeFormat)
		}
		return val.String()
	case float32:
		if c.FloatFormat != "" {
			return fmt.Sprintf(c.FloatFormat, val)
		}
		return strconv.FormatFloat(float64(val), 'f', -1, 32)
	case float64:
		if c.FloatFormat != "" {
			return fmt.Sprintf(c.FloatFormat, val)
		}
		return strconv.FormatFloat(val, 'f', -1, 64)
	}
	if jsonMarshaler, ok := v.(json.Marshaler); ok {
		if jsonData, err := jsonMarshaler.MarshalJSON(); err == nil {
			return strings.Trim(string(jsonData), `"`)
		}
	}
	if fmtStringer, ok := v.(fmt.Stringer); ok {
		return fmtStringer.String()
	}
	if jsonData, err := json.Marshal(v); err == nil {
		return strings.Trim(string(jsonData), `"`)
	}
	return fmt.Sprintf("%v", v)
}
