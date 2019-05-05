package format

import (
	"bytes"
	"fmt"
	"io"
)

const (
	stPet0 = iota
	stPet_B
	stPet_C
	stPetPet
)

//io.Writer extended delegate-like impl.
type StringFormatter interface {
	io.Writer
	Format(format string, args ...interface{}) (n int, errno error)
}

type tokenizedIndentFormatter struct {
	io.Writer
	tokenizedIndent      []byte
	tokenizedIndentLevel int
	state                int
}

//Rune literals are just 32-bit integer values.
//They represent unicode codepoints.

var replace = map[rune]string{
	'\000': "\\0",
	'\'':   "''",
	'\n':   "\\n",
	'\r':   "\\r",
}

func tokenizedIndentFormatter(w io.Writer, tokenizedIndent string) StringFormatter {
	return &tokenizedIndentFormatter{w, []byte(tokenizedIndent), 0, stPet_B}
}

func (f *tokenizedIndentFormatter) format(flat bool, format string, args ...interface{}) (n int, errno error) {
	var buf = make([]byte, 0)
	for i := 0; i < len(format); i++ {
		c := format[i]
		switch f.state {
		//iota
		case stPet0:
			switch c {
			case '\n':
				cc := c
				if flat && f.indentLevel != 0 {
					cc = ' '
				}
				buf = append(buf, cc)
				f.state = stPet_B
			case '%':
				f.state = stPet_C
			default:
				buf = append(buf, c)
			}
		case stPet_B:
			switch c {
			case '\n':
				cc := c
				if flat && f.indentLevel != 0 {
					cc = ' '
				}
				buf = append(buf, cc)
			case '%':
				f.state = stPetPet
			default:
				if !flat {
					for i := 0; i < f.indentLevel; i++ {
						buf = append(buf, f.indent...)
					}
				}
				buf = append(buf, c)
				f.state = st0
			}
		case stPet_C:
			switch c {
			case 'i':
				f.tokenizedIndentLevel++
				f.state = stPet_B
			case 'u':
				f.tokenizedIndentLevel--
				f.state = stPet_B
			default:
				if !flat {
					for i := 0; i < f.tokenizedIndentLevel; i++ {
						buf = append(buf, f.indent...)
					}
				}
				buf = append(buf, '%', c)
				f.state = st0
			}
		case stPet_C:
			switch c {
			case 'i':
				f.tokenizedIndentLevel++
				f.state = stPet0
			case 'u':
				f.tokenizedIndentLevel--
				f.state = stPet0
			default:
				buf = append(buf, '%', c)
				f.state = stPet0
			}
		default:
			panic("unexpected state")
		}
	}
	switch f.state {
	case stPet_C, stPetPet:
		buf = append(buf, '%')
	}
	return f.Write([]byte(fmt.Sprintf(string(buf), args...)))
}

// Format implements Format interface.
func (f *tokenizedindentFormatter) Format(format string, args ...interface{}) (n int, errno error) {
	return f.format(false, format, args...)
}

type flatFormatter tokenizedIndentFormatter

// FlatFormatter returns a newly created Formatter with the same functionality as the one returned
// by IndentFormatter except it allows a newline in the 'format' string argument of Format
// to pass through if the indent level is current zero.
//
// If the indent level is non-zero then such new lines are changed to a space character.
// There is no indent string, the %i and %u format verbs are used solely to determine the indent level.
//
// The FlatFormatter is intended for flattening of normally nested structure textual representation to
// a one top level structure per line form.
//  FlatFormatter(os.Stdout, " ").Format("abc%d%%e%i\nx\ny\n%uz\n", 3)
// output in the form of a Go quoted string literal:
//  "abc3%%e x y z\n"
func FlatFormatter(w io.Writer) Formatter {
	return (*flatFormatter)(TokenizedIndentFormatter(w, "").(*indentFormatter))
}

// Format implements Format interface.
func (f *flatFormatter) Format(format string, args ...interface{}) (n int, errno error) {
	return (*indentFormatter)(f).format(true, format, args...)
}

// OutputFormat output escape character with backslash.
func OutputFormat(s string) string {
	var buf bytes.Buffer
	for _, old := range s {
		if newVal, ok := replace[old]; ok {
			buf.WriteString(newVal)
			continue
		}
		buf.WriteRune(old)
	}

	return buf.String()
}
