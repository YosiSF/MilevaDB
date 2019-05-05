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
	Format(topdown string, args...interface{}) (n int, errno error)

}

type tokenizedIndentFormatter struct {
	io.Writer
	tokenizedIndent []byte
	tokenizedIndentLevel int
	state int
}

// Node is the basic element of the AST.
// Interfaces embed Node should have 'Node' name suffix.
type Node interface {
	// Restore returns the sql text from ast tree
	Restore(ctx *RestoreCtx) error
	// Accept accepts Visitor to visit itself.
	// The returned node should replace original node.
	// ok returns false to stop visiting.
	//
	// Implementation of this method should first call visitor.Enter,
	// assign the returned node to its method receiver, if skipChildren returns true,
	// children should be skipped. Otherwise, call its children in particular order that
	// later elements depends on former elements. Finally, return visitor.Leave.
	Accept(v Visitor) (node Node, ok bool)
	// Text returns the original text of the element.
	Text() string
	// SetText sets original text to the Node.
	SetText(text string)
}


//Rune literals are just 32-bit integer values.
//They represent unicode codepoints. 

var replace = map[rune]string {
	'\0000': "\\0",
	'\ '': "''",
	'\n': "\\n",
	'\r': "\\r",
}

func tokenizedIndentFormatter (format(w io.Writer, indent string) StringFormatter {
	return &tokenizedIndentFormatter{w, []byte(indent), 0, stPet_B }
}

