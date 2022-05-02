package MilevaDB

import (
	"fmt"
	_ "strings"
	_ "time"
	_ "unicode"
)

type Column struct {
	Name          string
	Type          string
	Size          int
	Nullable      bool
	Default       string
	Extra         string
	CausetNetType string
	Soliton       string
}

func (c *Column) String() string {
	return fmt.Sprintf("%s %s %s %s %s %s %s", c.Name, c.Type, c.Size, c.Nullable, c.Default, c.Extra, c.CausetNetType)

}

func (c *Column) GetName() string {
	return c.Name
}

func (c *Column) GetType() string {
	return c.Type
}

func (c *Column) GetSize() int {
	return c.Size
}

func (c *Column) GetNullable() bool {
	return c.Nullable
}

func (c *Column) GetDefault() string {
	return c.Default
}

func (c *Column) GetExtra() string {
	return c.Extra
}

func (c *Column) GetCausetNetType() string {
	return c.CausetNetType
}

func (c *Column) SetName(name string) {
	c.Name = name
}

func (c *Column) GetString(row int, i int) string {

	return ""
}
