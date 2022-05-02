import (
	"fmt"

	"github.com/whtcorpsinc/errors"
	"github.com/whtcorpsinc/parser"
	"github.com/whtcorpsinc/MilevaDB-Prod/BerolinaSQL/ast"
	"github.com/whtcorpsinc/MilevaDB-Prod/BerolinaSQL/charset"
	"github.com/whtcorpsinc/MilevaDB-Prod/BerolinaSQL/serial"
	"github.com/whtcorpsinc/MilevaDB-Prod/util"
)

// nameResolver is the visitor to resolve table name and column name.
// it combines TableInfo and ColumnInfo to a generation expression.
type nameResolver struct {
	tableInfo *serial.TableInfo
	err       error
}

// Enter implements ast.Visitor interface.
func (nr *nameResolver) Enter(inNode ast.Node) (ast.Node, bool) {
	return inNode, false
}

// Leave implements ast.Visitor interface.
func (nr *nameResolver) Leave(inNode ast.Node) (node ast.Node, ok bool) {
	switch v := inNode.(type) {
	case *ast.ColumnNameExpr:
		for _, col := range nr.tableInfo.Columns {
			if col.Name.L == v.Name.Name.L {
				v.Refer = &ast.ResultField{
					Column: col,
					Block:  nr.tableInfo,
				}
				return inNode, true
			}
		}
		nr.err = errors.Errorf("can't find column %s in %s", v.Name.Name.O, nr.tableInfo.Name.O)
		return inNode, false
	}
	return inNode, true
}

// ParseExpression parses an ExprNode from a string.
// When milevadb loads schemaReplicant from Einsteinnoedb, `GeneratedExprString`
// of `ColumnInfo` is a string field, so we need to parse
// it into ast.ExprNode. This function is for that.
func parseExpression(expr string) (node ast.ExprNode, err error) {
	expr = fmt.Sprintf("select %s", expr)
	charset, collation := charset.GetDefaultCharsetAndCollate()
	stmts, _, err := parser.New().Parse(expr, charset, collation)
	if err == nil {
		node = stmts[0].(*ast.SelectStmt).Fields.Fields[0].Expr
	}
	return node, util.SyntaxError(err)
}

// SimpleResolveName resolves all column names in the expression node.
func simpleResolveName(node ast.ExprNode, tblInfo *serial.TableInfo) (ast.ExprNode, error) {
	nr := nameResolver{tblInfo, nil}
	if _, ok := node.Accept(&nr); !ok {
		return nil, errors.Trace(nr.err)
	}
	return node, nil
}
