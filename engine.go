package gosqlfilter

import (
	"fmt"
	"strings"

	"github.com/akito0107/xsqlparser"
	"github.com/akito0107/xsqlparser/dialect"
	"github.com/akito0107/xsqlparser/sqlast"
)

type Engine struct {
	tableName   string
	selectQuery *sqlast.SQLSelect
}

func (e *Engine) Row(row map[string]interface{}) error {
	ok, err := evalCondition(row, e.selectQuery.WhereClause)
	if err != nil {
		return fmt.Errorf("eval condition: %w", err)
	}
	if !ok {
		return nil
	}
	return nil
}

func Query(sqlStr string) (*Engine, error) {
	p, err := xsqlparser.NewParser(strings.NewReader(sqlStr), &dialect.MySQLDialect{})
	if err != nil {
		return nil, fmt.Errorf("new parser: %w", err)
	}
	stmt, err := p.ParseStatement()
	if err != nil {
		return nil, fmt.Errorf("parse statement: %w", err)
	}
	query, ok := stmt.(*sqlast.QueryStmt)
	if !ok {
		return nil, fmt.Errorf("unsupported statement: %T", stmt)
	}
	selectQuery, ok := query.Body.(*sqlast.SQLSelect)
	if !ok {
		return nil, fmt.Errorf("unsupported query: %T", query.Body)
	}
	if len(selectQuery.FromClause) != 1 {
		return nil, fmt.Errorf("multiple from clauses are unsupported: %d", len(selectQuery.FromClause))
	}
	from := selectQuery.FromClause[0]
	table, ok := from.(*sqlast.Table)
	if !ok {
		return nil, fmt.Errorf("unsupported table reference: %T", from)
	}
	return &Engine{
		selectQuery: selectQuery,
		tableName:   table.Name.ToSQLString(),
	}, nil
}

func evalCondition(row map[string]interface{}, node sqlast.Node) (bool, error) {
	switch n := node.(type) {
	case *sqlast.Nested:
		return evalCondition(row, n.AST)
	case *sqlast.UnaryExpr:
		switch n.Op.Type {
		case sqlast.Not:
			ok, err := evalCondition(row, n.Expr)
			if err != nil {
				return false, err
			}
			return !ok, nil
		}
	case *sqlast.InList:
		column, ok := n.Expr.(*sqlast.Ident)
		if !ok {
			return false, fmt.Errorf("in list expr is must be ident: %T", n.Expr)
		}
		v, ok := row[column.Value]
		if !ok {
			return false, nil
		}
		for _, item := range n.List {
			switch n := item.(type) {
			case *sqlast.SingleQuotedString:
				s, ok := v.(string)
				if ok && s == n.String {
					return true, nil
				}
			case *sqlast.LongValue:
				i, ok := v.(int)
				if ok && i == int(n.Long) {
					return true, nil
				}
			case *sqlast.DoubleValue:
				f, ok := v.(float64)
				if ok && f == n.Double {
					return true, nil
				}
			}
		}
		return false, nil
	case *sqlast.BinaryExpr:
		switch n.Op.Type {
		case sqlast.And:
			lOk, err := evalCondition(row, n.Left)
			if err != nil {
				return false, err
			}
			if !lOk {
				return false, nil
			}
			rOk, err := evalCondition(row, n.Right)
			if err != nil {
				return false, err
			}
			return rOk, nil
		case sqlast.Or:
			lOk, err := evalCondition(row, n.Left)
			if err != nil {
				return false, err
			}
			if lOk {
				return true, nil
			}
			rOk, err := evalCondition(row, n.Right)
			if err != nil {
				return false, err
			}
			return rOk, nil
		}
		column, ok := n.Left.(*sqlast.Ident)
		if !ok {
			return false, fmt.Errorf("left is must be ident: %T", n.Left)
		}
		v, ok := row[column.Value]
		if !ok {
			return false, nil
		}
		switch rn := n.Right.(type) {
		case *sqlast.LongValue:
			l, ok := v.(int)
			if !ok {
				return false, nil
			}
			r := int(rn.Long)
			switch n.Op.Type {
			case sqlast.Eq:
				return l == r, nil
			case sqlast.NotEq:
				return l != r, nil
			case sqlast.Gt:
				return l > r, nil
			case sqlast.GtEq:
				return l >= r, nil
			case sqlast.Lt:
				return l < r, nil
			case sqlast.LtEq:
				return l <= r, nil
			default:
				return false, fmt.Errorf("unsupported int expression: %s", n.Op.ToSQLString())
			}
		case *sqlast.SingleQuotedString:
			l, ok := v.(string)
			if !ok {
				return false, nil
			}
			r := rn.String
			switch n.Op.Type {
			case sqlast.Eq:
				return l == r, nil
			case sqlast.NotEq:
				return l != r, nil
			case sqlast.Gt:
				return l > r, nil
			case sqlast.GtEq:
				return l >= r, nil
			case sqlast.Lt:
				return l < r, nil
			case sqlast.LtEq:
				return l <= r, nil
			case sqlast.Like:
				prefix := strings.HasPrefix(r, "%")
				suffix := strings.HasSuffix(r, "%")
				s := strings.Trim(r, "%")
				if prefix && suffix {
					return strings.Contains(l, s), nil
				}
				if prefix {
					return strings.HasSuffix(l, s), nil
				}
				if suffix {
					return strings.HasPrefix(l, s), nil
				}
				return l == r, nil
			default:
				return false, fmt.Errorf("unsupported string expression: %s", n.Op.ToSQLString())
			}
		case *sqlast.DoubleValue:
			l, ok := v.(float64)
			if !ok {
				return false, nil
			}
			r := rn.Double
			switch n.Op.Type {
			case sqlast.Eq:
				return l == r, nil
			case sqlast.NotEq:
				return l != r, nil
			case sqlast.Gt:
				return l > r, nil
			case sqlast.GtEq:
				return l >= r, nil
			case sqlast.Lt:
				return l < r, nil
			case sqlast.LtEq:
				return l <= r, nil
			}
		case *sqlast.BooleanValue:
			l, ok := v.(bool)
			if !ok {
				return false, nil
			}
			r := rn.Boolean
			switch n.Op.Type {
			case sqlast.Eq:
				return l == r, nil
			case sqlast.NotEq:
				return l != r, nil
			}
		}
	}
	return true, nil
}
