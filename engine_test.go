package gosqlfilter

import (
	"strings"
	"testing"

	"github.com/akito0107/xsqlparser"
	"github.com/akito0107/xsqlparser/dialect"
	"github.com/akito0107/xsqlparser/sqlast"
)

func Test_evalCondition(t *testing.T) {
	type args struct {
		row   map[string]interface{}
		query string
	}
	tests := []struct {
		name    string
		args    args
		want    bool
		wantErr bool
	}{
		{
			name: "IntEq",
			args: args{
				row: map[string]interface{}{
					"a": 1,
				},
				query: `SELECT * FROM t WHERE a = 1`,
			},
			want: true,
		},
		{
			name: "IntEq/2=1",
			args: args{
				row: map[string]interface{}{
					"a": 2,
				},
				query: `SELECT * FROM t WHERE a = 1`,
			},
			want: false,
		},
		{
			name: "IntNotEq",
			args: args{
				row: map[string]interface{}{
					"a": 1,
				},
				query: `SELECT * FROM t WHERE a != 1`,
			},
			want: false,
		},
		{
			name: "Not",
			args: args{
				row: map[string]interface{}{
					"a": 1,
				},
				query: `SELECT * FROM t WHERE NOT (a != 1)`,
			},
			want: true,
		},
		{
			name: "IntIn",
			args: args{
				row: map[string]interface{}{
					"a": 1,
				},
				query: `SELECT * FROM t WHERE a IN (2, 3, 1)`,
			},
			want: true,
		},
		{
			name: "IntLe",
			args: args{
				row: map[string]interface{}{
					"a": 2,
				},
				query: `SELECT * FROM t WHERE a < 1`,
			},
			want: false,
		},
		{
			name: "IntLte",
			args: args{
				row: map[string]interface{}{
					"a": 2,
				},
				query: `SELECT * FROM t WHERE a <= 1`,
			},
			want: false,
		},
		{
			name: "IntGe",
			args: args{
				row: map[string]interface{}{
					"a": 2,
				},
				query: `SELECT * FROM t WHERE a > 1`,
			},
			want: true,
		},
		{
			name: "IntGte",
			args: args{
				row: map[string]interface{}{
					"a": 2,
				},
				query: `SELECT * FROM t WHERE a >= 1`,
			},
			want: true,
		},
		{
			name: "IntEqDoubleQuote",
			args: args{
				row: map[string]interface{}{
					"a": 1,
				},
				query: `SELECT * FROM t WHERE a = "1"`,
			},
			want: true,
		},
		{
			name: "StrEq",
			args: args{
				row: map[string]interface{}{
					"a": "1",
				},
				query: `SELECT * FROM t WHERE a = '1'`,
			},
			want: true,
		},
		{
			name: "StrLike/Prefix",
			args: args{
				row: map[string]interface{}{
					"a": "abcdef",
				},
				query: `SELECT * FROM t WHERE a LIKE 'abc%'`,
			},
			want: true,
		},
		{
			name: "StrLike/Suffix",
			args: args{
				row: map[string]interface{}{
					"a": "abcdef",
				},
				query: `SELECT * FROM t WHERE a LIKE '%def'`,
			},
			want: true,
		},
		{
			name: "StrLike/Contains",
			args: args{
				row: map[string]interface{}{
					"a": "abcdef",
				},
				query: `SELECT * FROM t WHERE a LIKE '%cd%'`,
			},
			want: true,
		},
		{
			name: "StrLike/Eq",
			args: args{
				row: map[string]interface{}{
					"a": "abcdef",
				},
				query: `SELECT * FROM t WHERE a LIKE 'cd'`,
			},
			want: false,
		},
		{
			name: "StrEqInt",
			args: args{
				row: map[string]interface{}{
					"a": "1",
				},
				query: `SELECT * FROM t WHERE a = 1`,
			},
			want: false,
		},
		{
			name: "Or",
			args: args{
				row: map[string]interface{}{
					"a": 1,
					"b": 2,
				},
				query: `SELECT * FROM t WHERE a = 2 OR b = 2`,
			},
			want: true,
		},
		{
			name: "And",
			args: args{
				row: map[string]interface{}{
					"a": 1,
					"b": 2,
				},
				query: `SELECT * FROM t WHERE a = 2 AND b = 2`,
			},
			want: false,
		},
		{
			name: "Compound",
			args: args{
				row: map[string]interface{}{
					"a": 1,
					"b": 2,
					"c": "bar",
				},
				query: `SELECT * FROM t WHERE NOT (a = 2 AND b = 2) AND c IN ('foo', 'bar', 'foobar')`,
			},
			want: true,
		},
		{
			name: "NoVariable",
			args: args{
				row: map[string]interface{}{
				},
				query: `SELECT * FROM t WHERE a = 1`,
			},
			want: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p, err := xsqlparser.NewParser(strings.NewReader(tt.args.query), &dialect.MySQLDialect{})
			if err != nil {
				t.Fatal(err)
			}
			stmt, err := p.ParseStatement()
			if err != nil {
				t.Fatal(err)
			}
			where := stmt.(*sqlast.QueryStmt).Body.(*sqlast.SQLSelect).WhereClause
			got, err := evalCondition(tt.args.row, where)
			if (err != nil) != tt.wantErr {
				t.Errorf("evalCondition() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("evalCondition() got = %v, want %v", got, tt.want)
			}
		})
	}
}
