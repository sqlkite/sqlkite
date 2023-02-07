package parser

import (
	"src.goblgobl.com/utils/validation"
	"src.sqlkite.com/sqlkite/sql"
)

/*
Reminder for my future self:
1 - Suffix
The inner-most functions often don't have enough context to validate
the suffix. Consider the objectName function, which is meant to parse
things like column and table names. Clearly "s@and" isn't a valid
objectName. But what about "sand." or "sand)"? The first could be
valid if the objectName is an alias prefix (e.g t1.id), and the latter
could be valid if we ever support functions. Therefore, inner-most
functions can return invalid values, and it's up to the caller to determine
if the suffix is valid within that context (e.g. by using peek()).

2 - Spaces
It's tempting to call skipSpaces all over the place, but that would be
inefficient. These functions either piece multiple tokens together
or parse individual tokens. You could call these "outer" and "inner"
functions (though some do both). Anyways, the pattern I've settled on
is that the "outer" function is responsible for skipping spaces. That is
any code that's trying to group multiple tokens must advance beyond any
space before trying to parse an individual token.
*/

var EmptyParser = &parser{0, ""}

type parser struct {
	pos   int
	input string
}

// A column name with no table prefix or alias. The column list of an insert
// statement uses this.
func Column(raw any) (string, *validation.Invalid) {
	input, ok := raw.(string)
	if !ok {
		return "", invalidColumn(EmptyParser)
	}
	p := &parser{0, input}
	p.skipSpaces()

	name, err := p.requiredObjectName(invalidColumn)
	if err != nil {
		return "", err
	}

	// should have nothing else
	p.skipSpaces()
	if p.peek() != 0 {
		return "", invalidColumn(p)
	}

	return name, nil
}

// A DataField is a column or placeholder, possibly with an alias. This could
// eventually become fancier, like functions. DataFields are used throughout,
// most notably they're the elements that come between the "select" and "from"
// or the list of values in a "returning" statement. (Most of the time, these
// are just column names)
func DataField(raw any) (sql.DataField, *validation.Invalid) {
	// for now, we only support simple values
	input, ok := raw.(string)
	if !ok {
		return sql.DataField{}, invalidColumnType()
	}

	p := &parser{0, input}
	p.skipSpaces()

	field, err := p.dataField(invalidColumn)
	if err != nil {
		return sql.DataField{}, err
	}

	if next := p.skip1(); next == ' ' {
		p.skipSpaces()
		field.Alias, err = p.alias()
		if err != nil {
			return sql.DataField{}, err
		}
	} else if next != 0 {
		if field.Type == sql.DATA_FIELD_PLACEHOLDER {
			return sql.DataField{}, invalidPlaceholder(p)
		} else {
			return sql.DataField{}, invalidColumn(p)
		}
	}
	return field, nil
}

// Conditions are used in both where clauses and the on clause of a join
// A condition is an array of predicates and/or conditions, joined with
// the logical operators "or" or "and". As such, it is always an odd number
// For example:
//
//	[pred1]
//	[pred1, "or", pred2]
//	[pred1, "or", pred2, "and", [pred3, "or", pred4]]
//
// We enforce a maximum nesting to prevent abuse.
func Condition(input []any, depth int) (sql.Condition, *validation.Invalid) {
	if depth > 5 {
		return sql.Condition{}, invalidConditionDepth()
	}

	l := len(input)

	// we always expect an odd number of entries, beause we expect
	// predicate1 [AND/OR predicate2, [AND/OR predicate3, ...]]
	if l%2 != 1 {
		return sql.Condition{}, invalidConditionCount()
	}

	if l == 0 {
		return sql.EmptyCondition, nil
	}

	pairs := l / 2
	var logicals []sql.LogicalOperator
	if l > 1 {
		logicals = make([]sql.LogicalOperator, pairs)
	}
	parts := make([]sql.Part, pairs+1)

	var err *validation.Invalid
	parts[0], err = predicate(input[0], depth)
	if err != nil {
		return sql.Condition{}, err
	}

	for i := 1; i < l; {
		index := i / 2
		logical, _ := input[i].(string)
		switch trimSpace(logical) {
		case "and":
			logicals[index] = sql.LOGICAL_AND
		case "or":
			logicals[index] = sql.LOGICAL_OR
		default:
			return sql.Condition{}, invalidConditionLogical(logical)
		}

		inputIndex := i + 1
		parts[index+1], err = predicate(input[inputIndex], depth)
		if err != nil {
			return sql.Condition{}, err
		}
		i += 2
	}

	return sql.Condition{
		Parts:    parts,
		Logicals: logicals,
	}, nil
}

// Parses a single table name with an optional alias. This is
// different and simpler than a JoinableFrom as it doesn't allow join
// types and conditions (it's used for update/delele/insert).
func QualifiedTableName(raw any) (sql.Table, *validation.Invalid) {
	tableName, ok := raw.(string)
	if !ok {
		return sql.Table{}, invalidTable(EmptyParser)
	}
	return QualifiedTableNameString(tableName)
}

func QualifiedTableNameString(input string) (sql.Table, *validation.Invalid) {
	p := &parser{0, input}
	p.skipSpaces()

	name, err := p.requiredObjectName(invalidTable)
	if err != nil {
		return sql.Table{}, err
	}

	if name == "" {
		return sql.Table{}, invalidTable(p)
	}

	table := sql.Table{Name: name}

	if next := p.skip1(); next == ' ' {
		p.skipSpaces()
		table.Alias, err = p.alias()
		if err != nil {
			return sql.Table{}, err
		}
	} else if next != 0 {
		return sql.Table{}, invalidTable(p)
	}

	return table, nil
}

// Parses a single from for a select statement, which might include
// a join type and join condition (i.e. "on ...").
func JoinableFrom(input any) (sql.JoinableFrom, *validation.Invalid) {
	// no join, no condition, just a table name (+ optionally an alias)
	if tableName, ok := input.(string); ok {
		table, err := QualifiedTableNameString(tableName)
		if err != nil {
			return sql.JoinableFrom{}, err
		}
		return sql.JoinableFrom{Table: table}, nil
	}

	parts, ok := input.([]any)
	if !ok {
		return sql.JoinableFrom{}, invalidJoinableFrom()
	}

	if len(parts) != 3 {
		return sql.JoinableFrom{}, invalidJoinableFromCount()
	}

	part0, ok := parts[0].(string)
	if !ok {
		return sql.JoinableFrom{}, invalidJoinableFrom()
	}
	var join sql.JoinType
	switch trimSpace(part0) {
	case "inner":
		join = sql.JOIN_TYPE_INNER
	case "left":
		join = sql.JOIN_TYPE_LEFT
	case "right":
		join = sql.JOIN_TYPE_RIGHT
	case "full":
		join = sql.JOIN_TYPE_FULL
	default:
		return sql.JoinableFrom{}, invalidJoinableFromJoin(part0)
	}

	part1, ok := parts[1].(string)
	if !ok {
		return sql.JoinableFrom{}, invalidJoinableFromTable()
	}
	table, err := QualifiedTableNameString(part1)
	if err != nil {
		return sql.JoinableFrom{}, err
	}

	part2, ok := parts[2].([]any)
	if !ok {
		return sql.JoinableFrom{}, invalidJoinableFromOn()
	}

	on, err := Condition(part2, 0)
	if err != nil {
		return sql.JoinableFrom{}, err
	}

	return sql.JoinableFrom{
		Join:  join,
		Table: table,
		On:    &on,
	}, nil
}

func OrderBy(raw any) (sql.OrderBy, *validation.Invalid) {
	input, ok := raw.(string)
	if !ok {
		return sql.OrderBy{}, invalidOrderByType()
	}

	var desc bool
	p := &parser{0, input}
	p.skipSpaces()
	if p.peek() == '-' {
		desc = true
		p.pos += 1
	}

	field, err := p.dataField(invalidOrderBy)
	if err != nil {
		return sql.OrderBy{}, err
	}

	p.skipSpaces()
	if p.peek() != 0 {
		return sql.OrderBy{}, invalidOrderBy(p)
	}

	return sql.OrderBy{Field: field, Desc: desc}, nil
}

// ?# or column
func (p *parser) dataField(invalidFactory InvalidFactory) (sql.DataField, *validation.Invalid) {
	if p.peek() != '?' {
		return p.qualifiedColumn(invalidFactory)
	}

	start := p.pos
	input := p.input

	i := start + 1
	for ; i < len(input); i++ {
		if !isDigit(input[i]) {
			break
		}
	}

	if i == start+1 {
		return sql.DataField{}, invalidPlaceholder(p)
	}

	p.pos = i
	return sql.DataField{
		Name: input[start:i],
		Type: sql.DATA_FIELD_PLACEHOLDER,
	}, nil
}

// [table.]column_name
func (p *parser) qualifiedColumn(invalidFactory InvalidFactory) (sql.DataField, *validation.Invalid) {
	name, err := p.requiredObjectName(invalidFactory)
	if err != nil {
		return sql.DataField{}, err
	}

	var table string
	if next := p.peek(); next == '.' {
		p.pos += 1
		table = name
		name, err = p.requiredObjectName(invalidFactory)
		if err != nil {
			return sql.DataField{}, err
		}
	}

	return sql.DataField{
		Type:  sql.DATA_FIELD_COLUMN,
		Table: table,
		Name:  name,
	}, nil
}

// a column or table name, without an alias or table prefix.
// Essentially: [_a-zA-Z][_a-zA-Z0-9]*
// start with underscore or letter, followed by zero or more undescore, letters and numbers
func (p *parser) objectName(invalidFactory InvalidFactory) (string, *validation.Invalid) {
	start := p.pos
	input := p.input

	var i int
	for i = start; i < len(input); i++ {
		c := input[i]
		if !isAlphanumeric(c) && c != '_' {
			break
		}
	}

	if i == start {
		return "", nil
	}

	if isDigit(input[start]) {
		return "", invalidFactory(p)
	}
	p.pos = i
	return input[start:i], nil
}

func (p *parser) requiredObjectName(invalidFactory InvalidFactory) (string, *validation.Invalid) {
	name, err := p.objectName(invalidFactory)
	if err != nil {
		return "", err
	}
	if name == "" {
		return "", invalidFactory(p)
	}
	return name, nil
}

func (p *parser) alias() (*sql.Alias, *validation.Invalid) {
	if next := p.skip2(); next == "" {
		return nil, nil
	} else if next != "as" {
		return nil, invalidAlias(p)
	}

	p.skipSpaces()
	name, err := p.objectName(invalidAlias)
	if err != nil {
		return nil, err
	}
	if name == "" {
		return nil, invalidAlias(p)
	}
	p.skipSpaces()
	if p.peek() != 0 {
		return nil, invalidAlias(p)
	}

	return &sql.Alias{Alias: name}, nil
}

func (p *parser) skip1() byte {
	if next := p.peek(); next != 0 {
		p.pos += 1
		return next
	}
	return 0
}

func (p *parser) skip2() string {
	pos := p.pos
	input := p.input
	if pos+1 < len(input) {
		newPos := pos + 2
		p.pos = newPos
		return input[pos:newPos]
	}
	if next := p.skip1(); next != 0 {
		return string(next)
	}
	return ""
}

func (p *parser) peek() byte {
	pos := p.pos
	input := p.input
	if pos < len(input) {
		return input[pos]
	}
	return 0
}

func (p *parser) skipSpaces() bool {
	start := p.pos
	input := p.input
	len := len(input)

	i := start
	for ; i < len; i++ {
		if input[i] != ' ' {
			break
		}
	}

	// didn't skip anything
	if i == start {
		return false
	}

	p.pos = i
	return true
}

// ["column || ?#", "operator", "column || ?#"]
func predicate(input any, depth int) (sql.Part, *validation.Invalid) {
	parts, ok := input.([]any)
	if !ok {
		return sql.Predicate{}, invalidPredicate()
	}

	// if it isn't 3 parts, it cant be a predicate, maybe it's a nested condition
	if len(parts) != 3 {
		return Condition(parts, depth+1)
	}

	// if all 3 parts aren't strings, it can't e a predicate, maybe it's a nested condition
	leftInput, ok := parts[0].(string)
	if !ok {
		return Condition(parts, depth+1)
	}
	rightInput, ok := parts[2].(string)
	if !ok {
		return Condition(parts, depth+1)
	}
	opInput, ok := parts[1].(string)
	if !ok {
		return Condition(parts, depth+1)
	}

	// at this point, we're treating this like a predicate

	p := &parser{0, leftInput}
	p.skipSpaces()
	left, err := p.dataField(invalidPredicateLeft)
	if err != nil {
		return sql.Predicate{}, err
	}
	p.skipSpaces()
	if p.peek() != 0 {
		return sql.Predicate{}, invalidPredicateLeft(p)
	}

	p = &parser{0, rightInput}
	p.skipSpaces()
	right, err := p.dataField(invalidPredicateRight)
	if err != nil {
		return sql.Predicate{}, err
	}
	p.skipSpaces()
	if p.peek() != 0 {
		return sql.Predicate{}, invalidPredicateRight(p)
	}

	var op []byte
	switch trimSpace(opInput) {
	case "=":
		op = []byte(" = ")
	case "!=":
		op = []byte(" != ")
	case ">":
		op = []byte(" > ")
	case "<":
		op = []byte(" < ")
	case ">=":
		op = []byte(" >= ")
	case "<=":
		op = []byte(" <= ")
	case "is":
		op = []byte(" is ")
	case "is not":
		op = []byte(" is not ")
	default:
		return sql.Predicate{}, invalidPredicateOp(opInput)
	}

	return sql.Predicate{
		Left:  left,
		Op:    op,
		Right: right,
	}, nil
}

func isAlphanumeric(c byte) bool {
	return isAlphabet(c) || isDigit(c)
}

func isAlphabet(c byte) bool {
	return (c >= 'a' && c < 'z')
}

func isDigit(c byte) bool {
	return c >= '0' && c <= '9'
}

func trimSpace(input string) string {
	left := 0
	for ; left < len(input); left++ {
		if input[left] != ' ' {
			break
		}
	}

	right := len(input) - 1
	for ; right >= 0; right-- {
		if input[right] != ' ' {
			break
		}
	}
	return input[left : right+1]
}
