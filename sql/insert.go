package sql

import (
	"bytes"
	"strconv"

	"src.goblgobl.com/utils/buffer"
)

// This will be a like ([]byte(?1,?2,?3,...?9999)) up to MAX_PARAMETERS.
// We can slice into it to get the right sequence that we want.
// For example, if we're inserting the 2nd row of placeholders for a 4-column
// insert, we'll grab ?5,?6,?7,?8 from this
var insertPlaceholders []byte

func init() {
	var b bytes.Buffer
	b.WriteString("?0")
	for i := 1; i < MAX_PARAMETERS; i++ {
		b.WriteString(",?" + strconv.Itoa(i))
	}

	// don't use tmp directly, as it could be using more memory than we need
	tmp := b.Bytes()
	insertPlaceholders = make([]byte, len(tmp))
	copy(insertPlaceholders, tmp)
}

type Insert struct {
	Into       string
	Columns    []string
	Parameters []any
}

func (i Insert) Write(b *buffer.Buffer) {
	columns := i.Columns

	b.Write([]byte("insert into "))
	b.WriteUnsafe(i.Into)

	b.Write([]byte(" ("))
	for _, c := range columns {
		b.WriteUnsafe(c)
		b.Write([]byte(", "))
	}
	b.Truncate(2) // strip out the last trailing comma

	b.Write([]byte(") values"))
	numberOfCols := len(columns)
	numberOfRows := len(i.Parameters) / numberOfCols

	// TODO: We can figure out how much space we'll need in our buffer to write
	// all our placeholders. We can use b.Pad(SIZE) to make sure we don't have
	// to keep allocating more space

	writeInsertPlaceholders(0, numberOfCols, b)
	for i := 1; i < numberOfRows; i++ {
		writeInsertPlaceholders(i, numberOfCols, b)
	}
}

func writeInsertPlaceholders(rowIndex int, numberOfCols int, b *buffer.Buffer) {
	b.Write([]byte("\n ("))
	indexOffset := rowIndex * numberOfCols
	firstPlaceHolder := indexOffset + 1
	lastPlaceholder := firstPlaceHolder + numberOfCols - 1

	startIndex := insertPlaceholderIndex(firstPlaceHolder, false)
	endIndex := insertPlaceholderIndex(lastPlaceholder, true)
	b.Write(insertPlaceholders[startIndex:endIndex])
	b.WriteByte(')')
}

// placeholders < 10 each take 3 characters ?#,  (with the comma at the end)
// placeholders 10 - 99 each take 4 characters ?##,
// placeholders 100-9999 each take 5 characters.
// Knowing the above, we can find exactly where in our insertPlaceholderIndex
// to grab the placeholder value
// When isEnd is true, we want to include the next value, exlcuding the comma.
// So for ?55,?56,?57  if we wanted to grab ?56, we'll figure out where ?56 starts
// and then add 3.
func insertPlaceholderIndex(placeholder int, isEnd bool) (index int) {
	if placeholder < 10 {
		index = placeholder * 3
		if isEnd {
			index += 2
		}
	} else if placeholder < 100 {
		// 30 is where ?10 start (3 * 10)
		index = 30 + (placeholder-10)*4
		if isEnd {
			index += 3
		}
	} else {
		// 390 is where ?100 starts (3 * 10 + 90 * 4)
		index = 390 + (placeholder-100)*5
		if isEnd {
			index += 4
		}
	}
	return
}
