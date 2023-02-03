package sql

import (
	"src.goblgobl.com/sqlkite/data"
	"src.goblgobl.com/utils/buffer"
)

func TableAccessCreateTrigger(table string, action string, access *data.MutateTableAccess, buffer *buffer.Buffer) {
	buffer.Write([]byte("create trigger sqlkite_row_access_"))
	triggerNameSuffix(table, action, buffer)
	buffer.Write([]byte("\nbefore "))
	buffer.WriteUnsafe(action)

	buffer.Write([]byte(" on "))
	buffer.WriteUnsafe(table)
	buffer.Write([]byte(" for each row"))

	if w := access.When; w != "" {
		buffer.Write([]byte("\nwhen ("))
		buffer.WriteUnsafe(w)
		buffer.WriteByte(')')
	}

	buffer.Write([]byte("\nbegin\n "))
	trigger := access.Trigger
	buffer.WriteUnsafe(trigger)
	if trigger[len(trigger)-1] != ';' {
		buffer.WriteByte(';')
	}
	buffer.Write([]byte("\nend"))
}

func TableAccessDropTrigger(table string, action string, buffer *buffer.Buffer) {
	buffer.Write([]byte("drop trigger if exists sqlkite_row_access_"))
	triggerNameSuffix(table, action, buffer)
}

func triggerNameSuffix(table string, action string, buffer *buffer.Buffer) {
	buffer.WriteUnsafe(table)
	buffer.WriteByte('_')
	buffer.WriteUnsafe(action)
}
