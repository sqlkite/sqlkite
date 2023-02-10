package sql

import "src.goblgobl.com/utils/buffer"

type AlterTable struct {
	Name    string `json:"name"`
	Changes []Part `json:"changes"`
}

func (a *AlterTable) Write(b *buffer.Buffer) {
	name := a.Name
	for _, change := range a.Changes {
		b.Write([]byte("alter table "))
		b.WriteUnsafe(name)
		b.WriteByte(' ')
		change.Write(b)
		b.Write([]byte(";\n"))
	}
}

type AddColumn struct {
	Column Column `json:"column"`
}

func (a AddColumn) Write(b *buffer.Buffer) {
	b.Write([]byte("add column "))
	a.Column.Write(b)
}

type DropColumn struct {
	Name string `json:"name"`
}

func (d DropColumn) Write(b *buffer.Buffer) {
	b.Write([]byte("drop column "))
	b.WriteUnsafe(d.Name)
}

type RenameTable struct {
	To string `json:"to"`
}

func (r RenameTable) Write(b *buffer.Buffer) {
	b.Write([]byte("rename to "))
	b.WriteUnsafe(r.To)
}

type RenameColumn struct {
	Name string `json:"name"`
	To   string `json:"to"`
}

func (r RenameColumn) Write(b *buffer.Buffer) {
	b.Write([]byte("rename column "))
	b.WriteUnsafe(r.Name)
	b.Write([]byte(" to "))
	b.WriteUnsafe(r.To)
}
