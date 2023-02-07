module src.sqlkite.com/sqlkite

go 1.20

replace (
	github.com/goccy/go-json => github.com/aryehlev/go-json v0.0.0-20221129210459-f82fe12f9170
	src.goblgobl.com/sqlite => ../../gobl/sqlite
	src.goblgobl.com/tests => ../../gobl/tests
	src.goblgobl.com/utils => ../../gobl/utils
)

require (
	github.com/fasthttp/router v1.4.16
	github.com/jackc/pgx/v5 v5.2.0
	github.com/valyala/fasthttp v1.44.0
	src.goblgobl.com/sqlite v0.0.4
	src.goblgobl.com/tests v0.0.6
	src.goblgobl.com/utils v0.0.6
)

require (
	github.com/andybalholm/brotli v1.0.4 // indirect
	github.com/goccy/go-json v0.10.0 // indirect
	github.com/google/uuid v1.3.0 // indirect
	github.com/jackc/pgpassfile v1.0.0 // indirect
	github.com/jackc/pgservicefile v0.0.0-20221227161230-091c0ba34f0a // indirect
	github.com/jackc/puddle/v2 v2.1.2 // indirect
	github.com/klauspost/compress v1.15.15 // indirect
	github.com/savsgio/gotils v0.0.0-20220530130905-52f3993e8d6d // indirect
	github.com/valyala/bytebufferpool v1.0.0 // indirect
	go.uber.org/atomic v1.10.0 // indirect
	golang.org/x/crypto v0.5.0 // indirect
	golang.org/x/sync v0.1.0 // indirect
	golang.org/x/text v0.6.0 // indirect
)
