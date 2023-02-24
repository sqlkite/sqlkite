F=

.PHONY: t
t: commit.txt t_sqlite

.PHONY: t_all
t_all: commit.txt t_sqlite t_pg t_cr

.PHONY: t_sqlite
t_sqlite:
	@printf "\n==sqlite==\n"
	@printf "setting up test databases"
	@rm -f tests/sqlkite.super
	@go run tests/setup/main.go sqlite

	@printf "\nrunning tests\n"
	@GOBL_TEST_STORAGE=sqlite go test -race -count=1 ./... -run "${F}" \
		| grep -v "no tests to run" \
		| grep -v "no test files"

.PHONY: t_pg
t_pg:
	@printf "\n==postgres==\n"
	@printf "dropping and creating the sqlkite database"
	@psql $${GOBL_TEST_PG:-postgres://localhost:5432/postgres} --quiet -c "drop database sqlkite_test" || true
	@psql $${GOBL_TEST_PG:-postgres://localhost:5432/postgres} --quiet -c "create database sqlkite_test"

	@printf "\nsetting up test databases"
	@go run tests/setup/main.go pg

	@printf "\nrunning tests\n"
	@GOBL_TEST_STORAGE=postgres go test -count=1 ./... -run "${F}" \
		| grep -v "no tests to run" \
		| grep -v "no test files"

.PHONY: t_cr
t_cr:
	@printf "\n==cockroachdb==\n"
	@printf "dropping and creating the sqlkite database"
	@psql $${GOBL_TEST_CR:-postgres://root@localhost:26257/} --quiet -c "drop database sqlkite_test" || true
	@psql $${GOBL_TEST_CR:-postgres://root@localhost:26257/} --quiet -c "create database sqlkite_test"

	@printf "\nsetting up test databases"
	@go run tests/setup/main.go cr

	@printf "\nrunning tests\n"
	@GOBL_TEST_STORAGE=cockroach go test -count=1 ./... -run "${F}" \
		| grep -v "no tests to run" \
		| grep -v "no test files"

.PHONY: s
s: commit.txt
	go run cmd/main.go

.PHONY: commit.txt
commit.txt:
	@git rev-parse HEAD | tr -d "\n" > http/diagnostics/commit.txt

.PHONY: build
build: commit.txt
	go build -ldflags="-s -w" -o authen cmd/main.go
