package auth

import (
	"crypto/rand"
	"encoding/base64"
	"io"
	"time"

	"github.com/valyala/fasthttp"
	"src.goblgobl.com/sqlite"
	"src.goblgobl.com/utils/argon"
	"src.goblgobl.com/utils/http"
	"src.goblgobl.com/utils/typed"
	"src.goblgobl.com/utils/validation"
	"src.sqlkite.com/sqlkite"
	"src.sqlkite.com/sqlkite/codes"
)

const (
	NO_TTL = time.Duration(43830) * time.Hour
)

var (
	sessionCreateValidation = validation.Object[*sqlkite.Env]().
				Field("email", emailValidation.Required()).
				Field("password", passwordValidation.Required())

	resUserNotFound = http.StaticNotFound(codes.RES_SESSION_INVALID_CREDENTIALS)
)

func SessionCreate(conn *fasthttp.RequestCtx, env *sqlkite.Env) (http.Response, error) {
	projectAuth := env.Project.Auth
	if projectAuth.Disabled {
		return resAuthDisabled, nil
	}

	input, err := typed.Json(conn.PostBody())
	if err != nil {
		return http.InvalidJSON, nil
	}

	vc := env.VC
	if !sessionCreateValidation.ValidateInput(input, vc) {
		return http.Validation(vc), nil
	}

	email := input.String("email")

	var status int
	var role *string
	var userId, hashedPassword string

	err = env.WithDB(func(conn sqlite.Conn) error {
		row := conn.Row(`
			select id, password, status, role
			from sqlkite_users
			where email = ?1
		`, email)

		return row.Scan(&userId, &hashedPassword, &status, &role)
	})

	if err != nil {
		if err == sqlite.ErrNoRows {
			return resUserNotFound, nil
		}
		return nil, err
	}

	// TODO: check status when it means something

	ok, err := argon.Compare(input.String("password"), hashedPassword)
	if !ok || err != nil {
		return resUserNotFound, err
	}

	sessionId, err := createSession(env, userId, role)
	if err != nil {
		return nil, err
	}

	return http.Created(struct {
		Role   *string `json:"role"`
		Id     string  `json:"id"`
		UserId string  `json:"user_id"`
	}{
		Role:   role,
		Id:     sessionId,
		UserId: userId,
	}), nil
}

func createSession(env *sqlkite.Env, userId string, role *string) (string, error) {
	sessionId, err := createToken()
	if err != nil {
		return "", err
	}

	project := env.Project
	var sessionTTL time.Duration
	if ttl := project.Auth.Session.SessionTTL; ttl != 0 {
		sessionTTL = time.Duration(ttl) * time.Minute
	} else {
		sessionTTL = NO_TTL
	}

	expires := time.Now().Add(sessionTTL)
	err = project.WithDB(func(conn sqlite.Conn) error {
		return conn.Exec(`
			insert into sqlkite_sessions (id, user_id, role, expires)
			values (?1, ?2, ?3, ?4)
		`, sessionId, userId, role, expires)
	})

	return sessionId, err
}

func createToken() (string, error) {
	buffer := sqlkite.Buffer.Checkout()
	defer buffer.Release()

	data, err := buffer.TakeBytes(26)
	if err != nil {
		return "", err
	}

	_, err = io.ReadFull(rand.Reader, data)
	return base64.URLEncoding.EncodeToString(data), err
}
