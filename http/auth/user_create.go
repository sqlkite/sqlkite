package auth

import (
	"github.com/valyala/fasthttp"
	"src.goblgobl.com/sqlite"
	"src.goblgobl.com/utils/argon"
	"src.goblgobl.com/utils/http"
	"src.goblgobl.com/utils/typed"
	"src.goblgobl.com/utils/uuid"
	"src.goblgobl.com/utils/validation"
	"src.sqlkite.com/sqlkite"
	"src.sqlkite.com/sqlkite/codes"
)

var (
	userCreateValidation = validation.Object[*sqlkite.Env]().
				Field("email", emailValidation.Required()).
				Field("password", passwordValidation.Required())

	emailField = validation.BuildField("email")

	valEmailInUse = &validation.Invalid{
		Code:  codes.VAL_AUTH_EMAIL_IN_USE,
		Error: "The email is already in use",
	}
)

func UserCreate(conn *fasthttp.RequestCtx, env *sqlkite.Env) (http.Response, error) {
	if env.Project.Auth.Disabled {
		return resAuthDisabled, nil
	}

	input, err := typed.Json(conn.PostBody())
	if err != nil {
		return http.InvalidJSON, nil
	}

	vc := env.VC
	if !userCreateValidation.ValidateInput(input, vc) {
		return http.Validation(vc), nil
	}

	userId := uuid.String()
	email := input.String("email")
	passwordHash, err := argon.Hash(input.String("password"))
	if err != nil {
		return nil, err
	}

	err = env.WithDB(func(conn sqlite.Conn) error {
		return conn.Exec(`
			insert into sqlkite_users (id, email, password, status)
			values (?1, ?2, ?3, ?4)
		`, userId, email, passwordHash, 1)
	})

	if err != nil {
		if !sqlite.IsUniqueErr(err) {
			return nil, err
		}

		vc.InvalidWithField(valEmailInUse, emailField)
		return http.Validation(vc), nil
	}

	return http.OK(struct {
		Id string `json:"id"`
	}{
		Id: userId,
	}), nil
}
