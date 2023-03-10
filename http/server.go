package http

import (
	"bytes"
	"strings"
	"sync/atomic"
	"time"

	"src.goblgobl.com/sqlite"
	"src.goblgobl.com/utils"
	"src.goblgobl.com/utils/http"
	"src.goblgobl.com/utils/uuid"
	"src.sqlkite.com/sqlkite"
	"src.sqlkite.com/sqlkite/codes"
	"src.sqlkite.com/sqlkite/config"
	"src.sqlkite.com/sqlkite/http/admin/tables"
	"src.sqlkite.com/sqlkite/http/auth"
	"src.sqlkite.com/sqlkite/http/diagnostics"
	"src.sqlkite.com/sqlkite/http/sql"
	"src.sqlkite.com/sqlkite/http/super/projects"

	"github.com/fasthttp/router"
	"github.com/valyala/fasthttp"
	"github.com/valyala/fasthttp/pprofhandler"
	"src.goblgobl.com/utils/log"
)

type EnvLoader func(conn *fasthttp.RequestCtx) (*sqlkite.Env, http.Response, error)
type UserLoader func(conn *fasthttp.RequestCtx, env *sqlkite.Env) (*sqlkite.User, http.Response, error)
type ProjectIdLoader func(conn *fasthttp.RequestCtx) (string, http.Response)

var (
	resNotFoundPath         = http.StaticNotFound(codes.RES_UNKNOWN_ROUTE)
	resMissingProjectHeader = http.StaticError(400, codes.RES_MISSING_PROJECT_HEADER, "Project header required")
	resMissingSubdomain     = http.StaticError(400, codes.RES_MISSING_SUBDOMAIN, "Project id could not be loaded from subdomain")
	resProjectNotFound      = http.StaticError(400, codes.RES_PROJECT_NOT_FOUND, "Unknown project id")
	resAuthPrefix           = http.StaticError(401, codes.RES_AUTH_PREFIX, "Invalid authorization token type")
	resAuthEmpty            = http.StaticError(401, codes.RES_AUTH_EMPTY, "Empty authorization token")
	resAuthInvalid          = http.StaticError(401, codes.RES_AUTH_INVALID, "The authorization token is not valid")
	resAuthTrustPrefix      = http.StaticError(401, codes.RES_AUTH_TRUST_PREFIX, "Invalid authorization token type")
	resAuthTrustEmpty       = http.StaticError(401, codes.RES_AUTH_TRUST_EMPTY, "Empty authorization token")

	resUnauthorized = http.StaticError(401, codes.RES_INVALID_CREDENTIALS, "Invalid or missing credentials")
	resAccessDenied = http.StaticError(403, codes.RES_ACCESS_DENIED, "Access denied")
	globalRequestId = uint32(time.Now().Unix())
)

func Listen() {
	config := sqlkite.Config.HTTP

	if a := config.Admin; a != "" && a != "super" && a != "public" {
		err := log.Errf(codes.ERR_CONFIG_HTTP_ADMIN, "http.admin must be one of: 'super' (default) or 'public'")
		log.Fatal("server_admin_config").Err(err).Log()
		return
	}

	var userLoader UserLoader
	var projectIdLoader ProjectIdLoader
	logger := log.Info("http_server")

	switch strings.ToLower(config.Authentication.Type) {
	case "":
		logger.String("auth", "disabled")
		userLoader = loadUserDisabled
	case "trust-header":
		logger.String("auth", "trust-header")
		userLoader = loadUserFromTrustedHeader
	case "session":
		logger.String("auth", "session")
		userLoader = loadUserFromSession
	default:
		err := log.Errf(codes.ERR_CONFIG_HTTP_AUTHENTICATION_TYPE, "http.authentication.type must be one of: 'session', 'trust-header' or '' (empty)")
		log.Fatal("server_auth_config").Err(err).Log()
		return
	}

	switch strings.ToLower(config.Project.Type) {
	case "", "subdomain":
		logger.String("project", "subdomain")
		projectIdLoader = loadProjectIdFromSubdomain
	case "header":
		logger.String("project", "header")
		projectIdLoader = loadProjectIdFromHeader
	default:
		err := log.Errf(codes.ERR_CONFIG_HTTP_PROJECT_TYPE, "http.project.type must be one of: 'subdomain' or 'header'")
		log.Fatal("server_auth_project").Err(err).Log()
		return
	}
	envLoader := createEnvLoader(userLoader, projectIdLoader)

	superLoaded := make(chan bool, 1)
	go listenSuper(config, logger, envLoader, userLoader, superLoaded)
	if ok := <-superLoaded; !ok {
		return
	}

	// blocks
	listenMain(config, logger, envLoader)
}

func listenMain(config config.HTTP, logger log.Logger, envLoader EnvLoader) {
	listen := config.Listen
	if listen == "" {
		listen = "127.0.0.1:5100"
	}

	logger.String("public", listen)

	handler, err := mainHandler(config, logger, envLoader)
	if err != nil {
		log.Fatal("server_public_handler").Err(err).Log()
		return
	}

	fast := fasthttp.Server{
		Handler:                       handler,
		NoDefaultContentType:          true,
		NoDefaultServerHeader:         true,
		SecureErrorLogMessage:         true,
		DisablePreParseMultipartForm:  true,
		DisableHeaderNamesNormalizing: true,
	}

	logger.Log()
	err = fast.ListenAndServe(listen)
	log.Fatal("server_public_fail").Err(err).String("address", listen).Log()
}

func listenSuper(config config.HTTP, logger log.Logger, envLoader EnvLoader, userLoader UserLoader, loaded chan bool) {
	listen := config.Super

	if listen == "" {
		loaded <- true
		logger.Bool("supper", false)
		return
	}
	logger.String("super", listen)

	handler, err := superHandler(config, logger, envLoader, userLoader)
	if err != nil {
		log.Fatal("server_super_handler").Err(err).Log()
		loaded <- false
		return
	}

	fast := fasthttp.Server{
		Handler:                       handler,
		NoDefaultContentType:          true,
		NoDefaultServerHeader:         true,
		SecureErrorLogMessage:         true,
		DisablePreParseMultipartForm:  true,
		DisableHeaderNamesNormalizing: true,
	}

	loaded <- true
	err = fast.ListenAndServe(listen)
	log.Fatal("server_super_fail").Err(err).String("address", listen).Log()
}

func mainHandler(config config.HTTP, logger log.Logger, envLoader EnvLoader) (func(ctx *fasthttp.RequestCtx), error) {
	r := router.New()

	// diagnostics routes
	// only the ping route is available on the main handler. Other diagnostic routes
	// are exposed only onthe super handler
	r.GET("/v1/diagnostics/ping", http.NoEnvHandler("ping", diagnostics.Ping))

	r.POST("/v1/sql/select", http.Handler("sql_select", envLoader, sql.Select))
	r.POST("/v1/sql/insert", http.Handler("sql_insert", envLoader, sql.Insert))
	r.POST("/v1/sql/update", http.Handler("sql_update", envLoader, sql.Update))
	r.POST("/v1/sql/delete", http.Handler("sql_delete", envLoader, sql.Delete))

	r.POST("/v1/auth/users", http.Handler("auth_users_create", envLoader, auth.UserCreate))
	r.POST("/v1/auth/sessions", http.Handler("auth_session_create", envLoader, auth.SessionCreate))
	r.GET("/v1/auth/logout", http.Handler("auth_logout", envLoader, auth.Logout))

	if config.Admin == "public" {
		logger.String("admin", "public")
		attachAdmin(r, envLoader)
	}

	// catch all
	r.NotFound = func(ctx *fasthttp.RequestCtx) {
		resNotFoundPath.Write(ctx, log.Noop{})
	}

	return r.Handler, nil
}

func superHandler(config config.HTTP, logger log.Logger, envLoader EnvLoader, userLoader UserLoader) (func(ctx *fasthttp.RequestCtx), error) {
	r := router.New()

	// diagnostics routes
	r.ANY("/debug/pprof/{func:*}", pprofhandler.PprofHandler)
	r.GET("/v1/diagnostics/ping", http.NoEnvHandler("ping", diagnostics.Ping))
	r.GET("/v1/diagnostics/info", http.NoEnvHandler("info", diagnostics.Info))

	r.POST("/v1/super/projects", http.Handler("project_create", loadSuperEnv(userLoader), projects.Create))
	r.PUT("/v1/super/projects/{id}", http.Handler("project_update", loadSuperEnv(userLoader), projects.Update))

	if config.Admin == "" || config.Admin == "super" {
		logger.String("admin", "super")
		attachAdmin(r, envLoader)
	}

	// catch all
	r.NotFound = func(ctx *fasthttp.RequestCtx) {
		resNotFoundPath.Write(ctx, log.Noop{})
	}

	return r.Handler, nil
}

// Admin routes can either be attached to the main/public http listener or the
// super listener (assuming it's even enabled). This is done to support the two
// foreseen usecase of a relatively static/controlled deployment (likely single-
// tenancy) where the admins fully manage the system, including project definition,
// and a more dynamic deployment (likely multi-tenancy) where project owners
// fully manage their own projects.
func attachAdmin(r *router.Router, envLoader EnvLoader) {
	r.POST("/v1/admin/tables", http.Handler("table_create", requireRole("sqlite_admin", envLoader), tables.Create))
	r.PUT("/v1/admin/tables/{name}", http.Handler("table_update", requireRole("sqlite_admin", envLoader), tables.Update))
	r.DELETE("/v1/admin/tables/{name}", http.Handler("table_delete", requireRole("sqlite_admin", envLoader), tables.Delete))
}

// The "super" endpoints are powerful. They are executed outside of a typical
// project environment. We still use an *sqlite.Env, but the Project field is nil
// (this is the only time that happens).
// These endpoints are disabled by default, and by enabling them (via the config.json)
// we expect the administrator to also enable some form of security (such as using
// reverse proxy to only allow access to "/v1/super/*" from internal IPs).
func loadSuperEnv(userLoader UserLoader) func(conn *fasthttp.RequestCtx) (*sqlkite.Env, http.Response, error) {
	return func(conn *fasthttp.RequestCtx) (*sqlkite.Env, http.Response, error) {
		nextId := atomic.AddUint32(&globalRequestId, 1)
		requestId := utils.EncodeRequestId(nextId, sqlkite.Config.InstanceId)

		env := sqlkite.NewEnv(nil, requestId)
		user, res, err := userLoader(conn, env)

		if res != nil || err != nil {
			return nil, res, err
		}
		if user == nil {
			return nil, resUnauthorized, nil
		}
		if user.Role != "super" {
			return nil, resAccessDenied, nil
		}

		env.User = user
		return env, nil, nil
	}

}

func createEnvLoader(userLoader UserLoader, projectIdLoader ProjectIdLoader) EnvLoader {
	return func(conn *fasthttp.RequestCtx) (*sqlkite.Env, http.Response, error) {
		projectId, res := projectIdLoader(conn)
		if res != nil {
			return nil, res, nil
		}

		if !uuid.IsValid(projectId) {
			return nil, resProjectNotFound, nil
		}

		project, err := sqlkite.Projects.Get(projectId)

		if err != nil {
			return nil, nil, err
		}

		if project == nil {
			return nil, resProjectNotFound, nil
		}
		env := project.Env()
		user, res, err := userLoader(conn, env)
		if res != nil || err != nil {
			env.Release()
			return nil, res, err
		}
		env.User = user
		return env, nil, nil
	}
}

func loadProjectIdFromHeader(conn *fasthttp.RequestCtx) (string, http.Response) {
	projectId := conn.Request.Header.PeekBytes([]byte("Project"))
	if projectId == nil {
		return "", resMissingProjectHeader
	}

	// fasthttp says headers are valid until the connection is discarded, and
	// we know this won't outlive the connection
	return utils.B2S(projectId), nil
}

func loadProjectIdFromSubdomain(conn *fasthttp.RequestCtx) (string, http.Response) {
	host := conn.Host()
	si := bytes.IndexByte(host, '.')
	if si == -1 {
		return "", resMissingSubdomain
	}
	return utils.B2S(host[:si]), nil
}

func loadUserFromTrustedHeader(conn *fasthttp.RequestCtx, env *sqlkite.Env) (*sqlkite.User, http.Response, error) {
	header := &conn.Request.Header
	authHeader := header.PeekBytes([]byte("Authorization"))
	if authHeader == nil {
		return nil, nil, nil
	}

	if !bytes.HasPrefix(authHeader, []byte("sqlkite-trust ")) {
		return nil, resAuthTrustPrefix, nil
	}

	authHeader = bytes.TrimSpace(authHeader[14:])
	if len(authHeader) == 0 {
		return nil, resAuthTrustEmpty, nil
	}

	parts := bytes.SplitN(authHeader, []byte{','}, 2)

	// fasthttp says headers are valid until the connection is discarded, and
	// we know this won't outlive the connection
	userId := utils.B2S(parts[0])

	var role string
	if len(parts) == 2 {
		role = utils.B2S(parts[1])
	}

	return &sqlkite.User{
		Id:   userId,
		Role: role,
	}, nil, nil
}

func loadUserFromSession(conn *fasthttp.RequestCtx, env *sqlkite.Env) (*sqlkite.User, http.Response, error) {
	project := env.Project

	// env.Project will be nil for "super" requests (e.g. adding a project)
	if project == nil {
		return nil, nil, nil
	}

	projectAuth := project.Auth
	if projectAuth.Disabled {
		return nil, nil, nil
	}

	header := &conn.Request.Header
	authHeader := header.PeekBytes([]byte("Authorization"))
	if authHeader == nil {
		return nil, nil, nil
	}

	if !bytes.HasPrefix(authHeader, []byte("sqlkite ")) {
		return nil, resAuthPrefix, nil
	}

	sessionId := utils.B2S(bytes.TrimSpace(authHeader[8:]))
	if len(sessionId) == 0 {
		return nil, resAuthEmpty, nil
	}

	var userId, role string
	err := env.Project.WithDB(func(conn sqlite.Conn) error {
		row := conn.Row("select user_id, role from sqlkite_sessions where id = ?1 and expires > unixepoch()", sessionId)
		return row.Scan(&userId, &role)
	})

	if err != nil {
		if err == sqlite.ErrNoRows {
			return nil, resAuthInvalid, nil
		}
		return nil, nil, err
	}

	return &sqlkite.User{
		Id:   userId,
		Role: role,
	}, nil, nil
}

func loadUserDisabled(conn *fasthttp.RequestCtx, env *sqlkite.Env) (*sqlkite.User, http.Response, error) {
	return nil, nil, nil
}

func requireRole(role string, envLoader EnvLoader) func(conn *fasthttp.RequestCtx) (*sqlkite.Env, http.Response, error) {
	return func(conn *fasthttp.RequestCtx) (*sqlkite.Env, http.Response, error) {
		env, res, err := envLoader(conn)
		if env == nil {
			return nil, res, err
		}

		user := env.User
		if user == nil {
			return nil, resUnauthorized, nil
		}
		if user.Role != role {
			return nil, resAccessDenied, nil
		}

		return env, nil, nil
	}
}
