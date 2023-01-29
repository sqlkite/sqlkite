package http

import (
	"bytes"
	"strings"
	"sync/atomic"
	"time"

	"src.goblgobl.com/sqlkite"
	"src.goblgobl.com/sqlkite/codes"
	"src.goblgobl.com/sqlkite/config"
	"src.goblgobl.com/sqlkite/http/admin/tables"
	"src.goblgobl.com/sqlkite/http/diagnostics"
	"src.goblgobl.com/sqlkite/http/sql"
	"src.goblgobl.com/sqlkite/http/super/projects"
	"src.goblgobl.com/utils"
	"src.goblgobl.com/utils/http"
	"src.goblgobl.com/utils/uuid"

	"github.com/fasthttp/router"
	"github.com/valyala/fasthttp"
	"src.goblgobl.com/utils/log"
)

type EnvLoader func(conn *fasthttp.RequestCtx) (*sqlkite.Env, http.Response, error)
type UserLoader func(conn *fasthttp.RequestCtx) (*sqlkite.User, http.Response)
type ProjectIdLoader func(conn *fasthttp.RequestCtx) (string, http.Response)

var (
	resNotFoundPath         = http.StaticNotFound(codes.RES_UNKNOWN_ROUTE)
	resMissingProjectHeader = http.StaticError(400, codes.RES_MISSING_PROJECT_HEADER, "Project header required")
	resMissingSubdomain     = http.StaticError(400, codes.RES_MISSING_SUBDOMAIN, "Project id could not be loaded from subdomain")
	resProjectNotFound      = http.StaticError(400, codes.RES_PROJECT_NOT_FOUND, "unknown project id")

	globalRequestId = uint32(time.Now().Unix())
)

func Listen() {
	config := sqlkite.Config.HTTP

	if a := config.Admin; a != "" && a != "super" && a != "public" {
		err := log.Errf(codes.ERR_CONFIG_HTTP_ADMIN, "http.admin must be one of: 'super' (default) or 'public'")
		log.Fatal("server_admin_config").Err(err).Log()
		return
	}

	superLoaded := make(chan bool, 1)
	go listenSuper(config, superLoaded)
	if ok := <-superLoaded; !ok {
		return
	}

	// blocks
	listenMain(config)
}

func listenMain(config config.HTTP) {
	listen := config.Listen
	if listen == "" {
		listen = "127.0.0.1:5100"
	}

	logger := log.Info("public_server").String("address", listen)

	handler, err := mainHandler(config, logger)
	if err != nil {
		log.Fatal("server_public_handler").Err(err).Log()
		return
	}

	fast := fasthttp.Server{
		Handler:                      handler,
		NoDefaultContentType:         true,
		NoDefaultServerHeader:        true,
		SecureErrorLogMessage:        true,
		DisablePreParseMultipartForm: true,
	}

	logger.Log()
	err = fast.ListenAndServe(listen)
	log.Fatal("server_public_fail").Err(err).String("address", listen).Log()
}

func listenSuper(config config.HTTP, loaded chan bool) {
	listen := config.Super
	logger := log.Info("super_server").String("address", listen)

	if listen == "" {
		loaded <- true
		logger.Log()
		return
	}

	handler, err := superHandler(config, logger)
	if err != nil {
		log.Fatal("server_super_handler").Err(err).Log()
		loaded <- false
		return
	}

	fast := fasthttp.Server{
		Handler:                      handler,
		NoDefaultContentType:         true,
		NoDefaultServerHeader:        true,
		SecureErrorLogMessage:        true,
		DisablePreParseMultipartForm: true,
	}

	logger.Log()
	loaded <- true
	err = fast.ListenAndServe(listen)
	log.Fatal("server_super_fail").Err(err).String("address", listen).Log()
}

func mainHandler(config config.HTTP, logger log.Logger) (func(ctx *fasthttp.RequestCtx), error) {
	var userLoader UserLoader
	var projectIdLoader ProjectIdLoader

	switch strings.ToLower(config.Authentication.Type) {
	case "":
		logger.String("auth", "disabled")
		userLoader = loadUserDisabled
	case "header":
		logger.String("auth", "header")
		userLoader = loadUserFromHeader
	default:
		return nil, log.Errf(codes.ERR_CONFIG_HTTP_AUTHENTICATION_TYPE, "http.authentication.type must be one of: 'header'")
	}

	switch strings.ToLower(config.Project.Type) {
	case "", "subdomain":
		logger.String("project", "subdomain")
		projectIdLoader = loadProjectIdFromSubdomain
	case "header":
		logger.String("project", "header")
		projectIdLoader = loadProjectIdFromHeader
	default:
		return nil, log.Errf(codes.ERR_CONFIG_HTTP_PROJECT_TYPE, "http.project.type must be one of: 'subdomain' or 'header'")
	}

	envLoader := createEnvLoader(userLoader, projectIdLoader)

	r := router.New()

	r.POST("/v1/sql/select", http.Handler("sql_select", envLoader, sql.Select))

	adminIsPublic := config.Admin == "public"
	logger.Bool("admin", adminIsPublic)
	if adminIsPublic {
		attachAdmin(r, envLoader)
	}

	// catch all
	r.NotFound = func(ctx *fasthttp.RequestCtx) {
		resNotFoundPath.Write(ctx, log.Noop{})
	}

	return r.Handler, nil
}

func superHandler(config config.HTTP, logger log.Logger) (func(ctx *fasthttp.RequestCtx), error) {
	r := router.New()

	// diagnostics routes
	r.GET("/v1/diagnostics/ping", http.NoEnvHandler("ping", diagnostics.Ping))
	r.GET("/v1/diagnostics/info", http.NoEnvHandler("info", diagnostics.Info))

	r.POST("/v1/super/projects", http.Handler("project_create", loadSuperEnv, projects.Create))
	r.PUT("/v1/super/projects/{id}", http.Handler("project_update", loadSuperEnv, projects.Update))

	adminIsSuper := config.Admin == "" || config.Admin == "super"
	logger.Bool("admin", adminIsSuper)
	if adminIsSuper {
		attachAdmin(r, loadSuperEnv)
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
	r.POST("/v1/admin/tables", http.Handler("table_create", envLoader, tables.Create))
	r.PUT("/v1/admin/tables/{name}", http.Handler("table_update", envLoader, tables.Update))
	r.DELETE("/v1/admin/tables/{name}", http.Handler("table_delete", envLoader, tables.Delete))
}

// The "super" endpoints are powerful. They are executed outside of a typical
// project environment. We still use an *sqlite.Env, but the Project field is nil
// (this is the only time that happens).
// These endpoints are disabled by default, and by enabling them (via the config.json)
// we expect the administrator to also enable some form of security (such as using
// reverse proxy to only allow access to "/v1/super/*" from internal IPs).
func loadSuperEnv(conn *fasthttp.RequestCtx) (*sqlkite.Env, http.Response, error) {
	nextId := atomic.AddUint32(&globalRequestId, 1)
	requestId := utils.EncodeRequestId(nextId, sqlkite.Config.InstanceId)
	return sqlkite.NewEnv(nil, requestId), nil, nil
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
		return project.Env(), nil, nil
	}
}

func loadProjectIdFromHeader(conn *fasthttp.RequestCtx) (string, http.Response) {
	projectId := conn.Request.Header.PeekBytes([]byte("Project"))
	if projectId == nil {
		return "", resMissingProjectHeader
	}

	// we know this won't outlive conn
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

func loadUserFromHeader(conn *fasthttp.RequestCtx) (*sqlkite.User, http.Response) {
	return nil, nil
}

func loadUserDisabled(conn *fasthttp.RequestCtx) (*sqlkite.User, http.Response) {
	return nil, nil
}
