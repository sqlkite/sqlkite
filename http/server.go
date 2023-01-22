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

	"github.com/fasthttp/router"
	"github.com/valyala/fasthttp"
	"src.goblgobl.com/utils/log"
)

type ProjectIdLoader func(conn *fasthttp.RequestCtx) (string, http.Response)
type UserLoader func(conn *fasthttp.RequestCtx) (*sqlkite.User, http.Response)

var (
	resNotFoundPath         = http.StaticNotFound(codes.RES_UNKNOWN_ROUTE)
	resMissingProjectHeader = http.StaticError(400, codes.RES_MISSING_PROJECT_HEADER, "Project header required")
	resMissingSubdomain     = http.StaticError(400, codes.RES_MISSING_SUBDOMAIN, "Project id could not be loaded from subdomain")
	resProjectNotFound      = http.StaticError(400, codes.RES_PROJECT_NOT_FOUND, "unknown project id")

	globalRequestId = uint32(time.Now().Unix())
)

func Listen() {
	config := sqlkite.Config.HTTP

	listen := config.Listen
	if listen == "" {
		listen = "127.0.0.1:5200"
	}

	handler, err := handler(config)
	if err != nil {
		log.Fatal("server_build_handler").Err(err).Log()
		return
	}

	fast := fasthttp.Server{
		Handler:                      handler,
		NoDefaultContentType:         true,
		NoDefaultServerHeader:        true,
		SecureErrorLogMessage:        true,
		DisablePreParseMultipartForm: true,
	}

	log.Info("server_listening").String("address", listen).Log()
	err = fast.ListenAndServe(listen)
	log.Fatal("server_fail").Err(err).String("address", listen).Log()
}

func handler(config config.HTTP) (func(ctx *fasthttp.RequestCtx), error) {
	var userLoader UserLoader
	var projectIdLoader ProjectIdLoader

	handlerConfigLog := log.Info("server.handler_config")

	switch strings.ToLower(config.Authentication.Type) {
	case "", "header":
		handlerConfigLog.String("auth", "header")
		userLoader = loadUserFromHeader
	default:
		return nil, log.Errf(codes.ERR_CONFIG_HTTP_AUTHENTICATION_TYPE, "http.authentication.type must be one of: 'header'")
	}

	switch strings.ToLower(config.Project.Type) {
	case "", "subdomain":
		projectIdLoader = loadProjectIdFromSubdomain
		handlerConfigLog.String("project", "subdomain")
	case "header":
		log.Info("server.project_loader").String("type", "header").Log()
		projectIdLoader = loadProjectIdFromHeader
	default:
		return nil, log.Errf(codes.ERR_CONFIG_HTTP_PROJECT_TYPE, "http.project.type must be one of: 'subdomain' or 'header'")
	}

	envLoader := createEnvLoader(userLoader, projectIdLoader)

	r := router.New()

	r.POST("/v1/sql/select", http.Handler("sql_select", envLoader, sql.Select))

	if config.Admin != "" {
		handlerConfigLog.String("admin", "true")
		r.POST("/v1/admin/tables", http.Handler("table_create", envLoader, tables.Create))
	}

	// diagnostics routes
	r.GET("/v1/diagnostics/ping", http.NoEnvHandler("ping", diagnostics.Ping))
	r.GET("/v1/diagnostics/info", http.NoEnvHandler("info", diagnostics.Info))

	if config.Super != "" {
		handlerConfigLog.String("super", "true")
		r.POST("/v1/super/projects", http.Handler("project_create", loadSuperEnv, projects.Create))
		r.PUT("/v1/super/projects/{id}", http.Handler("project_update", loadSuperEnv, projects.Update))
	}

	handlerConfigLog.Log()

	// catch all
	r.NotFound = func(ctx *fasthttp.RequestCtx) {
		resNotFoundPath.Write(ctx, log.Noop{})
	}

	return r.Handler, nil
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

func createEnvLoader(userLoader UserLoader, projectIdLoader ProjectIdLoader) func(conn *fasthttp.RequestCtx) (*sqlkite.Env, http.Response, error) {
	return func(conn *fasthttp.RequestCtx) (*sqlkite.Env, http.Response, error) {
		projectId, res := projectIdLoader(conn)
		if res != nil {
			return nil, res, nil
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
