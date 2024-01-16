package api

import (
	"errors"
	"github.com/alikonhz/sqlitekv/kvstore"
	"github.com/labstack/echo/v4"
	"github.com/labstack/echo/v4/middleware"
	"net/http"
)

type Server struct {
	storage kvstore.KvStore
	e       *echo.Echo
}

type GetResponse struct {
	Value any `json:"value"`
}

type PutRequest struct {
	Value any `json:"value"`
}

type ErrResponse struct {
	Error string `json:"error"`
}

func (s *Server) handleGet(c echo.Context) error {
	key := c.Param("key")
	if key == "" {
		return c.JSON(http.StatusBadRequest, ErrResponse{Error: "key is empty"})
	}

	val, err := s.storage.Get(c.Request().Context(), key)
	if err != nil {
		if errors.Is(err, kvstore.ErrKeyNotFound) {
			return echo.ErrNotFound
		}

		return err
	}

	anyVal, err := kvstore.ConvertFromValue(val)
	if err != nil {
		c.Logger().Errorf("failed to convert value %v for key %s\n", val, key)
		return c.JSON(http.StatusInternalServerError, ErrResponse{Error: "conversion failed"})
	}

	resp := GetResponse{
		Value: anyVal,
	}

	return c.JSON(http.StatusOK, resp)
}

func (s *Server) handlePut(c echo.Context) error {
	key := c.Param("key")
	if key == "" {
		return c.JSON(http.StatusBadRequest, ErrResponse{Error: "key is empty"})
	}

	var req PutRequest
	if err := c.Bind(&req); err != nil {
		return c.JSON(http.StatusBadRequest, ErrResponse{Error: "invalid body format"})
	}

	val, err := kvstore.ConvertToValue(req.Value)
	if errors.Is(err, kvstore.ErrTypeNotSupported) {
		return c.JSON(http.StatusBadRequest, ErrResponse{Error: "provided value type is not supported"})
	}

	return s.storage.Set(c.Request().Context(), key, val)
}

func (s *Server) Start(address string) error {
	return s.e.Start(address)
}

func NewHttpApiServer(storage kvstore.KvStore) *Server {
	srv := &Server{
		storage: storage,
	}

	srv.e = createHandler(srv)

	return srv
}

func createHandler(s *Server) *echo.Echo {
	e := echo.New()

	e.Use(middleware.Logger())
	e.Use(middleware.CORS())
	e.Use(middleware.Recover())

	setupRoutes(e, s)

	return e
}

func setupRoutes(e *echo.Echo, s *Server) {
	api := e.Group("/api")
	apiV1 := api.Group("/v1")
	apiV1.GET("/:key", s.handleGet)
	apiV1.PUT("/:key", s.handlePut)
	//e.GET("/stats/v1", s.handleStats)
}
