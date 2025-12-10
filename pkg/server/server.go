package server

import (
	"context"
	"encoding/base64"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"go.uber.org/zap"
	"uveitis/backend/pkg/config"
	"uveitis/backend/pkg/storage"
)

type Server struct {
	cfg   *config.Config
	log   *zap.Logger
	store *storage.Storage
	token string
}

func New(cfg *config.Config, log *zap.Logger, store *storage.Storage) *Server {
	token := base64.StdEncoding.EncodeToString([]byte(cfg.Auth.Username + ":" + cfg.Auth.Password))
	return &Server{cfg: cfg, log: log, store: store, token: token}
}

func (s *Server) Router() *gin.Engine {
	r := gin.New()
	r.Use(gin.Recovery())
	r.Use(requestLogger(s.log))
	r.Use(corsMiddleware())

	r.GET("/api/ping", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{"status": "ok", "ts": time.Now().Unix()})
	})

	r.POST("/api/login", s.handleLogin)

	auth := r.Group("/api", s.authMiddleware())
	{
		auth.GET("/profile", s.profile)
		auth.GET("/tables", s.listTables)
		auth.POST("/tables", s.createTable)
		auth.POST("/tables/:table/columns", s.addColumns)
		auth.DELETE("/tables/:table/columns", s.dropColumns)
		auth.GET("/tables/:table/data", s.queryData)
		auth.POST("/tables/:table/data", s.insertRow)
		auth.PUT("/tables/:table/data/:id", s.updateRow)
		auth.DELETE("/tables/:table/data/:id", s.deleteRow)
		auth.POST("/tables/:table/import", s.importCSV)
		auth.GET("/tables/:table/summary", s.summary)
	}
	return r
}

func (s *Server) handleLogin(c *gin.Context) {
	var body struct {
		Username string `json:"username"`
		Password string `json:"password"`
	}
	if err := c.ShouldBindJSON(&body); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "参数错误"})
		return
	}
	if body.Username == s.cfg.Auth.Username && body.Password == s.cfg.Auth.Password {
		c.JSON(http.StatusOK, gin.H{"token": s.token, "user": gin.H{"name": "管理员"}})
		return
	}
	c.JSON(http.StatusUnauthorized, gin.H{"error": "用户名或密码错误"})
}

func (s *Server) profile(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{"name": s.cfg.Auth.Username})
}

func (s *Server) listTables(c *gin.Context) {
	ctx := c.Request.Context()
	s.log.Info("api list tables start")
	tables, err := s.store.ListTables(ctx)
	if err != nil {
		s.fail(c, err)
		return
	}
	s.log.Info("api list tables success", zap.Int("count", len(tables)))
	c.JSON(http.StatusOK, gin.H{"items": tables})
}

func (s *Server) createTable(c *gin.Context) {
	ctx := c.Request.Context()
	var schema storage.TableSchema
	if err := c.ShouldBindJSON(&schema); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "请求格式错误"})
		return
	}
	if err := s.store.CreateTable(ctx, schema); err != nil {
		s.fail(c, err)
		return
	}
	c.JSON(http.StatusOK, gin.H{"message": "创建成功"})
}

func (s *Server) addColumns(c *gin.Context) {
	ctx := c.Request.Context()
	table := c.Param("table")
	var payload struct {
		Fields []storage.FieldDefinition `json:"fields"`
	}
	if err := c.ShouldBindJSON(&payload); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "请求格式错误"})
		return
	}
	if err := s.store.AddColumns(ctx, table, payload.Fields); err != nil {
		s.fail(c, err)
		return
	}
	c.JSON(http.StatusOK, gin.H{"message": "已添加字段"})
}

func (s *Server) dropColumns(c *gin.Context) {
	ctx := c.Request.Context()
	table := c.Param("table")
	var payload struct {
		Columns []string `json:"columns"`
	}
	if err := c.ShouldBindJSON(&payload); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "请求格式错误"})
		return
	}
	if err := s.store.DropColumns(ctx, table, payload.Columns); err != nil {
		s.fail(c, err)
		return
	}
	c.JSON(http.StatusOK, gin.H{"message": "字段已删除"})
}

func (s *Server) queryData(c *gin.Context) {
	ctx := c.Request.Context()
	table := c.Param("table")
	page, _ := strconv.Atoi(c.DefaultQuery("page", "1"))
	size, _ := strconv.Atoi(c.DefaultQuery("size", "20"))
	desc := c.DefaultQuery("desc", "false") == "true"
	opts := storage.QueryOptions{
		Search:   c.Query("search"),
		Page:     page,
		PageSize: size,
		SortBy:   c.Query("sort_by"),
		Desc:     desc,
		Filters:  mapFromQuery(c, "filter."),
	}
	rows, total, err := s.store.Query(ctx, table, opts)
	if err != nil {
		s.fail(c, err)
		return
	}
	c.JSON(http.StatusOK, gin.H{
		"items": rows,
		"total": total,
	})
}

func (s *Server) insertRow(c *gin.Context) {
	ctx := c.Request.Context()
	table := c.Param("table")
	var body map[string]any
	if err := c.ShouldBindJSON(&body); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "请求格式错误"})
		return
	}
	id, err := s.store.InsertRow(ctx, table, body)
	if err != nil {
		s.fail(c, err)
		return
	}
	c.JSON(http.StatusOK, gin.H{"id": id})
}

func (s *Server) updateRow(c *gin.Context) {
	ctx := c.Request.Context()
	table := c.Param("table")
	id, _ := strconv.ParseInt(c.Param("id"), 10, 64)
	var body map[string]any
	if err := c.ShouldBindJSON(&body); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "请求格式错误"})
		return
	}
	if err := s.store.UpdateRow(ctx, table, id, body); err != nil {
		s.fail(c, err)
		return
	}
	c.JSON(http.StatusOK, gin.H{"message": "已更新"})
}

func (s *Server) deleteRow(c *gin.Context) {
	ctx := c.Request.Context()
	table := c.Param("table")
	id, _ := strconv.ParseInt(c.Param("id"), 10, 64)
	if err := s.store.DeleteRow(ctx, table, id); err != nil {
		s.fail(c, err)
		return
	}
	c.JSON(http.StatusOK, gin.H{"message": "已删除"})
}

func (s *Server) importCSV(c *gin.Context) {
	ctx := c.Request.Context()
	table := c.Param("table")
	file, err := c.FormFile("file")
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "请选择csv/excel文件"})
		return
	}
	f, err := file.Open()
	if err != nil {
		s.fail(c, err)
		return
	}
	defer f.Close()
	count, err := s.store.ImportCSV(ctx, table, f)
	if err != nil {
		s.fail(c, err)
		return
	}
	c.JSON(http.StatusOK, gin.H{"imported": count})
}

func (s *Server) summary(c *gin.Context) {
	ctx := c.Request.Context()
	table := c.Param("table")
	column := c.Query("column")
	if column == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "缺少统计字段"})
		return
	}
	res, err := s.store.Summary(ctx, table, column)
	if err != nil {
		s.fail(c, err)
		return
	}
	c.JSON(http.StatusOK, gin.H{"summary": res})
}

func (s *Server) fail(c *gin.Context, err error) {
	s.log.Error("api error", zap.String("path", c.FullPath()), zap.String("method", c.Request.Method), zap.Error(err))
	c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
}

func (s *Server) authMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		header := c.GetHeader("Authorization")
		if header == "" || !strings.HasPrefix(header, "Bearer ") {
			c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"error": "未授权"})
			return
		}
		token := strings.TrimPrefix(header, "Bearer ")
		if token != s.token {
			c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"error": "认证失败"})
			return
		}
		c.Next()
	}
}

func mapFromQuery(c *gin.Context, prefix string) map[string]string {
	result := map[string]string{}
	for key, vals := range c.Request.URL.Query() {
		if strings.HasPrefix(key, prefix) && len(vals) > 0 {
			result[strings.TrimPrefix(key, prefix)] = vals[0]
		}
	}
	return result
}

func requestLogger(log *zap.Logger) gin.HandlerFunc {
	return func(c *gin.Context) {
		start := time.Now()
		c.Next()
		latency := time.Since(start)
		log.Info("request",
			zap.String("method", c.Request.Method),
			zap.String("path", c.Request.URL.Path),
			zap.Int("status", c.Writer.Status()),
			zap.Duration("latency", latency),
			zap.String("client", c.ClientIP()),
			zap.String("ua", c.Request.UserAgent()),
		)
	}
}

func corsMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		c.Writer.Header().Set("Access-Control-Allow-Origin", "*")
		c.Writer.Header().Set("Access-Control-Allow-Credentials", "true")
		c.Writer.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization")
		c.Writer.Header().Set("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS")
		if c.Request.Method == http.MethodOptions {
			c.AbortWithStatus(http.StatusNoContent)
			return
		}
		c.Next()
	}
}

// Optional helper to enforce presence of meta tables; exported for service wiring.
func (s *Server) Health() error {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_, err := s.store.ListTables(ctx)
	return err
}
