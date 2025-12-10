# gyj 葡萄膜炎临床数据库后端

- 框架：Gin + SQLite + Viper(YAML 配置) + Zap 日志。
- 入口：`main.go`，默认读取同目录下的 `config.yaml`。
- 默认账号：`admin / uveitis`（可在配置文件修改）。

## 运行

```bash
cd backend
go run ./...      # 开发
# 或编译成 Windows 可执行文件
GOOS=windows GOARCH=amd64 go build -o gyj-backend.exe
```

## 主要 API

- `POST /api/login` 登录，返回 token。
- `GET /api/tables` 查询所有表及字段。
- `POST /api/tables` 创建表（包含字段中文别名与类型）。
- `POST /api/tables/:table/columns` 添加字段。
- `DELETE /api/tables/:table/columns` 删除字段。
- `GET /api/tables/:table/data` 带分页/搜索/排序的查询。
- `POST /api/tables/:table/data` 新增记录。
- `PUT /api/tables/:table/data/:id` 更新记录。
- `DELETE /api/tables/:table/data/:id` 删除记录。
- `POST /api/tables/:table/import` CSV/Excel 导入（按中文别名自动匹配）。
- `GET /api/tables/:table/summary?column=xxx` 数字字段的 SUM/AVERAGE/MAX/MIN/STD 等汇总。
