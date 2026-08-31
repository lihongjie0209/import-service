# Import Service

平台统一异步数据导入服务。它负责文件接收、流式解析、全量验证、错误报告和显式确认；最终写入只通过中央 `platform.import.v1.ImportProviderService` 交给数据所有者，不查询或修改其他服务的数据库。

## 导入流程

1. 页面调用 `imports/create`，获得短时效对象存储上传 URL。
2. 上传完成后调用 `imports/complete-upload`；服务验证对象大小并通过 JetStream 排队。
3. Worker 流式解析 CSV、JSONL 或 XLSX，以有界批次调用 Provider 验证和规范化。
4. 有错误时生成 CSV 报告，任务进入 `validation_failed`；用户修正文件后调用 `imports/retry` 重新上传。
5. 全部有效时进入 `ready`；页面调用 `imports/confirm` 后才会按幂等批次写入业务服务。
6. 每个状态变更通过事务 Outbox 发布，结果文件按保留时间清理。

任务状态：

```text
uploading -> queued -> validating -> validation_failed -> uploading
                                \-> ready -> apply_queued -> applying -> succeeded
任意可取消状态 -> canceled；失败 -> failed；保留期结束 -> expired
```

## 前端接口

所有业务接口使用 POST+JSON 和统一响应结构：

- `POST /api/v1/imports/create`
- `POST /api/v1/imports/complete-upload`
- `POST /api/v1/imports/get`
- `POST /api/v1/imports/list`
- `POST /api/v1/imports/cancel`
- `POST /api/v1/imports/retry`
- `POST /api/v1/imports/confirm`
- `POST /api/v1/imports/error-report`

健康检查支持 `GET|POST /live`、`GET|POST /ready`。Swagger UI 默认位于 `/swagger/index.html`。

## 服务间接口

中央契约来自 `github.com/lihongjie0209/platform-protos@v0.17.0`：

- `platform.import.v1.ImportService`：导入任务管理。
- `platform.import.v1.ImportProviderService`：业务服务实现数据集描述、批次验证/规范化和幂等批次应用。

Provider 必须在本服务内验证租户和调用者权限；`ApplyRows` 必须以 `job_id + batch_number` 或请求中的 idempotency key 建立持久幂等边界。禁止把整个文件加载到内存，也不能依赖 import-service 直接写业务表。

## 数据与配置

- 默认 PostgreSQL 数据库：`platform`
- 独立 Schema：`data_import`
- 独立迁移表：`import_schema_migrations`
- 支持 PostgreSQL、KingbaseES 和 MySQL
- 对象存储使用 S3/MinIO；事件总线使用 NATS JetStream `PLATFORM_EVENTS`
- 配置优先级：环境变量 > `config-{profile}.yaml` > `config.yaml` > 默认值

生产密钥、数据库 DSN、JWKS、NATS、对象存储和上游凭据必须由 Secret 系统通过 `APP_` 环境变量注入。

## 验证

```bash
make test
make test-race
make lint
make swagger-check
make test-integration   # 只编译 integration build tag，不启动容器
make build              # 注入 version、Git commit、build time
```

GitHub CI 使用 `make ci-test-integration` 执行 Testcontainers；本机开发不运行容器化集成或系统测试。
