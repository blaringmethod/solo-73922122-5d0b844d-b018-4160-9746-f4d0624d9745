# 河流水质站日 CSV 发布服务

面向科研核验的不可改原件发布 API：原件原字节存共享卷，SQLite 只存目录与摘要，
以 SHA-256 固定深度 Merkle 树出具检查点、单件纳入证明与检查点追加证明，
并提供**分片清单与按片取回**（支持并行读站、单片失败重取），
以及完全离线的验证函数/CLI。两个实例挂载同一卷并发发布，不分叉、不覆盖。

## 1. 目录结构

```
app/
  config.py     运行配置（DATA_DIR、1 MiB、128 件、树深度 7、清单最多 8 个发布）
  storage.py    原件字节存储：内容寻址 + 临时文件 fsync + 原子改名 + 按字节区间读
  db.py         SQLite 持久层：WAL、BEGIN IMMEDIATE 串行化、启动自检（含清单自检）
  merkle.py     Merkle 树纯函数：根、纳入证明、追加证明、离线验证
  csv_records.py 带表头 UTF-8 CSV 字节级解析：引号内换行不切断、字节区间定位
  sharding.py   分片纯函数：划片计划、片载荷组装与摘要、请求指纹
  verify.py     离线核验函数与 CLI（python -m app.verify ...）
  schemas.py    HTTP 请求/响应模型
  main.py       FastAPI 路由（阻塞调用均走线程池）
tests/          46 项 pytest 用例（算法、API、故障路径、分片与跨行记录）
scripts/
  demo.sh                 HTTP 端到端演示（含制单、跨行记录、按片取回）
  dual_instance_check.py  双进程共卷并发/故障转移/分片集成检查
Dockerfile
docker-compose.yml        两个实例共卷，端口 8001/8002
```

## 2. 启动

### Docker Compose（交付目标形态）

```bash
docker compose up --build
# 实例 A：http://localhost:8001/docs
# 实例 B：http://localhost:8002/docs
```

两实例镜像相同、挂载同一命名卷 `shared-data`（`/data`），
`MAX_FILE_BYTES=1048576`、`MAX_RELEASES=128`（对应 Merkle 深度 7，128 个叶子槽）。

### 本地直接运行

```bash
python -m venv .venv && . .venv/bin/activate
pip install -r requirements.txt
DATA_DIR=./data uvicorn app.main:app --port 8000
# 打开 http://localhost:8000/docs
```

## 3. API 一览（/docs 可交互演示）

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| POST | `/releases` | 表单 `station_id`、`sample_date`(YYYY-MM-DD)、`order_no`、`file`；同单同输入重试复用（`reused=true`），异输入 409，超 1 MiB 返回 413，超 128 件返回 409，写盘失败返回 507 |
| GET | `/releases` | 全部发布目录记录（不含字节） |
| GET | `/releases/{station}/{date}/{order}` | 单条目录记录 |
| GET | `/releases/{station}/{date}/{order}/download` | 下载已提交原件，文件名带站日单号 |
| GET | `/releases/{station}/{date}/{order}/inclusion-proof` | 单件纳入证明（恒 7 步路径） |
| GET | `/checkpoints` | 全部检查点（含 0 号空树检查点） |
| GET | `/checkpoints/latest` | 最新检查点（条数 + Merkle 根） |
| GET | `/checkpoints/{seq}` | 任意旧检查点 |
| GET | `/checkpoints/{old_seq}/append-proof` | 旧检查点 → 最新检查点的追加证明 |
| POST | `/manifests` | 制分片清单：JSON `checkpoint_seq`、`release_ids`（≤8 个发布序号，顺序不敏感）、`max_records_per_shard`；同请求重试返回原清单（`reused=true`） |
| GET | `/manifests` | 列出全部分片清单 |
| GET | `/manifests/{id}` | 单份清单：各片编号、发布序号、记录范围、字节范围与载荷摘要 |
| GET | `/manifests/{id}/shards/{shard_no}` | 按片号取回：仅读表头+该片字节，原样返回；摘要不符/读不全返回 409 |
| GET | `/health` | 启动自检结果（缺件/摘要不符时 `degraded`，并拒发证明、下载与制单） |

## 4. 核验模型（科研组离线使用）

### 叶子与树

* 叶子载荷为带版本号的规范化 JSON（键排序、无空白）：
  `{v, station_id, sample_date, order_no, blob_sha256, size}`，
  叶子哈希 = `SHA256(0x00 || 载荷)`；内部节点 = `SHA256(0x01 || left || right)`，域分隔防二阶碰撞。
* 固定深度 7 的完整二叉树，未用槽位以公开常量 `EMPTY_LEAF` 占位：
  追加只改变新叶子到根的路径，旧证明不会因树“长高”失效。
* **纳入证明**：7 个兄弟哈希（O(log n)），每步带 `bit` 与下标逐位绑定，防重放到其他下标。
* **追加证明**：把旧前缀与新增区间分别平铺为对齐的完美子树，条目数 ≤ 2×深度（实测 128 件规模最多 8 条）。

### 离线核验（无需服务端、无需网络）

```bash
# 单件：凭留存的纳入证明 + 待验原件，核验站日归属与检查点纳入
python -m app.verify inclusion --proof inclusion.json --file 原件.csv

# 历史前缀：凭两个留存检查点 + 追加证明，核验仅追加关系
python -m app.verify append --proof append.json \
  --old-checkpoint cp-old.json --new-checkpoint cp-new.json
```

也可直接导入纯函数：`app.verify.verify_release_bytes`、`app.verify.verify_append_proof`。
篡改原件、替换历史条目、伪造检查点根、证明重放到其他下标均会被拒绝（退出码 1）。

## 5. 分片清单与按片取回（并行读站、单片失败重取）

### 5.1 制单请求

`POST /manifests` 提交 JSON：

```json
{ "checkpoint_seq": 3, "release_ids": [12, 7], "max_records_per_shard": 1000 }
```

* `checkpoint_seq`：检查点序号（即截至该点的发布条数）；
* `release_ids`：该检查点覆盖范围内的发布序号，**至多 8 个**、不可重复，顺序不敏感；
* `max_records_per_shard`：每片记录数上限（正整数）。

### 5.2 字节级 CSV 划片规则

* 仅接受**带表头的合法 UTF-8 CSV**：在字节层面按 RFC 4180 风格解析，
  记录分隔为 `\n` 或 `\r\n`，**引号字段内的换行属于同一条记录**，
  `""` 为转义引号；非法 UTF-8、未闭合引号、裸 CR、空文件、缺表头等一律判无效。
* 第一条记录为表头，其余为数据记录；按 `max_records_per_shard` 对完整记录切分，
  **每条数据记录恰好分入一片**，片内记录连续、片间无重叠无缺口（制单时做整文件
  连续覆盖自检，拼回逐字节等于原件）。
* 片号在清单内从 1 起、按发布序号升序、再按片内顺序稠密分配。
* 清单逐片固定：所属发布序号、数据记录半开区间 `[record_lo, record_hi)`、
  数据段半开字节区间 `[byte_start, byte_end)`、表头字节区间、
  以及取回载荷（表头+数据段）的 SHA-256 与字节数。

### 5.3 原子性、幂等与片号稳定

* 制单前先核对每个发布的原件**存在且摘要相符**并完成 CSV 格式校验；
  任一原件缺失、摘要不符或格式无效，**整份新清单不生效**（不写入任何清单行）。
  缺件/摘要不符返回 409，请求参数或格式问题返回 422。
* 请求指纹 = 规范化 JSON（发布序号排序后）的 SHA-256，存入 `manifests.request_key`
  唯一列：**同样的请求重试返回原清单**（`reused=true`、编号与片号不变），
  并发同指纹请求由 `BEGIN IMMEDIATE` 串行化，恰有一次写入。
* 清单是绑定检查点的**只增快照**：后续发布与实例重启都不会改变既有片号、
  记录范围或字节范围；清单与片持久化在共享卷 SQLite，重启后从卷上复现。

### 5.4 按片取回与故障隔离

`GET /manifests/{id}/shards/{shard_no}`：

* 服务端只 `seek+read` 原件的**表头区间与该片数据段区间**（`BlobStore.read_range`），
  不全量加载原件；响应体 = 原件表头原样字节 + `\n` + 该片数据段原样字节，
  引号、CRLF、多字节字符、BOM 全部原样保留；元数据经 `X-Shard-Meta`、
  `X-Payload-SHA256` 响应头返回。
* 取回时复算载荷 SHA-256 并与制单摘要比对：**字节读不全或与制单时不符即拒绝（409）**，
  绝不返回损坏数据；片号越界返回 404。
* 校验按片独立：**某片数据段损坏只影响该片，其他完好片仍可正常取回**，
  故障片在原件恢复后即可重取成功——对应并行读取站单个作业失败后重取原分片。

## 6. 崩溃安全与并发不变量

1. **先落盘后入库**：原件写临时文件 → `fsync` → `os.replace` 原子改名 → `fsync` 目录，
   成功后才开数据库事务；写盘失败无库记录、临时文件清理、可原样重试。
2. **串行化发布**：每个发布事务 `BEGIN IMMEDIATE`，双实例写操作被 SQLite 锁串行化，
   后到者等待后重检唯一键 `(station_id, sample_date, order_no)`；无 UPDATE/DELETE 路径。
3. **只增不改**：同键同字节复用、同键异字节 409；检查点与记录均只追加。
4. **启动自检**：重算全部叶子与根，逐件校验卷上原件摘要并检查序号稠密、检查点连续；
   发现缺件即 `degraded`，拒绝发布/证明/下载，绝不输出缺件记录。
5. **重启有效**：历史检查点根由固定深度树确定，重启后逐点复算一致，留存证明持续有效。
6. 阻塞式磁盘/DB 调用均在任意线程池执行，锁等待不阻塞事件循环；
   写锁冲突在 `busy_timeout` 之外另有 3 次有限重试。

## 7. 测试结果（本环境实测）

* `pytest tests/`：**46 项全部通过**
  - Merkle：根稳定性、纳入证明、全部前缀追加证明、篡改/重放/重叠/缺口拒绝、平铺覆盖性质；
  - API：发布、幂等复用、409 冲突、413/422 限额与校验、128 上限、下载字节一致、两类证明与离线核验；
  - 故障：写盘失败不留记录且可重试、缺件自检与 503 拒服、孤立临时/原件无害、已引用原件被改可检出、重启后检查点可复算；
  - 分片（24 项）：字节级解析（引号内换行、CRLF、转义引号、无尾换行、UTF-8 多字节、BOM）、
    非法格式拒绝、划片恰好覆盖每条记录、制单幂等与片号稳定、后续发布/重启不改片号、
    缺件/摘要不符/格式无效整单不生效、跨行记录整片刻回、两片拼回逐字节等于原件、
    单片损坏 409 而邻片 200 的故障隔离、短读拒绝、404、重启后清单复现。
* `scripts/dual_instance_check.py`（两个独立 uvicorn 进程共卷）：**24 项全部通过**，
  覆盖并发同单（一写一复用）、10 件并发异单序号 1..11 稠密、跨实例取件/检查点/证明、
  第三进程挂同卷的故障转移视角与继续追加，以及**跨实例制单幂等、片号一致、
  第三实例按片取回跨行 CSV、两片拼回逐字节一致、越界片号 404**。
* `scripts/demo.sh`（真实 HTTP 端到端）：制单 → 同请求重试 `reused=true` →
  按片取回含引号内换行的跨行记录 → 两片数据段拼回与原件逐字节一致 → 篡改离线拒绝。
* 手工冒烟（真实 HTTP）：制单后篡改中间片数据段首字节，`shards/1`、`shards/3`
  返回 200、`shards/2` 返回 409 且提示摘要不符；恢复原件后三片均 200。
* 证明规模实测：纳入证明恒 7 步；追加证明最大条目随条数 1→2→3→…→8（对数增长）。

## 8. 剩余工作与已知限制（据实说明）

1. **本环境无 Docker 守护进程**，`docker compose` 未实际起容器验证；已用双 uvicorn
   进程共享同一 `DATA_DIR` 做等价并发/故障转移测试。Dockerfile 与 compose 配置已交付，
   需在具备 Docker 的环境执行一次 `docker compose up --build` 复核。
2. 沙箱以 seccomp 禁止 `kill` 系统调用，集成脚本无法终止演示进程，改为随机端口 +
   每次独立临时目录；脚本中的“重启”以第三进程挂同卷模拟（故障转移语义等价）。
3. 128 件上限在实现上是固定深度树的叶子槽位数（2 的幂，可经 `MAX_RELEASES` 调整）；
   非 2 的幂配置会在启动时直接报错。
4. 未引入鉴权/传输加密/TLS，定位为内网或网关后的发布服务；对外暴露需前置网关。
5. 共享卷自身的备份与异地容灾未包含在本服务内（卷快照/备份应由基础设施层提供）；
   服务能检出卷上缺件与篡改，但不会自动恢复字节。
6. **发布层不做 CSV 方言嗅探**（仅大小与空文件校验，保持对业务方言的宽容）；
   CSV 合法性在**制单时**严格校验，未纳入任何清单的原件不受影响。制单采用 RFC 4180
   风格的严格引用规则（引号只能出现在字段开头、仅接受 LF/CRLF 行尾），若上游存在
   非标准方言需先规范化。取回载荷固定以单个 `\n` 衔接表头与数据段，数据段内部
   字节（含原始 CRLF）原样保留；因此多片各自带表头，客户端按 `X-Shard-Meta` 的
   `byte_range` 取数据段即可无损拼回原件。
