# E-Commerce Big Data Analysis

Python, Spark, API, Streamlit

## 项目简介

该项目是一套电商大数据分析系统，基于 Apache Spark 分布式计算框架与 Python 生态构建完整的数据处理分析管道，结合 Streamlit 实现交互式可视化仪表板，支持业务人员实时查看销售趋势、用户分群、商品分类表现等核心业务指标。系统支持多源数据接入、自动化数据清洗转换，并通过 FastAPI 提供标准化 RESTful API，实现跨系统数据集成与共享。

---

## 技术栈

| 技术 / 框架    | 用途                                   |
| -------------- | -------------------------------------- |
| Python         | 核心开发语言                           |
| Apache Spark   | 分布式大规模数据处理                   |
| Streamlit      | 交互式数据可视化仪表板                 |
| FastAPI        | 高性能 RESTful API 服务                |
| Git            | 版本控制与协作                         |
| Docker         | 容器化部署，环境一致性保障             |
| Unit Testing   | 核心功能单元测试，保障代码质量         |
| Logging        | 系统日志记录，便于调试与问题定位       |

---

## 功能特性

### 1. 数据清洗与处理

- 自动化完成数据去重、缺失值填充、格式标准化等清洗操作
- 基于 Spark 分布式计算，高效处理 TB 级电商销售数据，兼顾处理速度与资源利用率
- 支持增量数据处理，适配实时业务数据更新场景

### 2. 数据分析与可视化

- **核心分析维度**：销售趋势（日/周/月）、用户活跃度、商品分类销量、区域销售分布等
- **Streamlit 交互式界面**：支持数据筛选、维度切换、图表联动，支持导出分析结果
- **可视化类型**：折线图（趋势）、饼图（占比）、热力图（区域分布）、柱状图（分类对比）

### 3. RESTful API 服务

- **标准化数据接口**：支持销售数据查询、用户分群结果获取、多维度数据过滤
- **接口特性**：自动生成 OpenAPI 文档（`/docs`）、请求参数校验、响应格式标准化
- 支持跨域访问，适配前端/第三方系统集成需求

### 4. 性能与稳定性保障

- Spark 任务优化：分区策略、内存管理、Shuffle 优化，提升数据处理效率
- 完善的日志体系：分级日志（INFO/ERROR/DEBUG）、关键流程日志记录、异常追踪
- 容错处理：数据读取失败降级、API 请求超时重试、计算任务异常捕获

### 5. 工程化保障

- 全覆盖单元测试：核心数据处理函数、API 接口、数据校验逻辑均配备测试用例
- CI/CD 适配：支持自动化测试、镜像构建、环境部署，提升开发迭代效率

---

## 快速开始

### 环境要求

- Python >= 3.7
- Apache Spark >= 3.x
- Docker（可选，简化部署流程）
- Git

### 安装依赖

```bash
# 克隆仓库
git clone https://github.com/ahkwy/e-commerce-big-data.git
cd e-commerce-big-data

# 创建并激活虚拟环境
python3 -m venv venv
# Mac/Linux
source venv/bin/activate
# Windows
venv\Scripts\activate

# 安装依赖包
pip install -r requirements.txt
```

### 运行项目

```bash
# 启动数据处理与分析核心服务
python app.py

# 启动 Streamlit 交互式仪表板（新终端）
streamlit run dashboard.py

# 测试 API 接口
curl http://127.0.0.1:8000/api/v1/sales
```

---

## 项目结构

```
e-commerce-big-data/
├── app.py                    # 数据处理/分析核心逻辑（Spark 任务、数据转换）
├── dashboard.py              # Streamlit 可视化仪表板入口
├── api/                      # API 服务模块
│   ├── __init__.py
│   └── sales_api.py          # 销售数据相关 API 路由与逻辑
├── data/                     # 数据目录
│   ├── raw_data.csv          # 原始未清洗电商数据
│   └── processed_data.csv    # 清洗/转换后的结构化数据
├── tests/                    # 单元测试目录
│   ├── test_app.py           # 数据处理逻辑测试
│   └── test_api.py           # API 接口测试
├── Dockerfile                # Docker 镜像构建配置
└── requirements.txt          # Python 依赖清单
```

---

## 开发与贡献

1. Fork 本仓库并克隆至本地开发环境
2. 创建功能分支：`git checkout -b feature/your-feature-name`
3. 开发新功能/修复问题，确保所有单元测试通过：`pytest -v`
4. 提交代码并推送至远程分支：`git push origin feature/your-feature-name`
5. 提交 Pull Request，清晰描述变更内容、解决的问题及测试验证结果

---

## 未来发展方向

- **算法增强**：引入机器学习模型，实现用户流失预测、销售趋势预测、商品推荐等
- **API 扩展**：增加批量数据导入/导出、自定义分析任务提交接口
- **性能优化**：Spark 集群化部署、数据分区策略优化、缓存机制引入
- **云原生部署**：适配 Kubernetes 编排，支持弹性扩缩容，对接云存储（S3/HDFS）
- **多源数据接入**：支持数据库（MySQL/PostgreSQL）、日志文件、第三方平台 API 数据接入

---

## 联系方式

- 问题反馈：通过 [GitHub Issues](https://github.com/ahkwy/e-commerce-big-data/issues) 提交
- 邮件咨询：邮箱 @example.com
