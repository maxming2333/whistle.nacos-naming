# whistle.nacos-naming

一个 [whistle](https://github.com/avwo/whistle) 插件，让域名映射规则通过 Nacos 服务发现动态解析目标地址，替代手写的静态 IP。

## 解决的问题

传统 whistle host 映射写法需要手写固定 IP：

```
admin-feature.example.com 10.73.244.159:8080
```

当后端服务部署在 K8s 上时，Pod 重新部署后 IP 会变化，这条规则需要频繁手动修改。用本插件后，规则改为引用 Nacos 服务名，插件会自动订阅该服务在 Nacos 中的实例列表，动态选出一个健康实例作为转发目标：

```
admin-feature.example.com nacos-naming://{"serverList":"127.0.0.1:8848","serviceName":"admin-feature"}
```

## 安装

```bash
npm i -g whistle.nacos-naming
```

## 使用

在 whistle Rules 里配置：

```
your-domain.example.com nacos-naming://{"serverList":"127.0.0.1:8848","namespace":"public","groupName":"DEFAULT_GROUP","serviceName":"your-service","metadata":{"version":"v2"}}
```

也可以引用 whistle Values 里的具名配置：

```
your-domain.example.com nacos-naming://{your-nacos-config}
```

### 配置字段

配置内容支持标准 JSON，也兼容 JSON5 语法（属性名可不加引号、单引号字符串、尾随逗号）：

| 字段 | 必填 | 默认值 | 说明 |
|---|---|---|---|
| `serverList` | 是 | 无 | Nacos 服务端地址，支持字符串（单个或逗号分隔）或字符串数组 |
| `serviceName` | 是 | 无 | Nacos 服务名 |
| `namespace` | 否 | `public` | Nacos 命名空间 |
| `groupName` | 否 | `DEFAULT_GROUP` | Nacos 分组 |
| `metadata` | 否 | 不过滤 | 用于筛选实例的 key-value，精确匹配（AND 逻辑） |

### 实例选择策略

- 只从 `healthy && enabled` 的实例中选择
- 按 `metadata` 过滤（如果配置了）
- 从剩余实例中选 `weight` 最高的一个（保证本地调试时目标地址稳定，不做负载均衡）

### 错误处理

配置错误、Nacos 连接失败、无健康实例等情况，插件会返回 `502` 并在响应体中给出具体错误原因，方便定位问题。

## 管理页面

在 whistle 插件列表中点击 `nacos-naming` 插件名（或 Option 按钮），可以查看当前所有订阅的状态（健康实例列表、最后推送时间等），并支持手动刷新或取消订阅。

## License

MIT
