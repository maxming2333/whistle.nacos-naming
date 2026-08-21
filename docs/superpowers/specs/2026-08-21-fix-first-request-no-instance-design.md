# 修复:首次命中 serviceName 请求提示"找不到实例"的设计文档

## 背景与问题

whistle.nacos-naming 插件启动后,当某个 `serviceName` **第一次**被请求命中时,总是会提示对应的服务"不存在或当前没有健康实例",导致请求以 502 失败。用户刷新(重试)同一个请求后,就能正常转发成功。

## 根因分析

调用链:`rules-server.ts` (`server.on('request', ...)`) → `handle-request.ts` 的 `handleRuleValue()` → `subscription-manager.ts` 的 `manager.getOrCreate(config)` → `select-instance.ts` 的 `selectInstance(snapshot.instances, ...)`。

`SubscriptionManager.getOrCreate()`(`src/nacos/subscription-manager.ts:88-127`)在某个 key 第一次被请求时:

1. 在 `entries` Map 中未命中,创建一个新的 `NacosNamingClient`;
2. 构造 `entry`,其中 `instances` 初始为空数组 `[]`,`status` 为 `'connecting'`;
3. 调用 `client.subscribe(...)` 注册一个**异步回调**(`listener`)——这一步只是告知 nacos-sdk"我要订阅",sdk 内部会去发起网络请求拉取首批实例、建立长轮询等,但 `subscribe()` 本身不等待、不返回 Promise,几乎立即返回;
4. `entries.set(key, entry)` 后,**同步**返回 `toSnapshot(entry)`,此刻 `entry.instances` 几乎必然还是第 2 步设置的空数组,因为"网络请求 nacos server → 拿到数据 → 触发 listener"这条异步链路不可能在这次同步函数调用内完成。

而 `handle-request.ts:44-45`:

```ts
const snapshot = manager.getOrCreate(config);
const result = selectInstance(snapshot.instances, config.metadata);
```

这两行同步顺序执行,中间没有任何等待。因此第一次请求拿到的 `snapshot.instances` 必然是空数组,`selectInstance` 返回 `NO_INSTANCE`,最终报错。

第二次请求命中的是同一个 Map 里已存在的 entry,此时 nacos 的推送回调(`listener`)早已被触发过一次,`entry.instances` 已经被真实数据填充,所以重试能成功。

`refresh()`(`subscription-manager.ts:130-146`)虽然是 `async` 并调用了 `client.getAllInstances(...)` 主动拉取数据,但当前代码没有在 `getOrCreate` 首次创建 entry 时调用它,即没有"创建后同步等待一次数据"的兜底逻辑,这是本次修复要补上的缺口。

现有测试(`subscription-manager.test.ts`、`handle-request.test.ts`)里的 `FakeNacosClient.subscribe()` 和 `pushInstances()` 都是同步方法,推送在"创建订阅"后立即同步触发,没有任何异步 gap,因此没有覆盖也没有捕获这个真实存在的竞态窗口。

## 修复方案

**核心思路:** 首次创建订阅 entry 时,不再立即同步返回空快照,而是复用同一个 `client` 先发起一次 `getAllInstances(...)` 网络请求拉取首批实例数据,等这次请求完成(成功、失败或超时)后再返回快照给调用方。已存在的 entry(非首次)不受影响,继续走原来的同步缓存路径。

### 1. `subscription-manager.ts` 改动

- `SubscriptionEntry` 新增字段 `initialLoad: Promise<void>`:仅在**首次创建** entry 时被赋值为"先调用 `getAllInstances` 拉首批数据、再调用 `subscribe` 注册增量推送"的组合 Promise;成功后把结果写入 `entry.instances`、`status = 'active'`;失败则 `status = 'error'`、记录 `lastError`,但不阻塞后续走推送更新。
- `getOrCreate(config): SubscriptionSnapshot` 方法签名和行为保持不变(仍是同步方法,供已有测试和 `listAll` 等内部场景复用),但新建 entry 时会把上述 `initialLoad` Promise 挂到 entry 上并存入 `entries` Map。
- 新增 `waitReady(key: string, timeoutMs?: number): Promise<SubscriptionSnapshot | null>` 方法:
  - 找不到对应 entry 时返回 `null`;
  - 如果 entry 存在 `initialLoad` 且尚未完成,则 `Promise.race([entry.initialLoad, timeout(timeoutMs)])` 等待其完成或超时;
  - 超时后不取消后台的 `initialLoad`(数据到达后仍会正常写入缓存,供下次请求使用),而是直接返回当前快照,并标记一个"仍在初始化中"的信号(通过 `status === 'connecting'` 已经能表达,无需新增字段);
  - 等待完成或者本来就没有 `initialLoad`(entry 早已 ready)时,返回最新快照。
  - 默认超时时长 `10000`(10 秒),可通过参数覆盖(主要用于测试)。

### 2. `handle-request.ts` 改动

- `handleRuleValue` 改为 `async function`,返回 `Promise<string>`。
- 调用 `manager.getOrCreate(config)` 拿到初始快照后,调用 `await manager.waitReady(snapshot.key)` 获取就绪(或超时)后的最新快照,再传给 `selectInstance`。
- 新增超时场景的错误文案:当 `waitReady` 判定为"等待超时且仍无健康实例"时(即 `status === 'connecting'`,说明首次拉取仍未完成),返回专门的提示文案,与"服务不存在/无健康实例"区分开:

  ```
  服务 ${serviceName} 订阅初始化超时(10s),请稍后重试 (namespace: ..., group: ...)
  ```

  其余错误分支(`METADATA_MISMATCH`、真正的 `NO_INSTANCE` 且 `status !== 'connecting'`)维持现有文案不变。

### 3. `rules-server.ts` 改动

- `server.on('request', ...)` 的回调改为 `async (req, res) => { ... }`,内部 `await handleRuleValue(...)` 后再 `res.end(response)`。Node 原生 `EventEmitter` 支持 async 监听器(未处理的 rejection 由现有 `try/catch` 包裹,`catch` 块逻辑不变)。

### 4. 超时时长与提示文案(已与用户确认)

- 超时时长:**10 秒**。
- 超时后的提示文案:采用更精确的提示,与"真的没有实例"区分,即:

  ```
  服务 ${serviceName} 订阅初始化超时(10s),请稍后重试 (namespace: ..., group: ...)
  ```

### 执行流程图(修复后)

**首次命中:**
```
请求到达 rules-server.ts:on('request') (async)
  → await handleRuleValue()
    → parseRuleValue() 得到 config
    → manager.getOrCreate(config)
        → entries.get(key) 未命中
        → new NacosNamingClient(...)
        → entry = { instances: [], status: 'connecting', ... }
        → client.subscribe(..., listener)         ← 注册增量推送
        → entry.initialLoad = client.getAllInstances(...)
            .then(hosts => { entry.instances = ...; entry.status = 'active'; })
            .catch(err => { entry.status = 'error'; entry.lastError = ...; })
        → entries.set(key, entry)
        → return snapshot(instances=[])            ← 同步返回的初始快照(不直接使用)
    → await manager.waitReady(key, 10000)
        → race(entry.initialLoad, timeout(10000))
        → 正常情况下 initialLoad 在几十~几百 ms 内 resolve
        → 返回最新快照(instances=真实数据)
    → selectInstance(真实数据, metadata) → ok:true
  → res.end('* host://ip:port')                     ← 第一次请求即可成功
```

**nacos server 异常/超时场景:**
```
  → await manager.waitReady(key, 10000)
      → 10 秒内 initialLoad 未完成 → 超时分支
      → 返回当前快照(instances=[], status='connecting')
  → selectInstance([], metadata) → NO_INSTANCE
  → handleRuleValue 判定 status==='connecting' → 返回"订阅初始化超时(10s),请稍后重试"
  → res.end(502 错误规则)
```

**非首次(已存在 entry):**
```
  → manager.getOrCreate(config) → entries.get(key) 命中,直接返回快照
  → manager.waitReady(key) → entry 没有 pending 的 initialLoad(已完成)→ 立即返回最新快照
  → selectInstance(真实数据, metadata) → 正常
```

对已有订阅的请求路径几乎没有额外开销(`waitReady` 在 `initialLoad` 已 resolve 时是同步快速返回)。

## 测试改造

1. **`subscription-manager.test.ts`**:
   - `FakeNacosClient.getAllInstances` 需要支持模拟"延迟 resolve"(比如通过外部可控的 Promise/resolve 函数),以便测试"首次 getOrCreate 后,在 initialLoad resolve 前 `waitReady` 应该等待;resolve 后应该拿到数据"。
   - 新增测试:验证 `waitReady` 在超时时长内 `initialLoad` 未完成时,会在超时后返回当前(仍为空)快照,而不是无限等待。可通过传入很小的 `timeoutMs`(如 10ms)加速测试,不依赖真实 10 秒。
   - 已有测试("相同 key 只创建一个 client"等)基本不受影响,因为 `getOrCreate` 签名和同步行为不变。

2. **`handle-request.test.ts`**:
   - `handleRuleValue` 改为 `async`,所有测试用例需要改为 `await handleRuleValue(...)` 并将 `it` 回调标记为 `async`。
   - `managerWithInstances` 辅助函数:让 `FakeNacosClient.getAllInstances` 直接返回传入的 `hosts`,这样首次 `getOrCreate` 后 `initialLoad` resolve 即可拿到数据,不再需要额外调用 `pushInstances` 来"补数据"(不过 `pushInstances` 仍保留,用于测试后续增量推送场景)。
   - 新增核心回归测试:**验证"首次请求"(不经过任何 `pushInstances` 模拟推送、不刷新)就能直接拿到实例并转发成功**,这是本次 bug 修复要保证的核心场景。
   - 新增测试:验证 `initialLoad` 超时(通过 `waitReady` 的可覆盖超时参数模拟极短超时)时返回"订阅初始化超时"的错误文案。

3. **`rules-server.ts`** 目前没有专门的单测文件,本次不额外新增(改动只是包一层 async/await,行为等价),如有需要可后续补充集成测试。

## 影响范围

- `src/nacos/subscription-manager.ts`:新增 `initialLoad` 字段与 `waitReady` 方法,`getOrCreate` 内部实现调整(新建 entry 时发起首拉),对外接口不变。
- `src/nacos/handle-request.ts`:`handleRuleValue` 签名从同步变为 `async`(返回 `Promise<string>`),调用方需要适配。
- `src/rules-server.ts`:`request` 事件回调改为 `async`。
- 不涉及 `select-instance.ts`、`parse-rule-value.ts`、`ui-server/*` 的改动。
- 对性能的影响:仅首次命中某个 `serviceName` 的请求会多等待一次网络往返(通常几十到几百毫秒,最多 10 秒超时),后续请求不受影响。
