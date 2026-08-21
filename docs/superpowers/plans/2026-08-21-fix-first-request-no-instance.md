# 修复首次命中 serviceName 请求提示"找不到实例" Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 修复 whistle.nacos-naming 插件中"首次命中某个 serviceName 的请求总是提示找不到实例,刷新一次才能成功"的 bug,让第一次请求就能等到首批实例数据后再做转发决策。

**Architecture:** `SubscriptionManager` 在首次创建订阅 entry 时,除了注册增量推送(`subscribe`),还会立即发起一次 `getAllInstances` 首拉请求,并把这个 Promise 挂在 entry 上(`initialLoad`)。新增 `waitReady(key, timeoutMs)` 方法,供 `handleRuleValue` 在拿到快照后 `await`,最多等待 10 秒;超时后返回专门的"订阅初始化超时"错误文案。`handleRuleValue` 与 `rules-server.ts` 的请求回调都改为 `async`。已存在的订阅(非首次)不受影响,继续走原同步缓存路径。

**Tech Stack:** TypeScript, Jest + ts-jest, nacos npm 包 (`NacosNamingClient`)

## Global Constraints

- 超时时长固定为 10000ms(10 秒),硬编码常量,不做成外部可配置项。
- 超时后的错误文案必须与"服务不存在/无健康实例"区分,统一为:`服务 ${serviceName} 订阅初始化超时(10s),请稍后重试 (namespace: ${namespace}, group: ${groupName})`。
- `getOrCreate(config): SubscriptionSnapshot` 方法签名和同步返回行为保持不变,不能破坏已有测试(`subscription-manager.test.ts` 中"相同 key 只创建一个 client"等用例)。
- `entry.client.getAllInstances(serviceName, groupName)` 参数顺序遵循现有 `refresh()` 方法中已验证过的调用方式(`subscription-manager.ts` 现有代码)。
- 所有新增/修改的中文文案、注释均使用简体中文,代码标识符使用英文。
- Commit message 格式:`{类型}: {message}`,不需要项目编号 `#xxxxx`,也不需要追加 `[HST_AI_Tag: AI_Generated]` 尾巴(已与用户确认)。

---

### Task 1: `SubscriptionManager` 新增 `initialLoad` 首拉逻辑与 `waitReady` 方法

**Files:**
- Modify: `src/nacos/subscription-manager.ts`
- Test: `src/nacos/subscription-manager.test.ts`

**Interfaces:**
- Consumes: 现有 `SubscriptionManager` 类、`SubscriptionSnapshot`、`NacosClientFactory`、`buildSubscriptionKey`(均已存在于 `subscription-manager.ts`,不改名)。
- Produces:
  - `SubscriptionManager.waitReady(key: string, timeoutMs?: number): Promise<SubscriptionSnapshot | null>` —— 供 Task 2 的 `handle-request.ts` 使用。找不到 entry 返回 `null`;否则等待该 entry 的 `initialLoad`(若存在且未完成)最多 `timeoutMs` 毫秒(默认 `10000`),返回等待后的最新快照(`toSnapshot(entry)`)。
  - `SubscriptionSnapshot.status` 语义扩展说明:等待超时后返回的快照如果 `status === 'connecting'`,表示"首拉仍未完成",供 `handle-request.ts` 判断走哪个错误文案分支。此字段本身已存在,不新增字段。

- [ ] **Step 1: 写失败测试 —— 首次 `getOrCreate` 后,`waitReady` 应等待 `getAllInstances` 首拉完成并返回真实数据**

在 `src/nacos/subscription-manager.test.ts` 现有 `FakeNacosClient` 类中,把 `getAllInstances` 改造为可外部控制何时 resolve(用于模拟真实网络延迟),同时保留原有"直接返回固定数据"的默认行为不破坏已有测试。在文件顶部 `FakeNacosClient` 类定义中,将 `getAllInstances` 方法替换为:

```ts
class FakeNacosClient {
  public subscribeCalls: any[] = [];
  public getAllInstancesCalls: any[] = [];
  private listener: ((hosts: any[]) => void) | null = null;
  // 可选:外部注入的 getAllInstances 返回值/延迟控制器，
  // 用于模拟"首拉尚未完成"的真实网络延迟场景。
  public getAllInstancesImpl: (
    serviceName: string,
    groupName: string
  ) => Promise<any[]> = async () => [
    {
      ip: '10.0.0.9',
      port: 8080,
      weight: 1,
      healthy: true,
      enabled: true,
      metadata: {},
    },
  ];

  subscribe(info: any, listener: (hosts: any[]) => void) {
    this.subscribeCalls.push(info);
    this.listener = listener;
  }

  unSubscribe(info: any, listener: (hosts: any[]) => void) {
    this.listener = null;
  }

  async getAllInstances(serviceName: string, groupName: string) {
    this.getAllInstancesCalls.push({ serviceName, groupName });
    return this.getAllInstancesImpl(serviceName, groupName);
  }

  close() {
    // no-op
  }

  // 测试辅助方法：模拟 Nacos 推送
  pushInstances(hosts: any[]) {
    this.listener && this.listener(hosts);
  }
}
```

然后在 `describe('SubscriptionManager', ...)` 块内新增测试用例:

```ts
  it('首次 getOrCreate 后，waitReady 会等待 getAllInstances 首拉完成并返回真实数据', async () => {
    let fakeClient!: FakeNacosClient;
    const manager = new SubscriptionManager((clientConfig) => {
      fakeClient = new FakeNacosClient();
      return fakeClient as any;
    });

    let resolveFirstLoad!: (hosts: any[]) => void;
    fakeClient = undefined as any; // 占位，factory 内会重新赋值
    const config = baseConfig();
    const managerWithDelay = new SubscriptionManager((clientConfig) => {
      fakeClient = new FakeNacosClient();
      fakeClient.getAllInstancesImpl = () =>
        new Promise((resolve) => {
          resolveFirstLoad = resolve;
        });
      return fakeClient as any;
    });

    const snapshot = managerWithDelay.getOrCreate(config);
    // 首次同步返回时，instances 仍为空（尚未等待）
    expect(snapshot.instances).toEqual([]);

    const waitPromise = managerWithDelay.waitReady(snapshot.key);

    // 模拟 nacos server 延迟返回首批实例
    resolveFirstLoad([
      {
        ip: '10.0.0.5',
        port: 8888,
        weight: 3,
        healthy: true,
        enabled: true,
        metadata: {},
      },
    ]);

    const ready = await waitPromise;
    expect(ready).not.toBeNull();
    expect(ready!.instances).toEqual([
      {
        ip: '10.0.0.5',
        port: 8888,
        weight: 3,
        healthy: true,
        enabled: true,
        metadata: {},
      },
    ]);
    expect(ready!.status).toBe('active');
  });

  it('waitReady 对已存在（非首次）的 entry 立即返回最新快照，不重复等待', async () => {
    let fakeClient!: FakeNacosClient;
    const manager = new SubscriptionManager((clientConfig) => {
      fakeClient = new FakeNacosClient();
      return fakeClient as any;
    });

    const snapshot = manager.getOrCreate(baseConfig());
    // 等首次 initialLoad 完成
    await manager.waitReady(snapshot.key);

    // 再次调用 getOrCreate（命中已存在 entry），waitReady 应立即返回，不再等待
    const snapshot2 = manager.getOrCreate(baseConfig());
    const ready2 = await manager.waitReady(snapshot2.key);
    expect(ready2).not.toBeNull();
    expect(ready2!.status).toBe('active');
  });

  it('waitReady 对不存在的 key 返回 null', async () => {
    const manager = new SubscriptionManager(
      () => new FakeNacosClient() as any
    );
    const ready = await manager.waitReady('not-exist-key');
    expect(ready).toBeNull();
  });

  it('waitReady 超过 timeoutMs 仍未完成首拉时，返回当前快照（status 仍为 connecting）', async () => {
    let fakeClient!: FakeNacosClient;
    const manager = new SubscriptionManager((clientConfig) => {
      fakeClient = new FakeNacosClient();
      fakeClient.getAllInstancesImpl = () => new Promise(() => {}); // 永不 resolve
      return fakeClient as any;
    });

    const snapshot = manager.getOrCreate(baseConfig());
    const ready = await manager.waitReady(snapshot.key, 10);

    expect(ready).not.toBeNull();
    expect(ready!.status).toBe('connecting');
    expect(ready!.instances).toEqual([]);
  });
```

- [ ] **Step 2: 运行测试确认失败**

Run: `npx jest src/nacos/subscription-manager.test.ts`
Expected: FAIL —— 报错信息中包含 `manager.waitReady is not a function` 或 `TypeError: managerWithDelay.waitReady is not a function`(因为 `waitReady` 方法尚不存在)。

- [ ] **Step 3: 实现 `initialLoad` 首拉逻辑与 `waitReady` 方法**

打开 `src/nacos/subscription-manager.ts`,做以下修改:

1. 在 `SubscriptionEntry` 接口中新增 `initialLoad` 字段:

```ts
interface SubscriptionEntry extends SubscriptionSnapshot {
  client: NacosNamingClient;
  listener: (hosts: any[]) => void;
  initialLoad: Promise<void>;
}
```

2. 在 `getOrCreate` 方法内,构造 `entry` 之后、调用 `client.subscribe(...)` 之前的位置，新增首拉逻辑。完整替换 `getOrCreate` 方法为:

```ts
  getOrCreate(config: NacosRuleConfig): SubscriptionSnapshot {
    const key = buildSubscriptionKey(config);
    const existing = this.entries.get(key);
    if (existing) {
      return toSnapshot(existing);
    }

    const client = this.clientFactory({
      serverList: config.serverList,
      namespace: config.namespace,
    });

    const entry: SubscriptionEntry = {
      key,
      serverList: config.serverList,
      namespace: config.namespace,
      groupName: config.groupName,
      serviceName: config.serviceName,
      instances: [],
      createdAt: Date.now(),
      lastPushedAt: null,
      status: 'connecting',
      client,
      listener: () => {},
      initialLoad: Promise.resolve(),
    };

    const listener = (hosts: any[]) => {
      entry.instances = (hosts || []).map(toNacosInstance);
      entry.lastPushedAt = Date.now();
      entry.status = 'active';
    };
    entry.listener = listener;

    try {
      client.subscribe(
        { serviceName: config.serviceName, groupName: config.groupName },
        listener
      );
    } catch (err: any) {
      entry.status = 'error';
      entry.lastError = err.message;
    }

    // 首次创建订阅时，同步发起一次 getAllInstances 网络请求拉取首批实例数据，
    // 避免"订阅刚注册、推送还没到达"这段窗口期内查询到空列表。
    // 这个 Promise 挂在 entry 上，供 waitReady() 等待。
    entry.initialLoad = client
      .getAllInstances(config.serviceName, config.groupName)
      .then((hosts) => {
        entry.instances = (hosts || []).map(toNacosInstance);
        entry.lastPushedAt = Date.now();
        entry.status = 'active';
        entry.lastError = undefined;
      })
      .catch((err: any) => {
        entry.status = 'error';
        entry.lastError = err.message;
      });

    this.entries.set(key, entry);
    return toSnapshot(entry);
  }
```

3. 在 `refresh` 方法之后（`remove` 方法之前）新增 `waitReady` 方法:

```ts
  async waitReady(
    key: string,
    timeoutMs: number = 10000
  ): Promise<SubscriptionSnapshot | null> {
    const entry = this.entries.get(key);
    if (!entry) {
      return null;
    }

    await Promise.race([
      entry.initialLoad,
      new Promise<void>((resolve) => setTimeout(resolve, timeoutMs)),
    ]);

    return toSnapshot(entry);
  }
```

注意:`Promise.race` 中即使超时分支先完成，`entry.initialLoad` 仍会在后台继续运行，完成后正常写入 `entry.instances`，供下一次请求使用（不需要取消它，也不需要额外处理"忽略后续结果"的逻辑，因为 `entry` 是同一个对象引用）。

- [ ] **Step 4: 运行测试确认通过**

Run: `npx jest src/nacos/subscription-manager.test.ts`
Expected: PASS —— 所有用例（包括原有用例和新增 4 个用例）全部通过。

- [ ] **Step 5: 提交**

```bash
git add src/nacos/subscription-manager.ts src/nacos/subscription-manager.test.ts
git commit -m "$(cat <<'EOF'
fix: SubscriptionManager 首次订阅时同步首拉实例数据

新增 waitReady 方法，首次创建订阅 entry 时立即发起一次 getAllInstances
首拉请求，避免订阅刚注册、推送还未到达时查询到空列表。
EOF
)"
```

---

### Task 2: `handle-request.ts` 改为 async，接入 `waitReady` 与超时错误文案

**Files:**
- Modify: `src/nacos/handle-request.ts`
- Test: `src/nacos/handle-request.test.ts`

**Interfaces:**
- Consumes: `SubscriptionManager.waitReady(key: string, timeoutMs?: number): Promise<SubscriptionSnapshot | null>`（Task 1 产出）、`SubscriptionManager.getOrCreate(config): SubscriptionSnapshot`（已存在）、`selectInstance(instances, metadata): SelectResult`（已存在，不变）。
- Produces: `handleRuleValue(ruleValue: string, manager: SubscriptionManager): Promise<string>` —— 供 Task 3 的 `rules-server.ts` 使用。签名从同步 `string` 返回值变为 `Promise<string>`。

- [ ] **Step 1: 写失败测试 —— 首次请求（不经过任何 `pushInstances` 模拟推送）应直接拿到实例并转发成功**

打开 `src/nacos/handle-request.test.ts`，先把 `FakeNacosClient` 的 `getAllInstances` 改造为可注入返回值（复用 Task 1 中同样的思路，但这是独立文件，需要单独改）：

```ts
class FakeNacosClient {
  private listener: ((hosts: any[]) => void) | null = null;
  public instancesToReturn: any[] = [];

  subscribe(info: any, listener: (hosts: any[]) => void) {
    this.listener = listener;
  }
  unSubscribe() {
    this.listener = null;
  }
  async getAllInstances(): Promise<any[]> {
    return this.instancesToReturn;
  }
  pushInstances(hosts: any[]) {
    this.listener && this.listener(hosts);
  }
}
```

将现有 `managerWithInstances(hosts)` 辅助函数替换为异步版本（因为现在首拉数据要通过 `getAllInstances` 返回，而不是靠 `pushInstances` 补数据）：

```ts
async function managerWithInstances(hosts: any[]): Promise<SubscriptionManager> {
  let fakeClient!: FakeNacosClient;
  const manager = new SubscriptionManager((clientConfig) => {
    fakeClient = new FakeNacosClient();
    fakeClient.instancesToReturn = hosts;
    return fakeClient as any;
  });
  // 触发创建订阅（首拉会立即从 fakeClient.instancesToReturn 里取数据）
  const snapshot = manager.getOrCreate({
    serverList: '10.0.0.1:8848',
    namespace: 'public',
    groupName: 'DEFAULT_GROUP',
    serviceName: 'admin-feature',
    metadata: null,
  });
  await manager.waitReady(snapshot.key);
  return manager;
}
```

将所有调用 `managerWithInstances(...)` 的测试用例改为 `await managerWithInstances(...)`，并把这些 `it(...)` 回调标记为 `async`。同时把所有 `handleRuleValue(...)` 调用改为 `await handleRuleValue(...)`。完整替换 `describe('handleRuleValue', ...)` 块为:

```ts
describe('handleRuleValue', () => {
  it('规则值不是合法 JSON 时返回 502 错误规则', async () => {
    const manager = new SubscriptionManager(() => new FakeNacosClient() as any);
    const response = await handleRuleValue('not-json', manager);
    const { rules, body } = parseErrorResponse(response);
    expect(rules).toContain('statusCode://502');
    expect(rules).toContain('resBody://{');
    expect(body).toContain('不是合法 JSON');
    expect(body).toContain('[whistle.nacos-naming]');
  });

  it('缺少必填字段时返回 502 错误规则', async () => {
    const manager = new SubscriptionManager(() => new FakeNacosClient() as any);
    const response = await handleRuleValue('{"serviceName":"svc"}', manager);
    const { rules, body } = parseErrorResponse(response);
    expect(rules).toContain('statusCode://502');
    expect(body).toContain('缺少必填字段: serverList');
  });

  it('没有健康实例时返回 502 错误规则', async () => {
    const manager = await managerWithInstances([]);
    const response = await handleRuleValue(
      '{"serverList":"10.0.0.1:8848","serviceName":"admin-feature"}',
      manager
    );
    const { rules, body } = parseErrorResponse(response);
    expect(rules).toContain('statusCode://502');
    expect(body).toContain('不存在或当前没有健康实例');
  });

  it('metadata 过滤后为空时返回 502 错误规则', async () => {
    const manager = await managerWithInstances([
      {
        ip: '10.0.0.2',
        port: 8080,
        weight: 1,
        healthy: true,
        enabled: true,
        metadata: { version: 'v1' },
      },
    ]);
    const response = await handleRuleValue(
      '{"serverList":"10.0.0.1:8848","serviceName":"admin-feature","metadata":{"version":"v2"}}',
      manager
    );
    const { rules, body } = parseErrorResponse(response);
    expect(rules).toContain('statusCode://502');
    expect(body).toContain('没有匹配 metadata 条件的健康实例');
  });

  it('成功选出实例时直接返回 host:// 规则文本（不含 values 包装）', async () => {
    const manager = await managerWithInstances([
      {
        ip: '10.0.0.2',
        port: 8080,
        weight: 5,
        healthy: true,
        enabled: true,
        metadata: {},
      },
    ]);
    const response = await handleRuleValue(
      '{"serverList":"10.0.0.1:8848","serviceName":"admin-feature"}',
      manager
    );
    expect(response.trim()).toBe('* host://10.0.0.2:8080');
  });

  it('首次请求（不经过 pushInstances 模拟推送）即可直接拿到实例并转发成功', async () => {
    const manager = new SubscriptionManager((clientConfig) => {
      const fakeClient = new FakeNacosClient();
      fakeClient.instancesToReturn = [
        {
          ip: '10.0.0.3',
          port: 9000,
          weight: 1,
          healthy: true,
          enabled: true,
          metadata: {},
        },
      ];
      return fakeClient as any;
    });

    // 直接调用 handleRuleValue，不预先调用 getOrCreate/waitReady/pushInstances，
    // 模拟"插件刚启动，第一次请求命中新 serviceName"的真实场景。
    const response = await handleRuleValue(
      '{"serverList":"10.0.0.1:8848","serviceName":"admin-feature"}',
      manager
    );
    expect(response.trim()).toBe('* host://10.0.0.3:9000');
  });

  it('首拉超时时返回"订阅初始化超时"的专门错误文案', async () => {
    class SlowFakeNacosClient {
      subscribe() {}
      unSubscribe() {}
      getAllInstances(): Promise<any[]> {
        return new Promise(() => {}); // 永不 resolve，模拟 nacos server 无响应
      }
    }
    const manager = new SubscriptionManager(
      () => new SlowFakeNacosClient() as any
    );

    const response = await handleRuleValue(
      '{"serverList":"10.0.0.1:8848","serviceName":"admin-feature"}',
      manager,
      10 // 注入极短超时，避免测试等待真实的 10 秒
    );
    const { rules, body } = parseErrorResponse(response);
    expect(rules).toContain('statusCode://502');
    expect(body).toContain('订阅初始化超时');
  });
});
```

- [ ] **Step 2: 运行测试确认失败**

Run: `npx jest src/nacos/handle-request.test.ts`
Expected: FAIL —— TypeScript 编译错误或运行时错误，提示 `handleRuleValue` 返回值不是 `Promise`（例如 `response.trim is not a function`，因为此刻 `response` 仍是同步返回的 `string`，`await` 一个 `string` 得到的还是 `string`，但真正会失败的是最后两个新增用例：第一个用例因为没有 `waitReady` 等待逻辑会立刻查询到空数组而失败；第二个用例因为 `handleRuleValue` 还不支持第三个超时参数，且没有超时错误文案分支而失败）。

- [ ] **Step 3: 实现 `handleRuleValue` 的 async 改造**

打开 `src/nacos/handle-request.ts`，完整替换为:

```ts
import { parseRuleValue, RuleConfigError } from '../config/parse-rule-value';
import { selectInstance } from './select-instance';
import { SubscriptionManager } from './subscription-manager';

const PREFIX = '[whistle.nacos-naming]';

// 首次创建订阅时，等待首拉（getAllInstances）完成的最长时间。
// 超过该时长仍未拿到数据，会返回专门的"订阅初始化超时"错误文案，
// 与"服务确实不存在/无健康实例"区分开。
const DEFAULT_INITIAL_LOAD_TIMEOUT_MS = 10000;

// whistle 的 resBody://(value) 内联写法不支持包含空格/换行/特殊字符的内容
// (参考 https://wproxy.org/docs/rules/operation.html:
//  "当操作内容（Value）包含空格、换行符或特殊字符时，无法直接使用内联方式（Inline）")
// 因此错误文案统一走"内嵌值"方式：随规则一起下发一个唯一 key 的 values，
// 用 resBody://{key} 引用，避免内联小括号对特殊字符的限制。
function buildResponse(rules: string, values?: Record<string, string>): string {
  if (!values) {
    return rules;
  }
  return JSON.stringify({ rules, values });
}

function errorRules(message: string): string {
  const body = `${PREFIX} ${message}`;
  const key = `whistle.nacos-naming/error-${Date.now()}-${Math.random()
    .toString(36)
    .slice(2)}`;
  const rules = `* statusCode://502\n* resBody://{${key}}`;
  return buildResponse(rules, { [key]: body });
}

export async function handleRuleValue(
  ruleValue: string,
  manager: SubscriptionManager,
  initialLoadTimeoutMs: number = DEFAULT_INITIAL_LOAD_TIMEOUT_MS
): Promise<string> {
  let config;
  try {
    config = parseRuleValue(ruleValue);
  } catch (err) {
    if (err instanceof RuleConfigError) {
      return errorRules(err.message);
    }
    return errorRules(`未知配置解析错误: ${(err as Error).message}`);
  }

  const initialSnapshot = manager.getOrCreate(config);
  // 首次创建订阅时，getOrCreate 同步返回的快照可能还是空列表（首拉尚未完成）。
  // 这里等待首拉完成（或超时），拿到最新快照后再做实例选择，
  // 避免"插件刚启动，第一次请求命中新 serviceName 就报找不到实例"的问题。
  const snapshot =
    (await manager.waitReady(initialSnapshot.key, initialLoadTimeoutMs)) ||
    initialSnapshot;

  const result = selectInstance(snapshot.instances, config.metadata);

  if (!result.ok) {
    if (result.failureReason === 'METADATA_MISMATCH') {
      return errorRules(
        `服务 ${config.serviceName} 没有匹配 metadata 条件的健康实例: ${JSON.stringify(
          config.metadata
        )}`
      );
    }
    if (snapshot.status === 'connecting') {
      return errorRules(
        `服务 ${config.serviceName} 订阅初始化超时(${
          initialLoadTimeoutMs / 1000
        }s)，请稍后重试 (namespace: ${config.namespace}, group: ${
          config.groupName
        })`
      );
    }
    return errorRules(
      `服务 ${config.serviceName} 不存在或当前没有健康实例 (namespace: ${config.namespace}, group: ${config.groupName})`
    );
  }

  const { ip, port } = result.instance!;
  return buildResponse(`* host://${ip}:${port}`);
}
```

- [ ] **Step 4: 运行测试确认通过**

Run: `npx jest src/nacos/handle-request.test.ts`
Expected: PASS —— 全部 7 个用例通过。

- [ ] **Step 5: 提交**

```bash
git add src/nacos/handle-request.ts src/nacos/handle-request.test.ts
git commit -m "$(cat <<'EOF'
fix: handleRuleValue 改为异步，等待首拉完成后再选实例

接入 SubscriptionManager.waitReady，首次请求会等待首批实例数据到达
（最长 10 秒），超时后返回专门的"订阅初始化超时"错误文案。
EOF
)"
```

---

### Task 3: `rules-server.ts` 请求回调改为 async

**Files:**
- Modify: `src/rules-server.ts`

**Interfaces:**
- Consumes: `handleRuleValue(ruleValue: string, manager: SubscriptionManager, initialLoadTimeoutMs?: number): Promise<string>`（Task 2 产出）。
- Produces: 无新增导出，`rules-server.ts` 是插件入口，行为对外不变（仍是 `export default (server, options) => {...}`）。

本任务没有对应的独立单测文件（`rules-server.ts` 目前没有单测），改动是把同步回调包一层 `async/await`，行为等价，通过 Task 4 的整体测试运行 + 手动检查来验证。

- [ ] **Step 1: 修改 `rules-server.ts` 为 async 回调**

打开 `src/rules-server.ts`，完整替换为:

```ts
import { handleRuleValue } from './nacos/handle-request';
import { sharedSubscriptionManager } from './nacos/shared-manager';

export default (server: Whistle.PluginServer, options: Whistle.PluginOptions) => {
  server.on('request', async (req: Whistle.PluginRequest, res: Whistle.PluginResponse) => {
    const ruleValue = req.originalReq.ruleValue || '';
    try {
      const response = await handleRuleValue(ruleValue, sharedSubscriptionManager);
      res.end(response);
    } catch (err: any) {
      console.error('[whistle.nacos-naming] rules-server 处理异常:', err);
      const key = `whistle.nacos-naming/error-${Date.now()}`;
      const body = `[whistle.nacos-naming] 插件内部异常: ${err.message}`;
      const rules = `* statusCode://502\n* resBody://{${key}}`;
      res.end(JSON.stringify({ rules, values: { [key]: body } }));
    }
  });
};
```

- [ ] **Step 2: 运行 TypeScript 编译检查，确认没有类型错误**

Run: `npx tsc --noEmit`
Expected: 无输出（无编译错误）。

- [ ] **Step 3: 提交**

```bash
git add src/rules-server.ts
git commit -m "fix: rules-server 请求回调改为 async，适配 handleRuleValue 异步签名"
```

---

### Task 4: 全量测试与构建验证

**Files:**
- 无新增/修改文件，仅运行验证命令。

**Interfaces:**
- Consumes: 前三个任务的全部产出。
- Produces: 无（验证任务）。

- [ ] **Step 1: 运行全量测试套件**

Run: `npx jest`
Expected: 所有测试套件（`parse-rule-value.test.ts`、`handle-request.test.ts`、`select-instance.test.ts`、`subscription-manager.test.ts`、`router.test.ts`）全部通过，无失败用例。

- [ ] **Step 2: 运行完整构建**

Run: `npm run build`
Expected: 构建成功退出码为 0，`dist/` 目录下生成对应的 `.js` 和 `.d.ts` 文件，无 TypeScript 编译错误。

- [ ] **Step 3: 若发现测试或构建失败，定位并修复**

如果 Step 1 或 Step 2 失败，根据错误信息定位是 Task 1/2/3 中哪个文件的问题（常见原因：`SubscriptionSnapshot`/`SubscriptionEntry` 类型不匹配、`waitReady` 返回值类型与调用处不一致、`async` 函数签名遗漏 `await`）。修复后重新运行 Step 1 和 Step 2 直至全部通过。**此步骤不需要额外的 commit，如果发生了修复，将修复内容合并进对应任务的 commit 中（使用 `git commit --amend`，前提是该 commit 尚未推送到远端）。**

- [ ] **Step 4: 清理构建产物（可选，视 .gitignore 情况）**

Run: `git status`
Expected: 确认 `dist/` 目录是否已被 `.gitignore` 忽略。如果 `dist/` 未被忽略且本次构建产生了未跟踪的变更，不要提交它们（构建产物不应随源码改动一起提交，除非项目现有约定是把 `dist/` 纳入版本控制——需先检查 `.gitignore` 内容确认）。

---

## Self-Review 记录

- **Spec 覆盖检查**：设计文档中的 4 个改动点（`subscription-manager.ts` 的 `initialLoad`/`waitReady`、`handle-request.ts` 的 async 化与超时文案、`rules-server.ts` 的 async 回调、测试改造）分别对应 Task 1、Task 2、Task 3，测试改造被拆分内�、并在 Task 4 做全量回归验证。10 秒超时与专门错误文案已按用户确认写入 Global Constraints 和 Task 2 的实现代码中。
- **占位符扫描**：全文无 TBD/TODO，所有代码块均为完整可运行代码，无"参考 Task N"的省略写法。
- **类型一致性检查**：`waitReady(key: string, timeoutMs?: number): Promise<SubscriptionSnapshot | null>` 在 Task 1 定义、Task 2 调用处签名一致；`handleRuleValue(ruleValue: string, manager: SubscriptionManager, initialLoadTimeoutMs?: number): Promise<string>` 在 Task 2 定义、Task 3 调用处签名一致（Task 3 未传第三个参数，使用默认值，符合设计）。
