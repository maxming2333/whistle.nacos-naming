import {
  SubscriptionManager,
  buildSubscriptionKey,
} from './subscription-manager';
import { NacosRuleConfig } from '../config/parse-rule-value';

// fake client：模拟 NacosNamingClient 的最小接口
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

function baseConfig(overrides: Partial<NacosRuleConfig> = {}): NacosRuleConfig {
  return {
    serverList: '10.0.0.1:8848',
    namespace: 'public',
    groupName: 'DEFAULT_GROUP',
    serviceName: 'admin-feature',
    metadata: null,
    ...overrides,
  };
}

describe('buildSubscriptionKey', () => {
  it('由 serverList+namespace+groupName+serviceName 拼接', () => {
    const key = buildSubscriptionKey(baseConfig());
    expect(key).toBe('10.0.0.1:8848|public|DEFAULT_GROUP|admin-feature');
  });

  it('metadata 不同不影响 key', () => {
    const keyA = buildSubscriptionKey(baseConfig({ metadata: { v: '1' } }));
    const keyB = buildSubscriptionKey(baseConfig({ metadata: { v: '2' } }));
    expect(keyA).toBe(keyB);
  });
});

describe('SubscriptionManager', () => {
  it('相同 key 只创建一个 client，第二次调用直接复用', () => {
    const created: FakeNacosClient[] = [];
    const manager = new SubscriptionManager((clientConfig) => {
      const client = new FakeNacosClient();
      created.push(client);
      return client as any;
    });

    manager.getOrCreate(baseConfig());
    manager.getOrCreate(baseConfig());

    expect(created.length).toBe(1);
    expect(created[0].subscribeCalls.length).toBe(1);
  });

  it('不同 namespace 创建不同的 client', () => {
    const created: FakeNacosClient[] = [];
    const manager = new SubscriptionManager((clientConfig) => {
      const client = new FakeNacosClient();
      created.push(client);
      return client as any;
    });

    manager.getOrCreate(baseConfig({ namespace: 'public' }));
    manager.getOrCreate(baseConfig({ namespace: 'dev' }));

    expect(created.length).toBe(2);
  });

  it('Nacos 推送后 getOrCreate 返回更新后的实例列表', () => {
    let fakeClient!: FakeNacosClient;
    const manager = new SubscriptionManager((clientConfig) => {
      fakeClient = new FakeNacosClient();
      return fakeClient as any;
    });

    manager.getOrCreate(baseConfig());
    fakeClient.pushInstances([
      {
        ip: '10.0.0.2',
        port: 9090,
        weight: 2,
        healthy: true,
        enabled: true,
        metadata: {},
      },
    ]);

    const snapshot = manager.getOrCreate(baseConfig());
    expect(snapshot.instances).toEqual([
      {
        ip: '10.0.0.2',
        port: 9090,
        weight: 2,
        healthy: true,
        enabled: true,
        metadata: {},
      },
    ]);
    expect(snapshot.status).toBe('active');
    expect(snapshot.lastPushedAt).not.toBeNull();
  });

  it('listAll 返回所有订阅快照', () => {
    const manager = new SubscriptionManager(
      () => new FakeNacosClient() as any
    );
    manager.getOrCreate(baseConfig({ serviceName: 'svc-a' }));
    manager.getOrCreate(baseConfig({ serviceName: 'svc-b' }));

    expect(manager.listAll().map((item) => item.serviceName).sort()).toEqual([
      'svc-a',
      'svc-b',
    ]);
  });

  it('refresh 调用 getAllInstances 并更新缓存', async () => {
    let fakeClient!: FakeNacosClient;
    const manager = new SubscriptionManager((clientConfig) => {
      fakeClient = new FakeNacosClient();
      return fakeClient as any;
    });

    const snapshot = manager.getOrCreate(baseConfig());
    // getOrCreate 本身已同步发起一次首拉 getAllInstances，
    // 这里等待首拉完成后再调用 refresh，验证 refresh 会再触发一次调用。
    await manager.waitReady(snapshot.key);
    const ok = await manager.refresh(snapshot.key);

    expect(ok).toBe(true);
    expect(fakeClient.getAllInstancesCalls.length).toBe(2);
    const updated = manager.listAll()[0];
    expect(updated.instances[0].ip).toBe('10.0.0.9');
  });

  it('refresh 对不存在的 key 返回 false', async () => {
    const manager = new SubscriptionManager(
      () => new FakeNacosClient() as any
    );
    const ok = await manager.refresh('not-exist-key');
    expect(ok).toBe(false);
  });

  it('remove 取消订阅并从表中移除', async () => {
    const manager = new SubscriptionManager(
      () => new FakeNacosClient() as any
    );
    const snapshot = manager.getOrCreate(baseConfig());

    const ok = await manager.remove(snapshot.key);

    expect(ok).toBe(true);
    expect(manager.listAll().length).toBe(0);
  });

  it('remove 对不存在的 key 返回 false', async () => {
    const manager = new SubscriptionManager(
      () => new FakeNacosClient() as any
    );
    const ok = await manager.remove('not-exist-key');
    expect(ok).toBe(false);
  });

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
});
