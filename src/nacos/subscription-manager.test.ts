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

  subscribe(info: any, listener: (hosts: any[]) => void) {
    this.subscribeCalls.push(info);
    this.listener = listener;
  }

  unSubscribe(info: any, listener: (hosts: any[]) => void) {
    this.listener = null;
  }

  async getAllInstances(serviceName: string, groupName: string) {
    this.getAllInstancesCalls.push({ serviceName, groupName });
    return [
      {
        ip: '10.0.0.9',
        port: 8080,
        weight: 1,
        healthy: true,
        enabled: true,
        metadata: {},
      },
    ];
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
    const ok = await manager.refresh(snapshot.key);

    expect(ok).toBe(true);
    expect(fakeClient.getAllInstancesCalls.length).toBe(1);
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
});
