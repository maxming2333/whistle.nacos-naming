import { handleRuleValue } from './handle-request';
import { SubscriptionManager } from './subscription-manager';

class FakeNacosClient {
  private listener: ((hosts: any[]) => void) | null = null;
  subscribe(info: any, listener: (hosts: any[]) => void) {
    this.listener = listener;
  }
  unSubscribe() {
    this.listener = null;
  }
  async getAllInstances(): Promise<any[]> {
    return [];
  }
  pushInstances(hosts: any[]) {
    this.listener && this.listener(hosts);
  }
}

function managerWithInstances(hosts: any[]): SubscriptionManager {
  let fakeClient!: FakeNacosClient;
  const manager = new SubscriptionManager((clientConfig) => {
    fakeClient = new FakeNacosClient();
    return fakeClient as any;
  });
  // 触发创建订阅
  manager.getOrCreate({
    serverList: '10.0.0.1:8848',
    namespace: 'public',
    groupName: 'DEFAULT_GROUP',
    serviceName: 'admin-feature',
    metadata: null,
  });
  fakeClient.pushInstances(hosts);
  return manager;
}

// 错误场景下 handleRuleValue 返回 JSON.stringify({rules, values})（内嵌值写法，
// 避免 resBody://(value) 内联小括号不支持空格/特殊字符的限制），
// 这个辅助函数把返回值解析回 { rules, body } 方便断言。
function parseErrorResponse(response: string): { rules: string; body: string } {
  const parsed = JSON.parse(response);
  const key = Object.keys(parsed.values)[0];
  return { rules: parsed.rules, body: parsed.values[key] };
}

describe('handleRuleValue', () => {
  it('规则值不是合法 JSON 时返回 502 错误规则', () => {
    const manager = new SubscriptionManager(() => new FakeNacosClient() as any);
    const response = handleRuleValue('not-json', manager);
    const { rules, body } = parseErrorResponse(response);
    expect(rules).toContain('statusCode://502');
    expect(rules).toContain('resBody://{');
    expect(body).toContain('不是合法 JSON');
    expect(body).toContain('[whistle.nacos-naming]');
  });

  it('缺少必填字段时返回 502 错误规则', () => {
    const manager = new SubscriptionManager(() => new FakeNacosClient() as any);
    const response = handleRuleValue('{"serviceName":"svc"}', manager);
    const { rules, body } = parseErrorResponse(response);
    expect(rules).toContain('statusCode://502');
    expect(body).toContain('缺少必填字段: serverList');
  });

  it('没有健康实例时返回 502 错误规则', () => {
    const manager = managerWithInstances([]);
    const response = handleRuleValue(
      '{"serverList":"10.0.0.1:8848","serviceName":"admin-feature"}',
      manager
    );
    const { rules, body } = parseErrorResponse(response);
    expect(rules).toContain('statusCode://502');
    expect(body).toContain('不存在或当前没有健康实例');
  });

  it('metadata 过滤后为空时返回 502 错误规则', () => {
    const manager = managerWithInstances([
      {
        ip: '10.0.0.2',
        port: 8080,
        weight: 1,
        healthy: true,
        enabled: true,
        metadata: { version: 'v1' },
      },
    ]);
    const response = handleRuleValue(
      '{"serverList":"10.0.0.1:8848","serviceName":"admin-feature","metadata":{"version":"v2"}}',
      manager
    );
    const { rules, body } = parseErrorResponse(response);
    expect(rules).toContain('statusCode://502');
    expect(body).toContain('没有匹配 metadata 条件的健康实例');
  });

  it('成功选出实例时直接返回 host:// 规则文本（不含 values 包装）', () => {
    const manager = managerWithInstances([
      {
        ip: '10.0.0.2',
        port: 8080,
        weight: 5,
        healthy: true,
        enabled: true,
        metadata: {},
      },
    ]);
    const response = handleRuleValue(
      '{"serverList":"10.0.0.1:8848","serviceName":"admin-feature"}',
      manager
    );
    expect(response.trim()).toBe('* host://10.0.0.2:8080');
  });
});
