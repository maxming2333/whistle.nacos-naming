import { NacosNamingClient } from 'nacos';
import { NacosRuleConfig } from '../config/parse-rule-value';
import { NacosInstance } from './select-instance';

export type SubscriptionStatus = 'connecting' | 'active' | 'error';

export interface SubscriptionSnapshot {
  key: string;
  serverList: string;
  namespace: string;
  groupName: string;
  serviceName: string;
  instances: NacosInstance[];
  createdAt: number;
  lastPushedAt: number | null;
  status: SubscriptionStatus;
  lastError?: string;
}

interface SubscriptionEntry extends SubscriptionSnapshot {
  client: NacosNamingClient;
  listener: (hosts: any[]) => void;
  initialLoad: Promise<void>;
}

export type NacosClientFactory = (clientConfig: {
  serverList: string;
  namespace: string;
}) => NacosNamingClient;

function toNacosInstance(host: any): NacosInstance {
  return {
    ip: host.ip,
    port: host.port,
    weight: typeof host.weight === 'number' ? host.weight : 1,
    healthy: host.healthy === true,
    enabled: host.enabled === true,
    metadata: host.metadata || {},
  };
}

export function buildSubscriptionKey(config: NacosRuleConfig): string {
  return [
    config.serverList,
    config.namespace,
    config.groupName,
    config.serviceName,
  ].join('|');
}

function toSnapshot(entry: SubscriptionEntry): SubscriptionSnapshot {
  return {
    key: entry.key,
    serverList: entry.serverList,
    namespace: entry.namespace,
    groupName: entry.groupName,
    serviceName: entry.serviceName,
    instances: entry.instances,
    createdAt: entry.createdAt,
    lastPushedAt: entry.lastPushedAt,
    status: entry.status,
    lastError: entry.lastError,
  };
}

const defaultClientFactory: NacosClientFactory = (clientConfig) =>
  new NacosNamingClient({
    logger: console,
    serverList: clientConfig.serverList,
    namespace: clientConfig.namespace,
  });

export class SubscriptionManager {
  private entries = new Map<string, SubscriptionEntry>();

  constructor(private clientFactory: NacosClientFactory = defaultClientFactory) {}

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

  listAll(): SubscriptionSnapshot[] {
    return Array.from(this.entries.values()).map(toSnapshot);
  }

  async refresh(key: string): Promise<boolean> {
    const entry = this.entries.get(key);
    if (!entry) {
      return false;
    }
    try {
      const hosts = await entry.client.getAllInstances(
        entry.serviceName,
        entry.groupName
      );
      entry.instances = (hosts || []).map(toNacosInstance);
      entry.lastPushedAt = Date.now();
      entry.status = 'active';
      entry.lastError = undefined;
    } catch (err: any) {
      entry.status = 'error';
      entry.lastError = err.message;
    }
    return true;
  }

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

  async remove(key: string): Promise<boolean> {
    const entry = this.entries.get(key);
    if (!entry) {
      return false;
    }
    try {
      entry.client.unSubscribe(
        { serviceName: entry.serviceName, groupName: entry.groupName },
        entry.listener
      );
    } catch {
      // 忽略取消订阅过程中的异常，仍然从表中移除
    }
    this.entries.delete(key);
    return true;
  }
}
