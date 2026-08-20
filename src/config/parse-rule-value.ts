import JSON5 from 'json5';

export interface NacosRuleConfig {
  serverList: string;
  namespace: string;
  groupName: string;
  serviceName: string;
  metadata: Record<string, string> | null;
}

export class RuleConfigError extends Error {
  constructor(message: string) {
    super(message);
    this.name = 'RuleConfigError';
  }
}

const DEFAULT_NAMESPACE = 'public';
const DEFAULT_GROUP_NAME = 'DEFAULT_GROUP';

// serverList 支持字符串（单个或逗号分隔的多个地址）或字符串数组（元素为地址），
// 统一转换成 NacosNamingClient 期望的逗号分隔字符串格式。
// 返回 null 表示未提供合法的 serverList。
function normalizeServerList(value: unknown): string | null {
  if (typeof value === 'string' && value) {
    return value;
  }
  if (Array.isArray(value)) {
    const items = value.filter(
      (item): item is string => typeof item === 'string' && item.length > 0
    );
    return items.length > 0 ? items.join(',') : null;
  }
  return null;
}

export function parseRuleValue(ruleValue: string): NacosRuleConfig {
  let raw: any;
  try {
    // 用 JSON5 解析，兼容属性名不加引号、单引号字符串、尾随逗号等
    // JS 对象字面量常见写法，同时严格 JSON 本身也是合法的 JSON5。
    raw = JSON5.parse(ruleValue);
  } catch (err: any) {
    throw new RuleConfigError(`配置内容不是合法 JSON: ${err.message}`);
  }

  if (!raw || typeof raw !== 'object') {
    throw new RuleConfigError('配置内容不是合法 JSON: 顶层必须是一个对象');
  }

  const serverList = normalizeServerList(raw.serverList);
  if (!serverList) {
    throw new RuleConfigError('配置缺少必填字段: serverList');
  }

  if (!raw.serviceName || typeof raw.serviceName !== 'string') {
    throw new RuleConfigError('配置缺少必填字段: serviceName');
  }

  const metadata =
    raw.metadata && typeof raw.metadata === 'object'
      ? (raw.metadata as Record<string, string>)
      : null;

  return {
    serverList,
    namespace:
      typeof raw.namespace === 'string' && raw.namespace
        ? raw.namespace
        : DEFAULT_NAMESPACE,
    groupName:
      typeof raw.groupName === 'string' && raw.groupName
        ? raw.groupName
        : DEFAULT_GROUP_NAME,
    serviceName: raw.serviceName,
    metadata,
  };
}
