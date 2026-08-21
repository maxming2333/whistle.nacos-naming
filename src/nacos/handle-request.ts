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
