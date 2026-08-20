import { parseRuleValue, RuleConfigError } from '../config/parse-rule-value';
import { selectInstance } from './select-instance';
import { SubscriptionManager } from './subscription-manager';

const PREFIX = '[whistle.nacos-naming]';

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

export function handleRuleValue(
  ruleValue: string,
  manager: SubscriptionManager
): string {
  let config;
  try {
    config = parseRuleValue(ruleValue);
  } catch (err) {
    if (err instanceof RuleConfigError) {
      return errorRules(err.message);
    }
    return errorRules(`未知配置解析错误: ${(err as Error).message}`);
  }

  const snapshot = manager.getOrCreate(config);
  const result = selectInstance(snapshot.instances, config.metadata);

  if (!result.ok) {
    if (result.failureReason === 'METADATA_MISMATCH') {
      return errorRules(
        `服务 ${config.serviceName} 没有匹配 metadata 条件的健康实例: ${JSON.stringify(
          config.metadata
        )}`
      );
    }
    return errorRules(
      `服务 ${config.serviceName} 不存在或当前没有健康实例 (namespace: ${config.namespace}, group: ${config.groupName})`
    );
  }

  const { ip, port } = result.instance!;
  return buildResponse(`* host://${ip}:${port}`);
}
