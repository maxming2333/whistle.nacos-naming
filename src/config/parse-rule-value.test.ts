import { parseRuleValue, RuleConfigError } from './parse-rule-value';

describe('parseRuleValue', () => {
  it('解析合法 JSON 并填充默认值', () => {
    const result = parseRuleValue(
      '{"serverList":"10.0.0.1:8848","serviceName":"admin-feature"}'
    );
    expect(result).toEqual({
      serverList: '10.0.0.1:8848',
      namespace: 'public',
      groupName: 'DEFAULT_GROUP',
      serviceName: 'admin-feature',
      metadata: null,
    });
  });

  it('保留显式配置的 namespace/groupName/metadata', () => {
    const result = parseRuleValue(
      '{"serverList":"10.0.0.1:8848","namespace":"dev","groupName":"G1","serviceName":"svc","metadata":{"version":"v2"}}'
    );
    expect(result).toEqual({
      serverList: '10.0.0.1:8848',
      namespace: 'dev',
      groupName: 'G1',
      serviceName: 'svc',
      metadata: { version: 'v2' },
    });
  });

  it('不是合法 JSON 时抛出 RuleConfigError', () => {
    expect(() => parseRuleValue('not-json')).toThrow(RuleConfigError);
    expect(() => parseRuleValue('not-json')).toThrow(/不是合法 JSON/);
  });

  it('缺少 serverList 时抛出 RuleConfigError', () => {
    expect(() => parseRuleValue('{"serviceName":"svc"}')).toThrow(
      /缺少必填字段: serverList/
    );
  });

  it('缺少 serviceName 时抛出 RuleConfigError', () => {
    expect(() => parseRuleValue('{"serverList":"10.0.0.1:8848"}')).toThrow(
      /缺少必填字段: serviceName/
    );
  });

  it('支持 JSON5 语法：属性名不加引号、单引号字符串、尾随逗号', () => {
    const result = parseRuleValue(
      `{
        serverList: 'a:8848',
        namespace: 'gateway-feature-ksa',
        groupName: 'fe',
        serviceName: 'web-gateway-core',
      }`
    );
    expect(result).toEqual({
      serverList: 'a:8848',
      namespace: 'gateway-feature-ksa',
      groupName: 'fe',
      serviceName: 'web-gateway-core',
      metadata: null,
    });
  });

  it('serverList 为数组时拼接成逗号分隔的字符串', () => {
    const result = parseRuleValue(
      `{
        serverList: ['a:8848', 'b:8848', 'c:8848'],
        serviceName: 'web-gateway-core',
      }`
    );
    expect(result.serverList).toBe('a:8848,b:8848,c:8848');
  });

  it('serverList 数组元素为空字符串时被过滤', () => {
    const result = parseRuleValue(
      JSON.stringify({
        serverList: ['a:8848', '', 'b:8848'],
        serviceName: 'svc',
      })
    );
    expect(result.serverList).toBe('a:8848,b:8848');
  });

  it('serverList 为空数组时视为缺少必填字段', () => {
    expect(() =>
      parseRuleValue(JSON.stringify({ serverList: [], serviceName: 'svc' }))
    ).toThrow(/缺少必填字段: serverList/);
  });

  it('即使是 JSON5 语法，解析失败时仍抛出 RuleConfigError 并给出错误提示', () => {
    expect(() => parseRuleValue('{ serverList: ')).toThrow(RuleConfigError);
    expect(() => parseRuleValue('{ serverList: ')).toThrow(/不是合法 JSON/);
  });
});
