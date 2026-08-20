import { selectInstance, NacosInstance } from './select-instance';

function inst(overrides: Partial<NacosInstance>): NacosInstance {
  return {
    ip: '10.0.0.1',
    port: 8080,
    weight: 1,
    healthy: true,
    enabled: true,
    metadata: {},
    ...overrides,
  };
}

describe('selectInstance', () => {
  it('实例列表为空时返回 NO_INSTANCE', () => {
    const result = selectInstance([], null);
    expect(result).toEqual({ ok: false, failureReason: 'NO_INSTANCE' });
  });

  it('全部不健康时返回 NO_INSTANCE', () => {
    const result = selectInstance(
      [inst({ healthy: false }), inst({ enabled: false })],
      null
    );
    expect(result).toEqual({ ok: false, failureReason: 'NO_INSTANCE' });
  });

  it('无 metadata 过滤时按 weight 降序选最高的实例', () => {
    const low = inst({ ip: '10.0.0.1', weight: 1 });
    const high = inst({ ip: '10.0.0.2', weight: 5 });
    const result = selectInstance([low, high], null);
    expect(result.ok).toBe(true);
    expect(result.instance?.ip).toBe('10.0.0.2');
  });

  it('weight 并列时取原始列表顺序中第一个', () => {
    const first = inst({ ip: '10.0.0.1', weight: 3 });
    const second = inst({ ip: '10.0.0.2', weight: 3 });
    const result = selectInstance([first, second], null);
    expect(result.instance?.ip).toBe('10.0.0.1');
  });

  it('按 metadata 精确匹配过滤（AND 逻辑）', () => {
    const matched = inst({
      ip: '10.0.0.1',
      weight: 1,
      metadata: { version: 'v2', env: 'test' },
    });
    const notMatched = inst({
      ip: '10.0.0.2',
      weight: 9,
      metadata: { version: 'v1' },
    });
    const result = selectInstance([matched, notMatched], { version: 'v2' });
    expect(result.ok).toBe(true);
    expect(result.instance?.ip).toBe('10.0.0.1');
  });

  it('metadata 过滤后为空时返回 METADATA_MISMATCH', () => {
    const notMatched = inst({ metadata: { version: 'v1' } });
    const result = selectInstance([notMatched], { version: 'v2' });
    expect(result).toEqual({ ok: false, failureReason: 'METADATA_MISMATCH' });
  });
});
