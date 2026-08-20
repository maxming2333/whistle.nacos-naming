export interface NacosInstance {
  ip: string;
  port: number;
  weight: number;
  healthy: boolean;
  enabled: boolean;
  metadata: Record<string, string>;
}

export type SelectFailureReason = 'NO_INSTANCE' | 'METADATA_MISMATCH';

export interface SelectResult {
  ok: boolean;
  instance?: NacosInstance;
  failureReason?: SelectFailureReason;
}

function matchesMetadata(
  instance: NacosInstance,
  metadata: Record<string, string> | null
): boolean {
  if (!metadata) {
    return true;
  }
  return Object.keys(metadata).every(
    (key) => instance.metadata && instance.metadata[key] === metadata[key]
  );
}

export function selectInstance(
  instances: NacosInstance[],
  metadata: Record<string, string> | null
): SelectResult {
  const healthyInstances = instances.filter(
    (item) => item.healthy === true && item.enabled === true
  );

  if (healthyInstances.length === 0) {
    return { ok: false, failureReason: 'NO_INSTANCE' };
  }

  const filtered = healthyInstances.filter((item) =>
    matchesMetadata(item, metadata)
  );

  if (filtered.length === 0) {
    return { ok: false, failureReason: 'METADATA_MISMATCH' };
  }

  let best = filtered[0];
  for (let i = 1; i < filtered.length; i += 1) {
    if (filtered[i].weight > best.weight) {
      best = filtered[i];
    }
  }

  return { ok: true, instance: best };
}
