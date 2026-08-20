import { handleRuleValue } from './nacos/handle-request';
import { sharedSubscriptionManager } from './nacos/shared-manager';

export default (server: Whistle.PluginServer, options: Whistle.PluginOptions) => {
  server.on('request', (req: Whistle.PluginRequest, res: Whistle.PluginResponse) => {
    const ruleValue = req.originalReq.ruleValue || '';
    try {
      const response = handleRuleValue(ruleValue, sharedSubscriptionManager);
      res.end(response);
    } catch (err: any) {
      console.error('[whistle.nacos-naming] rules-server 处理异常:', err);
      const key = `whistle.nacos-naming/error-${Date.now()}`;
      const body = `[whistle.nacos-naming] 插件内部异常: ${err.message}`;
      const rules = `* statusCode://502\n* resBody://{${key}}`;
      res.end(JSON.stringify({ rules, values: { [key]: body } }));
    }
  });
};
