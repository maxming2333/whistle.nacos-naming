import Router from '@koa/router';
import { sharedSubscriptionManager } from '../nacos/shared-manager';

export default (router: Router) => {
  router.get('/cgi-bin/subscriptions', (ctx) => {
    ctx.body = { subscriptions: sharedSubscriptionManager.listAll() };
  });

  router.post('/cgi-bin/subscriptions/refresh', async (ctx) => {
    const { key } = ctx.request.body as { key?: string };
    if (!key) {
      ctx.status = 400;
      ctx.body = { ok: false, error: 'key is required' };
      return;
    }
    const ok = await sharedSubscriptionManager.refresh(key);
    ctx.body = { ok };
  });

  router.post('/cgi-bin/subscriptions/remove', async (ctx) => {
    const { key } = ctx.request.body as { key?: string };
    if (!key) {
      ctx.status = 400;
      ctx.body = { ok: false, error: 'key is required' };
      return;
    }
    const ok = await sharedSubscriptionManager.remove(key);
    ctx.body = { ok };
  });
};
