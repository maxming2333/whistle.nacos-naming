import Koa from 'koa';
import bodyParser from 'koa-bodyparser';
import Router from '@koa/router';
import request from 'supertest';
import setupRouter from './router';

jest.mock('../nacos/shared-manager', () => {
  const { SubscriptionManager } = require('../nacos/subscription-manager');
  return { sharedSubscriptionManager: new SubscriptionManager(() => fakeClient()) };
});

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
}

function fakeClient() {
  return new FakeNacosClient() as any;
}

function buildApp() {
  const app = new Koa();
  const router = new Router();
  setupRouter(router);
  app.use(bodyParser());
  app.use(router.routes());
  app.use(router.allowedMethods());
  return app;
}

describe('ui-server router', () => {
  it('GET /cgi-bin/subscriptions 返回空列表', async () => {
    const app = buildApp();
    const res = await request(app.callback()).get('/cgi-bin/subscriptions');
    expect(res.status).toBe(200);
    expect(res.body).toEqual({ subscriptions: [] });
  });

  it('创建订阅后 GET /cgi-bin/subscriptions 能看到该条目', async () => {
    const { sharedSubscriptionManager } = require('../nacos/shared-manager');
    sharedSubscriptionManager.getOrCreate({
      serverList: '10.0.0.1:8848',
      namespace: 'public',
      groupName: 'DEFAULT_GROUP',
      serviceName: 'admin-feature',
      metadata: null,
    });

    const app = buildApp();
    const res = await request(app.callback()).get('/cgi-bin/subscriptions');
    expect(res.status).toBe(200);
    expect(res.body.subscriptions.length).toBe(1);
    expect(res.body.subscriptions[0].serviceName).toBe('admin-feature');
  });

  it('POST /cgi-bin/subscriptions/refresh 对存在的 key 返回 ok:true', async () => {
    const { sharedSubscriptionManager } = require('../nacos/shared-manager');
    const snapshot = sharedSubscriptionManager.getOrCreate({
      serverList: '10.0.0.2:8848',
      namespace: 'public',
      groupName: 'DEFAULT_GROUP',
      serviceName: 'svc-refresh',
      metadata: null,
    });

    const app = buildApp();
    const res = await request(app.callback())
      .post('/cgi-bin/subscriptions/refresh')
      .send({ key: snapshot.key });

    expect(res.status).toBe(200);
    expect(res.body).toEqual({ ok: true });
  });

  it('POST /cgi-bin/subscriptions/refresh 对不存在的 key 返回 ok:false', async () => {
    const app = buildApp();
    const res = await request(app.callback())
      .post('/cgi-bin/subscriptions/refresh')
      .send({ key: 'not-exist' });

    expect(res.status).toBe(200);
    expect(res.body).toEqual({ ok: false });
  });

  it('POST /cgi-bin/subscriptions/remove 移除后列表不再包含该条目', async () => {
    const { sharedSubscriptionManager } = require('../nacos/shared-manager');
    const snapshot = sharedSubscriptionManager.getOrCreate({
      serverList: '10.0.0.3:8848',
      namespace: 'public',
      groupName: 'DEFAULT_GROUP',
      serviceName: 'svc-remove',
      metadata: null,
    });

    const app = buildApp();
    const removeRes = await request(app.callback())
      .post('/cgi-bin/subscriptions/remove')
      .send({ key: snapshot.key });
    expect(removeRes.body).toEqual({ ok: true });

    const listRes = await request(app.callback()).get('/cgi-bin/subscriptions');
    expect(
      listRes.body.subscriptions.find((item: any) => item.key === snapshot.key)
    ).toBeUndefined();
  });
});
