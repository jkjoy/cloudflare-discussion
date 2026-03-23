# 极简论坛

当前分支已经改成 `Nuxt 静态前端 + Cloudflare Workers API + Cloudflare D1`。

- 前端页面结构、路由和交互尽量保持 `discussion` 原项目一致。
- 后端不再依赖 Prisma / Postgres / Nuxt server。
- 静态资源由 Worker 同域托管，`/api/*` 由 Worker 处理。

## 架构

- 前端：Nuxt 3，`ssr: false`，通过 `nuxt generate` 生成静态站点
- 后端：Cloudflare Workers
- 数据库：Cloudflare D1
- 入口配置：[wrangler.jsonc](./wrangler.jsonc)
- 数据库迁移：[worker/migrations](./worker/migrations)

## 快速开始

### 1. 创建 D1 数据库

```bash
npx wrangler d1 create bbs
```

把命令返回的 `database_id` 填入 [wrangler.jsonc](./wrangler.jsonc) 的 `d1_databases[0].database_id`。

### 2. 创建 R2 Bucket

```bash
npx wrangler r2 bucket create discussion-images
```

把实际 bucket 名称填入 [wrangler.jsonc](./wrangler.jsonc) 的 `r2_buckets[0].bucket_name` 和 `preview_bucket_name`。

### 3. 配置环境变量

- 前端构建变量：复制 [.env.example](./.env.example) 为 `.env`
- Worker 运行时变量：复制 [.dev.vars.example](./.dev.vars.example) 为 `.dev.vars`

注意：

- `NUXT_PUBLIC_TOKEN_KEY` 必须和 `TOKEN_KEY` 保持一致
- `NUXT_PUBLIC_AVATAR_CDN` 建议和 `AVATAR_CDN` 保持一致
- 生产环境建议把 `COOKIE_SECURE` 设为 `"true"`

### 4. 安装依赖

```bash
npm install
```

### 5. 本地初始化 D1

```bash
npm run d1:migrate:local
```

这里的脚本会直接对 `wrangler.jsonc` 里声明的 `DB` 绑定执行 migration，不需要再手动改数据库名。

### 6. 本地预览完整站点

```bash
npm run cf:preview
```

这条命令会先执行 `nuxt generate`，然后用 Wrangler 在本地启动 Worker，并把 `.output/public` 作为同域静态资源。

`npm run dev` 仅适合单独调前端 UI，不包含 Worker API。

## 部署到 Cloudflare

```bash
npm run d1:migrate:remote
npm run cf:deploy
```

如果使用 GitHub Actions，仓库需要配置：

- `CLOUDFLARE_API_TOKEN`
- `CLOUDFLARE_ACCOUNT_ID`

默认工作流会先跑远端 D1 migration，再执行 `wrangler deploy`。

## 当前已覆盖的核心能力

- 注册、登录、个人设置
- 发帖、回帖、收藏、帖子支持
- 点赞 / 点踩评论
- 站内消息、私信、Telegram webhook 绑定通知
- 节点、头衔、用户、帖子、评论的后台管理
- 站点配置持久化到 D1
- 邀请码、积分、签到、隐藏内容付费查看
- R2 图片上传
邮件发送已经切到 Resend。部署后请在后台“系统设置 > 邮件设置”中填写 `Resend API Key`、发件邮箱和发件人名称；如果启用了邮箱验证码注册，注册验证码和找回密码邮件也会走 Resend。
