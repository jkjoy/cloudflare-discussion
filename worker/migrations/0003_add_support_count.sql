-- 添加 support_count 列到 posts 表，避免列表查询时的 N+1 子查询
ALTER TABLE posts ADD COLUMN support_count INTEGER NOT NULL DEFAULT 0;

-- 回填已有数据
UPDATE posts
SET support_count = (
  SELECT COUNT(*) FROM post_support ps WHERE ps.pid = posts.pid
);
