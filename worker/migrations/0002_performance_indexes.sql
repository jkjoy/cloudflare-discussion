CREATE INDEX IF NOT EXISTS idx_posts_uid_created_at ON posts(uid, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_posts_tag_pinned_point ON posts(tag_id, pinned, point DESC);
CREATE INDEX IF NOT EXISTS idx_posts_pinned_created_at ON posts(pinned, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_comments_pid_created_at ON comments(pid, created_at ASC);
CREATE INDEX IF NOT EXISTS idx_comments_uid_created_at ON comments(uid, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_favorites_user_created_at ON favorites(user_id, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_point_history_uid_reason_created_at ON point_history(uid, reason, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_point_history_created_at_uid ON point_history(created_at DESC, uid);

CREATE INDEX IF NOT EXISTS idx_messages_to_uid_created_at ON messages(to_uid, created_at DESC, id DESC);
CREATE INDEX IF NOT EXISTS idx_messages_type_to_from_created_at ON messages(type, to_uid, from_uid, created_at DESC, id DESC);
CREATE INDEX IF NOT EXISTS idx_messages_to_uid_relation_read ON messages(to_uid, relation_id, read);
