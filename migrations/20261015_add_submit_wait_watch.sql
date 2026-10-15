-- Run after 20261015_create_submit_wait.sql. Existing votes are unchanged.
CREATE TABLE IF NOT EXISTS submit_wait_watch (
    id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
    post_id INT NOT NULL,
    user_id INT NOT NULL,
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY uq_sw_watch (post_id, user_id),
    KEY ix_submit_wait_watch_user_id (user_id),
    FOREIGN KEY (post_id) REFERENCES submit_wait_post(id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
