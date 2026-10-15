-- Select the WQ Manager database before running this additive migration.
-- No existing tables or data are modified. Run before deploying the new API.

CREATE TABLE IF NOT EXISTS submit_wait_group (
    id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(60) NOT NULL,
    invite_code VARCHAR(16) NOT NULL,
    owner_id INT NOT NULL,
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY uq_sw_group_invite (invite_code)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS submit_wait_member (
    id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
    group_id INT NOT NULL,
    user_id INT NOT NULL,
    UNIQUE KEY uq_sw_member (group_id, user_id),
    KEY ix_submit_wait_member_user_id (user_id),
    FOREIGN KEY (group_id) REFERENCES submit_wait_group(id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS submit_wait_post (
    id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
    author_id INT NOT NULL,
    group_id INT NULL,
    title VARCHAR(120) NOT NULL,
    content TEXT NOT NULL,
    anonymous BOOLEAN NOT NULL DEFAULT TRUE,
    status VARCHAR(16) NOT NULL DEFAULT 'open',
    outcome_note VARCHAR(1000) NOT NULL DEFAULT '',
    deleted BOOLEAN NOT NULL DEFAULT FALSE,
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    closed_at DATETIME NULL,
    KEY ix_sw_post_feed (group_id, deleted, created_at),
    KEY ix_sw_post_author (author_id, deleted),
    FOREIGN KEY (group_id) REFERENCES submit_wait_group(id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS submit_wait_image (
    id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
    uploader_id INT NOT NULL,
    post_id INT NULL,
    object_name VARCHAR(512) NOT NULL,
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    KEY ix_submit_wait_image_uploader_id (uploader_id),
    KEY ix_submit_wait_image_post_id (post_id),
    FOREIGN KEY (post_id) REFERENCES submit_wait_post(id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS submit_wait_vote (
    id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
    post_id INT NOT NULL,
    user_id INT NOT NULL,
    decision VARCHAR(16) NOT NULL,
    reason VARCHAR(1000) NOT NULL,
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY uq_sw_vote (post_id, user_id),
    KEY ix_submit_wait_vote_user_id (user_id),
    FOREIGN KEY (post_id) REFERENCES submit_wait_post(id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS submit_wait_flower (
    id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
    vote_id INT NOT NULL,
    user_id INT NOT NULL,
    UNIQUE KEY uq_sw_flower (vote_id, user_id),
    FOREIGN KEY (vote_id) REFERENCES submit_wait_vote(id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
