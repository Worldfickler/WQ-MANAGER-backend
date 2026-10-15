CREATE TABLE IF NOT EXISTS submit_wait_sticker (
    id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(60) NOT NULL,
    object_name VARCHAR(255) NOT NULL UNIQUE,
    width INT NOT NULL,
    height INT NOT NULL,
    sort_order INT NOT NULL DEFAULT 0,
    active BOOLEAN NOT NULL DEFAULT TRUE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS submit_wait_vote_sticker (
    id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
    vote_id INT NOT NULL,
    sticker_id INT NOT NULL,
    position INT NOT NULL DEFAULT 0,
    UNIQUE KEY uq_sw_vote_sticker (vote_id, sticker_id),
    FOREIGN KEY (vote_id) REFERENCES submit_wait_vote(id),
    FOREIGN KEY (sticker_id) REFERENCES submit_wait_sticker(id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
