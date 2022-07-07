DROP TABLE IF EXISTS details;
DROP TABLE IF EXISTS tab;
CREATE TABLE details(id INTEGER , Brand Text, specs Text);
INSERT INTO details(id,Brand,specs) VALUES(1, 'samsung','6g ram, 125 go');
CREATE TABLE tab(id INTEGER , datetime TIMESTAMP,visitor_id BIGINT,revenue REAL,tax REAL,device_type INTEGER,device_name TEXT)
