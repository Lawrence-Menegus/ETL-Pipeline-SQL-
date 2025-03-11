IF NOT EXISTS (SELECT name FROM sys.databases WHERE name = 'gpt_db')
            BEGIN
                CREATE DATABASE gpt_db;
            END
ELSE
BEGIN
    USE gpt_db;
END

IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'messages')
            BEGIN
                CREATE TABLE messages (
                id INT IDENTITY(1,1) PRIMARY KEY,
                role VARCHAR(50),
                content VARCHAR(MAX)
);
            END


SELECT * FROM messages;