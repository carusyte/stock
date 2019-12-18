-- Must customize password before executing on the MySQL server

CREATE USER 'mysql'@'localhost' IDENTIFIED BY 'password';

GRANT ALL PRIVILEGES ON *.* TO 'mysql'@'localhost' WITH GRANT OPTION;

CREATE USER 'mysql'@'%' IDENTIFIED BY 'password';

GRANT ALL PRIVILEGES ON *.* TO 'mysql'@'%' WITH GRANT OPTION;

FLUSH PRIVILEGES;