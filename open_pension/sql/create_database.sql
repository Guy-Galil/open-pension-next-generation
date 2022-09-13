CREATE DATABASE `PENSSION` /*!40100 DEFAULT CHARACTER SET utf8 */
CREATE USER penssion@localhost IDENTIFIED BY 'H454dn@';
grant all on PENSSION.* to penssion@localhost with GRANT OPTION;


CREATE TABLE `Gemelnet_monthly_portfolios` ( `קופה` text DEFAULT NULL, `תקופה` text DEFAULT NULL, `קוד` text DEFAULT NULL, `נכס` text DEFAULT NULL, `כמות` text DEFAULT NULL , TIMESTAMP TIMESTAMP) ENGINE=InnoDB DEFAULT CHARSET=utf8;