CREATE TABLE parent_table(
    parent_table_id    INT    	AUTO_INCREMENT,
    name         VARCHAR(20),
    PRIMARY KEY (parent_table_id), 
    UNIQUE INDEX parent_ak(name)
 )ENGINE=INNODB
;
