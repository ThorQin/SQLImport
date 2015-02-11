Drop Table if exists `t_business_data`;
Create Table `t_business_data` (
	`BUSINESS_CODE`	varchar(36) Not NULL COMMENT '业务编码',
	`BUSINESS_CONTENT`	varchar(255) COMMENT '业务描述',
	`CREATE_DT`	timestamp Not NULL COMMENT '创建时间',
	`CREATE_USER`	varchar(36) Not NULL COMMENT '创建用户',
	`UPDATE_DT`	timestamp Not NULL COMMENT '更新时间',
	`UPDATE_USER`	varchar(36) Not NULL COMMENT '更新用户',
	`DELETE_FLAG`	char(1) Not NULL COMMENT '删除标识',
	`VERSION`	integer Not NULL COMMENT '版本号',
primary key (
`BUSINESS_CODE`
)
)ENGINE=InnoDB DEFAULT CHARSET=utf8;
