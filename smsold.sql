use marketsys_houtai;

create temporary table login_db select channel,mobile_no from login.login_user;
create temporary table user_db select id as user_id,mobile_no,created_at from account_center.user_account;
create temporary table user_channel select l.channel as channel,u.user_id as uid,u.created_at from login_db l left join user_db u on u.mobile_no = l.mobile_no;
create temporary table on_time_repay_table select user_id,bill_id,status,overdue_days,if(status>1 and overdue_days=0,1,0) as on_time_repay,due_at,if(ifNULL(stage_status,100)=100,0,1) as is_installment,ifnull(stage_num,1) as stage_num from bill.bill_sub order by bill_id,due_at;

-- 有注册时间的 bill_sub_channel 表
drop table if exists bill_sub_channel;

create table bill_sub_channel
(
    user_id        bigint                                 not null comment '用户id',
    bill_id        bigint unsigned                        not null comment '主账单id',
    status         tinyint(4) unsigned                    not null comment '0未还、1部分还款、2已还清、3已还款待确认',
    overdue_days   int unsigned default 0                 not null comment '逾期天数',
    on_time_repay  int(1)       default 0                 not null,
    due_at         datetime                               not null comment '到期时间',
    is_installment int(1)       default 0                 not null,
    stage_num      bigint(11)   default 0                 not null,
    channel        varchar(255) default 'google_play'     null comment '安装来源',
    uid            bigint       default 0                 null,
    created_at     datetime     default CURRENT_TIMESTAMP null
);
insert into bill_sub_channel select * from on_time_repay_table left join user_channel on user_channel.uid = on_time_repay_table.user_id;


-- 计算sql每个渠道放款笔数,并生成表channel_bill_sub +++++
drop table if exists channel_bill_sub;

create table channel_bill_sub
(
    channel_bill_sub bigint(21)   default 0             not null,
    ispay            decimal(32)                        null,
    channel          varchar(255) default 'google_play' null comment '安装来源',
    is_installment   int(1)       default 0             not null,
    stage_num        bigint(11)   default 0             not null
);
insert into channel_bill_sub select count(bill_id) as channel_bill_sub,sum(on_time_repay) as ispay,channel,is_installment,stage_num from bill_sub_channel group by channel,is_installment,stage_num;

create temporary table is_human_check select application_id as human_check_apid from audit.audit where auto_judgment_status=1;
create temporary table ap_ishumancheck select bill.application.id as ap_id,if(bill.application.status= 110,1,0 ) as badloan,is_human_check.human_check_apid as is_human, bill.application.user_id as ap_uid from bill.application left join is_human_check on is_human_check.human_check_apid = bill.application.id;

-- 有时间的ap_humancheck
drop table if exists ap_humancheck;
create table ap_humancheck
(
    badloan    int(1)          default 0                 not null,
    ap_id      bigint unsigned default 0                 not null comment 'Primary key',
    is_human   bigint                                    null comment '申请件id',
    ap_uid     bigint                                    not null comment '用户id',
    ap_success bigint unsigned                           null comment 'application_id',
    channel    varchar(255)    default 'google_play'     null comment '安装来源',
    created_at datetime        default CURRENT_TIMESTAMP null
);

insert into ap_humancheck  select a.badloan, a.ap_id,a.is_human,a.ap_uid,b.application_id as ap_success,user_channel.channel,user_channel.created_at from ap_ishumancheck a left join bill.bill b on b.application_id=a.ap_id left join user_channel on user_channel.uid = a.ap_uid;


create temporary table ap_num_humancheck select count(ap_id) as ap_num,count(is_human) as human_check,count(ap_success) as ap_success, sum(badloan) as badloan, channel from ap_humancheck group by channel;

-- 有时间的channel_regesit_auth 
drop table if exists channel_auth_table;
create table channel_auth_table
(
    channel    varchar(255) default 'google_play'     not null comment '安装来源',
    id         bigint       default 0                 null,
    is_auth    bigint                                 null,
    created_at datetime     default CURRENT_TIMESTAMP null
);
insert into channel_auth_table select l.channel,u.id,auth.user_id as is_auth,u.created_at  from login.login_user l left join account_center.user_account u  on l.mobile_no =u.mobile_no left join account_center.identity auth on u.id = auth.user_id;

create temporary table channel_regesit_auth select count(id) as regesit,count(is_auth) as is_auth,channel  from channel_auth_table group by channel;

drop table if exists channel_regist_auth_ap_remit;

create table channel_regist_auth_ap_remit
(
    channel     varchar(255) default 'google_play' not null comment '安装来源',
    regesit     bigint(21)   default 0             not null,
    is_auth     bigint(21)   default 0             not null,
    ap_num      bigint(21)   default 0             null,
    ap_success  bigint(21)   default 0             null,
    human_check bigint(21)   default 0             null,
    badloan     decimal(32)                        null
);
insert into channel_regist_auth_ap_remit select c.channel, c.regesit,c.is_auth,a.ap_num,a.ap_success,a.human_check,a.badloan from channel_regesit_auth c left join ap_num_humancheck a on a.channel=c.channel;