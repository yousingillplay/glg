create table lpt_equip_data
(
  lot varchar2(7),
  logpoint number,
  operation number,
  transaction varchar2(3),
  tran_dttm date,
  login_dttm date,
  equipment varchar2(20)
);

create table genealogy_data
(
  src_key varchar2(100),
  src_facility varchar2(10),
  src_lot varchar2(7),
  src_event varchar2(20),
  dst_key varchar2(100),
  dst_facility varchar2(10),
  dst_lot varchar2(7),
  dst_event varchar2(20),
  tran_dttm date,
  sequence number
);

create table lpt_equip_genealogy
(
  id raw(16),
  lot varchar2(7),
  logpoint number,
  facility varchar2(20) default 'PHI',
  equipment varchar2(20),
  tran_dttm date,
  arrival_dttm date,
  parent_lot varchar2(7),
  parent_logpoint number,
  parent_facility varchar2(20) default 'PHI',
  parent_id raw(16)
);

create global temporary table genealogy_data_groups
(
  src_key varchar2(100),
  src_facility varchar2(10),
  src_lot varchar2(7),
  src_event varchar2(20),
  dst_key varchar2(100),
  dst_facility varchar2(10),
  dst_lot varchar2(7),
  dst_event varchar2(20),
  tran_dttm date,
  sequence number,
  grouping_id number
)
on commit preserve rows;

create table glg_monitor
(
  start_sequence number,
  end_sequence number,
  direction varchar2(20),
  tran_dttm date
);

declare
  previous_logpoint number;
  l_id raw(16);
begin
  delete from lpt_equip_genealogy;
  --lpt loop
  for lpt_data in (select distinct logpoint 
                     from lpt_equip_data
                    where transaction = 'W03'
                   order by logpoint desc)
  loop
    --lot loop
    for lot_data in (select distinct lot, equipment
                       from lpt_equip_data
                      where logpoint = lpt_data.logpoint
                        and transaction = 'W03')
    loop
      l_id := sys_guid;
      --if rightmost
      if (previous_logpoint is null)
      then
        insert into lpt_equip_genealogy (id, lot, logpoint, equipment, tran_dttm, arrival_dttm)
        select l_id, lot_data.lot, lpt_data.logpoint, lot_data.equipment, tran_dttm, arrival_dttm
          from lpt_equip_data
         where lot = lot_data.lot
           and logpoint = lpt_data.logpoint
           and transaction = 'W03';
      else
        insert into lpt_equip_genealogy (id, lot, logpoint, equipment, tran_dttm, arrival_dttm, parent_lot, parent_logpoint)
        select l_id, lot_data.lot, lpt_data.logpoint, lot_data.equipment, tran_dttm, arrival_dttm, lot_data.lot, previous_logpoint
          from lpt_equip_data
         where lot = lot_data.lot
           and logpoint = previous_logpoint
           and transaction = 'W03'
        union all
        select l_id, lot_data.lot, lpt_data.logpoint, lot_data.equipment, led.tran_dttm, led.arrival_dttm, gd.dst_lot, previous_logpoint
          from lpt_equip_data led,
               genealogy_data gd
         where gd.src_lot = lot_data.lot
           and gd.dst_lot = led.lot
           and led.logpoint = previous_logpoint
           and transaction = 'W03';
      end if;
      
    end loop;
    
    previous_logpoint := lpt_data.logpoint;
  end loop;
  commit;
end;


create or replace package p_glg_data
as

  procedure add_seq;
  
  procedure generate
  (
    i_lot_to_glg in varchar2,
    i_group_no in number
  );
end p_glg_data;
/

create or replace package body p_glg_data
as
  procedure add_seq
  as
    i_seq number := 1;
  begin
    
    for rec in (select rowid from genealogy_data)
    loop
      update genealogy_data
        set seq = i_seq
       where rowid = rec.rowid;
       
       i_seq := i_seq + 1;
    end loop;
  end add_seq;

  procedure find_grouping
  (
    i_lot_to_glg in varchar2,
    i_group_no in number
  )
  as
  begin
    null;
  end find_grouping;

  procedure generate
  (
    i_lot_to_glg in varchar2,
    i_group_no in number
  )
  as
  begin
    --find its grouping
    null;
  end generate;
end p_glg_data;
/