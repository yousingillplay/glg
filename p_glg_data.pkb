create or replace package body p_glg_data
as
  /****************************************************************************/
  procedure add_seq
  as
    i_seq number := 1;
  begin
    
    for rec in (select rowid from genealogy_data)
    loop
      update genealogy_data
        set sequence = i_seq
       where rowid = rec.rowid;
       
       i_seq := i_seq + 1;
    end loop;
  end add_seq;
  
  /****************************************************************************/
  procedure add_mock_operation
  as
  begin
    
    merge into lpt_equip_data tgt
    using (
      select distinct lot, 
             logpoint,
             transaction,
             equipment,
             login_dttm,
             rank() over (partition by lot, logpoint, transaction 
                          order by login_dttm) as operation
        from lpt_equip_data
    ) src
    on (tgt.lot = src.lot
        and tgt.logpoint = src.logpoint
        and tgt.transaction = src.transaction
        and tgt.equipment = src.equipment
        and tgt.login_dttm = src.login_dttm)
    when matched then
    update set tgt.operation = src.operation;
    
    commit;
    
  end add_mock_operation;
  
  /****************************************************************************/
  procedure clear_temp_tables
  as
  begin
    
    execute immediate 'truncate table genealogy_data_groups';
    execute immediate 'truncate table glg_monitor';
    
  end clear_temp_tables;
  
  /****************************************************************************/
  procedure log_monitor
  (
    i_start_sequence in number,
    i_end_sequence in number,
    i_direction in varchar2,
    i_tran_dttm in date default sysdate
  )
  as
  begin
    insert into glg_monitor
    values(i_start_sequence, i_end_sequence, i_direction, i_tran_dttm);
    
    commit;
  end log_monitor;
  
  /****************************************************************************/
  function is_start_event
  (
    i_src_event in varchar2,
    i_end_event in varchar2
  )
  return boolean
  as
    r_retval boolean := false;
    lc_event_to_check constant varchar2(20) := 'START';
  begin
    if i_src_event = lc_event_to_check
       and i_end_event = lc_event_to_check
    then
      r_retval := true;
    end if;
    
    return r_retval;
  end is_start_event;
  
  /****************************************************************************/
  function get_next_sequence
  (
    i_sequence in number,
    i_direction in varchar2
  )
  return number
  as
    r_retval number;
  begin
    if i_direction = 'UP'
    then
      r_retval := i_sequence + 1;
    elsif i_direction = 'DOWN'
    then
      r_retval := i_sequence - 1;
    else
      r_retval := i_sequence + 1;
    end if;
    
    return r_retval;
  end get_next_sequence;
  
  /****************************************************************************/
  function find_seq_of_start_event
  (
    i_start_sequence in number,
    i_direction in varchar2
  )
  return number
  as
    r_retval number;
    l_current_sequence number := i_start_sequence;
    l_src_event varchar2(50);
    l_dst_event varchar2(50);
  begin
    
    loop
      l_current_sequence := get_next_sequence(l_current_sequence, i_direction);
      
      begin
        select src_event,
               dst_event
          into l_src_event,
               l_dst_event
          from genealogy_data
         where sequence = l_current_sequence;
      exception
        when no_data_found
        then
          r_retval := l_current_sequence;
          exit;
      end;
      
      if is_start_event(l_src_event, l_dst_event)
      then
        r_retval := l_current_sequence;
        exit;
      end if;
      
      log_monitor(l_current_sequence, 0, i_direction);
      
    end loop;
    
    return r_retval;
  end find_seq_of_start_event;
  
  /****************************************************************************/
  procedure insert_glg_group
  (
    i_start_sequence in number,
    i_end_sequence in number,
    i_grouping_id in number
  )
  as
  begin
    insert into genealogy_data_groups
    (
      src_key,
      src_facility,
      src_lot,
      src_event,
      dst_key,
      dst_facility,
      dst_lot,
      dst_event,
      tran_dttm,
      sequence,
      grouping_id
    )
    select src_key,
           src_facility,
           src_lot,
           src_event,
           dst_key,
           dst_facility,
           dst_lot,
           dst_event,
           tran_dttm,
           sequence,
           i_grouping_id
      from genealogy_data
     where sequence between i_start_sequence
                        and i_end_sequence;
     
    commit;
    
  end insert_glg_group;
  
  /****************************************************************************/
  procedure find_glg_groups
  (
    i_lot_to_glg in varchar2
  )
  as
    l_start_sequence number;
    l_end_sequence number;
    l_group_id number := 1;
    
    cursor glg_rec_found is
    select *
      from genealogy_data
     where dst_lot = i_lot_to_glg;
  begin
    for rec in glg_rec_found
    loop
      if is_start_event(rec.src_event, rec.dst_event)
      then
        l_start_sequence := rec.sequence;
      else
        l_start_sequence := find_seq_of_start_event(rec.sequence, 'DOWN');
      end if;
      
      l_end_sequence := find_seq_of_start_event(rec.sequence, 'UP') - 1;
      
      insert_glg_group(l_start_sequence, l_end_sequence, l_group_id);
      
      l_group_id := l_group_id + 1;
    end loop;
    
  end find_glg_groups;
  
  /****************************************************************************/
  procedure determine_lpt_opn_sequence
  (
  
  )
  
  /****************************************************************************/
  procedure generate
  (
    i_lot_to_glg in varchar2
  )
  as
  begin
    clear_temp_tables;
    
    find_glg_groups(i_lot_to_glg);
    
    --determine_lpt_opn_sequence;
    --determine_parents;
    --prepare_output;
    
  end generate;
  
end p_glg_data;
/