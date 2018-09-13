create or replace package body p_glg_data
as
  /****************************************************************************/
  function boolean_to_char
  (
    i_boolean in boolean
  )
  return varchar2
  as
    r_retval varchar2(20);
  begin
    r_retval := case when i_boolean then 'true' else 'false' end;
    return r_retval;
  end boolean_to_char;

  /****************************************************************************/
  FUNCTION get_synonym_object 
  (
    i_synonym IN VARCHAR2
  )
  RETURN VARCHAR2
  AS
    l_table_owner VARCHAR2(100);
    l_table_name VARCHAR2(100);
    l_db_link VARCHAR2(100);
    l_result VARCHAR2(100);
  BEGIN
    
    SELECT table_owner,
           table_name,
           db_link
      INTO l_table_owner,
           l_table_name,
           l_db_link
      FROM all_synonyms
     WHERE owner = user
       AND synonym_name = UPPER(i_synonym);
    
    IF l_db_link IS NOT NULL
    THEN
      l_result := l_table_owner || '.' || l_table_name || '@' || l_db_link;
    ELSE
      l_result := l_table_name;
    END IF;
    
    RETURN l_result;
  END get_synonym_object;

  /****************************************************************************/
  procedure add_seq
  as
    i_seq number := 1;
  begin
    
    for rec in (select rowid from genealogy_data)
    loop
      update s_genealogy_data
        set sequence = i_seq
       where rowid = rec.rowid;
       
       i_seq := i_seq + 1;
    end loop;
  end add_seq;
  
  /****************************************************************************/
  procedure add_mock_operation
  as
  begin
    
    merge into s_lpt_equip_data tgt
    using (
      select distinct lot, 
             logpoint,
             transaction,
             equipment,
             login_dttm,
             rank() over (partition by lot, logpoint, transaction 
                          order by login_dttm) as operation
        from s_lpt_equip_data
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
    execute immediate 'truncate table gtt_lpt_opn_sequence';
    execute immediate 'truncate table gtt_lpt_equip_data_out';
    execute immediate 'truncate table gtt_lpt_equip_parents';
    
  end clear_temp_tables;
  
  /****************************************************************************/
  procedure clear_gtt_lpt_equip_data
  as
  begin
    
    execute immediate 'truncate table gtt_lpt_equip_data';
    
  end clear_gtt_lpt_equip_data;
  
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
          from s_genealogy_data
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
      from s_genealogy_data
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
      from s_genealogy_data
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
  procedure find_all_glg_groups
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
           grouping_id
      from s_genealogy_data;
    
  end find_all_glg_groups;
  
  /****************************************************************************/
  procedure insert_gtt_lpt_equip_data
  (
    i_lot in varchar2
  )
  as
  begin
    clear_gtt_lpt_equip_data;
  
    insert into gtt_lpt_equip_data
    select t.*,
           rownum 
     from (
      select *
        from s_lpt_equip_data
       where lot = i_lot
      order by login_dttm
    ) t;
    
    commit;
  end insert_gtt_lpt_equip_data;
  
  /****************************************************************************/
  function get_lpt_opn_sequence
  (
    i_append in boolean default true,
    i_insert_after_id in raw default null
  )
  return number
  as
    lc_sequence_gap constant number := 1000;
    l_start_sequence number;
    l_end_sequence number;
    r_retval number;
  begin
  
    if i_append = true
    then
      select nvl(max(sequence), 0) + lc_sequence_gap
        into r_retval
        from gtt_lpt_opn_sequence;
    else
      select sequence
        into l_end_sequence
        from gtt_lpt_opn_sequence
       where id = i_insert_after_id;
      
      select max(sequence)
        into l_start_sequence
        from gtt_lpt_opn_sequence
       where sequence < l_end_sequence;
      
      r_retval := l_start_sequence +
                  ((l_end_sequence - l_start_sequence) / 2);
    end if;
    
    return r_retval;
  end get_lpt_opn_sequence;
  
  /****************************************************************************/
  procedure insert_lpt_opn_sequence
  (
    i_logpoint in number,
    i_operation in number,
    o_next_id out raw,
    o_next_sequence out number,
    i_append in boolean default true,
    i_insert_after_id in raw default null
  )
  as
    l_id constant raw(16) := sys_guid;
    l_sequence number;
  begin

    l_sequence := get_lpt_opn_sequence(i_append, i_insert_after_id);
    
    dbms_output.put_line('        inserting sequence on append ' || boolean_to_char(i_append) || ' insert_after_id ' || i_insert_after_id);
    
    insert into gtt_lpt_opn_sequence
    values(l_id, i_logpoint, i_operation, l_sequence);
    
    o_next_id := l_id;
    o_next_sequence := l_sequence;
    
    commit;
  end;
  
  /****************************************************************************/
  procedure find_insert_lpt_opn_sequence
  (
    i_logpoint in number,
    i_operation in number,
    o_next_id in out raw,
    o_next_sequence in out number,
    o_found in out boolean,
    o_curr_lpt_opn_seq_id out raw
  )
  as
    cursor curr_lpt_opn_seq is
    select *
      from gtt_lpt_opn_sequence
     where sequence >= o_next_sequence
    order by sequence;
    
    l_found boolean := false;
    l_recommend_out_for_next boolean := false;
    
    l_found_num number;
  begin
    
    for rec in curr_lpt_opn_seq
    loop
      dbms_output.put_line('      checking seq on lpt ' || rec.logpoint || ' opn ' || rec.operation || ' seq ' || rec.sequence);
      if o_found = true
         and not (rec.logpoint = i_logpoint
                  and rec.operation = i_operation)
      then
        dbms_output.put_line('        o_found = true and not equal lptopn');
        insert_lpt_opn_sequence
        (
          i_logpoint,
          i_operation,
          o_next_id,
          o_next_sequence,
          false,
          o_next_id
        );
        
        o_found := false;
        l_found := true;
        o_curr_lpt_opn_seq_id := o_next_id;
        exit;
      end if;
      
      if l_recommend_out_for_next = true
         and not (rec.logpoint = i_logpoint
                  and rec.operation = i_operation)
      then
        dbms_output.put_line('        l_recommend_out_for_next = true and not equal lptopn');
        o_next_id := rec.id;
        o_next_sequence := rec.sequence;
        o_found := true;
        
        exit;
      end if;
      
      if rec.logpoint = i_logpoint
         and rec.operation = i_operation
      then
        dbms_output.put_line('        equal lptopn');
        l_found := true;
        l_recommend_out_for_next := true;
        o_found := false;
        o_curr_lpt_opn_seq_id := rec.id;
      end if;
    end loop;
    
    if not l_found
    then
      dbms_output.put_line('        l_found = true');
      insert_lpt_opn_sequence
      (
        i_logpoint,
        i_operation,
        o_next_id,
        o_next_sequence
      );
      o_curr_lpt_opn_seq_id := o_next_id;
    end if;
    
    dbms_output.put_line('        finished lpt ' || i_logpoint || ' opn ' || i_operation || ' id ' || o_curr_lpt_opn_seq_id);
    
    commit;
  end find_insert_lpt_opn_sequence;
  
  /****************************************************************************/
  procedure insert_lpt_equip_data_out
  (
    i_grouping_id in number,
    i_lpt_opn_seq_id in raw,
    i_lot in varchar2,
    i_logpoint in number,
    i_operation in number,
    i_transaction in varchar2,
    i_tran_dttm in date,
    i_login_dttm in date,
    i_equipment in varchar2,
    i_sequence in number
  )
  as
    lc_id constant raw(16) := sys_guid;
  begin
    dbms_output.put_line('    inserting out data');
    dbms_output.put_line('      lpt_opn_seq_id ' || i_lpt_opn_seq_id);
    
    insert into gtt_lpt_equip_data_out
    (
      id,
      grouping_id,
      lpt_opn_seq_id,
      lot,
      logpoint,
      operation,
      transaction,
      tran_dttm,
      login_dttm,
      equipment,
      sequence
    )
    values
    (
      lc_id,
      i_grouping_id,
      i_lpt_opn_seq_id,
      i_lot,
      i_logpoint,
      i_operation,
      i_transaction,
      i_tran_dttm,
      i_login_dttm,
      i_equipment,
      i_sequence
    );
    
    commit;
    
  end insert_lpt_equip_data_out;
  
  /****************************************************************************/
  procedure merge_lpt_opn_sequence
  (
    i_grouping_id in number
  )
  as
    cursor curr_lpt_equip_data is
    select *
      from gtt_lpt_equip_data
    order by sequence;
    
    l_next_id raw(16);
    l_next_sequence number := 0;
    l_found boolean := false;
    l_prev_logpoint number;
    l_prev_operation number;
    l_curr_lpt_opn_seq_id raw(16);
  begin
    
    for rec in curr_lpt_equip_data
    loop
      dbms_output.put_line('  lpt ' || rec.logpoint || ' opn ' || rec.operation);
      
      if (l_prev_logpoint is null
         and l_prev_operation is null)
         or
         not (l_prev_logpoint = rec.logpoint
              and l_prev_operation = rec.operation)
      then
        dbms_output.put_line('    inserting...');
        find_insert_lpt_opn_sequence
        (
          rec.logpoint,
          rec.operation,
          l_next_id,
          l_next_sequence,
          l_found,
          l_curr_lpt_opn_seq_id
        );
      else
        dbms_output.put_line('    skipping...');
      end if;
      
      l_prev_logpoint := rec.logpoint;
      l_prev_operation := rec.operation;
      
      insert_lpt_equip_data_out
      (
        i_grouping_id,
        l_curr_lpt_opn_seq_id,
        rec.lot,
        rec.logpoint,
        rec.operation,
        rec.transaction,
        rec.tran_dttm,
        rec.login_dttm,
        rec.equipment,
        rec.sequence
      );
    end loop;
  end merge_lpt_opn_sequence;
  
  /****************************************************************************/
  procedure determine_lpt_opn_sequence
  as
    cursor glg_data_groups is
    select *
      from genealogy_data_groups
    order by grouping_id, sequence;
    
    l_lpt_equip_data_rec lpt_equip_data%rowtype;
    l_grouping_id number;
  begin
    for rec in glg_data_groups
    loop
      dbms_output.put_line('lot ' || rec.dst_lot);
      l_grouping_id := rec.grouping_id;
      insert_gtt_lpt_equip_data(rec.dst_lot);
      merge_lpt_opn_sequence(l_grouping_id);
    end loop;
  end determine_lpt_opn_sequence;
  
  /****************************************************************************/
  procedure determine_lpt_opn_seq_rec
  as
  begin
    insert into gtt_lpt_equip_data_out
    (
      id,
      grouping_id,
      lpt_opn_seq_id,
      lot,
      logpoint,
      operation,
      transaction,
      tran_dttm,
      login_dttm,
      equipment
    )
    select sys_guid,
           1,
           b.id,
           a.lot,
           a.logpoint,
           a.operation,
           a.transaction,
           a.tran_dttm,
           a.login_dttm,
           a.equipment
      from s_lpt_equip_data a,
           gtt_lpt_opn_sequence b
     where a.logpoint = b.logpoint
       and a.operation = b.operation;
    
    commit;
      
  end determine_lpt_opn_seq_rec;
  
  /****************************************************************************/
  procedure determine_lpt_opn_sequence_ord
  as
    cursor distinct_lpt_opn is
    select distinct logpoint, operation
      from s_lpt_equip_data
    order by logpoint, operation;
    
    l_seq number := 1;
  begin
    for rec in distinct_lpt_opn
    loop
      insert into gtt_lpt_opn_sequence
      (
        id,
        logpoint,
        operation,
        sequence
      )
      values
      (
        sys_guid,
        rec.logpoint,
        rec.operation,
        l_seq
      );
      
      l_seq := l_seq + 1;
      
      commit;
    end loop;
  end determine_lpt_opn_sequence_ord;
  
  /****************************************************************************/
  procedure determine_lpt_opn_sequence_2
  as
  begin
    determine_lpt_opn_sequence_ord;
    determine_lpt_opn_seq_rec;
  end determine_lpt_opn_sequence_2;
  
  /****************************************************************************/
  function get_prev_lpt_opn_seq_id
  (
    i_curr_lpt_opn_seq_id in raw
  )
  return raw 
  as
    r_retval raw(16);
  begin
  
    begin
      select id
        into r_retval
        from gtt_lpt_opn_sequence
       where sequence = (select max(sequence)
                           from gtt_lpt_opn_sequence
                          where sequence < (select sequence
                                              from gtt_lpt_opn_sequence
                                             where id = i_curr_lpt_opn_seq_id));
    exception
      when no_data_found
      then null;
    end;
    
    return r_retval;
  end get_prev_lpt_opn_seq_id;
  
  /****************************************************************************/
  procedure insert_lpt_equip_parents
  (
    i_lpt_equip_id in raw,
    i_prev_lpt_opn_seq_id in raw
  )
  as
  begin
    insert into gtt_lpt_equip_parents
    with
    src_lots as
    (
      select distinct src_lot, a.grouping_id
        from gtt_lpt_equip_data_out a,
             genealogy_data_groups b
       where a.lot = b.dst_lot
         and a.grouping_id = b.grouping_id
         and a.id = i_lpt_equip_id
      union
      select lot, grouping_id
        from gtt_lpt_equip_data_out
       where id = i_lpt_equip_id
    )
    select distinct i_lpt_equip_id, a.id
      from gtt_lpt_equip_data_out a,
           src_lots b
     where a.lot = b.src_lot
       and a.grouping_id = b.grouping_id
       and a.lpt_opn_seq_id = i_prev_lpt_opn_seq_id
    ;
    
    commit;
    
  end insert_lpt_equip_parents;
  
  /****************************************************************************/
  procedure insert_lpt_equip_parent
  (
    i_lpt_equip_id in raw,
    i_lpt_equip_parent_id in raw
  )
  as
  begin
    insert into gtt_lpt_equip_parents
    values (i_lpt_equip_id, i_lpt_equip_parent_id);
    
    commit;
    
  end insert_lpt_equip_parent;
  
  /****************************************************************************/
  procedure determine_parents
  as
    cursor lpt_equip_data_out is
    select *
      from gtt_lpt_equip_data_out
    order by grouping_id, sequence desc;
    
    l_prev_lpt_opn_seq_id raw(16);
  begin
    dbms_output.put_line('determining parents...');
    for rec in lpt_equip_data_out
    loop
      dbms_output.put_line('lot ' || rec.lot || ' lpt ' || rec.logpoint || ' opn ' || rec.operation || ' curr_lpt_opn_seq_id ' || rec.lpt_opn_seq_id);
      l_prev_lpt_opn_seq_id := get_prev_lpt_opn_seq_id(rec.lpt_opn_seq_id);
      dbms_output.put_line('  prev_lpt_opn_seq_id ' || l_prev_lpt_opn_seq_id);
      insert_lpt_equip_parents(rec.id, l_prev_lpt_opn_seq_id);
    end loop;
  end determine_parents;
  
  /****************************************************************************/
  function check_if_parent_found_on_lpt
  (
    i_parent_lot in varchar2,
    i_lpt_opn_seq_id in raw
  )
  return number
  as
    l_found number := 0;
    r_retval number := 0;
  begin
    begin
      select count(*)
        into l_found
        from gtt_lpt_equip_data_out
       where lpt_opn_seq_id = i_lpt_opn_seq_id
         and lot = i_parent_lot;
    exception
      when no_data_found
      then l_found := 0;
    end;
    
    if l_found > 0
    then
      r_retval := 1;
    else
      r_retval := 0;
    end if;
    
    return r_retval;
  
  end check_if_parent_found_on_lpt;

  /****************************************************************************/
  procedure update_parent_status
  (
    i_lot in varchar2,
    i_parent_lot in varchar2,
    i_complete_flag in number
  )
  as
  begin
    update gtt_stage_parents
       set complete_flag = i_complete_flag
     where lot = i_lot
       and parent_lot = i_parent_lot;
    
    commit;
  end update_parent_status;
  
--  /****************************************************************************/
--  procedure check_each_parent
--  (
--    i_lot in varchar2,
--    i_lpt_equip_id in raw,
--    i_lpt_opn_seq_id in raw
--  )
--  as
--    cursor stage_parents_for_lot is
--    select *
--      from gtt_stage_parents
--     where lot = i_lot
--       and complete_flag <> 0;
--    
--    l_found number := 0;
--  begin
--    for rec in stage_parents_for_lot
--    loop
--      l_found := check_if_parent_found_on_lpt(rec.parent_id, i_lpt_opn_seq_id);
--      
--      if l_found
--      then
--        update_parent_status(i_lot, rec.parent_id, 1);
--        insert_lpt_equip_parent(i_lpt_equip_id, null);
--      end if;
--      
--    end loop;
--  end check_each_parent;
  
  /****************************************************************************/
  procedure check_lpts_for_parent
  (
    i_lpt_equip_id in raw,
    i_lot in varchar2
  )
  as
--    cursor lpt_opns is
--    with sequence as
--      select logpoint,
--             operation,
--             sequence
--        from gtt_lpt_equip_data_out
--       where lot = i_lot
--        order by sequence desc;
  begin
    dbms_output.put_line('determining parents...');
--    for rec in lpt_equip_data_out
--    loop
--      null;
--    end loop;
  end check_lpts_for_parent;
  
  /****************************************************************************/
  procedure load_stage_parents_data
  as
  begin
    execute immediate 'truncate table gtt_stage_parents';
    
    insert into gtt_stage_parents
    (
      lot,
      parent_lot
    )
    select distinct dst_lot,
           src_lot
      from s_genealogy_data
     where dst_lot <> src_lot;
    
    commit;
  end load_stage_parents_data;
  
  /****************************************************************************/
  procedure insert_parent_of_diff_lot
  as
    cursor all_lots is
    select distinct lot
      from gtt_lpt_equip_data_out;
  begin
    insert into gtt_lpt_equip_parents
    with
    parent_child as
    (
      select distinct src_lot,
             dst_lot
        from s_genealogy_data
       where src_lot <> dst_lot
    ),
    childs as
    (
      select a.lot, c.src_lot, a.id, b.sequence
        from gtt_lpt_equip_data_out a,
             gtt_lpt_opn_sequence b,
             parent_child c
       where c.dst_lot = a.lot
         and a.lpt_opn_seq_id = b.id
    ),
    parents as
    (
      select id,
             lot,
             sequence
        from (
          select a.id,
                 a.lot,
                 b.sequence,
                 rank() over (partition by a.lot order by b.sequence desc) as rank
            from gtt_lpt_equip_data_out a,
                 gtt_lpt_opn_sequence b,
                 parent_child c
           where c.src_lot = a.lot
             and a.lpt_opn_seq_id = b.id
        )
        where rank = 1
    ),
    parent_child_seqs as
    (
      select a.lot as child_lot,
             b.lot parent_lot,
             a.id as parent_id,
             b.id as child_id,
             a.sequence as parent_sequence,
             b.sequence as child_sequence
        from childs a,
             parents b
       where a.src_lot = b.lot
    )
    select distinct
           child_id,
           parent_id
      from (
        select child_id,
               parent_id,
               rank() over (partition by child_lot, parent_lot order by parent_sequence desc) as rank
          from parent_child_seqs
         where parent_sequence <= child_sequence
      )
     where rank = 1;
        
    
    commit;
  end insert_parent_of_diff_lot;
  
  /****************************************************************************/
  procedure insert_parent_of_same_lot
  as
    cursor all_lots is
    select distinct lot
      from gtt_lpt_equip_data_out;
  begin
    for rec in all_lots
    loop
      insert into gtt_lpt_equip_parents
      select *
        from (
          select a.id, 
                 lag(a.id, 1) over (order by b.sequence) as parent_id
            from gtt_lpt_equip_data_out a,
                 gtt_lpt_opn_sequence b
           where lot = rec.lot
             and a.lpt_opn_seq_id = b.id
        )
        where parent_id is not null;
      
      commit;
    end loop;
  end insert_parent_of_same_lot;
  
  /****************************************************************************/
  procedure determine_parents_2
  as
    cursor lpt_equip_data_out is
    select *
      from gtt_lpt_equip_data_out
    order by logpoint, operation, sequence desc;
  begin
    dbms_output.put_line('determining parents...');
--    load_stage_parents_data;
    insert_parent_of_same_lot;
    insert_parent_of_diff_lot;
    
--    for rec in lpt_equip_data_out
--    loop
--      check_lpts_for_parent(rec.id);
--      null;
--    end loop;
  end determine_parents_2;
  
  /****************************************************************************/
  procedure generate
  (
    i_lot_to_glg in varchar2
  )
  as
  begin
    clear_temp_tables;
    find_all_glg_groups;
--    find_glg_groups(i_lot_to_glg);
    determine_lpt_opn_sequence_2;
    determine_parents_2;
    --prepare_output;
    
  end generate;
  
end p_glg_data;
/