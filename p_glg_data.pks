create or replace package p_glg_data
as

  procedure add_seq;
  procedure add_mock_operation;
  
  procedure generate
  (
    i_lot_to_glg in varchar2
  );
  
end p_glg_data;
/