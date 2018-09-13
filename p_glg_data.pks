create or replace package p_glg_data
as

  FUNCTION get_synonym_object 
  (
    i_synonym IN VARCHAR2
  )
  RETURN VARCHAR2;

  procedure add_seq;
  procedure add_mock_operation;
  
  procedure generate
  (
    i_lot_to_glg in varchar2
  );
  
end p_glg_data;
/