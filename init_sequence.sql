-- initialize all the sequences
-- sequence naming convention should as follows:
-- SEQ_TABLE_NAME
CREATE OR REPLACE PROCEDURE init_seq
IS
l_dummy number;
m_number number;
sequenceValue Number;
step_val number;
BEGIN
FOR x IN (
	SELECT ut.table_name, utc.column_name, us.last_number, us.sequence_name
	FROM user_tables ut,
	user_tab_columns utc
	, user_sequences us
	WHERE ut.table_name = utc.table_name
	AND us.sequence_name = 'SEQ_'||UT.TABLE_NAME
	AND utc.column_id = 1
	AND utc.column_name like '%ID'
	--AND ut.table_name = 'MSTR_FUNC'
	ORDER BY 1
)
LOOP
	execute immediate 'select NVL(max('||x.column_name||'),0) from '||x.table_name into m_number;
	execute immediate 'select ' || x.sequence_name || '.nextval from dual' into l_dummy;
	dbms_output.put_line('handling : ' || x.sequence_name) ;

	step_val := m_number - l_dummy;

	execute immediate 'alter sequence ' || x.sequence_name || ' increment by ' || step_val || ' minvalue 0';
	execute immediate 'select ' || x.sequence_name || '.nextval from dual' into sequenceValue;
	execute immediate 'alter sequence ' || x.sequence_name || ' increment by 1 minvalue 0';
	dbms_output.put_line( 'last selected value from ' || x.sequence_name || ' was ' || sequenceValue ||' for table: '|| x.table_name );

END LOOP;
END;
/