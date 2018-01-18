/*
--2,840,130����
--�ѹ��� �ߴܾ��� ��ȸ�ϰų� ��ȸ�ϰ� �ѹ��� ��ȸ�� ���� �ֵ�. ���� SUM ���̺� �ϳ��� �����Ͱ� �־����.
CREATE TABLE YHD_CS_TRAIN_MBR AS
SELECT MANDT,KUNNR,MATNR,ZCLS_ID,ZSTD_TYPE,ZSTD_STS,ZGRADE_CDE,ZSTD_STDT,ZSTD_EDDT,ZSTD_RSTDT,ZNEW_STDAREA1_TC,ZNEW_STDAREA2_TC,ZEND_STDAREA1_TC,ZEND_STDAREA2_TC 
FROM ZSD7_WKMBR T1 WHERE MANDT='300' AND MATNR='M'
AND ((ZSTD_EDDT > ZSTD_RSTDT) OR (ZSTD_EDDT=' ')) AND ZAPPRV_GB IN ('7','9')
AND NOT EXISTS (
    SELECT 1 FROM ZSD7_STDPROC_SUM WHERE MANDT=T1.MANDT AND KUNNR=T1.KUNNR AND MATNR=T1.MATNR
)
;
CREATE UNIQUE INDEX YHD_CS_TRAIN_MBR_I01 ON YHD_CS_TRAIN_MBR (MANDT,KUNNR,MATNR)
;

--���ø� �ּ� �ʿ�
SELECT KUNNR, MATNR, ZCLSVST_YM
    ,LENGTH(DECODE(ZTXTCRS_1WK,' ',NULL,ZTXTCRS_1WK)||DECODE(ZTXTCRS_2WK,' ',NULL,ZTXTCRS_2WK)||DECODE(ZTXTCRS_3WK,' ',NULL,ZTXTCRS_3WK)||DECODE(ZTXTCRS_4WK,' ',NULL,ZTXTCRS_4WK)||DECODE(ZTXTCRS_5WK,' ',NULL,ZTXTCRS_5WK)||DECODE(ZTXTCRS_6WK,' ',NULL,ZTXTCRS_6WK)) TOT_LEN
    ,DECODE(ZTXTCRS_1WK,' ',NULL,ZTXTCRS_1WK) AS ZTXTCRS_1WK, DECODE(ZTXTCRS_2WK,' ',NULL,ZTXTCRS_2WK) AS ZTXTCRS_2WK, DECODE(ZTXTCRS_3WK,' ',NULL,ZTXTCRS_3WK) AS ZTXTCRS_3WK, DECODE(ZTXTCRS_4WK,' ',NULL,ZTXTCRS_4WK) AS ZTXTCRS_4WK, DECODE(ZTXTCRS_5WK,' ',NULL,ZTXTCRS_5WK) AS ZTXTCRS_5WK, DECODE(ZTXTCRS_6WK,' ',NULL,ZTXTCRS_6WK) AS ZTXTCRS_6WK
FROM ZSD7_STDPROC_SUM WHERE MANDT='300' AND MATNR='M' AND KUNNR IN ('0051023926','0051689801')
ORDER BY KUNNR,MATNR,ZCLSVST_YM
;
*/

--������� üũ�� �ð� �÷�
ALTER TABLE YHD_CS_TRAIN_MBR drop column mod_time 
;
ALTER TABLE YHD_CS_TRAIN_MBR ADD mod_time DATE
;
CREATE INDEX YHD_CS_TRAIN_MBR_I02 ON YHD_CS_TRAIN_MBR (MOD_TIME)
;

--DICTIONARY ���̺� ����
DROP TABLE YHD_COURSESTEP_DICT
;
CREATE TABLE YHD_COURSESTEP_DICT (COURSE_STEP VARCHAR2(5), CNT NUMBER, CSID NUMBER)
;
CREATE UNIQUE INDEX YHD_COURSESTEP_DICT_I01 ON YHD_COURSESTEP_DICT (COURSE_STEP)
;

--DICTIONARY�� ����ID(CSID) ä���� ORACLE SEQUENCE
DROP SEQUENCE SEQ_ID
;
CREATE SEQUENCE SEQ_ID INCREMENT BY 1 START WITH 1
;

--TRAIN DATA ���ε�� ���̺� ����
DROP TABLE YHD_MBR_UPLOAD
;
CREATE TABLE YHD_MBR_UPLOAD (KUNNR VARCHAR2(10), CSIDS VARCHAR2(4000), COURSE_STEPS VARCHAR2(4000), SEQLEN NUMBER)
;

set serveroutput on

DECLARE
--�������Է��� ���� �ʱ⺯����
    v_course_steps varchar2(4000) := null;
    v_csids varchar2(4000) := null;
    v_seqlen number := 0;
    v_delimiter varchar2(1) := ',';

--�������� ���ڿ��� ����
    v_1wk varchar2(5) := null;
    v_2wk varchar2(5) := null;
    v_3wk varchar2(5) := null;
    v_4wk varchar2(5) := null;
    v_5wk varchar2(5) := null;
    v_6wk varchar2(5) := null;

--����Ƚ���� ����
    v_1wk_num number := 0;
    v_2wk_num number := 0;
    v_3wk_num number := 0;
    v_4wk_num number := 0;
    v_5wk_num number := 0;
    v_6wk_num number := 0;

--CSIDS�� ����
    v_1wkid number := null;
    v_2wkid number := null;
    v_3wkid number := null;
    v_4wkid number := null;
    v_5wkid number := null;
    v_6wkid number := null;
    
--������ Ȯ�ο� ����(�� ������ 3,4��°�� �������̾����.)
    v_check_number varchar2(12) := null;

--������ Ȯ�ο� Function (1�̸� ����, 0�̸� ����)
FUNCTION is_number (p_string IN VARCHAR2)
   RETURN INT
IS
   v_new_num NUMBER;
BEGIN
   v_new_num := TO_NUMBER(p_string);
   RETURN 1;
EXCEPTION
WHEN VALUE_ERROR THEN
   RETURN 0;
END is_number;

BEGIN
    DBMS_OUTPUT.ENABLE;
FOR MAIN IN (
    SELECT * FROM YHD_CS_TRAIN_MBR WHERE MANDT='300' AND MATNR='M' AND MOD_TIME IS NULL
--    AND KUNNR IN ('0051023926')
)LOOP
    v_course_steps := null;
    v_csids := null;
    v_seqlen := 0;
    
    FOR STDCS IN (
        SELECT KUNNR, MATNR, ZCLSVST_YM
            ,LENGTH(DECODE(ZTXTCRS_1WK,' ',NULL,ZTXTCRS_1WK)) AS LEN1WK, LENGTH(DECODE(ZTXTCRS_2WK,' ',NULL,ZTXTCRS_2WK)) AS LEN2WK, LENGTH(DECODE(ZTXTCRS_3WK,' ',NULL,ZTXTCRS_3WK)) AS LEN3WK, LENGTH(DECODE(ZTXTCRS_4WK,' ',NULL,ZTXTCRS_4WK)) AS LEN4WK, LENGTH(DECODE(ZTXTCRS_5WK,' ',NULL,ZTXTCRS_5WK)) AS LEN5WK, LENGTH(DECODE(ZTXTCRS_6WK,' ',NULL,ZTXTCRS_6WK)) AS LEN6WK
            ,DECODE(ZTXTCRS_1WK,' ',NULL,ZTXTCRS_1WK) AS ZTXTCRS_1WK, DECODE(ZTXTCRS_2WK,' ',NULL,ZTXTCRS_2WK) AS ZTXTCRS_2WK, DECODE(ZTXTCRS_3WK,' ',NULL,ZTXTCRS_3WK) AS ZTXTCRS_3WK, DECODE(ZTXTCRS_4WK,' ',NULL,ZTXTCRS_4WK) AS ZTXTCRS_4WK, DECODE(ZTXTCRS_5WK,' ',NULL,ZTXTCRS_5WK) AS ZTXTCRS_5WK, DECODE(ZTXTCRS_6WK,' ',NULL,ZTXTCRS_6WK) AS ZTXTCRS_6WK
        FROM ZSD7_STDPROC_SUM WHERE MANDT='300' AND MATNR='M' AND KUNNR=MAIN.KUNNR
        ORDER BY KUNNR,MATNR,ZCLSVST_YM
    )LOOP

--����+��Ʈ ���ڿ��� 4�ڸ��� �ƴ� ��찡 ����. exit;
        IF MOD(STDCS.LEN1WK,4) != 0 OR MOD(STDCS.LEN2WK,4) != 0 OR MOD(STDCS.LEN3WK,4) != 0 OR MOD(STDCS.LEN4WK,4) != 0 OR MOD(STDCS.LEN5WK,4) != 0 OR MOD(STDCS.LEN6WK,4) != 0 THEN
            v_course_steps := null;
            v_csids := null;
            v_seqlen := 0;
            EXIT;
        END IF;

--�� ������ 3��°4��°�ڸ��� �������¿�����. ex) A 07A 08(o), A 7A 8(x)
        v_check_number := SUBSTR(STDCS.ZTXTCRS_1WK,3,2)||SUBSTR(STDCS.ZTXTCRS_2WK,3,2)||SUBSTR(STDCS.ZTXTCRS_3WK,3,2)||SUBSTR(STDCS.ZTXTCRS_4WK,3,2)||SUBSTR(STDCS.ZTXTCRS_5WK,3,2)||SUBSTR(STDCS.ZTXTCRS_6WK,3,2);
        if is_number(v_check_number)=0 then v_course_steps := null;
            v_csids := null;
            v_seqlen := 0;
            EXIT;
        end if;

--���� �ʱ�ȭ
        v_1wk := null; v_2wk := null; v_3wk := null; v_4wk := null; v_5wk := null; v_6wk := null;
        v_1wkid := null; v_2wkid := null; v_3wkid := null; v_4wkid := null; v_5wkid := null; v_6wkid := null;
        v_1wk_num := 0; v_2wk_num := 0; v_3wk_num := 0; v_4wk_num := 0; v_5wk_num := 0; v_6wk_num := 0;
        
--����Ƚ�� ����. ������ 6ȸ �̻��� ��� 6ȸ�� ������. ������ �������� ���̱� ����. 
        v_1wk_num := (LENGTH(STDCS.ZTXTCRS_1WK)/4); if v_1wk_num > 6 then v_1wk_num:=6; end if;
        v_2wk_num := (LENGTH(STDCS.ZTXTCRS_2WK)/4); if v_2wk_num > 6 then v_2wk_num:=6; end if;
        v_3wk_num := (LENGTH(STDCS.ZTXTCRS_3WK)/4); if v_3wk_num > 6 then v_3wk_num:=6; end if;
        v_4wk_num := (LENGTH(STDCS.ZTXTCRS_4WK)/4); if v_4wk_num > 6 then v_4wk_num:=6; end if;
        v_5wk_num := (LENGTH(STDCS.ZTXTCRS_5WK)/4); if v_5wk_num > 6 then v_5wk_num:=6; end if;
        v_6wk_num := (LENGTH(STDCS.ZTXTCRS_6WK)/4); if v_6wk_num > 6 then v_6wk_num:=6; end if;

--���� ���� ���ڿ� ����. �������� = ���ʰ�����Ʈ + ����Ƚ�� ��) 4A014A024A03 --> 4A013
        v_1wk := SUBSTR(STDCS.ZTXTCRS_1WK,0,4)||v_1wk_num;
        v_2wk := SUBSTR(STDCS.ZTXTCRS_2WK,0,4)||v_2wk_num;
        v_3wk := SUBSTR(STDCS.ZTXTCRS_3WK,0,4)||v_3wk_num;
        v_4wk := SUBSTR(STDCS.ZTXTCRS_4WK,0,4)||v_4wk_num;
        v_5wk := SUBSTR(STDCS.ZTXTCRS_5WK,0,4)||v_5wk_num;
        v_6wk := SUBSTR(STDCS.ZTXTCRS_6WK,0,4)||v_6wk_num;

--�ش� ������ ������ ������ ���, NULL���� �ʵ尡 ����. ��� NULL�� ��ȯ. ���� ó���� ���ϰ� �ϱ� ����.
        if v_1wk = '0' then v_1wk := null; elsif v_2wk = '0' then v_2wk := null; elsif v_3wk = '0' then v_3wk := null; elsif v_4wk = '0' then v_4wk := null; elsif v_5wk = '0' then v_5wk := null; elsif v_6wk = '0' then v_6wk := null; end if;
        
--NULL�� �����ϰ�, DICTIONARY�� �ϼ���. ������ �ִ� ��� COUNT ����, ���� ��� ������ ����. NULL�� �����߱� ������ �̶� sequence length�� �������� ���.
        if v_1wk is not null then 
            merge into YHD_COURSESTEP_DICT T
            using DUAL
            on (T.course_step=v_1wk)
            when matched then 
                update set 
                    cnt = cnt + 1
            when not matched then
                insert (COURSE_STEP,CNT,CSID) values (v_1wk,1,SEQ_ID.NEXTVAL);
            select csid into v_1wkid from YHD_COURSESTEP_DICT where course_step=v_1wk;
            v_seqlen := v_seqlen + 1;
        end if;
        if v_2wk is not null then 
            merge into YHD_COURSESTEP_DICT T
            using DUAL
            on (T.course_step=v_2wk)
            when matched then 
                update set 
                    cnt = cnt + 1
            when not matched then
                insert (COURSE_STEP,CNT,CSID) values (v_2wk,1,SEQ_ID.NEXTVAL);
            select csid into v_2wkid from YHD_COURSESTEP_DICT where course_step=v_2wk;
            v_seqlen := v_seqlen + 1;
        end if;
        if v_3wk is not null then 
            merge into YHD_COURSESTEP_DICT T
            using DUAL
            on (T.course_step=v_3wk)
            when matched then 
                update set 
                    cnt = cnt + 1
            when not matched then
                insert (COURSE_STEP,CNT,CSID) values (v_3wk,1,SEQ_ID.NEXTVAL);
            select csid into v_3wkid from YHD_COURSESTEP_DICT where course_step=v_3wk;
            v_seqlen := v_seqlen + 1;
        end if;
        if v_4wk is not null then 
            merge into YHD_COURSESTEP_DICT T
            using DUAL
            on (T.course_step=v_4wk)
            when matched then 
                update set 
                    cnt = cnt + 1
            when not matched then
                insert (COURSE_STEP,CNT,CSID) values (v_4wk,1,SEQ_ID.NEXTVAL);
            select csid into v_4wkid from YHD_COURSESTEP_DICT where course_step=v_4wk;
            v_seqlen := v_seqlen + 1;
        end if;
        if v_5wk is not null then 
            merge into YHD_COURSESTEP_DICT T
            using DUAL
            on (T.course_step=v_5wk)
            when matched then 
                update set 
                    cnt = cnt + 1
            when not matched then
                insert (COURSE_STEP,CNT,CSID) values (v_5wk,1,SEQ_ID.NEXTVAL);
            select csid into v_5wkid from YHD_COURSESTEP_DICT where course_step=v_5wk;
            v_seqlen := v_seqlen + 1;
        end if;
        if v_6wk is not null then 
            merge into YHD_COURSESTEP_DICT T
            using DUAL
            on (T.course_step=v_6wk)
            when matched then 
                update set 
                    cnt = cnt + 1
            when not matched then
                insert (COURSE_STEP,CNT,CSID) values (v_6wk,1,SEQ_ID.NEXTVAL);
            select csid into v_6wkid from YHD_COURSESTEP_DICT where course_step=v_6wk;
            v_seqlen := v_seqlen + 1;
        end if;
        
--��ǥ�� �����Ͽ� ���� ���ڿ��� ID�� �ϼ�.
        if v_course_steps is null then v_course_steps := v_1wk; elsif v_1wk > '0' then v_course_steps :=  v_course_steps || v_delimiter || v_1wk; end if;
        if v_course_steps is null then v_course_steps := v_2wk; elsif v_2wk > '0' then v_course_steps :=  v_course_steps || v_delimiter || v_2wk; end if;
        if v_course_steps is null then v_course_steps := v_3wk; elsif v_3wk > '0' then v_course_steps :=  v_course_steps || v_delimiter || v_3wk; end if;
        if v_course_steps is null then v_course_steps := v_4wk; elsif v_4wk > '0' then v_course_steps :=  v_course_steps || v_delimiter || v_4wk; end if;
        if v_course_steps is null then v_course_steps := v_5wk; elsif v_5wk > '0' then v_course_steps :=  v_course_steps || v_delimiter || v_5wk; end if;
        if v_course_steps is null then v_course_steps := v_6wk; elsif v_6wk > '0' then v_course_steps :=  v_course_steps || v_delimiter || v_6wk; end if;
        
        if v_csids is null then v_csids := v_1wkid; elsif v_1wkid > '0' then v_csids :=  v_csids || v_delimiter || v_1wkid; end if;
        if v_csids is null then v_csids := v_2wkid; elsif v_2wkid > '0' then v_csids :=  v_csids || v_delimiter || v_2wkid; end if;
        if v_csids is null then v_csids := v_3wkid; elsif v_3wkid > '0' then v_csids :=  v_csids || v_delimiter || v_3wkid; end if;
        if v_csids is null then v_csids := v_4wkid; elsif v_4wkid > '0' then v_csids :=  v_csids || v_delimiter || v_4wkid; end if;
        if v_csids is null then v_csids := v_5wkid; elsif v_5wkid > '0' then v_csids :=  v_csids || v_delimiter || v_5wkid; end if;
        if v_csids is null then v_csids := v_6wkid; elsif v_6wkid > '0' then v_csids :=  v_csids || v_delimiter || v_6wkid; end if;

    END LOOP;

--�ϼ��� ����/ID���� ���ε� ��� Target ���̺� ����
    insert into yhd_mbr_upload (KUNNR,CSIDS,COURSE_STEPS,SEQLEN) VALUES(MAIN.KUNNR,v_csids,v_course_steps,v_seqlen);
--1�ΰ�� �ѹ�. 2�̻���͸� �Ʒü����� ��ġ�� ����. �����н��� �ؾ��ϱ� ������
    if v_seqlen > 1 then commit; else rollback; end if;
    
-- ��������� Ȯ���ϰ� �ߺ������� ���� ���� Source ���̺� �ش� ȸ�� ������Ʈ
    update YHD_CS_TRAIN_MBR set mod_time = sysdate where mandt=main.mandt and kunnr=main.kunnr and matnr=main.matnr;
    COMMIT;
--    dbms_output.put_line(v_course_steps);   
--    dbms_output.put_line(v_csids);
--    dbms_output.put_line(v_seqlen);

END LOOP;
    COMMIT;
END;
/

select round(count(*)/2840130*100,4)||'%' "������" from YHD_CS_TRAIN_MBR where mod_time is not null
;
SELECT KUNNR, MATNR, ZCLSVST_YM
    ,LENGTH(DECODE(ZTXTCRS_1WK,' ',NULL,ZTXTCRS_1WK)) AS LEN1WK, LENGTH(DECODE(ZTXTCRS_2WK,' ',NULL,ZTXTCRS_2WK)) AS LEN2WK, LENGTH(DECODE(ZTXTCRS_3WK,' ',NULL,ZTXTCRS_3WK)) AS LEN3WK, LENGTH(DECODE(ZTXTCRS_4WK,' ',NULL,ZTXTCRS_4WK)) AS LEN4WK, LENGTH(DECODE(ZTXTCRS_5WK,' ',NULL,ZTXTCRS_5WK)) AS LEN5WK, LENGTH(DECODE(ZTXTCRS_6WK,' ',NULL,ZTXTCRS_6WK)) AS LEN6WK
    ,DECODE(ZTXTCRS_1WK,' ',NULL,ZTXTCRS_1WK) AS ZTXTCRS_1WK, DECODE(ZTXTCRS_2WK,' ',NULL,ZTXTCRS_2WK) AS ZTXTCRS_2WK, DECODE(ZTXTCRS_3WK,' ',NULL,ZTXTCRS_3WK) AS ZTXTCRS_3WK, DECODE(ZTXTCRS_4WK,' ',NULL,ZTXTCRS_4WK) AS ZTXTCRS_4WK, DECODE(ZTXTCRS_5WK,' ',NULL,ZTXTCRS_5WK) AS ZTXTCRS_5WK, DECODE(ZTXTCRS_6WK,' ',NULL,ZTXTCRS_6WK) AS ZTXTCRS_6WK
FROM ZSD7_STDPROC_SUM WHERE MANDT='300' AND MATNR='M' AND KUNNR IN ('0053570619')
ORDER BY KUNNR,MATNR,ZCLSVST_YM
