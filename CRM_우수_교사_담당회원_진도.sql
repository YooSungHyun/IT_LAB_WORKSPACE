DROP TABLE YHD_GOOD_STDDTL
;
CREATE TABLE YHD_GOOD_STDDTL AS(
SELECT MANDT,KUNNR,MATNR,ZPROC_SEQ,ZDTL_SEQ,ZSTDAREA_ID,ZPROC,ZPROC_MAN_TYPE,ZSETNO,ZMODEL_SEQ,ZLINE,ZTXT_SEQ,ZTXT_SETNO,ZRESTD_CNT FROM ZSD7_STDPROC_DTL@DKP_LINK WHERE MANDT='300' AND MATNR='M' AND KUNNR IN 
(SELECT KUNNR FROM ZSD7_WKMBR_MON@DKP_LINK WHERE MANDT='300' AND ZRCD_YM='201711' AND MATNR='M' AND ZEMP_ID IN (
    SELECT '00'||ZEMP_ID AS ZEMP_ID
    FROM(
    SELECT RANK() OVER (ORDER BY  HUHOI_UL ASC ) R_ID, 
           A.* 
    FROM ZTB_M_MON1 A
    WHERE STELL IN('50000727','50000004','50000002','50000051')
    ) WHERE R_ID < 3000
))
)
;
create unique index yhd_good_stddtl_ui01 on yhd_good_stddtl(mandt, kunnr, matnr, zproc_seq, zdtl_seq)
;
--한번에 6SET 이상 잡은 진도 있는 회원은 모두 삭제
delete from YHD_GOOD_STDDTL where kunnr in (
select kunnr
from yhd_good_stddtl group by kunnr, matnr, zproc_seq
having count(*) > 6
)
;
commit
;
-- 한번도 휴회가 없는(진도가 끊긴적인 없는) 회원만 수집
DELETE FROM YHD_GOOD_STDDTL WHERE KUNNR IN (
--SELECT * FROM YHD_GOOD_STDDTL WHERE KUNNR IN (
SELECT T1.KUNNR FROM ZSD7_WKMBR@DKP_LINK T1
INNER JOIN YHD_GOOD_STDDTL T2
ON T1.MANDT=T2.MANDT
AND T1.KUNNR=T2.KUNNR
AND T1.MATNR=T2.MATNR
WHERE T1.MANDT='300' AND T1.MATNR='M'
AND T1.ZSTD_STS=1 AND ZAPPRV_GB='7'
AND T1.ZSTD_EDDT<>' '
)
;
COMMIT
;
--ZPROC이나 ZSETNO가 없으면 삭제
DELETE FROM YHD_GOOD_STDDTL WHERE ZPROC=' ' OR ZSETNO=' '
;
COMMIT
;
DROP TABLE YHD_MBR_COURSES
;
CREATE TABLE YHD_MBR_COURSES AS 
SELECT 
KUNNR, 
RANK() OVER (PARTITION BY KUNNR ORDER BY TO_NUMBER(ZPROC_SEQ) ASC) SEQ, 
COURSE FROM (
SELECT
    KUNNR,ZPROC_SEQ,
    MAX(CASE WHEN R_ID=1 THEN PROCSET ELSE '' END) ||
    MAX(CASE WHEN R_ID=2 THEN PROCSET ELSE '' END) ||
    MAX(CASE WHEN R_ID=3 THEN PROCSET ELSE '' END) ||
    MAX(CASE WHEN R_ID=4 THEN PROCSET ELSE '' END) ||
    MAX(CASE WHEN R_ID=5 THEN PROCSET ELSE '' END) ||
    MAX(CASE WHEN R_ID=6 THEN PROCSET ELSE '' END) AS COURSE 
FROM (
    select 
        RANK() OVER (PARTITION BY MANDT,KUNNR,MATNR,ZPROC_SEQ ORDER BY TO_NUMBER(ZPROC_SEQ), TO_NUMBER(ZDTL_SEQ) ASC) R_ID, 
        CASE WHEN LENGTH(ZPROC)=1 THEN ZPROC||' ' ELSE ZPROC END||ZSETNO AS PROCSET,
        T1.* 
    from YHD_GOOD_STDDTL T1
--    WHERE KUNNR='0050581624'
)
GROUP BY KUNNR, ZPROC_SEQ
)
ORDER BY KUNNR, ZPROC_SEQ
;
CREATE INDEX YHD_MBR_COURSES_I01 ON YHD_MBR_COURSES (COURSE)
;
CREATE INDEX YHD_MBR_COURSES_U01 ON YHD_MBR_COURSES (KUNNR)
;
--LENGTH가 4의 배수가 아닌 경우가 있음. 4A09(O), 4A9(X), 나중에 자리수 맞추기로 하고, 우선은 삭제
DELETE FROM YHD_MBR_COURSES
WHERE KUNNR IN (
    SELECT KUNNR FROM YHD_MBR_COURSES
    WHERE MOD(LENGTH(COURSE),4) > 0
)
;
COMMIT
;
ALTER TABLE YHD_MBR_COURSES ADD(COURSE_STEP VARCHAR2(5), CSID NUMBER)
;
--COURSE_STEP 패턴 = COURSE 맨앞 4자리||STEP수
UPDATE YHD_MBR_COURSES 
SET COURSE_STEP = SUBSTR(COURSE,0,4)||(LENGTH(COURSE) / 4)
;
COMMIT
;
--COURSE STEP 패턴의 DICTIONARY 생성
DROP TABLE YHD_COURSESTEP_DICT;
CREATE TABLE YHD_COURSESTEP_DICT AS
SELECT T1.*
    ,RANK() OVER (ORDER BY TO_NUMBER(CNT) DESC, COURSE_STEP ASC) CSID
FROM (
    SELECT COURSE_STEP, COUNT(*) AS CNT FROM YHD_MBR_COURSES GROUP BY COURSE_STEP
) T1
;
--CSID = COURSE||STEP 패턴의 ID
UPDATE YHD_MBR_COURSES T1 
SET CSID = (SELECT CSID FROM YHD_COURSESTEP_DICT WHERE COURSE_STEP = T1.COURSE_STEP)
;
COMMIT
;
DROP TABLE YHD_COURSES_DICT;
CREATE TABLE YHD_COURSES_DICT AS
SELECT T1.*
    ,RANK() OVER (ORDER BY TO_NUMBER(CNT) DESC, COURSE ASC) CID
FROM (
SELECT COURSE, COUNT(*) AS CNT FROM YHD_MBR_COURSES
GROUP BY COURSE 
--HAVING COUNT(*)>9
) T1
ORDER BY CNT DESC, COURSE ASC
;
CREATE UNIQUE INDEX YHD_COURSES_DICT_U01 ON YHD_COURSES_DICT(COURSE)
;
CREATE UNIQUE INDEX YHD_COURSES_DICT_U02 ON YHD_COURSES_DICT(CID)
;

ALTER TABLE YHD_MBR_COURSES ADD CID NUMBER;
COMMIT;

UPDATE YHD_MBR_COURSES T1 
SET CID = (SELECT CID FROM YHD_COURSES_DICT WHERE COURSE = T1.COURSE)
;
COMMIT
;
--set serveroutput on
--;
DROP TABLE YHD_MBR_UPLOAD
;
create table YHD_MBR_UPLOAD (KUNNR VARCHAR2(10), CIDS VARCHAR2(4000), COURSES LONG, CSIDS VARCHAR2(4000), COURSE_STEPS VARCHAR2(4000), SEQLEN NUMBER, GRADE VARCHAR2(2), GID NUMBER)
;
DECLARE
    v_cids varchar2(4000) := '';
    v_courses varchar2(14000) := '';
    v_csids varchar2(4000) := '';
    v_course_steps varchar2(4000) :=  '';
    v_grade varchar2(2) := '';
    v_gid number := 0;
    v_maxseq number := 0;
BEGIN
FOR CS IN (
    SELECT * FROM YHD_MBR_COURSES 
--    WHERE KUNNR in ('0050008938','0051198162','0050559154') 
    order by kunnr, seq
)LOOP
--    null;
    select max(seq) into v_maxseq from yhd_mbr_courses where kunnr=CS.KUNNR;
--    DBMS_OUTPUT.PUT_LINE(CS.KUNNR||' '||V_MAXSEQ||' '||CS.SEQ);
    if cs.seq = 1 then 
        v_courses := cs.course;
        v_cids := cs.cid;
        v_course_steps := cs.course_step;
        v_csids := cs.csid;
    else
        v_courses := v_courses || ',' ||cs.course;
        v_cids := v_cids || ',' ||cs.cid;
        v_course_steps := v_course_steps || ',' ||cs.course_step;
        v_csids := v_csids || ',' ||cs.csid;
        
        if cs.seq = v_maxseq then
            SELECT ZGRADE_CDE INTO v_grade FROM ZSD7_WKMBR@DKP_LINK WHERE MANDT='300' AND MATNR='M' AND KUNNR=cs.kunnr;
            SELECT DECODE(v_grade,'K1',0,'K2',1,'K3',2,'K4',3,'K5',4,'K6',5,'P1',6,'P2',7,'P3',8,'P4',9,'P5',10,'P6',11,'M1',12,'M2',13,'M3',14,'H1',15,'H2',16,'H3',17,'A',18) INTO v_gid FROM DUAL;
            INSERT INTO YHD_MBR_UPLOAD(KUNNR,CIDS,COURSES,SEQLEN,COURSE_STEPS,CSIDS,GRADE,GID) VALUES (CS.KUNNR, v_cids, v_courses, v_maxseq, v_course_steps, v_csids, v_grade, v_gid);
            v_courses := '';
            v_cids := '';
            v_course_steps := '';
            v_csids := '';
            v_grade := '';
            v_gid := 0;
            v_maxseq := 0;
            COMMIT;
        end if;
    end if;
END LOOP;
    COMMIT;
END;
/

--유아123세는 삭제
DELETE FROM YHD_MBR_UPLOAD WHERE GRADE IN ('K1','K2','K3')
;
COMMIT
;

SELECT * FROM YHD_MBR_COURSES
;
SELECT KUNNR, COURSES, COURSE_STEPS FROM YHD_MBR_UPLOAD
WHERE KUNNR='0050008938'
;
SELECT * FROM YHD_COURSESTEP_DICT
;
--
--;
--
--SELECT GRADE,COUNT(*) FROM YHD_MBR_UPLOAD GROUP BY GRADE
--;
--
----62,866
--SELECT COURSES FROM YHD_MBR_UPLOAD
--;
--select * from yhd_mbr_courses
--;
--select sum(cnt) from yhd_courses_dict where cnt<2
--;
--select * from YHD_COURSES_DICT
--;
--select * from zsd7_stdproc_
--;
--select count(*) from YHD_MBR_UPLOAD
--;
--
--73,401
--  121,704
--6,671,339
--;
----195,105
--SELECT * FROM YHD_COURSES_DICT ORDER BY CNT ASC
--;
--SELECT * FROM v$version WHERE banner LIKE 'Oracle%'
--;
--SELECT max(SEQLEN) FROM YHD_MBR_UPLOAD
----where rownum < 100000
--;
--select * from yhd_mbr_upload where rownum < 10
--;
--
--UPDATE yhd_mbr_upload 
--SET cids = replace(cids,'[','')
----    courses = replace(courses,'[','')
--;
--commit
--;
--select * from yhd_mbr_courses where kunnr='0050448514'
--;
--select * from yhd_mbr_upload where rownum < 100
--;
--select count(*) from yhd_courses_dict 
----where cid='10000'
--where cnt>9
--;
----select 
----    top/sum*100
----from (
--select 
--    sum(case when cid <= 10000 then cnt else 0 end)  top,
--    sum(case when cid > 10000 then cnt else 0 end)   bottom,
--    sum(cnt)   as  sum
--from yhd_courses_dict
----) 
--;
----6793043
--select course, cnt, round(cnt/6793043*100,5), cid from yhd_courses_dict
--;
--select SEQLEN,COUNT(*) from yhd_mbr_upload GROUP BY SEQLEN
--;
SELECT * FROM yhd_mbr_upload sample(0.05)
;
--SELECT COURSE_STEP, CSID, CNT FROM YHD_COURSESTEP_DICT
--;
--select * from ZSD7_WKMBR@DKP_LINK where mandt='300' 
--;
--select * from ZSD7_PRDT_SET@DKP_LINK
--;
--select * from ZSD7_PRDT_SET@DKP_LINK
--;
--SELECT kunnr, csids, seqlen FROM yhd_mbr_upload
--;
select * from (
    SELECT T1.*, (SELECT OTEXT FROM ZHRT010@DKP_LINK WHERE KUNNR='00'||T1.ZEMP_ID) as dept_nm
    FROM(
    SELECT RANK() OVER (ORDER BY  HUHOI_UL ASC ) R_ID, 
           A.* 
    FROM ZTB_M_MON1 A
    WHERE STELL IN('50000727','50000004','50000002','50000051')
    )T1 WHERE R_ID < 3000
) where zemp_id='32055928'
;

select * from YHD_MBR_UPLOAD
where kunnr='0054595197'
;
--SELECT 
--    KUNNR, ZPROC_SEQ,
--    COURSE_STEP,
--    NVL((SELECT CSID FROM YHD_COURSESTEP_DICT WHERE COURSE_STEP = T2.COURSE_STEP),0) AS CSID
--FROM (
--    SELECT 
--        KUNNR, ZPROC_SEQ,
--        CASE WHEN LENGTH(ZPROC)=1 THEN ZPROC||' ' ELSE ZPROC END ||
--        CASE WHEN LENGTH(ZSETNO)=1 THEN '0'||ZSETNO ELSE ZSETNO END ||
--        (SELECT MAX(ZDTL_SEQ) FROM ZSD7_STDPROC_DTL@DKP_LINK WHERE MANDT=T1.MANDT AND KUNNR=T1.KUNNR AND MATNR=T1.MATNR AND ZPROC_SEQ=T1.ZPROC_SEQ) AS COURSE_STEP
--    FROM ZSD7_STDPROC_DTL@DKP_LINK T1 
--    WHERE MANDT='300' AND MATNR='M'
--    AND KUNNR='H438020553'
--    AND ZDTL_SEQ='1'
--)T2 ORDER BY KUNNR, ZPROC_SEQ
--;
select KUNNR, ZPROC_SEQ,
    MAX(CASE WHEN ZDTL_SEQ=1 THEN PROCSET ELSE '' END) ||
    MAX(CASE WHEN ZDTL_SEQ=2 THEN PROCSET ELSE '' END) ||
    MAX(CASE WHEN ZDTL_SEQ=3 THEN PROCSET ELSE '' END) ||
    MAX(CASE WHEN ZDTL_SEQ=4 THEN PROCSET ELSE '' END) ||
    MAX(CASE WHEN ZDTL_SEQ=5 THEN PROCSET ELSE '' END) ||
    MAX(CASE WHEN ZDTL_SEQ=6 THEN PROCSET ELSE '' END) AS COURSE 
from (
SELECT 
    KUNNR, ZPROC_SEQ, ZDTL_SEQ, ZPROC, ZSETNO,
    CASE WHEN LENGTH(ZPROC)=1 THEN ZPROC||' ' ELSE ZPROC END||
    CASE WHEN LENGTH(ZSETNO)=1 THEN '0'||ZSETNO ELSE ZSETNO END AS PROCSET
--    kunnr, ZPROC_SEQ,
--    count(*)
FROM ZSD7_STDPROC_DTL@DKP_LINK WHERE MANDT='300' AND MATNR='M'
AND KUNNR='0054229390'
)
group by kunnr, ZPROC_SEQ

;
