-- Prepare a throwaway Oracle Database Free container (gvenzl/oracle-free,
-- CDB FREE + PDB FREEPDB1) for the live Oracle CI job: LogMiner-ready redo,
-- the PDB login the connector tests use, and the common user LogMiner mines
-- with from CDB$ROOT (ADD_LOGFILE is illegal inside a PDB: ORA-65040).
--
-- Run by .github/scripts/start_ci_databases.sh oracle, as SYSDBA inside the
-- container, AFTER the database is in ARCHIVELOG mode. The password arrives as
-- the substitution variable ci_pw on stdin (never on a command line); VERIFY
-- OFF keeps sqlplus from echoing it into the job log.
--
-- Grants are the documented LogMiner set (V$ views, LOGMINING, DBMS_LOGMNR).
-- The PDB login gets direct grants only, never SELECT_CATALOG_ROLE: the
-- buffer-off LogMiner path needs V_$TRANSACTION and GV_$INSTANCE (open
-- transaction and RAC detection), and tests/test_cdc_oracle_logminer_txn_live.py
-- revokes and restores exactly those two grants to prove the connector fails
-- closed without them, which a catalog role would mask.
SET VERIFY OFF
SET ECHO OFF
SET FEEDBACK OFF
WHENEVER SQLERROR EXIT SQL.SQLCODE

ALTER DATABASE ADD SUPPLEMENTAL LOG DATA;

-- Common user for the CDB$ROOT mining session.
CREATE USER C##DATAFLOW IDENTIFIED BY "&ci_pw" CONTAINER = ALL;
GRANT CREATE SESSION, SET CONTAINER, LOGMINING, SELECT ANY TRANSACTION,
      SELECT ANY DICTIONARY, SELECT ANY TABLE, FLASHBACK ANY TABLE
   TO C##DATAFLOW CONTAINER = ALL;
GRANT EXECUTE_CATALOG_ROLE, SELECT_CATALOG_ROLE TO C##DATAFLOW CONTAINER = ALL;
GRANT EXECUTE ON SYS.DBMS_LOGMNR TO C##DATAFLOW CONTAINER = ALL;
GRANT EXECUTE ON SYS.DBMS_LOGMNR_D TO C##DATAFLOW CONTAINER = ALL;

-- The application login inside the PDB: owns the test tables, reads its own
-- redo position, and runs the PDB-side LogMiner probes.
ALTER SESSION SET CONTAINER = FREEPDB1;
CREATE USER DATAFLOW IDENTIFIED BY "&ci_pw"
  DEFAULT TABLESPACE USERS QUOTA UNLIMITED ON USERS;
GRANT CREATE SESSION, CREATE TABLE, CREATE VIEW, CREATE SEQUENCE,
      CREATE PROCEDURE, CREATE TRIGGER, CREATE TYPE, CREATE SYNONYM,
      UNLIMITED TABLESPACE, LOGMINING
   TO DATAFLOW;
GRANT SELECT ON SYS.V_$DATABASE TO DATAFLOW;
GRANT SELECT ON SYS.V_$LOG TO DATAFLOW;
GRANT SELECT ON SYS.V_$LOGFILE TO DATAFLOW;
GRANT SELECT ON SYS.V_$ARCHIVED_LOG TO DATAFLOW;
GRANT SELECT ON SYS.V_$LOGMNR_CONTENTS TO DATAFLOW;
GRANT SELECT ON SYS.V_$LOGMNR_LOGS TO DATAFLOW;
GRANT EXECUTE ON SYS.DBMS_LOGMNR TO DATAFLOW;
GRANT SELECT ON SYS.V_$TRANSACTION TO DATAFLOW;
GRANT SELECT ON SYS.GV_$INSTANCE TO DATAFLOW;

-- A read-only login for the destination-privilege probe's deny case
-- (tests/test_saas_privilege_deny_live.py): SELECT on one table, nothing else.
CREATE TABLE DATAFLOW.DF_DENY_PROBE (ID NUMBER PRIMARY KEY, LABEL VARCHAR2(32));
CREATE USER DATAFLOW_RO IDENTIFIED BY "&ci_pw";
GRANT CREATE SESSION TO DATAFLOW_RO;
GRANT SELECT ON DATAFLOW.DF_DENY_PROBE TO DATAFLOW_RO;

EXIT
