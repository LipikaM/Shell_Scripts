#!/bin/ksh
#set -x
# ----------------------------------------------------------------------------
# Name : PONTIS CDR extract
#
# Purpose : To extract Pontis service usage feed
#
# What does it do :Generates the file from prepay_cdr_hourly_extract and pontis_subscriber tables.
#
# Files generated in :/stage/outgoing/ted/liv/pontis/report
#    $
# Filenames will look like :
#    1.3.1      PONTIS_SERVICE_USAGE_YYYYMMDDHH24MISS_N.txt
#
#Change History:
#
# Changed Date By       Reason
# -----------------------------------------------
# 10/05/2011   Ramesh   CDR feed
# ----------------------------------------------------------------------------
#. /usr/local/bin/tedmngr.env

#PARAMETERS
PROCESS_ID=$1
RUNDTTM=$3
tedstats_user_pw='tedowner/tawny@strider_ted'
sqlplus="sqlplus -s $tedstats_user_pw"
SEPERATOR="|"
OUT_FILE=$2_${RUNDTTM}.txt
v_success="C"
v_failed="F"
v_extract_id=$2

LOADREP=$TEDSNDATADIR/pontis/datatmp


#common function
PRINT ()
{
dttm=`date "+%d-%m-%Y %H:%M:%S"`
echo "$dttm $1"
}

# FUNCTION
#---------
insert_audit_entry ()
{
PRINT "Insert entry into Pontis Audit Control table "
   echo "set lines 2000 head off feedback off pages 0\n
         insert into pontis_audit_ctrl
                (Process_Id,
                 Extract_Identifier,
                 Data_Start_Dttm,
                 Data_End_Dttm,
                 Data_Load_Dttm,
                 Collection_Status,
                 Extract_Gen_status,
                 rec_insert_dttm)
        Select    $1,
                  '$2',
                 Min(min_bill_end_period),
                 Max(max_bill_end_period),
                 to_date('${RUNDTTM}','yyyymmddhh24miss'),
                 'C',
                 'S',
                 sysdate
        From cdr_extracts_audit_ctrl
        Where partition_id in (${v_part_ids})
        And  job_name = 'CDR_EXTRACT';"|$sqlplus

        #if [ $? -ne 0 ]
        #then
           #PRINT "Failed to insert the Audit Control Entry..."
           #exit 1
        #fi

        PRINT "Update CDR Audit control table with pontis process id..."
        echo "set lines 2000 head off feedback off pages 0\n -
         update cdr_extracts_audit_ctrl -
            set pontis_process_id = $1 -
         where nvl(pontis_cdr_collection,'F')='F'
         and   job_name='CDR_EXTRACT' and job_status='C'
         and   partition_id in (${v_part_ids});"|$sqlplus

        #if [ $? -ne 0 ]
        #then
           #PRINT "Failed to insert CDR Audit Control Entry..."
           #exit 1
        #fi

}

# FUNCTION
# --------
update_audit_ctrl ()
{
if [ "$2" = "C" ]
then
 PRINT "Update Pontis and CDR Audit Control tables "
   echo "set lines 2000 head off feedback off pages 0\n -
         update pontis_audit_ctrl -
            set extract_gen_status='$2', -
                extract_end_dttm=sysdate, -
                extract_name = '${OUT_FILE}', -
                record_count = ${cunt}, -
                file_seqnum  = ${file_seqnum} -
          where process_id = $1;"|$sqlplus
        if [ $? -ne 0 ]
        then
           PRINT "Failed to update success status pontis Audit Control Entry..."
           exit 1
        fi

 PRINT "Update CDR Audit control table..."
 echo "set lines 2000 head off feedback off pages 0\n -
         update cdr_extracts_audit_ctrl -
            set pontis_cdr_collection='$2' -
          where nvl(pontis_cdr_collection,'F')='F'
          and   job_name='CDR_EXTRACT' and job_status='C' and partition_id in (${v_part_ids});"|$sqlplus
        if [ $? -ne 0 ]
        then
           PRINT "Failed to update success status in CDR Audit Control Entry..."
           exit 1
        fi

elif [ "$2" = "F" ]
then
 PRINT "Update Pontis Audit Control table "
   echo "set lines 2000 head off feedback off pages 0\n -
         update pontis_audit_ctrl -
            set extract_gen_status='$2', -
                extract_end_dttm=sysdate -
          where process_id = $1;"|$sqlplus
        if [ $? -ne 0 ]
        then
           PRINT "Failed to update failed status in Audit Control Entry..."
           exit 1
        fi

   PRINT "Update CDR Audit control table..."
   echo "set lines 2000 head off feedback off pages 0\n -
         update cdr_extracts_audit_ctrl -
            set pontis_cdr_collection='$2' -
          where nvl(pontis_cdr_collection,'F')='F'
          and   job_name='CDR_EXTRACT' and job_status='C' and partition_id in (${v_part_ids});"|$sqlplus
        if [ $? -ne 0 ]
        then
           PRINT "Failed to update failed status in CDR Audit Control Entry..."
           exit 1
        fi

fi
}


# Main Program
PRINT "-------------------------------------------------------------"

file_seqnum=`${sqlplus} << !EOF | grep -v "^Connected" 2>&1
                set termout off echo off feedback off pause off timing off time off
                set pages 0
                clear breaks
                set termout on
                select decode(trunc(max(rec_insert_dttm)),trunc(sysdate),nvl(max(file_seqnum)+1,1),1) from pontis_audit_ctrl where extract_identifier = '${v_extract_id}'
                and rec_insert_dttm >= trunc(sysdate);
                EXIT
                !EOF`
if [ $? -ne 0 ]
  then
    PRINT "Failed while fetching the sequence number ..."
    exit 1
fi

file_seqnum=`echo ${file_seqnum} | sed 's/ //g' `
OUT_FILE=$2_${RUNDTTM}_${file_seqnum}.txt

PRINT "Extracting cdrs to ${OUT_FILE}"

   #Tromboning
   PRINT "Collect CDR Partition ids for processing.."

   v_part_ids=`${sqlplus} << !EOF | grep -v "^Connected" 2>&1
               set termout off echo off feedback off pause off timing off time off
               set pages 0
               clear breaks
               set termout on
               select collect_partid_tromboning_func('PONTIS_SERVICE_USAGE') from dual;
               EXIT
               !EOF`
   echo "Here goes the Partition IDS"
   echo $v_part_ids

   PRINT ${v_part_ids}



if [ "${v_part_ids}" != "0" ]
then
        PRINT "Inserting Audit Control entry"
        insert_audit_entry ${PROCESS_ID} $2

        COLUMNLIST="nvl(null,'D') record_type
              , rownum rnum
              , c.seqnum serviceUsageId
              , nvl(d.account_id,'') accountId
              , nvl(c.CALLTYPE,'') serviceType
              , nvl(c.SERVICETYPE,'') serviceSubType
              , nvl(to_char(c.billstart,'ddmmyyyyhh24miss'),'') serviceStartDttm
              , nvl(c.billunits,'') billunits
              , nvl(c.RATETYPE,'') billunitstype
              , nvl(c.TIMEPARTITION,'') timePeriod
              , nvl(c.NORMALCHARGES*100,'') chargeBeforeDiscount
              , nvl((c.MONEYCHARGE+c.BONUSCHARGE)*100,'') Actualcharge
              , nvl((ltrim(c.STARTINGBAL,'-')+ltrim(c.STARTINGBONUSBAL,'-'))*100,'') STARTINGBALANCE
              , nvl((ltrim(c.endingbalance,'-')+ltrim(c.ENDINGBONUSBAL,'-'))*100,'') endingbalance
              , nvl(c.TERMZONE,'') TERMZONE"


        SQL_QUERY="select
                        ${COLUMNLIST}
                from prepay_cdr_hourly_extract c,
                        pontis_subscriber d
                where c.subscriber = d.subscriber_id
                and   c.serviceprovider in (283,993)
                and   c.partition_id in (${v_part_ids})
                and   EXISTS (SELECT 1
                              FROM pontis_service_type_map map
                              WHERE map.call_type = c.calltype)
                and   d.status = 1"


        echo "$SQL_QUERY" |/apps/ted/liv/bin/SQLUnloader userid=$tedstats_user_pw fieldsep="${SEPERATOR}" datefmt="YYYYMMDDHH24MISS" > ${LOADREP}/${OUT_FILE}
        #echo "$SQL_QUERY" |/apps/ted/liv/bin/SQLUnloader userid=$tedstats_user_pw fieldsep="${SEPERATOR}" datefmt="YYYYMMDDHH24MISS" header="TEDS|PONTIS|${RUNDTTM}|${cunt}" > ${LOADREP}/${OUT_FILE}
        #echo "$SQL_QUERY" |/apps/ted/liv/bin/SQLUnloader userid=$tedstats_user_pw fieldsep="${SEPERATOR}" datefmt="YYYYMMDDHH24MISS" header="H|TEDS|PONTIS|${RUNDTTM}|10" > ${LOADREP}/${OUT_FILE}
        #echo "$SQL_QUERY" |/apps/ted/liv/bin/SQLUnloader userid=$tedstats_user_pw fieldsep="${SEPERATOR}" datefmt="YYYYMMDDHH24MISS"  > ${LOADREP}/${OUT_FILE}

        cnt=`wc -l ${LOADREP}/${OUT_FILE}| awk '{print $1}'`
        PRINT "Total Count"
        cunt=`echo ${cnt} | sed 's/ //g' `
        PRINT ${cunt}
        echo "H|TEDS|PONTIS|${RUNDTTM}|${cunt}" > ${LOADREP}/header.txt
        cat ${LOADREP}/header.txt ${LOADREP}/${OUT_FILE} > ${LOADREP}/${OUT_FILE}.tmp
        rm -f ${LOADREP}/header.txt
        rm -f ${LOADREP}/${OUT_FILE}
        mv ${LOADREP}/${OUT_FILE}.tmp ${LOADREP}/${OUT_FILE}

        if [ $? -eq 0 ]
        then
                v_status=${v_success}
                if [ -s ${LOADREP}/${OUT_FILE} ]
                then
                   update_audit_ctrl ${PROCESS_ID} ${v_status}
                else
                   v_status=${v_failed}
                   update_audit_ctrl ${PROCESS_ID} ${v_status}
                   PRINT "ERROR: Zero Byte file generated. Please investigate"
                   exit 1
                fi

        else
                v_status=${v_failed}
                update_audit_ctrl ${PROCESS_ID} ${v_status}
                PRINT "ERROR: Extract generation failed"
                if [ -s ${LOADREP}/${OUT_FILE} ]
                then
                        PRINT "Removing the incorrect file if exists...."
                        rm -f ${LOADREP}/${OUT_FILE}
                fi
                PRINT "Extract generation process completed with errors."
                exit 1
        fi
else
         PRINT "There are no CDR partitions to collect the data. Please check the CDR load process"
        echo "Warning:There are no CDR partitions to collect the data. Please check the CDR load process" |mailx -s "Warning:Pontis Service Usage extract alert" tedsupport@o2.com
fi
PRINT "Extract generation process completed successfully."
PRINT "-------------------------------------------------------------"
exit 0

