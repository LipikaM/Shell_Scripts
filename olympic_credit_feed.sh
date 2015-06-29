#!/bin/ksh

#######################################################################################################################
# Name          : olympic_credit_feed.sh
# Purpose       : This script will load the flat file for olympic Family Service into database using SQLLDR utility.
#               : and process the records to feed to the CPI Batch Top up Interface.
# Usage         :
#                 olympic_credit_feed.sh  [param1] [param2] [param3] [param4] [param5]
#                 Where
#                 param1  =>  Absolute path of the source directory where data file is stored
#                             Example: /stage/incoming/ted/liv/olympic/received
#
#                 param2  =>  Fixed string appearing in the filename
#                             Example: TEDS_OLYMPIC_????????  ***************************** <TBC> <><><><><><><><><><><>
#
#                 param3  =>  Name of the control file with absolute directory path
#                             Example: /apps/ted/liv/ldctl/olympic_data_load.ctl
#
#                 param4  =>  Absolute path of the archive directory where loaded files will be archived
#                             Example: /stage/incoming/ted/liv/olympic/archive
#
#                 param5  =>  Email ID details to send email to Operations
#                             Example: tedsupport@o2.com
#
# Modification History:
# --------------------------------------------------------------------------------------------------------------
# Version       Date            Author                  Remarks
# --------------------------------------------------------------------------------------------------------------
# 1.0           21/01/2012      Lipika Majumdar           Created.
#
#######################################################################################################################

source_dir=$1
filename_string=$2
ctl_file=$3
arch_dir=$4
ops_email=$5


ora_uid=`cat /home/tedload/.ted_uid | grep ted: | awk -F ":" '{ print $2 }'`
log_file=/apps/ted/liv/logs/cpi_load_files_sqlldr_$$.log

### Function PRINT_MSG - to print messages to log
print_msg ()
{
    date +"%Y/%m/%d %H:%M:%S : $1"
}

### Function ERROR_MSG - to print error messages and exit the processing
error_msg ()
{
    print_msg "ERROR: $1"
    print_msg "Exiting ..."
    ops_email "$1"
    exit 1
}

### Function ERROR_TAG - to rename the file with error suffix
error_tag ()
{
    print_msg "ERROR: $1"
    print_msg "Renaming the file with error suffix ..."
    mv -f ${data_file} ${data_file}.error 2>/dev/null
    print_msg "Exiting ..."
    exit 1
}

### Function OPS_EMAIL - to send email to Opeartions
ops_email ()
{
  
  print_msg "Sending email to $ops_email"
  echo "$1" | mailx -s "Olympic FIle Load ALARM !!" $ops_email
}


ls -l ${source_dir}/${filename_string}.csv 1>/dev/null 2>&1
if [ $? -ne 0 ]
then
    error_msg "No file found in directory ${source_dir} to be loaded!!"
fi

for data_file in `ls -l ${source_dir}/${filename_string}.csv | awk '{print $NF}'`
do
    abs_filename=`basename ${data_file}`
    print_msg "Started process to load text file ${abs_filename} ..."

    print_msg "Checking presence of header and trailer record ..."

    header_char=`head -1 ${data_file} | awk -F',' '{print $1}'`
    trailer_char=`tail -1 ${data_file} | awk -F',' '{print $1}'`
    if [ "${header_char}" = "H" ]
    then
        print_msg "Header is present in the file. OK to go ahead ..."
    else
        error_tag "Header is missing in the file ${abs_filename}."
    fi
    if [ "${trailer_char}" = "T" ]
    then
        print_msg "Trailer is present in the file. OK to go ahead ..."
    else
        error_tag "Trailer is missing in the file ${abs_filename}."
    fi

    print_msg "Verifying the record count from Trailer with actual detail records ..."

    actual_count=`wc -l < ${data_file} | awk '{print $NF}'`
    actual_count=`expr ${actual_count} - 2`
    actual_count2=`grep -c '^D' ${data_file}`
    trailer_count=`tail -1 ${data_file} | awk -F',' '{print $2}'`
    trailer_count=`expr ${trailer_count} + 0`
    if [ ${trailer_count} -eq ${actual_count} ]
    then
	if [ ${actual_count2} -ne ${trailer_count} ]
	then
	    error_tag "Unrecognized character in the start of a line in the file ${abs_filename}."
	fi
        print_msg "Trailer row count matches with actual line count from the file. OK to go ahead ..."
    else
	error_tag "There is mismatch between trailer row count and actual line count from the file ${abs_filename}."
    fi

    print_msg "Checking whether the file has previously been loaded ..."
    file_already_loaded=`sqlplus -s ${ora_uid} <<-EOF
	whenever sqlerror exit failure rollback
	set echo off feedback off verify off term off pages 0 linesize 200

	SELECT 'Y'
	FROM TED_CONTROL
	WHERE table_name = '${abs_filename}';
	EOF`

    if [ $? -ne 0 ]
    then
	error_msg "Failed during reading values from database."
    fi

    if [ "${file_already_loaded}" = "Y" ]
    then
	error_tag "The file ${abs_filename} has already been loaded or being loaded in parallel."
    else
        print_msg "The file ${abs_filename} has not been loaded previously. OK to go ahead ..."
    fi

    print_msg "Loading the file to the table OLYMPIC_DATA_LOAD_TMP ..."
    print_msg "Invoking SQLLDR to load the file ..."
    print_msg "CTL File = ${ctl_file} "
    print_msg "Log File = ${log_file} "

    sqlldr userid=${ora_uid} control=${ctl_file} data=${data_file} log=${log_file}
    
    retcode=`echo $?` 
    case "$retcode" in 
      0) echo "SQL*Loader execution successful" ;; 
      1) echo "SQL*Loader execution exited with EX_FAIL, see logfile" ;; 
      2) echo "SQL*Loader execution exited with EX_WARN, see logfile" ;; 
      3) echo "SQL*Loader execution encountered a fatal error" ;; 
      *) echo "unknown return code";; 
    esac
   
    no_of_data_errors=`grep 'Total logical records rejected' ${log_file} | awk '{print $NF}'`
    if [ ${no_of_data_errors} -gt 0 ]
    then
        error_tag "Failed during loading file ${abs_filename}. The file seems to have data errors."
    fi 


    print_msg "Populated the table OLYMPIC_DATA_LOAD_TMP successfully."
	
    print_msg "Populating data into detail table..."
    
    sqlplus -s ${ora_uid} <<-EOFF
    whenever sqlerror exit failure rollback
    set serveroutput on

    DECLARE
	v number:=0;
	v_part_date date;
	v_sql_stmt  varchar2(1000);
	v_prt_number number;
    BEGIN
	select trunc(sysdate+1)
	into v_part_date
	from dual;

	select max(partition_id)+1
	into v_prt_number
	from OLYMPIC_DATA_LOAD_DETAIL;

        UPDATE OLYMPIC_DATA_LOAD_TMP SET initial_credit_value=initial_credit_value*100;

	v_sql_stmt:='alter table OLYMPIC_DATA_LOAD_DETAIL ADD partition PART_'||v_prt_number||' values less than ('''||v_part_date||''')';
	EXECUTE IMMEDIATE v_sql_stmt;        
	
	EXECUTE IMMEDIATE 'ALTER TABLE OLYMPIC_DATA_LOAD_DETAIL EXCHANGE PARTITION PART_'||v_prt_number||' WITH TABLE OLYMPIC_DATA_LOAD_TMP';
        UPDATE OLYMPIC_DATA_LOAD_DETAIL set partition_id=v_prt_number where trunc(bt_insert_date)=trunc(sysdate);
        COMMIT;
    END;
	/
	EOFF

    if [ $? -ne 0 ]
    then
        error_msg "Failed during Olympic Detail Table Load."
    fi
    print_msg "Detail table load - OLYMPIC_DATA_LOAD_DETAIL is successfully done."

    print_msg "Moving data file to archive directory ..."
    mv ${data_file} ${arch_dir}/loaded_${abs_filename}
    if [ $? -ne 0 ]
    then
        error_msg "Failed during archiving data file ${abs_filename} to archive directory ${arch_dir}"
    fi
    print_msg "----------------------------------------------------------------------------"
done

print_msg "Finished the process to load the file ${abs_filename} successfully."
