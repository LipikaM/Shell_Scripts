#########################################################################
# Filename	:  foot2head_teds.sh
# Purpose	:  Move footers to headers
# Created by	:  Murari 
# Date		:   29/09/2010
# Parameters	:  Source_DIR => Name of Source directory
#                       	   File_Pattern => Pattern of file name
# Modification History  
# Date
# Descrition
##############################################################################################################################################

Source_DIR=$1
File_Pattern=$2

file_count=`ls -ltr ${Source_DIR} | awk -F " " '{print $9}' | grep CPI_OL_CREDITS | wc -l`
echo "Number of files $file_count"
if [ $file_count -ge 1 ] 
 then
 for file in `ls ${Source_DIR}/${File_Pattern}`
 do
  /apps/ted/liv/bin/foot2head $file 
  echo "\nMoving footer to header for file $file ...\n" 
 done
else
 echo "No file to process"
fi
