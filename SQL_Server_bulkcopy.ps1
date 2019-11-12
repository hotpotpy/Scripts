# REQUIRES SQLSERVER MODULE FOR GET-SQLDATABASE COMMAND
<#
.SYNOPSIS
Imports a csv file into SQL Server through bulk copy.

.DESCRIPTION
The script was 4 main parts.
1. Check that all parameters are valid objects (file/database/schema exists, table not a duplicate)
2. Convert the csv to a data table (this will take the longest)
3. Use the columns of the csv to create a new table in SQL Server
4. Bulkcopy the contents of the data table into the newly created table.

The script will output the minutes used to finish each part.
The data types for all columns of the new table will always be varchar(500).

Given a table of 13 columns, approximately 20,000 rows per minute can be converted to the data table
(that's 1000 every 3 seconds).

.PARAMETER file
The file to be imported. Must be a csv file and the full path must be specified.

.PARAMETER server
The specific Sql Server for the new table. Default can be set to a specific server.

.PARAMETER database
The database for the new table. Default can be configured, depending on the computer name of the user.

.PARAMETER schema
The schema for the new table. Default is 'dbo'.

.PARAMETER table
The table name for the new table. Default will use the file name without the extension.

.EXAMPLE
PS C:\USERS\AARON\PS> .\SQl_Server_bulkcopy.ps1 -FILE C:\USERS\AARON\DOB\DOB_CERTIFICATE_OF_OCCUPANCY.CSV

Because this was run on Aaron's computer and no other parameters were specified,
the server variable is DEFAULT_SRV, database variable is DEFAULT_DB, schema variable is DBO,
and table name is DOB_CERTIFICATE_OF_OCCUPANCY.

The new table is [DEFAULT_DB].[DBO].[DOB_CERTIFICATE_OF_OCCUPANCY]

.EXAMPLE 
PS C:\USERS\AARON\PS> .\SQL_Server_bulkcopy.ps1 -FILE C:\USERS\AARON\TEMP\TEST3\DF1_TEST_DETAIL.csv
-DATABASE AFUNG_DEVELOP -SCHEMA TEMP -TABLE TEST_DETAIL

This creates the table [AFUNG_DEVELOP].[TEMP].[TEST_DETAIL]
#>

param(
[Parameter(Mandatory=$true)][string]$file, 
[string]$server= 'DEFAULT_SRV',
[alias("sqldb")][string]$database='DEFAULT',
[string]$schema = 'dbo',
[alias("tb")][string]$table = 'DEFAULT',
[string]$insert = "FALSE",
[string]$delimiter = "DEFAULT"
 )


$ErrorActionPreference="Stop"
#[System.Reflection.Assembly]::LoadWithPartialName("Microsoft.SqlServer.SMO") | Out-Null
Import-Module SQLServer

##################################
### PART ZERO: VALIDATE PARAMETERS
##################################
$start_date = Get-Date

# CHECK IF FILE EXISTS
if ((test-path $file) -eq $False)
    {
    Write-Host $File "does not exist"
    exit
    }
Write-Host "File:"$file

# IF PARAMETER STRING IS DEFAULT, SET TO PERSONAL DB AND FILE NAME AS TABLE NAME
if ($database -eq 'DEFAULT')
    {
    $comp_name = $env:COMPUTERNAME
    if ($comp_name -eq 'COMP_NAME')
        {$database = 'DEFAULT_DB'}
    elseif (($comp_name -eq 'COMP2') -OR ($comp_name -eq 'COMP3'))
        {$database = 'DEFAULT_DB2'}
    elseif ($comp_name -eq 'COMP4') 
        {$database = 'DEFAULT_DB3'}
    else 
        {
        Write-Host "Computer name does not match."
        Write-Host "The default database is not set for other computer names."
        exit
        }
    }
# IF TABLE NAME IS DEFAULT, SET TABLE NAME TO BE FILE NAME

if ($table -eq 'DEFAULT')
    {$table = [System.IO.Path]::GetFileNameWithoutExtension($file)}

$srv_obj = new-Object Microsoft.SqlServer.Management.Smo.Server($server)
  
#$db_obj = New-Object Microsoft.SqlServer.Management.Smo.Database
#$db_obj = $srv_obj.Databases.Item($database)  ### <--DOES NOT WORK DESPITE BEING THE SYNTAX IN MICROSOFT DOCS

$db_check = $srv_obj.Databases[$database]

## If the database exists, db_check will be the database object and not be null
if ($db_check -eq $null)
    {
    Write-Host "Database" $database "does not exist"
    exit
    }

# CHECK IF SCHEMA EXISTS IN THE DATABASE

$schema_check = $db_check.Schemas[$schema]
if ($schema_check -eq $null)
    {
    Write-Host "Schema" $schema "does not exist"
    exit
    }

# CHECK IF TABLE NAME CAN BE USED

$table_check = $db_check.Tables[$table]
if ($table_check -ne $null)
    {
	if ($insert -eq "FALSE") 
		{
		Write-Host "Table" $table "already exists, so it cannot be used as a new table."
		exit
		}
    }

# CHECK DELIMITER VARIABLE
if ($delimiter -eq "DEFAULT") 
	{
	$delimiter = ","
	}


$end_date = Get-Date
$time_diff = (New-TimeSpan -start $start_date -end $end_date).TotalMinutes
$time_diff = [Math]::Round($time_diff,2)
Write-Host "Part 0: Validating parameters - FINISHED - $time_diff minutes"
#######################################
### PART ONE: CONVERT CSV TO DATA TABLE
#######################################
$start_date = Get-Date

function csv_to_datatable {
    param ([Parameter(Mandatory=$true)][string]$file)
    $dt = New-Object system.Data.DataTable
    $data = import-csv $file -delimiter $delimiter

    $cols = $data | Get-Member -MemberType 'NoteProperty' | Select-Object -ExpandProperty 'Name'
    foreach ($col in $cols) 
        {[void]$dt.Columns.Add($col)}

    for ($i=0; $i -lt $data.count; $i++) 
        {
        $input_row = $data[$i]
        $row = $dt.NewRow()
        $arr = New-Object System.Collections.ArrayList
        foreach ($col in $cols) 
            {$row.$col=$input_row.$col.trimstart()} 
        [void]$dt.Rows.Add($row)
        Write-Progress -Activity "Converting csv file to data table" -status "$i ROWS DONE" -PercentComplete ($i*100/$data.count)
        }
    return ,$dt
    }
# Call function
$dt = csv_to_datatable($file)

$end_date = Get-Date
$time_diff = (New-TimeSpan -start $start_date -end $end_date).TotalMinutes
$time_diff = [Math]::Round($time_diff,2)
Write-Host "Part 1: Converting csv to data table - FINISHED - $time_diff minutes"

########################################
### PART TWO: CREATE TABLE IN SQL SERVER
########################################
$start_date = Get-Date

$db_obj = Get-SQLDatabase -ServerInstance $server -Name $database

# Get the columns of the data table again because the previous cols variable was in the function and not in global scope
$cols = $dt | Get-Member -MemberType Property

### Create new table and add in the columns from the data table, all as varchar(500)
$table_check = $db_obj.Tables[$table]
if ($table_check -eq $null)
	{
	$table_obj = new-object Microsoft.SqlServer.Management.Smo.Table($db_obj, $table, $schema)  
	foreach ($col in $cols) 
		{
		$add_col = new-object Microsoft.SqlServer.Management.Smo.Column($table_obj, $col.Name, [Microsoft.SqlServer.Management.Smo.DataType]::VarChar(500))  
		$table_obj.Columns.Add($add_col)  
		}
	$table_obj.Create()  
	} else {
	Write-Host "The table exists, so this step is skipped."}
$end_date = Get-Date
$time_diff = (New-TimeSpan -start $start_date -end $end_date).TotalMinutes
$time_diff = [Math]::Round($time_diff,2)
Write-Host "Part 2: Created table in SQL Server - FINISHED - $time_diff minutes"

########################################################
### PART THREE: BULK COPY DATA TABLE TO SQL SERVER TABLE
########################################################
$start_date = Get-Date
$ServerConnection = "Data Source='$Server';Integrated Security=true;Initial Catalog=$Database";

$BULKCOPY_obj = NEW-OBJECT -TYPENAME DATA.SQLCLIENT.SQLBULKCOPY -ARGUMENTLIST $SERVERCONNECTION;
$BULKCOPY_obj.DESTINATIONTABLENAME='['+$schema+'].['+$Table+']'
# Map column order so values are not inserted into the wrong columns
foreach ($col in $dt.Columns) 
    {
    [void]$BULKCOPY_obj.ColumnMappings.Add($col.Columnname, $col.Columnname)
    }
$BULKCOPY_obj.WRITETOSERVER($dt)

$end_date = Get-Date
$time_diff = (New-TimeSpan -start $start_date -end $end_date).TotalMinutes
$time_diff = [Math]::Round($time_diff,2)
Write-Host "Part 3: Bulk copy to SQL Server - FINISHED - $time_diff minutes"
Write-Host "New table:" '['$database'].['$schema'].['$table']'