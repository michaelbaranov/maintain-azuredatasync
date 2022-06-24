<#
.SYNOPSIS
    Helper cmdlet for Data Sync release process. Acts as both pre and post deployment action.
.DESCRIPTION
    With PreDeployment switch cmdlet disables auto sync and waits for current sync to finish (if any)
    With PostDeployment switch cmdlet refreshes database schema add/removes tables and columns, pushes
    updated schema to sync group, re-enables auto sync.
    You need to be logged in with Connect-AzAccount before using cmdlet
.PARAMETER ResourceGroupName
    Azure Resource group name where database is hosted
.PARAMETER ServerName
    SQL Server name
.PARAMETER DatabaseName
    Hub Database name
.PARAMETER SyncGroupName
    Data Sync Group name
.PARAMETER PreDeployment
    Switch indicating that Pre database deploymnet actions should be performed
.PARAMETER PostDeployment
    Switch indicating that Post database deploymnet actions should be performed
.PARAMETER SkipTables
    Array of regexes defining table names that should be excluded from sync. Format is [<schema name>].[<table name>]
.PARAMETER IncludeTables
    Array of table names to be included even if confirms to regex in SkipTables. Regex not supported, format is [<schema name>].[<table name>]
.PARAMETER SchemaRefreshTimeoutInSeconds
    For big databases schema refresh operation might run for a while. Try increasing this interval if experience timeouts
.PARAMETER DryRun
    Switch indicating that schema should be analyzed but not applied
.PARAMETER SyncIntervalInSeconds
    Auto Sync interval in seconds
.EXAMPLE
    Run pre Deployment activities
    PS C:\> .\MaintainDatasync.ps1 -ResourceGroupName datasyncautomation -ServerName datasyncautomation -DatabaseName primary -SyncGroupName ReportingSyncGroup -PreDeployment

    Dry run for post-deployment, ignoring tables starting with "_", include [dbo].[__DDLChanges] table
    PS C:\> .\MaintainDatasync.ps1 -ResourceGroupName datasyncautomation -ServerName datasyncautomation -DatabaseName primary -SyncGroupName ReportingSyncGroup -PostDeployment 
-SkipTables "\[dbo\]\.\[_.*\]","\[dbo\]\.\[NewTable1\]" -IncludeTables "[dbo].[__DDLChanges]" -DryRun

    Run post-deployment, ignoring tables starting with "_", include [dbo].[__DDLChanges] table
    PS C:\> .\MaintainDatasync.ps1 -ResourceGroupName datasyncautomation -ServerName datasyncautomation -DatabaseName primary -SyncGroupName ReportingSyncGroup -PostDeployment 
-SkipTables "\[dbo\]\.\[_.*\]","\[dbo\]\.\[NewTable1\]" -IncludeTables "[dbo].[__DDLChanges]" 
#>


using namespace Microsoft.Azure.Commands.Sql.DataSync.Model
using namespace System.Collections.Generic
param (
    [Parameter(Mandatory = $true)]
    [Parameter(ParameterSetName = 'PreDeployment')]
    [Parameter(ParameterSetName = 'PostDeployment')]
    [string]
    $ResourceGroupName,
    [Parameter(Mandatory = $true)]
    [Parameter(ParameterSetName = 'PreDeployment')]
    [Parameter(ParameterSetName = 'PostDeployment')]
    [string]
    $ServerName,
    [Parameter(Mandatory = $true)]
    [Parameter(ParameterSetName = 'PreDeployment')]
    [Parameter(ParameterSetName = 'PostDeployment')]
    [string]
    $DatabaseName,
    [Parameter(Mandatory = $true)]
    [Parameter(ParameterSetName = 'PreDeployment')]
    [Parameter(ParameterSetName = 'PostDeployment')]
    [string]
    $SyncGroupName,
    [Parameter(Mandatory = $false)]
    [Parameter(ParameterSetName = 'PreDeployment')]
    [switch]
    $PreDeployment,
    [Parameter(Mandatory = $false)]
    [Parameter(ParameterSetName = 'PostDeployment')]
    [switch]
    $PostDeployment,
    [Parameter(Mandatory = $false)]
    [Parameter(ParameterSetName = 'PostDeployment')]
    [String[]]
    $SkipTables,
    [Parameter(Mandatory = $false)]
    [Parameter(ParameterSetName = 'PostDeployment')]
    [String[]]
    $IncludeTables,
    [Parameter(Mandatory = $false)]
    [Parameter(ParameterSetName = 'PostDeployment')]
    [Int]
    $SchemaRefreshTimeoutInSeconds = 3000,
    [Parameter(Mandatory = $false)]
    [Parameter(ParameterSetName = 'PostDeployment')]
    [switch]
    $DryRun,
    [Parameter(Mandatory = $false)]
    [Parameter(ParameterSetName = 'PostDeployment')]
    [int]
    $SyncIntervalInSeconds = 600
)

$ErrorActionPreference = "Stop"
if ($PreDeployment) {
    Write-LogInfo "Running predeployment actions"
    Write-LogInfo "Disabling autosync"
    Update-AzSqlSyncGroup -ResourceGroupName $ResourceGroupName `
        -ServerName $ServerName `
        -DatabaseName $DatabaseName `
        -SyncGroupName $SyncGroupName `
        -IntervalInSeconds -1
    Write-LogInfo "Checking if sync is running"
    while ($(Get-AzSqlSyncGroup -ResourceGroupName $ResourceGroupName `
                -ServerName $ServerName `
                -DatabaseName $DatabaseName `
                -SyncGroupName $SyncGroupName).SyncState -eq "Progressing") {
        Write-LogInfo "Waiting for sync to finish"
        Start-Sleep -Seconds 5
    }
    Write-LogInfo "All good, fell free to update schema of secondary database"
}

if ($PostDeployment) {
    $startTime = Get-Date

    # Get schema of Hub database
    $syncGroup = Get-AzSqlSyncGroup -ResourceGroupName $ResourceGroupName `
            -ServerName $ServerName `
            -DatabaseName $DatabaseName `
            -SyncGroupName $SyncGroupName

    Write-LogInfo "Refreshing database schema from hub database"
    Update-AzSqlSyncSchema -ResourceGroupName $ResourceGroupName `
        -ServerName $ServerName `
        -DatabaseName $DatabaseName `
        -SyncGroupName $SyncGroupName

    ## Wait until the database schema is refreshed
    $timeoutTimeSpan = New-TimeSpan -Start $startTime -End $startTime
    $isSucceeded = $false
    While ($timeoutTimeSpan.TotalSeconds -le $SchemaRefreshTimeoutInSeconds) {
        Start-Sleep -s 10
    
        $databaseSchema = Get-AzSqlSyncSchema -SyncGroupName $SyncGroupName `
            -ServerName $ServerName `
            -DatabaseName $DatabaseName `
            -ResourceGroupName $ResourceGroupName
        
        if ($databaseSchema.LastUpdateTime -gt $startTime.ToUniversalTime()) {
            Write-LogInfo "Database schema refreshed"
            $isSucceeded = $true
            break;
        }
        Write-LogInfo "Waiting untill database schema is refreshed"
    }

    if (-not $isSucceeded) {
        Write-LogError "Refresh failed or timeout"
        exit;
    }
    
    # Removing tables and columns which is not defined in the database after database schema is refreshed and checking ignore list
    $tablesToRemove = New-Object "System.Collections.Generic.List[AzureSqlSyncGroupSchemaTableModel]";
    foreach ($tableSchema in $syncGroup.Schema.Tables) {
        # Checking if table should be skipped
        $shouldIncludeTable = $true
        if (!$IncludeTables.Contains($tableSchema.QuotedName)) {
            foreach ($skipPattern in $SkipTables) {
                if ($tableSchema.QuotedName -match $skipPattern) {
                    $shouldIncludeTable = $false
                    break
                }
            }
        }
        if (!$shouldIncludeTable) {
            $tablesToRemove.Add($tableSchema);
            continue
        }

        $tableInDatabase = $databaseSchema.Tables | Where-Object QuotedName -eq $tableSchema.QuotedName

        if ($null -eq $tableInDatabase) {
            $tablesToRemove.Add($tableSchema);
        }
        else {
            $columnsToRemove = New-Object "System.Collections.Generic.List[AzureSqlSyncGroupSchemaColumnModel]";
            foreach ($columnSchema in $tableSchema.Columns) {
                $columnInTable = $tableInDatabase[0].Columns | Where-Object QuotedName -eq $columnSchema.QuotedName
                $fullName = $tableSchema.QuotedName + "." + $columnSchema.QuotedName
                if ($null -eq $columnInTable) {
                    $columnsToRemove.Add($columnSchema);
                }
            }

            if ($columnsToRemove.Count -gt 0) {
                foreach ($columnToRemove in $columnsToRemove) {
                    $fullName = $tableSchema.QuotedName + "." + $columnToRemove.QuotedName
                    Write-LogInfo "Removing $fullName is being removed from sync schema"
                    $tableSchema.Columns.Remove($columnToRemove) | Out-Null
                }
            }
        }
    }

    if ($tablesToRemove.Count -gt 0) {
        foreach ($tableToRemove in $tablesToRemove) {
            Write-LogInfo "Removing $($tableToRemove.QuotedName) is being removed from sync schema"
            $syncGroup.Schema.Tables.Remove($tableToRemove) | Out-Null
        }
    }

    ## Add new tables and columns to the sync schema
    foreach ($tableSchema in $databaseSchema.Tables) {
        # Checking if table should be skipped
        $shouldIncludeTable = $true
        if (!$IncludeTables.Contains($tableSchema.QuotedName)) {
            foreach ($skipPattern in $SkipTables) {
                if ($tableSchema.QuotedName -match $skipPattern) {
                    $shouldIncludeTable = $false
                    break
                }
            }
        }
        if (!$shouldIncludeTable) {
            Write-LogInfo "Table $($tableSchema.QuotedName) does not fit any incule condition, skipping"
            continue
        }

        # Reuse if the table already exists in the schema; Otherwise, create a new one
        $newTableSchema = $syncGroup.Schema.Tables | Where-Object QuotedName -eq $TableSchema.QuotedName
        $addNewTable = $false
        if ($null -eq $newTableSchema) {
            $addNewTable = $true
            $newTableSchema = [AzureSqlSyncGroupSchemaTableModel]::new()
            $newTableSchema.QuotedName = $TableSchema.QuotedName
            $newTableSchema.Columns = [List[AzureSqlSyncGroupSchemaColumnModel]]::new();
        }

        ## If the table is not supported, move to next table
        if ($tableSchema.HasError) {
            Write-LogWarning "Can't add table $($tableSchema.QuotedName) to the sync schema $($tableSchema.ErrorId)" 
            continue;
        }

        ## Add columns
        foreach ($columnSchema in $tableSchema.Columns) {
            $fullColumnName = $tableSchema.QuotedName + "." + $columnSchema.QuotedName
            ## If the column already exists in the sync schema or not supported, ignore
            $column = $newTableSchema.Columns | Where-Object QuotedName -eq $columnSchema.QuotedName
            if ($null -ne $column) {
                Write-LogInfo "Column $fullColumnName is already in the schema"
            }
            elseif ($columnSchema.HasError) {
                Write-LogWarning "Can't add column $fullColumnName to the sync schema $($columnSchema.ErrorId)" 
            }
            else {
                Write-LogInfo "Adding $fullColumnName to the sync schema"
                $newColumnSchema = [AzureSqlSyncGroupSchemaColumnModel]::new()
                $newColumnSchema.QuotedName = $columnSchema.QuotedName
                $newColumnSchema.DataSize = $columnSchema.DataSize
                $newColumnSchema.DataType = $columnSchema.DataType
                $newTableSchema.Columns.Add($newColumnSchema)
            }
        }
        if ($newTableSchema.Columns.Count -gt 0 -and $addNewTable) {
            $syncGroup.Schema.Tables.Add($newTableSchema)
        }
    }
    $schemaString = $syncGroup.Schema | ConvertTo-Json -depth 5 -Compress
    $tempFile = "$($env:TEMP)\syncSchema.json"
    Write-LogInfo "Write the schema to $tempFile"
    $schemaString | Out-File $tempFile

    if (!$DryRun){
        Write-LogInfo "Update the sync schema"
        Update-AzSqlSyncGroup -ResourceGroupName $ResourceGroupName `
            -ServerName $ServerName `
            -DatabaseName $DatabaseName `
            -Name $SyncGroupName `
            -SchemaFile $tempFile         
        Write-LogInfo "Re-enablind autosync"
        Update-AzSqlSyncGroup -ResourceGroupName $ResourceGroupName `
            -ServerName $ServerName `
            -DatabaseName $DatabaseName `
            -SyncGroupName $SyncGroupName `
            -IntervalInSeconds $SyncIntervalInSeconds
    }
}