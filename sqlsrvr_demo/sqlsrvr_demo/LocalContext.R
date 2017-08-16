sqlConnString <- "Driver=SQL Server;Server=srramsql1.southcentralus.cloudapp.azure.com; Database=demo;Uid=srramadmin;Pwd=Lz8oq1dn$Lz1"
sqlDS <- RxSqlServerData(connectionString = sqlConnString,
    table = "Trainingdataset", rowsPerRead = 5000)
rxGetVarInfo(data = sqlDS)
Query <- "SELECT cast([horizon] as integer) as horizon
,cast([ID1] as integer) as ID1
,cast([ID2] as integer) as ID2
,CAST([time] as Date) AS tstamp 
,CAST([value] as decimal(12,8)) AS value
,CAST([RDPI] as decimal(12,1)) AS RDPI
,CAST([year] as integer) as year
,cast([month] as integer) as month
,cast([weekofmonth] as integer) as weekofmonth
,cast([weekofyear] as integer) as weekofyear
,CASE WHEN [USNewYearsDay] = 'False' THEN 0 ELSE 1 END AS USNewYearsDay
,CASE WHEN [USLaborDay] = 'False' THEN 0 ELSE 1 END AS USLaborDay
,CASE WHEN [USThanksgivingDay] = 'False' THEN 0 ELSE 1 END AS USThanksgivingDay
,CASE WHEN [CyberMonday] = 'False' THEN 0 ELSE 1 END AS CyberMonday
,CASE WHEN [ChristmasDay] = 'False' THEN 0 ELSE 1 END AS ChristmasDay
,CAST(SUBSTRING(FreqCos1,1,15) as DECIMAL(15,13)) as FreqCos1
,CAST(SUBSTRING(Freqsin1,1,15) as DECIMAL(15,13)) as Freqsin1
,CAST(SUBSTRING(FreqCos2,1,15) as DECIMAL(15,13)) as FreqCos2
,CAST(SUBSTRING(Freqsin2,1,15) as DECIMAL(15,13)) as Freqsin2
,CAST(SUBSTRING(FreqCos3,1,15) as DECIMAL(15,13)) as FreqCos3
,CAST(SUBSTRING(Freqsin3,1,15) as DECIMAL(15,13)) as Freqsin3
,CAST(SUBSTRING(FreqCos4,1,15) as DECIMAL(15,13)) as FreqCos4
,CAST(SUBSTRING(Freqsin4,1,15) as DECIMAL(15,13)) as Freqsin4
,CAST([lag1] as DECIMAL(12,8)) as lag1
,CAST([lag2] as DECIMAL(12,8)) as lag2
,CAST([lag3] as DECIMAL(12,8)) as lag3
,CAST([lag4] as DECIMAL(12,8)) as lag4
,CAST([lag5] as DECIMAL(12,8)) as lag5
,CAST([lag6] as DECIMAL(12,8)) as lag6
,CAST([lag7] as DECIMAL(12,8)) as lag7
,CAST([lag8] as DECIMAL(12,8)) as lag8
,CAST([lag9] as DECIMAL(12,8)) as lag9
,CAST([lag10] as DECIMAL(12,8)) as lag10
,CAST([lag11] as DECIMAL(12,8)) as lag11
,CAST([lag12] as DECIMAL(12,8)) as lag12
,CAST([lag13] as DECIMAL(12,8)) as lag13
,CAST([lag14] as DECIMAL(12,8)) as lag14
,CAST([lag15] as DECIMAL(12,8)) as lag15
,CAST([lag16] as DECIMAL(12,8)) as lag16
,CAST([lag17] as DECIMAL(12,8)) as lag17
,CAST([lag18] as DECIMAL(12,8)) as lag18
,CAST([lag19] as DECIMAL(12,8)) as lag19
,CAST([lag20] as DECIMAL(12,8)) as lag20
,CAST([lag21] as DECIMAL(12,8)) as lag21
,CAST([lag22] as DECIMAL(12,8)) as lag22
,CAST([lag23] as DECIMAL(12,8)) as lag23
,CAST([lag24] as DECIMAL(12,8)) as lag24
,CAST([lag25] as DECIMAL(12,8)) as lag25
,CAST([lag26] as DECIMAL(12,8)) as lag26
FROM [demo].[dbo].[Trainingdataset]
WHERE ID1 = 2 and ID2 = 1"
sqlDSTransformed <- RxSqlServerData(sqlQuery = Query,
   connectionString = sqlConnString, rowsPerRead = 5000)
rxGetVarInfo(data = sqlDSTransformed)
trainingdata <- rxImport(sqlDSTransformed)
head(trainingdata)
rxSummary( ~ ., data = trainingdata)

DforestModelObj <- rxDForest(value ~ RDPI + year + month + weekofmonth +
    weekofyear + USNewYearsDay +
    USLaborDay + USThanksgivingDay + CyberMonday + ChristmasDay +
    FreqCos1 + Freqsin1 + FreqCos2 + Freqsin2 + FreqCos3 + Freqsin3 + FreqCos4 + Freqsin4 +
    lag1 + lag2 + lag3 + lag4 + lag5 + lag6 + lag7 + lag8 + lag9 + lag10 +
    lag11 + lag12 + lag13 + lag14 + lag15 + lag16 + lag17 + lag18 + lag19 + lag20 +
    lag21 + lag22 + lag23 + lag24 + lag25 + lag26 ,data = trainingdata)
summary(DforestModelObj)

QueryTst <- "SELECT 
cast([horizon] as integer) as horizon
,cast([ID1] as integer) as ID1
,cast([ID2] as integer) as ID2
,CAST([time] as Date) AS tstamp 
,CAST(value as decimal(12,10)) AS value
,CAST([RDPI] as decimal(12,1)) AS RDPI
,CAST([year] as integer) as year
,cast([month] as integer) as month
,cast([weekofmonth] as integer) as weekofmonth
,cast([weekofyear] as integer) as weekofyear
,CASE WHEN [USNewYearsDay] = 'False' THEN 0 ELSE 1 END AS USNewYearsDay
,CASE WHEN [USLaborDay] = 'False' THEN 0 ELSE 1 END AS USLaborDay
,CASE WHEN [USThanksgivingDay] = 'False' THEN 0 ELSE 1 END AS USThanksgivingDay
,CASE WHEN [CyberMonday] = 'False' THEN 0 ELSE 1 END AS CyberMonday
,CASE WHEN [ChristmasDay] = 'False' THEN 0 ELSE 1 END AS ChristmasDay
,CAST(SUBSTRING(FreqCos1,1,15) as DECIMAL(15,13)) as FreqCos1
,CAST(SUBSTRING(Freqsin1,1,15) as DECIMAL(15,13)) as Freqsin1
,CAST(SUBSTRING(FreqCos2,1,15) as DECIMAL(15,13)) as FreqCos2
,CAST(SUBSTRING(Freqsin2,1,15) as DECIMAL(15,13)) as Freqsin2
,CAST(SUBSTRING(FreqCos3,1,15) as DECIMAL(15,13)) as FreqCos3
,CAST(SUBSTRING(Freqsin3,1,15) as DECIMAL(15,13)) as Freqsin3
,CAST(SUBSTRING(FreqCos4,1,15) as DECIMAL(15,13)) as FreqCos4
,CAST(SUBSTRING(Freqsin4,1,15) as DECIMAL(15,13)) as Freqsin4
,CAST([lag1] as DECIMAL(12,8)) as lag1
,CAST([lag2] as DECIMAL(12,8)) as lag2
,CAST([lag3] as DECIMAL(12,8)) as lag3
,CAST([lag4] as DECIMAL(12,8)) as lag4
,CAST([lag5] as DECIMAL(12,8)) as lag5
,CAST([lag6] as DECIMAL(12,8)) as lag6
,CAST([lag7] as DECIMAL(12,8)) as lag7
,CAST([lag8] as DECIMAL(12,8)) as lag8
,CAST([lag9] as DECIMAL(12,8)) as lag9
,CAST([lag10] as DECIMAL(12,8)) as lag10
,CAST([lag11] as DECIMAL(12,8)) as lag11
,CAST([lag12] as DECIMAL(12,8)) as lag12
,CAST([lag13] as DECIMAL(12,8)) as lag13
,CAST([lag14] as DECIMAL(12,8)) as lag14
,CAST([lag15] as DECIMAL(12,8)) as lag15
,CAST([lag16] as DECIMAL(12,8)) as lag16
,CAST([lag17] as DECIMAL(12,8)) as lag17
,CAST([lag18] as DECIMAL(12,8)) as lag18
,CAST([lag19] as DECIMAL(12,8)) as lag19
,CAST([lag20] as DECIMAL(12,8)) as lag20
,CAST([lag21] as DECIMAL(12,8)) as lag21
,CAST([lag22] as DECIMAL(12,8)) as lag22
,CAST([lag23] as DECIMAL(12,8)) as lag23
,CAST([lag24] as DECIMAL(12,8)) as lag24
,CAST([lag25] as DECIMAL(12,8)) as lag25
,CAST([lag26] as DECIMAL(12,8)) as lag26
FROM [demo].[dbo].[testingdataset]
WHERE ID1 = 2 and ID2 = 1 and value <> '';
"

sqlDSTest <- RxSqlServerData(sqlQuery = QueryTst,
            connectionString = sqlConnString, rowsPerRead = 5000)
rxGetVarInfo(data = sqlDSTest)
testdata <- rxImport(sqlDSTest)
head(testdata)

##outData = sqlServerOutDS,



predictionoutput <- rxPredict(modelObject = DforestModelObj,
            data = testdata,
            predVarNames = "DforestScore",
            type = "response",
            writeModelVars = FALSE,
            computeResiduals = TRUE,

            extraVarsToWrite = c("tstamp","value"),
            overwrite = TRUE)

Predvalues <- cbind(exp(predictionoutput$value_Pred), exp(predictionoutput$value))
