SET ANSI_NULLS ON  
GO  
SET QUOTED_IDENTIFIER ON  
GO  

CREATE PROCEDURE [dbo].[forecast] @inquery nvarchar(max)  
AS  
BEGIN  

  DECLARE @lmodel2 varbinary(max) = (SELECT TOP 1 model  
  FROM [dbo].[srram_rx_models] );  
  EXEC sp_execute_external_script @language = N'R',  
                                  @script = N'  
mod <- unserialize(as.raw(model));  
print(summary(mod))  
OutputDataSet<-rxPredict(modelObject = mod, data = InputDataSet, outData = NULL,   
          predVarNames = "Score", type = "response", writeModelVars = FALSE, overwrite = TRUE);  
str(OutputDataSet)  
print(OutputDataSet)  
',  
                                  @input_data_1 = @inquery,  
                                  @params = N'@model varbinary(max)',  
                                  @model = @lmodel2  
  WITH RESULT SETS ((Score float));  

END 

DECLARE @query_string nvarchar(max)  
SET @query_string='SELECT 
cast([horizon] as integer) as horizon
,cast([ID1] as integer) as ID1
,cast([ID2] as integer) as ID2
,CAST([time] as Date) AS tstamp 
,-1 AS value
,CAST([RDPI] as decimal(12,1)) AS RDPI
,CAST([year] as integer) as year
,cast([month] as integer) as month
,cast([weekofmonth] as integer) as weekofmonth
,cast([weekofyear] as integer) as weekofyear
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
FROM [dbo].[testingdataset]
WHERE ID1 = 2 and ID2 = 1'  

-- Call stored procedure for scoring  
EXEC [dbo].[forecast] @inquery = @query_string; 







---Not needed
UPDATE [dbo].[testingdataset]
SET value = '-1' 
WHERE ID1 = 2 and ID2 = 1 and value = '-100' 
