sampleDataDir <- "C:\\Users\\srram\\Desktop\\"
infile <- file.path(paste0(sampleDataDir, "Trainingdataset.csv"))
trainingxdf <- rxImport(inData = infile, outFile = "C:\\Users\\srram\\Desktop\\training.xdf",
    overwrite = TRUE)
colnames(trainingxdf)
length(trainingxdf)
data <- RxXdfData(trainingxdf,
    varsToKeep = c("horizon", "ID1", "ID2","time","value"), blocksPerRead = 15)
rxOpen(data)
summary(data)
rxClose(data)
