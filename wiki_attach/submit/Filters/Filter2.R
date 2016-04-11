#!/usr/bin/Rscript

VarsToAnalyze = c('temp', 'OXY_oxy')
Depths = list(NULL, NULL)

setwd(paste(getwd(), "Results", sep="/"))
library(glmtools)
 #Read the simulations for this experiment
 #SimDir = paste(ExperimentsDir,'sims/',SimNumber,'/',sep = "") # from the nc file
 #SimFile = paste(SimDir,'output.nc',sep = "") # from the nc file
 SimDir = paste(getwd(),"", sep="/") 
 SimFile = 'output.nc' 
 # Load NETCDF file
  if (file.exists(SimFile)){
    Message = paste('Simulation successfully loaded', sep="")
    print(Message)
    for (iV in 1:length(VarsToAnalyze)){
      # ExperimentPC
      VarName = VarsToAnalyze[iV]
      
      print('==============================================')
      print(paste('Analysis of ', VarName))
      print('==============================================')
      zDepth = Depths[[iV]]
      print(paste('Depth: ',zDepth))
      if (is.null(zDepth)){
        myOriginalDataPC = get_var(SimFile,var_name = VarName,reference = "surface")
        } 
      else{
        myOriginalDataPC = get_var(SimFile,var_name = VarName,reference = "surface", z_out = zDepth)}
      # Write output to disk
      write.csv(myOriginalDataPC,file = paste(SimDir, "/", VarName,'.csv',sep=""))
    }
    file.remove(SimFile) 
    }else { Message = paste('Experiment ', i, ' unsuccessful', sep="") 
    print(Message)
    }
