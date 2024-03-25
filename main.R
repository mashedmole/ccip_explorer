rm(list = ls())
library(httr)
library(data.table)
library(ggplot2)
library(anytime)
library(dplyr)
library(stringr)




# test ---------------------------------------------------------------

  request <- GET("https://ccip.chain.link/api/query?query=LATEST_PUBLIC_TRANSACTIONS_QUERY&variables={{
                 \"first\":{batch_size},\"offset\":{offset},\"condition\":{{\"{sourceOrDest}\":\"{networkName}\"}}}}")
  
  
  request <- GET("https://ccip.chain.link/api/query?query=LATEST_PUBLIC_TRANSACTIONS_QUERY&variables=%7B%22first%22%3A5%2C%22offset%22%3A0%2C%22condition%22%3A%7B%7D%7D")
  
  # https://www.urldecoder.org/
  # 
  data <- content(request)
  
  # res <- data.table("date" = sapply(data,'[[',"date"),
                    # "tvl_totalDefi_v2" = sapply(data,'[[',"tvl"))
  
