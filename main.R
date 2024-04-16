rm(list = ls())
library(httr)
library(data.table)
library(ggplot2)
library(anytime)
library(dplyr)
library(stringr)
library(xml2)
library(lubridate)
library(assertthat)



# functions - utils ---------------------------------------------

# get tx_data batch
get_tx_data_js <- function(batch_size, offset, condition, this_timeout){
  
  # params
  # batch_size <- 1000
  # offset <- 0
  # condition <- ""
  # timeout <- 10e3
  
  # build request
  request <- paste0("https://ccip.chain.link/api/query/LATEST_TRANSACTIONS_QUERY?variables=",
    "%7B%22first%22%3A", batch_size, 
    "%2C%22offset%22%3A", offset,
    "%2C%22condition%22%3A%7B", condition,
    "%7D%7D"
    )
  
  
  # get
  get_request <- GET(request, timeout(this_timeout))
  
  # err
  status_code <- status_code(get_request)
  
  # return
  return(list(data = get_request, status_code = status_code))

}

tx_data_js_to_dataframe <- function(tx_data_js){
  
  # params
  # tx_data_js <- get_tx_data_js(5, 0, "", 20e3)$data
  
  # trim (manual)
  tx_data <- content(tx_data_js)
  names(tx_data[[1]])
  allCcipTransactionsFlats <- tx_data[[1]][[1]]
  names(allCcipTransactionsFlats)
  nodes <- allCcipTransactionsFlats[[1]]
  node_1 <- nodes[[1]]
  names(node_1)
  
  # list to datatable (manual) 
  tx_data <- data.table("transactionHash" = sapply(nodes,'[[',"transactionHash"),
                        "destTransactionHash" = sapply(nodes,'[[',"destTransactionHash"),
                        "onrampAddress" = sapply(nodes,'[[',"onrampAddress"),
                        "offrampAddress" = sapply(nodes,'[[',"offrampAddress"),
                        "commitStoreAddress" = sapply(nodes,'[[',"commitStoreAddress"),
                        "state" = sapply(nodes,'[[',"state"),
                        "sourceChainId" = sapply(nodes,'[[',"sourceChainId"),
                        "sourceNetworkName" = sapply(nodes,'[[',"sourceNetworkName"),
                        "sender" = sapply(nodes,'[[',"sender"),             
                        "receiver" = sapply(nodes,'[[',"receiver"),          
                        "messageId" = sapply(nodes,'[[',"messageId"),     
                        "destChainId" = sapply(nodes,'[[',"destChainId"),     
                        "destNetworkName" = sapply(nodes,'[[',"destNetworkName"),   
                        "blockTimestamp" = sapply(nodes,'[[',"blockTimestamp"),  
                        "data" = sapply(nodes,'[[',"data"),          
                        "strict" = sapply(nodes,'[[',"strict"),       
                        "nonce" = sapply(nodes,'[[',"nonce"),      
                        "gasLimit" = sapply(nodes,'[[',"gasLimit"),   
                        "sequenceNumber" = sapply(nodes,'[[',"sequenceNumber"), 
                        "feeToken" = sapply(nodes,'[[',"feeToken"), 
                        "feeTokenAmount" = sapply(nodes,'[[',"feeTokenAmount")
  )
  
  
  # get token amounts (sublist) (manual)
  nodes_num <- length(nodes)
  tx_data_tokenAmounts <- NULL
  for ( i in 1:nodes_num){
    
    this_node <- nodes[[i]]
    this_transactionHash <- this_node$transactionHash
    if(is.null(this_node$destTransactionHash)){this_destTransactionHash <- "NULL"}else{this_destTransactionHash <- this_node$destTransactionHash}
    this_nonce <- this_node$nonce 
    this_node_token_amounts <- node_1[["tokenAmounts"]]
    tx_tokenAmounts <- data.table("token" = sapply(this_node_token_amounts,'[[',"token"),
                                  "amount" = sapply(this_node_token_amounts,'[[',"amount"))
    tx_tokenAmounts <- tx_tokenAmounts %>% mutate(transactionHash = this_transactionHash,
                                                  destTransactionHash = this_destTransactionHash,
                                                  nonce = this_nonce,
                                                  .before = "token")
    
    tx_data_tokenAmounts <- tx_data_tokenAmounts %>% rbind(tx_tokenAmounts)
    
  }
  
  
  # exclude transactions still waiting for finality (causes unicity issues)
  tx_data_waitingforfinality <- tx_data[as.character(tx_data$destTransactionHash) == "NULL",]
  tx_data_tokenAmounts_waitingforfinality <- tx_data_tokenAmounts[as.character(tx_data_tokenAmounts$destTransactionHash) == "NULL",]
  tx_data <- tx_data[as.character(tx_data$destTransactionHash) != "NULL", ]
  tx_data_tokenAmounts <- tx_data_tokenAmounts[as.character(tx_data_tokenAmounts$destTransactionHash) != "NULL",]
  
  # return res
  return(list(tx_data = tx_data, tx_data_tokenAmounts = tx_data_tokenAmounts))
  
}

get_bulk_tx_data_js <- function(condition, offset_start, n_batches, max_tries, batch_size, timeout){
  
  # params
  # condition <- this_condition
  # offset_start <- 0 # if you want to start with a specific offset
  # n_batches <- 3 # number of successful iterations of get_tx_data_js
  # max_tries <- 2 # number of tries of the same get_tx_data_js before giving up
  # batch_size <- 100
  # timeout <- 10e3
  # network <- "ethereum-mainnet"
  # condition <- paste0("%22sourceNetworkName%22%3A%22", network, "%22")
  
  # init
  tx_data_agg <- NULL
  tx_data_tokenAmounts_agg <- NULL
  error_log <- NULL
  overlap <- 10 # offset might not be perfect so better have calls overlap a bit

  
  # loop on batches
  for(i in c(1:n_batches)){
    
    this_batch_num <- i
    try_count <- 1
    print(paste0("------------------------------------------------------------------------------"))
    
    # loop on tries to get data
    while (try_count <= max_tries){
      
      # get tx_data_js
      print(paste0("calling ccip explorer api for a batch size of (transactions): ", batch_size))
      tx_data_js <- get_tx_data_js(batch_size, offset = max((i-1)*batch_size + offset_start - overlap, 0), condition, timeout)
      tx_data_js_error_status <- tx_data_js$status_code
      if(tx_data_js_error_status == 200){ 
         if(length(content(tx_data_js$data)$data$allCcipTransactionsFlats$nodes) == 0){tx_data_js_error_status <- "no_data"}
      }
      
      # if success
      if(tx_data_js_error_status == 200){
        
        print(paste0("call success"))
        
        this_tx_data <- tx_data_js_to_dataframe(tx_data_js$data)
        this_tx_data$tx_data <- this_tx_data$tx_data # %>% mutate(batch_num = this_batch_num, .before = "transactionHash")
        
        # todel # # ## ###
        # first_batch <- as.data.table(this_tx_data$tx_data)
        # second_batch <- as.data.table(this_tx_data$tx_data)
        # ## ## ## ##
        
        
        tx_data_agg <- tx_data_agg %>% rbind(this_tx_data$tx_data)
        tx_data_tokenAmounts_agg <- tx_data_tokenAmounts_agg %>% rbind(this_tx_data$tx_data_tokenAmounts)
        
        first_blockTimestamp = this_tx_data$tx_data[nrow(this_tx_data$tx_data),]$blockTimestamp
        first_transactionHash = this_tx_data$tx_data[nrow(this_tx_data$tx_data),]$transactionHash
        last_transactionHash = this_tx_data$tx_data[1,]$transactionHash
        last_blockTimestamp = this_tx_data$tx_data[1,]$blockTimestamp
        
        
        this_errorlog <- data.table(
          #batch_num = this_batch_num,
          first_blockTimestamp,
          first_transactionHash,
          last_transactionHash,
          last_blockTimestamp
        )
        
        error_log <- error_log %>% rbind(this_errorlog)
        
        print(paste0("ccip tx data collected"))
        print(paste0("batch first_blockTimestamp: ", first_blockTimestamp))
        print(paste0("batch last_blockTimestamp: ", last_blockTimestamp))
        
        
        try_count <- max_tries + 1
        
      }else{
        
        print(paste0("call failed - error status: ", tx_data_js_error_status," - try ", try_count))
        
        if(try_count == max_tries){
          this_errorlog <- data.table(
            #batch_num = this_batch_num,
            first_blockTimestamp = paste0("call failed after ", try_count, " tries - error status: ", tx_data_js_error_status),
            first_transactionHash = paste0("call failed after ", try_count, " tries - error status: ", tx_data_js_error_status),
            last_transactionHash = paste0("call failed after ", try_count, " tries - error status: ", tx_data_js_error_status),
            last_blockTimestamp = paste0("call failed after ", try_count, " tries - error status: ", tx_data_js_error_status)
          )
          
          error_log <- error_log %>% rbind(this_errorlog)
          
        }
        
        try_count <- try_count + 1
        
      }
      
    }
    
  }
  
  # return
  return(list(tx_data = tx_data_agg, tx_data_tokenAmounts = tx_data_tokenAmounts_agg, error_log = error_log))
  
}

get_sourceNetwork_bulk_tx_data_js <- function(sourceNetwork, offset_start, n_batches, max_tries, batch_size, timeout){
  
  # network <- "ethereum-mainnet"
  condition <- paste0("%22sourceNetworkName%22%3A%22", sourceNetwork, "%22")
  
  # console msg
  print(paste0("------------------------------------------------------------------------------"))
  print(paste0("NETWORK NAME: ", sourceNetwork))
  
  # get res
  return(
    get_bulk_tx_data_js(
      condition = condition,
      offset_start,
      n_batches,
      max_tries,
      batch_size,
      timeout
    )
  )
}

get_targetDate_sourceNetwork_bulk_tx_data_js <- function(target_date, max_getbulkdata_calls, sourceNetwork, offset_start, n_batches, max_tries, batch_size, timeout){
  
  # # params target date
  # target_date <- as.Date("2023-06-22") # will get tx_data from offset_start to this target date
  # max_getbulkdata_calls <- Inf # in case something goes wrong with the while loop, will stop calling get_sourceNetwork_bulk_tx_data_js
  # 
  # 
  # # params get_sourceNetwork_bulk_tx_data_js
  # offset_start <- 0 #14.1e3 # if you want to start with a specific offset
  # n_batches <- 10 # number of successful iterations of get_tx_data_js
  # max_tries <- 3 # number of tries of the same get_tx_data_js before giving up
  # batch_size <- 100
  # timeout <- 10e3
  # sourceNetwork <- "ethereum-mainnet"
  
  
  # loop on i (will call get_sourceNetwork_bulk_tx_data_js until reaching target_date)
  target_reached <- FALSE 
  bulk_tx_data_js <- NULL
  overlap <- 5 # offset might not be perfect so better have calls overlap a bit
  i <- 1
  while(target_reached == FALSE){
    
    # init
    tx_data <- NULL
    tokenamount <- NULL
    
    # get bulk data
    this_bulk_tx_data_js <- get_sourceNetwork_bulk_tx_data_js(sourceNetwork, offset_start = max((i-1)*batch_size*n_batches + offset_start - overlap, 0), n_batches, max_tries, batch_size, timeout)
    
    this_bulk_num <- i
    if(!is.null(this_bulk_tx_data_js$tx_data)){
      tx_data <- this_bulk_tx_data_js$tx_data #%>% mutate(bulk_num = this_bulk_num, .before = "batch_num")
      }
    if(!is.null(this_bulk_tx_data_js$tx_data_tokenAmounts)){
      tokenamount <- this_bulk_tx_data_js$tx_data_tokenAmounts #%>% mutate(bulk_num = this_bulk_num, .before = "transactionHash")
    }
    error_log <- this_bulk_tx_data_js$error_log #%>% mutate(bulk_num = this_bulk_num, .before = "batch_num")
    
    
    # collate bulk data
    bulk_tx_data_js$tx_data <- rbind(bulk_tx_data_js$tx_data, tx_data)
    bulk_tx_data_js$tx_data_tokenAmounts <- rbind(bulk_tx_data_js$tx_data_tokenAmounts, tokenamount)
    bulk_tx_data_js$error_log <- rbind(bulk_tx_data_js$error_log, error_log)
    
    # get oldest block timestamp
    col_oldest_blockTimestamp <- this_bulk_tx_data_js$error_log$first_blockTimestamp
    col_oldest_blockTimestamp_short <- substr(col_oldest_blockTimestamp, 1, 10)
    col_oldest_blockTimestamp_short2 <- col_oldest_blockTimestamp_short[col_oldest_blockTimestamp_short != "call faile"]
    oldest_blockTimestamp <- as.Date(last(col_oldest_blockTimestamp_short2))
    
    # is target date reached?
    if(!is.na(oldest_blockTimestamp)){
      
      if(oldest_blockTimestamp <= target_date){
        target_reached <- TRUE
        print(paste0("------------------------------------------------------------------------------"))
        print(paste0("target_date reached - ", target_date))
        
        this_errorlog <- data.table(
          #bulk_num = paste0("target_date reached"),
          #batch_num = paste0("target_date reached"),
          first_blockTimestamp = paste0("target_date reached"),
          first_transactionHash = paste0("target_date reached"),
          last_transactionHash = paste0("target_date reached"),
          last_blockTimestamp = paste0("target_date reached")
        )
        
        bulk_tx_data_js$error_log <- bulk_tx_data_js$error_log %>% rbind(this_errorlog)
      }
      
    }
    
    # is there no data to retrieve anymore?
    if(is.na(oldest_blockTimestamp)){
      
      
      target_reached <- TRUE
      print(paste0("------------------------------------------------------------------------------"))
      print(paste0("no data in last batch so stopped calling the api"))
      
      this_errorlog <- data.table(
        #bulk_num = paste0("no data in last batch so stopped calling the api"),
        #batch_num = paste0("no data in last batch so stopped calling the api"),
        first_blockTimestamp = paste0("no data in last batch so stopped calling the api"),
        first_transactionHash = paste0("no data in last batch so stopped calling the api"),
        last_transactionHash = paste0("no data in last batch so stopped calling the api"),
        last_blockTimestamp = paste0("no data in last batch so stopped calling the api")
      )
      
      bulk_tx_data_js$error_log <- bulk_tx_data_js$error_log %>% rbind(this_errorlog)
      
      
    }
    
    # is it over max_getbulkdata_calls?
    if(i >= max_getbulkdata_calls){
      
      target_reached <- TRUE
      print(paste0("------------------------------------------------------------------------------"))
      print(paste0("stopped calling before reaching target_date because reached max_getbulkdata_calls"))
      
      this_errorlog <- data.table(
        #bulk_num = paste0("stopped calling before reaching target_date because reached max_getbulkdata_calls"),
        #batch_num = paste0("stopped calling before reaching target_date because reached max_getbulkdata_calls"),
        first_blockTimestamp = paste0("stopped calling before reaching target_date because reached max_getbulkdata_calls"),
        first_transactionHash = paste0("stopped calling before reaching target_date because reached max_getbulkdata_calls"),
        last_transactionHash = paste0("stopped calling before reaching target_date because reached max_getbulkdata_calls"),
        last_blockTimestamp = paste0("stopped calling before reaching target_date because reached max_getbulkdata_calls")
      )
      
      bulk_tx_data_js$error_log <- bulk_tx_data_js$error_log %>% rbind(this_errorlog)
      
      
    }
    
    i <- i+1
    
    
  }
  
  return(bulk_tx_data_js)
  
}

gsub.remove.quotes <- function(x){
  
  if (is.character(x))
    return(gsub('"$','',gsub('^"','',x)))
  else
    return(x)
}

readcsv_clean <- function(fpath){
  
  # fpath <- paste0(this_network, "/tx_data.csv")
  
  res <- read.csv(fpath, stringsAsFactors=F, quote="")
  names(res) <- names(read.csv(fpath))
  res <- data.frame(lapply(res,gsub.remove.quotes),stringsAsFactors = F) %>% as.data.table()
  
  return(res)
  
}

get_networks_last_timestamp_update <- function(networks){
  
  # networks = c(
  #   'ethereum-mainnet', 'avalanche-mainnet', 'binance_smart_chain-mainnet',
  #   'ethereum-mainnet-arbitrum-1', 'ethereum-mainnet-optimism-1',
  #   'ethereum-mainnet-base-1', 'polygon-mainnet', 'wemix-mainnet',
  #   'ethereum-mainnet-kroma-1'
  # )
  
  # init
  res <- NULL
  timestamp_last_tx_data <- NULL
  
  
  for (i in c(1:(length(networks)))){
    # i=1
    
    # init
    this_timestamp_last_tx_data <- NA
    
    # SourceNetwork
    this_network <- networks[i]
    
    # tx data exists
    tx_data_exists <- file.exists(paste0(this_network, "/tx_data.csv"))
    
    # get date
    if(tx_data_exists){
      this_timestamp_last_tx_data <- read.csv(paste0(this_network, "/tx_data.csv"))$blockTimestamp[1]
    }
    
    timestamp_last_tx_data[i] <- this_timestamp_last_tx_data
    
  }
  
  res$network <- networks
  res$timestamp_last_tx_data <- timestamp_last_tx_data
  
  return(res)
  
}


# functions - get_data ------------------------------------------------------



# get_tx_data_networks_targetdates <- function(networks, targetdates){
  

# params
networks = c(
  'ethereum-mainnet', 'avalanche-mainnet', 'binance_smart_chain-mainnet',
  'ethereum-mainnet-arbitrum-1', 'ethereum-mainnet-optimism-1',
  'ethereum-mainnet-base-1', 'polygon-mainnet', 'wemix-mainnet',
  'ethereum-mainnet-kroma-1'
)
targetdates <- as.Date(str_sub(get_networks_last_timestamp_update(networks)$timestamp_last_tx_data, 1, 10))


# check params
assert_that(length(networks) == length(targetdates), msg = paste0("networks vector and targetdates vector should be same length"))
assert_that(is.Date(targetdates), msg = paste0("targetdates vector should only contain dates"))

# max_getbulkdata_calls
max_getbulkdata_calls <- Inf # in case something goes wrong with the while loop, will stop calling get_sourceNetwork_bulk_tx_data_js

# params get_sourceNetwork_bulk_tx_data_js
offset_start <- 0 #14.1e3 # if you want to start with a specific offset
n_batches <- 10 # number of successful iterations of get_tx_data_js
max_tries <- 3 # number of tries of the same get_tx_data_js before giving up
batch_size <- 100
timeout <- 10e3


# loop on networks
for (i in c(1:(length(networks)))){
# i=1

  # SourceNetwork
  this_network <- networks[i]
  # this_network <- "lala"
  
  # Target date
  target_date <- as.Date(targetdates[i])
  if(is.na(target_date)){target_date <-  as.Date("2023-06-01")} # initial date when CCIP was created
  
  # get tx_data for source 
  this_network_tx_data <- get_targetDate_sourceNetwork_bulk_tx_data_js(target_date, max_getbulkdata_calls, this_network, offset_start, n_batches, max_tries, batch_size, timeout)
  
  # create output dir
  dir.create(file.path(this_network))
  
  # check if there's already data in the output dir
  call_success <- !is.null(this_network_tx_data$tx_data)
  tx_data_exists <- file.exists(paste0(this_network, "/tx_data.csv"))
  tx_data_tokenAmounts_exists <- file.exists(paste0(this_network, "/tx_data.csv"))
  error_log_exists <- file.exists(paste0(this_network, "/error_log.csv"))
  
  if(call_success){
  
    # output tx_data
    if(tx_data_exists){
      
      # get existing tx_data and append new data
      old_tx_data <- readcsv_clean(paste0(this_network, "/tx_data.csv"))
      if(nrow(old_tx_data) == 0){old_tx_data <- NULL}
      new_tx_data <- this_network_tx_data$tx_data
      
      # merge old and new data
      tx_data <- new_tx_data %>% rbind(old_tx_data) %>% as.data.table()
      
      # get rid of duplicates (based on transactionhash)
      tx_data <- tx_data %>% mutate(across(where(is.list), as.character))
      tx_data <- tx_data %>% mutate(across(where(is.numeric), as.character))
      unicity_cols <- tx_data # tx_data[, .(transactionHash, destTransactionHash, nonce)]
      tx_data <- tx_data[!duplicated(unicity_cols), ]
      
      
      # write output
      # lapply(tx_data, class)
      write.csv(tx_data, paste0(this_network, "/tx_data.csv"), row.names = F)
      
      # console msg
      print(paste0("NETWORK NAME: ", this_network))
      print(paste0("successfully updated transaction data"))
      print(paste0("number of transactions added: ", nrow(tx_data) - nrow(old_tx_data)))
      print(paste0("from: ", old_tx_data$blockTimestamp[1], " to: ", tx_data$blockTimestamp[1]))
  
      
    }else{
      
      tx_data <- this_network_tx_data$tx_data
      tx_data <- tx_data %>% mutate(across(where(is.list), as.character))
      tx_data <- tx_data %>% mutate(across(where(is.numeric), as.character))
      unicity_cols <- tx_data # tx_data[, .(transactionHash, destTransactionHash, nonce)]
      tx_data <- tx_data[!duplicated(unicity_cols), ]
      write.csv(tx_data, paste0(this_network, "/tx_data.csv"), row.names = F)
      
    }
    
    # output tx_data_tokenAmounts
    if(tx_data_tokenAmounts_exists){
      
      # get existing tx_data_tokenAmounts and append new data
      old_tx_data_tokenAmounts <- readcsv_clean(paste0(this_network, "/tx_data_tokenAmounts.csv"))
      if(nrow(old_tx_data_tokenAmounts) == 0){old_tx_data_tokenAmounts <- NULL}
      new_tx_data_tokenAmounts <- this_network_tx_data$tx_data_tokenAmounts
      
      # merge old and new data
      tx_data_tokenAmounts <- new_tx_data_tokenAmounts %>% rbind(old_tx_data_tokenAmounts) %>% as.data.table()
      
      # get rid of duplicates (based on transactionhash)
      tx_data_tokenAmounts <- tx_data_tokenAmounts %>% mutate(across(where(is.list), as.character))
      tx_data_tokenAmounts <- tx_data_tokenAmounts %>% mutate(across(where(is.numeric), as.character))
      unicity_cols <- tx_data_tokenAmounts # tx_data_tokenAmounts[, .(transactionHash, destTransactionHash, nonce)]
      tx_data_tokenAmounts <- tx_data_tokenAmounts[!duplicated(unicity_cols), ]
      
      # write output
      # lapply(tx_data_tokenAmounts, class)
      write.csv(tx_data_tokenAmounts, paste0(this_network, "/tx_data_tokenAmounts.csv"), row.names = F)
      
      
    }else{
      
      tx_data_tokenAmounts <- this_network_tx_data$tx_data_tokenAmounts
      tx_data_tokenAmounts <- tx_data_tokenAmounts %>% mutate(across(where(is.list), as.character))
      tx_data_tokenAmounts <- tx_data_tokenAmounts %>% mutate(across(where(is.numeric), as.character))
      unicity_cols <- tx_data_tokenAmounts # tx_data_tokenAmounts[, .(transactionHash, destTransactionHash, nonce)]
      tx_data_tokenAmounts <- tx_data_tokenAmounts[!duplicated(unicity_cols), ]
      write.csv(tx_data_tokenAmounts, paste0(this_network, "/tx_data_tokenAmounts.csv"), row.names = F)
      
    }
    
  }
  
  # output error log (error log is replaced everytime)
  error_log <- this_network_tx_data$error_log
  error_log <- error_log %>% mutate(across(where(is.list), as.character))
  error_log <- error_log %>% mutate(across(where(is.numeric), as.character))
  write.csv(error_log, paste0(this_network, "/error_log.csv"), row.names = F, quote = F)

}


# }


# test --------------------------------------------------------------------

# # SourceNetwork
# sourceNetwork <- "ethereum-mainnet"
# 
# # params target date
# target_date <- as.Date("2023-06-22") # will get tx_data from offset_start to this target date
# max_getbulkdata_calls <- Inf # in case something goes wrong with the while loop, will stop calling get_sourceNetwork_bulk_tx_data_js
# 
# # params get_sourceNetwork_bulk_tx_data_js
# offset_start <- 0 #14.1e3 # if you want to start with a specific offset
# n_batches <- 10 # number of successful iterations of get_tx_data_js
# max_tries <- 3 # number of tries of the same get_tx_data_js before giving up
# batch_size <- 100
# timeout <- 10e3
# 
# 
# all_ethereum_mainnet_tx <- get_targetDate_sourceNetwork_bulk_tx_data_js(target_date, max_getbulkdata_calls, sourceNetwork, offset_start, n_batches, max_tries, batch_size, timeout)
# 
# 
# test <- all_ethereum_mainnet_tx$tx_data
# 
# 
# 
# last_tx_data <- read.csv("ethereum-mainnet/tx_data.csv")$blockTimestamp[1]
# 
# tx_data[transactionHash == "0x08d17f8da48284621e8bbb2188bf197c18d5ad810ceb882112553d5f03c949ed",]
# 
# 
# test <- data.table(var1 = c(1,1,3,4,4,4), var2 = c(1, 1, 1, 4, 4, 4), var3 = c("la", "lo", "li", "lu", "le", "le"))
# unicity_cols <- test #test[, .(var1, var2)]
# test[!duplicated(unicity_cols),]
# 
# 
# 
# 
# 
# # get rid of duplicates (based on transactionhash)
# old_tx_data <- old_tx_data %>% mutate(across(where(is.list), as.character))
# old_tx_data <- old_tx_data %>% mutate(across(where(is.numeric), as.character))
# # unicity_cols <- old_tx_data[, .(transactionHash, destTransactionHash, nonce)]
# # old_tx_data <- old_tx_data[!duplicated(unicity_cols), ]
# 
# 
# # write output
# # lapply(tx_data, class)
# write.csv(old_tx_data, paste0(this_network, "/old_tx_data.csv"), row.names = F)
# 
# 
# 
# 
# 
# 
# # oldest_timestamp_for_ethereum-mainnet <- "2023-06-22T19:15:59" 
# # n_batches = 14
# 
# # oldest_timestamp_for_avalanche-mainnet <- "2023-07-07T00:06:34"
# # n_batches = 8
# 
# # oldest_timestamp_for_ethereum-mainnet-arbitrum-1 <- "2023-09-17T00:15:56"
# # n_batches = 18
# 
# # oldest_timestamp_for_ethereum-mainnet-optimism-1 <- "2023-07-07T19:10:53"
# # n_batches = 18
# 
# # get_bulk_tx_data_js(
# #   condition = "",
# #   offset_start = 0,
# #   n_batches = 3,
# #   max_tries = 3,
# #   batch_size = 1000,
# #   timeout = 10e3
# # )
# 
# # mainnetNetworks = [
# #   'ethereum-mainnet', 'avalanche-mainnet', 'binance_smart_chain-mainnet',
# #   'ethereum-mainnet-arbitrum-1', 'ethereum-mainnet-optimism-1', 
# #   'ethereum-mainnet-base-1', 'polygon-mainnet', 'wemix-mainnet',
# #   'ethereum-mainnet-kroma-1'
# # ]
# 
# # testnetNetworks = ['polygon-testnet-mumbai', 'ethereum-testnet-sepolia', 'avalanche-testnet-fuji', 'ethereum-testnet-sepolia-arbitrum-1', 'binance_smart_chain-testnet', 
# #                    'wemix-testnet', 'ethereum-testnet-sepolia-kroma-1', 'ethereum-testnet-sepolia-optimism-1', 'ethereum-testnet-sepolia-base-1']
# 
# 
# 
# # tx_data_js <- get_tx_data_js(batch_size = 5, offset = 0, condition = "", this_timeout = 10e3)
# # tx_data_js_to_dataframe(tx_data_js$data)
# # bulk_tx_data_js <- get_sourceNetwork_bulk_tx_data_js(
# #   sourceNetwork = network,
# #   offset_start = 0,
# #   n_batches = 1,
# #   max_tries = 3,
# #   batch_size = 5,
# #   timeout = 10e3)
# # 
# # network <- "ethereum-mainnet"
# # this_condition <- paste0("%22sourceNetworkName%22%3A%22", network, "%22")
# # tx_data_js <- get_tx_data_js(batch_size = 100, offset = 100, condition = this_condition, this_timeout = 10e3)
# # tx_data_js_to_dataframe(tx_data_js$data)
# # tx_data_js2 <- get_tx_data_js(batch_size = 100, offset = (i-1)*batch_size + offset_start, condition, timeout)
# # tx_data_js_to_dataframe(tx_data_js2$data)
# # 
# # tx_data_js_to_dataframe(tx_data_js$data)
# # 
# # all_ethereum_mainnet_tx <- get_sourceNetwork_bulk_tx_data_js(
# #   sourceNetwork = "ethereum-mainnet",
# #   offset_start = 0,
# #   n_batches = 14,
# #   max_tries = 3,
# #   batch_size = 100,
# #   timeout = 10e3
# # )
# # 
# # 
# # all_avalanche_mainnet_tx <- get_sourceNetwork_bulk_tx_data_js(
# #   sourceNetwork = 'avalanche-mainnet',
# #   offset_start = 0,
# #   n_batches = 8,
# #   max_tries = 3,
# #   batch_size = 1000,
# #   timeout = 10e3
# # )
# # 
# # 
# # all_ethereum_mainnet_arbitrum_1_tx <- get_sourceNetwork_bulk_tx_data_js(
# #   sourceNetwork = "ethereum-mainnet-arbitrum-1",
# #   offset_start = 0,
# #   n_batches = 18,
# #   max_tries = 3,
# #   batch_size = 1000,
# #   timeout = 10e3
# # )
# # 
# # 
# # all_ethereum_mainnet_optimism_1_tx <- get_sourceNetwork_bulk_tx_data_js(
# #   sourceNetwork = "ethereum-mainnet-optimism-1",
# #   offset_start = 0,
# #   n_batches = 18,
# #   max_tries = 3,
# #   batch_size = 1000,
# #   timeout = 10e3
# # )
# # 
# # all_ethereum_mainnet_optimism_1_tx$error_log
# # all_ethereum_mainnet_optimism_1_tx$tx_data
# # 
# # all_ethereum_mainnet_base_1_tx <- get_sourceNetwork_bulk_tx_data_js(
# #   sourceNetwork = "ethereum-mainnet-base-1",
# #   offset_start = 18000,
# #   n_batches = 1,
# #   max_tries = 3,
# #   batch_size = 1000,
# #   timeout = 10e3
# # )
# 
# 
# 
# # output res --------------------------------------------------------------
# # write.csv(head(apply(tx_data,2,as.character), 100), "tx_data_sample.csv",row.names = F, quote = F)
# # write.csv(head(tx_data_tokenAmounts, 100), "tx_data_tokenAmounts_sample.csv",row.names = F, quote = F)
# 
# 
# 
# 
# "https://ccip.chain.link/api/query/LATEST_TRANSACTIONS_QUERY?variables=%7B%22first%22%3A5%2C%22offset%22%3A0%2C%22condition%22%3A%7B%7D%7D"
# "https://ccip.chain.link/api/query/LATEST_TRANSACTIONS_QUERY?variables=%7B%22first%22%3A1000%2C%22offset%22%3A0%2C%22condition%22%3A%7B%7D%7D"
# 
# 
# 
# 
# 
# 
# 
# 
# 
# 
# 
# 
# 
# 
# 
# 
# 
# 
# 
# 
# 
# 
# 
# 
# 
# 











