rm(list = ls())
library(httr)
library(data.table)
library(ggplot2)
library(anytime)
library(dplyr)
library(stringr)
library(xml2)



# functions ---------------------------------------------

# get tx_data batch
get_tx_data_js <- function(batch_size, offset, condition, this_timeout){
  
  # # params
  # batch_size <- 1000
  # offset <- 0
  # condition <- ""
  # timeout <- 10e3
  
  # build request
  request <- paste0("https://ccip.chain.link/api/query/LATEST_PUBLIC_TRANSACTIONS_QUERY?variables=",
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
  allCcipPublicTransactionsFlats <- tx_data[[1]][[1]]
  names(allCcipPublicTransactionsFlats)
  nodes <- allCcipPublicTransactionsFlats[[1]]
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
    this_node_token_amounts <- node_1[["tokenAmounts"]]
    tx_tokenAmounts <- data.table("token" = sapply(this_node_token_amounts,'[[',"token"),
                                  "amount" = sapply(this_node_token_amounts,'[[',"amount"))
    tx_tokenAmounts <- tx_tokenAmounts %>% mutate(transactionHash = this_transactionHash, .before = "token")
    
    tx_data_tokenAmounts <- tx_data_tokenAmounts %>% rbind(tx_tokenAmounts)
    
  }
  
  
  return(list(tx_data = tx_data, tx_data_tokenAmounts = tx_data_tokenAmounts))
  
  
}

get_bulk_tx_data_js <- function(condition, offset_start, n_batches, max_tries, batch_size, timeout){
  
  # params
  # condition <- ""
  # offset_start <- 100 # if you want to start with a specific offset
  # n_batches <- 3 # number of successful iterations of get_tx_data_js
  # max_tries <- 2 # number of tries of the same get_tx_data_js before giving up 
  # batch_size <- 1000
  # timeout <- 10e3
  
  # init
  tx_data_agg <- NULL
  tx_data_tokenAmounts_agg <- NULL
  error_log <- NULL
  # network <- "ethereum-mainnet"
  # condition <- paste0("%22sourceNetworkName%22%3A%22", network, "%22")
  
  # loop on batches
  for(i in c(1:n_batches)){
    
    this_batch_num <- i
    try_count <- 1
    print(paste0("------------------------------------------------------------------------------"))
    
    # loop on tries to get data
    while (try_count <= max_tries){
      
      # get tx_data_js
      print(paste0("calling ccip explorer api for batch number: ", this_batch_num))
      tx_data_js <- get_tx_data_js(batch_size, offset = (i-1)*batch_size + offset_start, condition, timeout)
      tx_data_js_error_status <- tx_data_js$status_code
      if(tx_data_js_error_status == 200){ 
         if(length(content(tx_data_js$data)$data$allCcipPublicTransactionsFlats$nodes) == 0){tx_data_js_error_status <- "no_data"}
      }
      
      # if success
      if(tx_data_js_error_status == 200){
        
        print(paste0("call success for batch number: ", this_batch_num))
        
        this_tx_data <- tx_data_js_to_dataframe(tx_data_js$data)
        this_tx_data$tx_data <- this_tx_data$tx_data %>% mutate(batch_num = this_batch_num, .before = "transactionHash")
        tx_data_agg <- tx_data_agg %>% rbind(this_tx_data$tx_data)
        tx_data_tokenAmounts_agg <- tx_data_tokenAmounts_agg %>% rbind(this_tx_data$tx_data_tokenAmounts)
        
        first_blockTimestamp = this_tx_data$tx_data[nrow(this_tx_data$tx_data),]$blockTimestamp
        first_transactionHash = this_tx_data$tx_data[nrow(this_tx_data$tx_data),]$transactionHash
        last_transactionHash = this_tx_data$tx_data[1,]$transactionHash
        last_blockTimestamp = this_tx_data$tx_data[1,]$blockTimestamp
        
        
        this_errorlog <- data.table(
          this_batch_num,
          first_blockTimestamp,
          first_transactionHash,
          last_transactionHash,
          last_blockTimestamp
        )
        
        error_log <- error_log %>% rbind(this_errorlog)
        
        print(paste0("ccip tx data collected for batch number: ", this_batch_num))
        print(paste0("batch number ", this_batch_num, " first_blockTimestamp: ", first_blockTimestamp))
        print(paste0("batch number ", this_batch_num, " last_blockTimestamp: ", last_blockTimestamp))
        
        
        try_count <- max_tries + 1
        
      }else{
        
        print(paste0("call failed for batch number: ", this_batch_num, " - error status: ", tx_data_js_error_status," - try ", try_count))
        
        if(try_count == max_tries){
          this_errorlog <- data.table(
            this_batch_num,
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



# test --------------------------------------------------------------------

# oldest_timestamp_for_ethereum-mainnet <- "2023-07-06T22:34:59" 
# n_batches = 14

# oldest_timestamp_for_avalanche-mainnet <- "2023-07-07T00:06:34"
# n_batches = 8

# oldest_timestamp_for_ethereum-mainnet-arbitrum-1 <- "2023-09-17T00:15:56"
# n_batches = 18

# oldest_timestamp_for_ethereum-mainnet-optimism-1 <- "2023-07-07T19:10:53"
# n_batches = 18

# get_bulk_tx_data_js(
#   condition = "",
#   offset_start = 0,
#   n_batches = 3,
#   max_tries = 3,
#   batch_size = 1000,
#   timeout = 10e3
# )

# mainnetNetworks = [
#   'ethereum-mainnet', 'avalanche-mainnet', 'binance_smart_chain-mainnet',
#   'ethereum-mainnet-arbitrum-1', 'ethereum-mainnet-optimism-1', 
#   'ethereum-mainnet-base-1', 'polygon-mainnet', 'wemix-mainnet',
#   'ethereum-mainnet-kroma-1'
# ]

# testnetNetworks = ['polygon-testnet-mumbai', 'ethereum-testnet-sepolia', 'avalanche-testnet-fuji', 'ethereum-testnet-sepolia-arbitrum-1', 'binance_smart_chain-testnet', 
#                    'wemix-testnet', 'ethereum-testnet-sepolia-kroma-1', 'ethereum-testnet-sepolia-optimism-1', 'ethereum-testnet-sepolia-base-1']


# get_tx_data_js(batch_size = 5, offset = 50000, condition = "", this_timeout = 10e3)

all_ethereum_mainnet_tx <- get_sourceNetwork_bulk_tx_data_js(
  sourceNetwork = "ethereum-mainnet",
  offset_start = 0,
  n_batches = 14,
  max_tries = 3,
  batch_size = 1000,
  timeout = 10e3
)


all_avalanche_mainnet_tx <- get_sourceNetwork_bulk_tx_data_js(
  sourceNetwork = 'avalanche-mainnet',
  offset_start = 0,
  n_batches = 8,
  max_tries = 3,
  batch_size = 1000,
  timeout = 10e3
)


all_ethereum_mainnet_arbitrum_1_tx <- get_sourceNetwork_bulk_tx_data_js(
  sourceNetwork = "ethereum-mainnet-arbitrum-1",
  offset_start = 0,
  n_batches = 18,
  max_tries = 3,
  batch_size = 1000,
  timeout = 10e3
)


all_ethereum_mainnet_optimism_1_tx <- get_sourceNetwork_bulk_tx_data_js(
  sourceNetwork = "ethereum-mainnet-optimism-1",
  offset_start = 0,
  n_batches = 18,
  max_tries = 3,
  batch_size = 1000,
  timeout = 10e3
)

all_ethereum_mainnet_optimism_1_tx$error_log
all_ethereum_mainnet_optimism_1_tx$tx_data


# output res --------------------------------------------------------------
# write.csv(head(apply(tx_data,2,as.character), 100), "tx_data_sample.csv",row.names = F, quote = F)
# write.csv(head(tx_data_tokenAmounts, 100), "tx_data_tokenAmounts_sample.csv",row.names = F, quote = F)











































