
#' Authenticates against an Oracle Object Store
#'
#' @param id identify domain to be connected to,
#' @param username Oracle Object Store username
#' @param password Oracle Object Store pawword for username
#' @return returns a list with 5 elements.  This object should be passed to other functions.
#' auth_token which contains the authorization token needed for other commands
#' url - the full url command used for curl commands
#' user_id - the well formed user id of the form storage domain:user
#' identity_domain - Identity domain used to authenticate
#' auth_url - url in the form used for authentication
#' @import httr
#' @examples
#' #my_credentials <- oos_authenticate("a9999999","brian@oracle.com","1234")
#' @export

oos_authenticate <- function(id,username,password) {
#  library(httr)
  #Build required urls and user id
  storage_name <- paste("Storage-",id,sep="")

  #storage cloud authentication URL of the form "https://identitydomain.storage.cloud.com"  replace identitydomain with your identty domain i.e.  a433433
  # do not uinclude /auth/v1.0 do not include forward slash
  url <-   paste("https://",id,".storage.oraclecloud.com",sep="")
  user_id <- paste(storage_name,":",username,sep="")
  auth_url <- paste(url,"/auth/v1.0",sep="")
  fetch_url <- paste(url,"/v1/",storage_name,sep="")

  #Get authentication token
  auth_string <- httr::GET(url = auth_url, add_headers("X-Storage-User" = user_id, "X-Storage-Pass" = password)   , verbose())
  list(auth_token=auth_string$headers$`x-auth-token`,url=fetch_url,user_id=user_id,identity_domain=id,auth_url=auth_url)
}

#' Downloads a file as a stream from the Oracle Object store.  Used to load files into a dataframe
#'
#' @param credentials List Object returned from a call to oos_autheticate
#' @param container Full container name where a file resides.
#' @param file_name The file name to download
#' @return returns the contents of the file as a result of a read.csv.  ONly tested with csvs.
#'
#' @examples
#' #my_data <- oos_get_file(my_credentials,"sales" ,"data.csv")
#' #my_data2 <- oos_get_file(my_credentials,"sales/2004" ,"data.csv")
#' @export
#'
oos_get_file <- function(credentials,container,file_name) {
  fetch_url <- paste(credentials$url,"/",container,"/",file_name,sep="")
  remote_file <- content(httr::GET(url = fetch_url, add_headers ( "X-Auth-Token" = credentials$auth_token)), as="text")
  data <- read.csv(file = textConnection(remote_file))
  data
}

#' Lists the contents of a container and all sub containers.
#'
#' @param credentials List Object returned from a call to oos_autheticate
#' @param container Full container name to show contents.
#' @return reurns a dataframe which contains the container path and file name, the size in bytes of the file and when it was last modified.
#' @examples
#' #oos_ls(my_credentials,"sales" )
#' #oos_ls(my_credentials,"sales/2004" )
#' @export
#'
oos_ls <- function(credentials,container) {
  fetch_url <- paste(credentials$url,"/",container,sep="")
  remote_file <- content(httr::GET(url = fetch_url, add_headers ( "X-Auth-Token" = credentials$auth_token)), as="parsed")
  remote_file <- lapply(remote_file,'[',c('name','bytes','last_modified'))
  remote_file <- do.call(rbind.data.frame, remote_file)
  remote_file
}

#' Creates a new container in the Oracle object Store
#'
#' @param credentials List Object returned from a call to oos_autheticate
#' @param container Full container name to be added.  If container is part of another container, include the full path.
#' @return returns any errors.  NULL means success.
#' @examples
#' #oos_mkdir(my_credentials,'sales' )
#' @export
oos_mkdir <- function(credentials,container) {
  fetch_url <- paste(credentials$url,"/",container,sep="")
  create_response <- content(httr::PUT(url = fetch_url, add_headers ( "X-Auth-Token" = credentials$auth_token)), as="parsed")
  create_response
}

#' Uploads a single file to the object store
#'
#' @param credentials List Object returned from a call to oos_autheticate
#' @param container Full container name where the file should be uploaded.
#' @param directory Local filesystem directory where the file resides
#' @param file_name the name of the file to be uploaded.
#' @return returns any errors.  NULL means success.
#' @examples
#' #oos_upload_file(my_credentials,"sales","c://data/sales","sales.csv" )
#' @export
oos_upload_file <- function(credentials,container,directory,file_name) {
  temp_wd <- getwd()
  setwd(directory)
  fetch_url <- paste(credentials$url,"/",container,"/",file_name,sep="")
  create_response <- content(httr::PUT(url = fetch_url, add_headers ( "X-Auth-Token" = credentials$auth_token )))
  setwd(temp_wd)
  create_response
}
# copy a file from one container to another

# move a file from one container to another

#' removes a file from the Oracle Object Store.
#'
#' @param credentials List Object returned from a call to oos_autheticate
#' @param container Full container name where the file resides.
#' @param file_name the name of the file to be removed
#' @return returns any errors.  NULL means success.
#' @examples
#' #oos_rm(my_credentials,"sales","sales.csv" )
#' @export
oos_rm <- function(credentials,container,file_name) {
  fetch_url <- paste(credentials$url,"/",container,"/",file_name,sep="")
  create_response <- content(httr::DELETE(url = fetch_url, add_headers ( "X-Auth-Token" = credentials$auth_token )))
  create_response
}

