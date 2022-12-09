library(DBI) # needed to connect to data.dfbase
library(dbplyr) # needed to connect to data.dfbase
library(RPostgreSQL) # needed to connect to our data.dfbase
library(rstudioapi) # just so we can type the password as we run the script, so it is not written in the clear
library(tidyverse)
library(lubridate)

# con is short for connection
# # Create connection to the data.dfbase
con <- dbConnect(
  PostgreSQL(),
  host = "192.168.1.17",
  dbname = 'smc',
  user = 'smcread',
  password = '1969$Harbor' # if we post to github, we might want to do rstudioapi::askForPassword()
)

lustations_sql <- #paste0("SELECT stationid, masterid, stationname, latitude, longitude, huc, county, psa_lu, comid FROM sde.lu_stations")
  paste0("SELECT * FROM sde.lu_stations")
# pull data, writing to tibble
lustations_df <- tbl(con, sql(lustations_sql)) %>%
  as_tibble() %>% 
  # just in case, make sure no NA masterids
  filter(!is.na(masterid))

chan_df<-tbl(con, sql("SELECT * FROM sde.unified_channelengineering")) %>%
  as_tibble()   %>%
  select(stationcode, sampledate, evaluator, channeltype, bottom, bottomcomments, rightsideofstructure, rightsidecomments, leftsideofstructure, leftsidecomments) %>%
  inner_join(lustations_df %>% select(masterid, stationcode=stationid, smc_lu, latitude, longitude, huc))


chan_df2<-chan_df %>% 
  #First, change to lower-case to handle inconsistencies in capitalization
  mutate(bottom=tolower(bottom),
         leftsideofstructure=tolower(leftsideofstructure),
         rightsideofstructure=tolower(rightsideofstructure)) %>%
  mutate( bottom=case_when(is.na(bottom) ~ NA_character_,
                           bottom %in% c("na","nr") ~ NA_character_,
                           bottom %in% c("concrete","grouted rock","rock") ~ "Hard",
                           bottom %in% c("soft/natural") ~ "Soft",
                           stationcode %in% c("SMC18169") ~ "Soft", #based on comments, update in database
                           stationcode %in% c("412M08599","801M12652","SMC03048") ~ "Hard", #based on comments, update in database
                           T ~ bottom),
          leftsideofstructure=case_when(is.na(leftsideofstructure)~NA_character_,
                                        leftsideofstructure %in% c("nr","na")~NA_character_,
                                        leftsideofstructure %in% c("rock","grouted rock", "concrete")~"Hard",
                                        leftsideofstructure %in% c("earthen","earthen bare", "vegetative/natural")~"Soft",
                                        stationcode %in% c("SMC09091","SMC06926","801M12652","404M07365")~"Hard", #based on comments, update in database
                                        T~"leftsideofstructure"),
          
          rightsideofstructure=case_when(is.na(rightsideofstructure)~NA_character_,
                                         rightsideofstructure %in% c("nr","na")~NA_character_,
                                         rightsideofstructure %in% c("rock","grouted rock", "concrete")~"Hard",
                                         rightsideofstructure %in% c("earthen","earthen bare", "vegetative/natural")~"Soft",
                                         stationcode %in% c("SMC09091","SMC06926","801M12652","404M07365")~"Hard", #based on comments, update in database
                                         stationcode %in% c("SMC04308")~"Soft", #this is natural bedrock.
                                         T~"rightsideofstructure"),
          class_do=case_when(channeltype =="Natural"~"Natural",
                             bottom =="Hard"~"Hard bottom",
                             bottom !="Hard" & leftsideofstructure!="Hard" & rightsideofstructure!="Hard"~"Soft bottom-0 hard sides",
                             bottom !="Hard" & leftsideofstructure=="Hard" & rightsideofstructure!="Hard"~"Soft bottom-1 hard side",
                             bottom !="Hard" & leftsideofstructure!="Hard" & rightsideofstructure=="Hard"~"Soft bottom-1 hard side",
                             bottom !="Hard" & leftsideofstructure=="Hard" & rightsideofstructure=="Hard"~"Soft bottom-2 hard sides",
                             T~"Misclassified"), #Should be no misclassifications
          #Correct misclassification of SMC01881
          channeltype=case_when(stationcode == "SMC01881"~"Natural",T~channeltype), #Based on GE inspection. Consult with field crew and update in database
          bottom=case_when(stationcode == "SMC01881"~NA_character_,T~bottom), #Based on GE inspection. Consult with field crew and update in database
          leftsideofstructure=case_when(stationcode == "SMC01881"~NA_character_,T~leftsideofstructure), #Based on GE inspection. Consult with field crew and update in database
          rightsideofstructure=case_when(stationcode == "SMC01881"~NA_character_,T~rightsideofstructure), #Based on GE inspection. Consult with field crew and update in database
          class_do=case_when(stationcode == "SMC01881"~"Natural",T~class_do) #Based on GE inspection. Consult with field crew and update in database
  ) %>%
  #Some sites have multiple, inconsistent classifications. For now, assume that the most severely altered class applies. But consult with field crews and update database
  mutate(class_do_f = case_when(class_do == "Natural"~0,
                                class_do == "Soft bottom-0 hard sides"~1,
                                class_do == "Soft bottom-1 hard side"~2,
                                class_do == "Soft bottom-2 hard sides"~3,
                                class_do == "Hard bottom"~4)) %>%
  group_by(masterid) %>% 
  slice_max(class_do_f,n=1, with_ties = F) %>% 
  ungroup()

write_csv(chan_df2,"Data/chan_df2.csv")
