source("Import_Modified_Channels_12092022.R")



########
asci_df <- dbGetQuery(con, "SELECT * FROM
                   sde.analysis_asci") %>%
  left_join(lustations_df %>% transmute(stationcode = stationid, masterid)) %>%
  filter(!is.na(result) & #replicate == 1 &
           metric == "ASCI" ) %>%
  pivot_wider(id_cols = c(masterid, sampledate, sampleid, replicate),
              names_prefix = "ASCI_", names_from = assemblage, values_from = result) 

asci_df2<-asci_df %>%
  # mutate(sampleid2 = paste(masterid, sampledate ,sep="_")) # Use this to join with CSCI and chem - add to both datasets
  group_by(masterid) %>%
  summarise(ASCI_H=max(ASCI_Hybrid, na.rm=T),
            ASCI_D=max(ASCI_Diatom, na.rm=T))


# create SQL for csci data pull
csci_sql <- paste0("SELECT stationcode,sampleid,sampledate,collectionmethodcode,fieldreplicate,count,
    pcnt_ambiguous_individuals, pcnt_ambiguous_taxa,e, mean_o,oovere,oovere_percentile,
     mmi, mmi_percentile,csci,csci_percentile FROM sde.analysis_csci_core")
# pull data, writing to tibble
analysis_csci_df <- tbl(con, sql(csci_sql)) %>% 
  as_tibble() 



# prep csci data for  analysis
# it is ok to have replicates for CSCI data, will be treated as unique sample events
csci_df <- analysis_csci_df %>% 
  inner_join(lustations_df %>% select(masterid,stationid), by = c("stationcode" = "stationid")) %>% #get masterid
  mutate(sampledate = as.Date(as.character(sampledate))) %>% 
  # select final required fields for functions
  select(stationcode,masterid,sampledate,collectionmethodcode,fieldreplicate,pcnt_ambiguous_individuals,pcnt_ambiguous_taxa,e, mean_o,oovere,oovere_percentile,
         mmi,mmi_percentile,csci,csci_percentile, count) %>%
  mutate(csci_qa_usable = count>=250 & pcnt_ambiguous_individuals <=50 & pcnt_ambiguous_taxa<=50) %>%
  filter(csci_qa_usable)


#Select the highest-scoring sample among same-day replicates

csci_df2<-csci_df %>%
  group_by(masterid) %>%
  summarise(CSCI=max(csci, na.rm=T)) %>%
  ungroup()
csci_df2 %>% filter(masterid=="907S00577")

chan_csci_asci<-lustations_df %>%
  select(masterid, 
         stationcode=stationid,
         latitude, longitude, huc, county, smcshed, smc_lu, comid) %>% unique() %>%
  inner_join(chan_df2 %>% select(stationcode, channeltype, class_do)) %>%
  left_join(csci_df2) %>%
  left_join(asci_df2) 

hard_good_csci<-chan_csci_asci %>%
  filter(class_do=="Hard bottom") %>%
  # filter(CSCI >= 0.79 | ASCI_H>=0.86 | ASCI_D >= 0.86) %>%
  mutate(csci_p = CSCI >= 0.79,
         asci_h_p = ASCI_H>=0.86,
         asci_d_p = ASCI_D>=0.86) %>%
  filter(csci_p) %>% select(masterid)

hard_good_asci_h<-chan_csci_asci %>%
  filter(class_do=="Hard bottom") %>%
  # filter(CSCI >= 0.79 | ASCI_H>=0.86 | ASCI_D >= 0.86) %>%
  mutate(csci_p = CSCI >= 0.79,
         asci_h_p = ASCI_H>=0.86,
         asci_d_p = ASCI_D>=0.86) %>%
  filter(asci_h_p) %>% select(masterid)

hard_good_asci_d<-chan_csci_asci %>%
  filter(class_do=="Hard bottom") %>%
  # filter(CSCI >= 0.79 | ASCI_H>=0.86 | ASCI_D >= 0.86) %>%
  mutate(csci_p = CSCI >= 0.79,
         asci_h_p = ASCI_H>=0.86,
         asci_d_p = ASCI_D>=0.86) %>%
  filter(asci_d_p) %>% select(masterid)

gen_data<-lustations_df %>%
  select(masterid, stationcode=stationid, stationname, latitude, longitude, huc, smcshed, smc_lu, comid) %>% unique() %>%
  inner_join(chan_df2 %>% select(stationcode, class_do)) %>%
  left_join(csci_df %>%
              select(stationcode, sampledate,  replicate=fieldreplicate, csci,count,pcnt_ambiguous_individuals, pcnt_ambiguous_taxa)) %>%
  left_join(asci_df %>% select(masterid, sampledate, replicate, ASCI_D=ASCI_Diatom, ASCI_H=ASCI_Hybrid))

gen_data_csci<-gen_data %>%
  filter(masterid %in% hard_good_csci$masterid)

gen_data_asci_d<-gen_data %>%
  filter(masterid %in% hard_good_asci_d$masterid)

gen_data_asci_h<-gen_data %>%
  filter(masterid %in% hard_good_asci_h$masterid)


write_csv(gen_data_csci, "Data/HighScoringModChannels/hard_good_csci.csv")
write_csv(gen_data_asci_d, "Data/HighScoringModChannels/hard_good_asci_d.csv")
write_csv(gen_data_asci_h, "Data/HighScoringModChannels/hard_good_asci_h.csv")
