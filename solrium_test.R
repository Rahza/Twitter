####################
# INSTALL PACKAGES #
####################

install.packages("dplyr")
install.packages("solrium")


#################
# USE LIBRARIES #
#################

library(dplyr)
library(solrium)


##########
# SCRIPT #
##########

solr_connect("http://localhost:8983")
df <- data.frame(content = c("blablabla", "bliblob"), date = c("2016-06-03T14:12:32Z", "2016-06-03T14:12:32Z"), fav_count = c(2, 3), hashtag_count = c(1, 4), id = c("143613461177", "1241641346"), link_count = c(1, 0), mention_count = c(1, 3), rt_count = c(1, 4), user_name = c("Alex", "bobl"), user_screen_name = c("alex", "biblo"))

write.csv(df, file="C:\\Programming\\test.csv", row.names=FALSE)
update_csv("C:\\Programming\\test.csv", "twitter", commit = TRUE)

solr_connect("http://localhost:8983/solr/twitter/select")
solr_search(q = "*:*")
