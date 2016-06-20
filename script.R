##### NOTES
### linkCount, hashtagCount WRONG (for large values)
### Für Indexieren in Solr: Einfach na.omit?
### Event ID 150107 doppelt!

#############
# LIBRARIES #
#############

library(dplyr)
library(solrium)
library(solr)

detach("package:solrium")

###########
# HELPERS #
###########
View(tweets.content[tweets.content$tweetID == 622316523005308928,]) # View a single tweets
events.unique[events.unique$tweetID == 600756087797583872,]
View(events.unique[events.unique$ID == 150107,])

#############
# VORARBEIT #
#############

# Different event types
table(events.infos$special_key)

# Interaction types
View(table(events.unique$type))
View(table(events.unique$tweet.event.cat))

# All event types that are related to a specific tweet
View(table((events.unique %>% filter(tweetID != "NA"))$type))

####################################

# Get all search queries
search.queries = events.infos %>% filter(special_key == "query")
# Merge search queries with their corresponding event
queries.events = merge(search.queries, events.unique, by.x="eventID", by.y = "ID")

# Filter relevant columns
queries.events.filtered = queries.events[,c("ID", "special_value", "context", "timestamp", "sessionID", "pluginUserID")]
queries.events.filtered$type = "search"

# Get relevant data of dataframe
refinding.sessions = RF.EXP.DATA.COMPLETE[,c("sessionID", "pluginUserID", "reclick")]
# Set NA values to 0
refinding.sessions[is.na(refinding.sessions)] = 0

# Merge search queries with refinding table
queries.events.filtered = merge(queries.events.filtered, refinding.sessions, by=c("sessionID", "pluginUserID"))

# Get "event" queries
event.queries = events.unique %>% filter(type == "recentTweetAuthorClick" | type == "recentTweetClickOnHashtag" | type == "recentTweetMentionClick" | type == "rightClickonHashtag" | type == "rightClickonMention" | type == "TweetAuthorClick" | type == "TweetClickonHashtag" | type == "TweetMentionClick")

getAuthor = function(id) {
  tweet = tweets.content %>% filter(tweetID == id)
  return (tweet$tweetAuthorScreenname)
}

getHashtag = function(user, queryTime) {
  result = NA
  filtered = events.unique %>% filter(pluginUserID == user & timestamp > queryTime & context != "https://twitter.com/" & type != "scrollSummary") %>% arrange(timestamp)
  result = gsub("https://twitter.com/hashtag/([^/]+)\\?.*", "\\1", filtered[1,]$context)
  
  return (result)
}

profileViewComplete = read.csv("D:\\TwitterWork\\profileViewComplete.csv")

getMention = function(user, queryTime) {
  maxTime = as.numeric(queryTime) + 120
  filtered = events.unique %>% filter(pluginUserID == user & timestamp > queryTime & timestamp < maxTime) %>% arrange(timestamp)
  profileEvent = filtered[which(filtered$type == "profileSummaryViewed"),]
  
  if (nrow(profileEvent) == 0) {
    return ("NO PROFILE SUMMARY VIEWED")
  }
  
  mention = as.character(profileViewComplete[which(profileViewComplete$ID == profileEvent[1,]$ID),]$userScreenName)
  if (length(mention) == 0) {
    return ("NO MENTION FOUND")
  }
  
  filtered = filtered[which(filtered$type == "userPageVisit"),]
  
  if (nrow(filtered) > 0) {
    test = paste("@", gsub("https://twitter.com/([^/]+)/?.*", "\\1", filtered[1,]$context), sep="")
    if (test == mention) {
      return (mention)
    }
    else {
      error = paste("ERROR MATCHING", test, "-", mention)
      return (error)
    }
  }
  else {
    return ("NO USER PAGE VISIT")
  }
}

# getMention = function(id) {
#  tweet = tweets.content %>% filter(tweetID == id)
#  
#  return (tweet$mention_count)
# }

getSpecialValue = function(type, id, user, queryTime) {
  if (type == "TweetAuthorClick" | type == "recentTweetAuthorClick") return (getAuthor(id))
  else if (type == "TweetClickonHashtag" | type == "rightClickonHashtag") return (getHashtag(user, queryTime))
  else if (type == "TweetMentionClick" | type == "recentTweetMentionClick") return (getMention(user, queryTime))
}

# View(events.unique %>% filter(pluginUserID == 53 & timestamp >= 1431086552))
# View(tweets.content %>% filter(tweetID == 596550480790233088))

event.queries$special_value = apply(event.queries, 1, function(x) getSpecialValue(x["type"], x["tweetID"], x["pluginUserID"], x["timestamp"]))

# Filter relevant columns
event.queries.filtered = event.queries[,c("ID", "type", "special_value", "context", "timestamp", "sessionID", "pluginUserID")]

# Merge event queries with refinding table
event.queries.filtered = merge(event.queries.filtered, refinding.sessions, by=c("sessionID", "pluginUserID"))


getFirstHovered = function(session, userID, time) {
  filtered = events.unique %>% filter(sessionID == as.numeric(session) & pluginUserID == as.numeric(userID) & timestamp >= as.numeric(time) & type == "tweetHover")
  if (nrow(filtered) > 0) return (filtered[filtered$ID == min(filtered$ID),]$tweetID)
  else return (NA)
}

getFirstClick = function(session, userID, time) {
  filtered = events.unique %>% filter(sessionID == as.numeric(session) & pluginUserID == as.numeric(userID) & timestamp >= as.numeric(time) & type == "tweetClicked")
  if (nrow(filtered) > 0) return (filtered[filtered$ID == min(filtered$ID),]$tweetID)
  else return (NA)
}

getLastHovered = function(session, userID, time) {
  filtered = events.unique %>% filter(sessionID == as.numeric(session) & pluginUserID == as.numeric(userID) & timestamp >= as.numeric(time) & type == "tweetHover")
  if (nrow(filtered) > 0) return (filtered[filtered$ID == max(filtered$ID),]$tweetID)
  else return (NA)
}

getLastClick = function(session, userID, time) {
  filtered = events.unique %>% filter(sessionID == as.numeric(session) & pluginUserID == as.numeric(userID) & timestamp >= as.numeric(time) & type == "tweetClicked")
  if (nrow(filtered) > 0) return (filtered[filtered$ID == max(filtered$ID),]$tweetID)
  else return (NA)
}

queries = rbind(event.queries.filtered, queries.events.filtered)
queries = queries %>% filter(special_value != "NA")

queries$firstHover = apply(queries, 1, function(x) getFirstHovered(x["sessionID"], x["pluginUserID"], x["timestamp"]))
queries$firstClick = apply(queries, 1, function(x) getFirstClick(x["sessionID"], x["pluginUserID"], x["timestamp"]))
queries$lastHover = apply(queries, 1, function(x) getLastHovered(x["sessionID"], x["pluginUserID"], x["timestamp"]))
queries$lastClick = apply(queries, 1, function(x) getLastClick(x["sessionID"], x["pluginUserID"], x["timestamp"]))

timetable = queries
timetable$timestamp = as.numeric(timetable$timestamp)
timetable$firstHover = apply(timetable, 1, function(x) getTweetTime(x["firstHover"], x["timestamp"]))
timetable$firstClick = apply(timetable, 1, function(x) getTweetTime(x["firstClick"], x["timestamp"]))
timetable$lastHover = apply(timetable, 1, function(x) getTweetTime(x["lastHover"], x["timestamp"]))
timetable$lastClick = apply(timetable, 1, function(x) getTweetTime(x["lastClick"], x["timestamp"]))

plot(table(timetable$firstHover), type="l")
plot(table(timetable$firstClick), type="l")
plot(table(timetable$lastHover), type="l")
plot(table(timetable$lastClick), type="l")

getTweetTime = function(id, time) {
  if (is.na(id)) return (NA)
  
  tweet = tweets.unique %>% filter(tweetID == id)
  result = tweet$tweetTimeStamp
  
  if (length(result) == 0) return (NA)
  else {
    result = as.numeric(time) - as.numeric(result)
    result =  round(result / (60*60*24))
    return (result)
  }
}

queries = queries %>% filter(reclick == 1) # Only Refinding Queries

proxies = queries[,c("special_value", "firstHover", "firstClick", "lastHover", "lastClick", "timestamp")]


#########
# INDEX #
#########
all.tweets = tweets.unique[,c("tweetID", "tweetTimeStamp", "favoriteCount", "retweetCount")]
relevant.content = tweets.content[,c("tweetID", "tweetAuthorUsername", "tweetAuthorScreenname", "tweetText", "tweetAbsolutPath", "link_count", "hashtag_count")] # CHEK RT/FAV

duplicates = relevant.content %>% group_by(tweetID) %>% filter(n() >= 2) # duplicate entries

parseURL = function(url) {
  split = strsplit(url, "/")[[1]]
  return (split[length(split)])
}

duplicates$tweetID = apply(duplicates, 1, function(x) parseURL(x["tweetAbsolutPath"]))

relevant.content = relevant.content[!(relevant.content$tweetID %in% duplicates$tweetID),] # filter out all duplicates for now

tweets = merge(all.tweets, relevant.content, by.x="tweetID", by.y="tweetID") # merge both dataframes
Encoding(tweets$tweetText) = "UTF-8" # Change text encoding to UTF-8

tweets.filtered = na.omit(tweets)
write.csv(tweets.filtered, file="D:\\TwitterWork\\tweets.csv", row.names=FALSE)


########
# SOLR #
########
url = "http://localhost:8983/solr/twitter/select"

##### FILTER PROXIES TO PREVENT SOLR ERRORS #####
proxies$remove = apply(proxies, 1, function(x) length(grep("lang:", x["special_value"])))
proxies = proxies %>% filter (remove == 0)
proxies$remove = apply(proxies, 1, function(x) length(grep(")$", x["special_value"])))
proxies = proxies %>% filter (remove == 0)
proxies$remove = apply(proxies, 1, function(x) length(grep(":$", x["special_value"])))
proxies = proxies %>% filter (remove == 0)
proxies$remove = apply(proxies, 1, function(x) length(grep("]$", x["special_value"])))
proxies = proxies %>% filter (remove == 0)

proxies = proxies %>% filter(special_value != "NULL")
proxies = proxies[,c("special_value", "firstHover", "firstClick", "lastHover", "lastClick")]
#################################################

nrows = 1000

getPosition = function(query, nrows, target) {
  # Get the query proxies
  if (is.na(target)) return (NA)
  result = solr_search(q=query, rows=nrows, base=url)
  
  position = which(result$tweetID == target)
  
  if (length(position) == 0) return (-1)
  else return(position)
}

# INEFFECIENT!!! TODO: Each query once, then return vector of positions for each proxy
proxies$lastClickPosition = apply(proxies, 1, function(x) getPosition(x["special_value"], nrows, x["lastClick"])) #0.17
proxies$firstClickPosition = apply(proxies, 1, function(x) getPosition(x["special_value"], nrows, x["firstClick"])) #0.17
proxies$lastHoverPosition = apply(proxies, 1, function(x) getPosition(x["special_value"], nrows, x["lastHover"])) #0.19
proxies$firstHoverPosition = apply(proxies, 1, function(x) getPosition(x["special_value"], nrows, x["firstHover"])) #0.19

# EXPERIMENT CHRONOLOGICAL LIST
getChronoPosition = function(query, nrows, target, queryTime) {
  if (is.na(target)) return (NA)

  filtered = tweets.filtered %>% filter(tweetTimeStamp < queryTime) %>% arrange (tweetTimeStamp)
  position = which(filtered$tweetID == target)
  
  if (length(position) == 0) return (-1)
  else return(position)
}

proxies$lastClickPositionC = apply(proxies, 1, function(x) getChronoPosition(x["special_value"], nrows, x["lastClick"], x["timestamp"]))
proxies$firstClickPositionC = apply(proxies, 1, function(x) getChronoPosition(x["special_value"], nrows, x["firstClick"], x["timestamp"]))
proxies$lastHoverPositionC = apply(proxies, 1, function(x) getChronoPosition(x["special_value"], nrows, x["lastHover"], x["timestamp"]))
proxies$firstHoverPositionC = apply(proxies, 1, function(x) getChronoPosition(x["special_value"], nrows, x["firstHover"], x["timestamp"]))

proxies = proxies %>% filter(!is.na(firstHover) & !is.na(firstClick) & !is.na(lastHover) & !is.na(lastClick))

getMRR(proxies$lastClickPositionC, replace.value)
getMRR(proxies$firstClickPositionC, replace.value)
getMRR(proxies$lastHoverPositionC, replace.value)
getMRR(proxies$firstHoverPositionC, replace.value)

getMRR(proxies$lastClickPosition, replace.value)
getMRR(proxies$firstClickPosition, replace.value)
getMRR(proxies$lastHoverPosition, replace.value)
getMRR(proxies$firstHoverPosition, replace.value)
# EXPERIMENT END

getMRR = function(positions, replace.value) {
  positions = na.omit(positions)
  mrr = (1/length(positions)) * sum(1/positions[positions > 0])
  return (mrr)
}

# For now, replace positions that weren't found with a very high value
replace.value = 500

getMRR(proxies$lastClickPosition, replace.value)
getMRR(proxies$firstClickPosition, replace.value)
getMRR(proxies$lastHoverPosition, replace.value)
getMRR(proxies$firstHoverPosition, replace.value)


getMRR = function(queries, nrows, proxy) {
  positions = unlist(lapply(queries, function(x) getPosition(x, nrows, proxy)))
  mrr = (1/length(positions)) * sum(1/positions)
  
  return (mrr)
}

getMRR = function(positions, replace.value) {
  positions = na.omit(positions)
  # positions[positions == -1] = replace.value
  positions = positions[positions > 0]
  mrr = (1/length(positions)) * sum(1/positions)
  return (mrr)
}


proxies = proxies %>% mutate(hoverEqual = (firstHover == lastHover))
proxies = proxies %>% mutate(hoverEqual = (firstClick == lastClick))
table(proxies$hoverEqual)

View(proxies %>% filter(!is.na(firstHover) & !is.na(firstClick) & !is.na(lastHover) & !is.na(lastClick)))

