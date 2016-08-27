##### NOTES
### linkCount, hashtagCount WRONG (for large values)
### Für Indexieren in Solr: Einfach na.omit?
### Event ID 150107 doppelt!

#############
# LIBRARIES #
#############

library(dplyr)
library(solrium)
library(sentimentr)
library(stringr)
library(koRpus)

###########
# HELPERS #
###########
View(tweets.content[tweets.content$tweetID == 617898398541221888,]) # View a single tweets
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
search.queries = merge(search.queries, events.unique, by.x="eventID", by.y = "ID")

# Filter relevant columns
search.queries = search.queries[,c("ID", "special_value", "context", "timestamp", "sessionID", "pluginUserID")]
search.queries$type = "search"

# Get relevant data of dataframe
refinding.sessions = RF.EXP.DATA.COMPLETE[,c("sessionID", "pluginUserID", "reclick")]
# Set NA values to 0
refinding.sessions[is.na(refinding.sessions)] = 0

# Merge search queries with refinding table
search.queries = merge(search.queries, refinding.sessions, by=c("sessionID", "pluginUserID"))

# Get "event" queries
event.queries = events.unique %>% filter(type == "recentTweetAuthorClick" | type == "recentTweetClickOnHashtag" | type == "recentTweetMentionClick" | type == "rightClickonHashtag" | type == "rightClickonMention" | type == "TweetAuthorClick" | type == "TweetClickonHashtag" | type == "TweetMentionClick")


getAuthor = function(id) {
  tweet = tweets.content %>% filter(tweetID == id)
  return (tweet$tweetAuthorScreenname)
}

getHashtag = function(user, queryTime) {
  filtered = events.unique %>% filter(pluginUserID == user & timestamp > queryTime) %>% arrange(timestamp)
  first.hashtag = filtered[min(which(grepl("/hashtag/", filtered$context))),]
  result = paste("#", gsub("https://twitter.com/hashtag/([^/]+)\\?.*", "\\1", first.hashtag$context), sep="")
  
  return (result)
}

profileViewComplete = read.csv("D:\\TwitterWork\\profileViewComplete.csv")

getMention = function(user, queryTime) {
  maxTime = as.numeric(queryTime) + 120
  filtered = events.unique %>% filter(pluginUserID == user & timestamp > queryTime & timestamp < maxTime) %>% arrange(timestamp)
  profileEvent = filtered[which(filtered$type == "profileSummaryViewed"),]
  
  if (nrow(profileEvent) == 0) return ("NO PROFILE SUMMARY VIEWED")
  
  mention = as.character(profileViewComplete[which(profileViewComplete$ID == profileEvent[1,]$ID),]$userScreenName)
  if (length(mention) == 0) return ("NO MENTION FOUND")
  
  filtered = filtered[which(filtered$type == "userPageVisit"),]
  
  if (nrow(filtered) > 0) {
    test = paste("@", gsub("https://twitter.com/([^/]+)/?.*", "\\1", filtered[1,]$context), sep="")
    if (test == mention) return (mention)
    else return (paste("ERROR MATCHING", test, "-", mention))
  }
  else return ("NO USER PAGE VISIT")
}














getMentionTest = function(user, queryTime) {
  maxTime = as.numeric(queryTime) + 120
  filtered = events.unique %>% filter(pluginUserID == user & timestamp > queryTime & timestamp < maxTime) %>% arrange(timestamp)
  profileEvent = filtered[which(filtered$type == "profileSummaryViewed"),]
  
  if (nrow(profileEvent) == 0) return ("NO PROFILE SUMMARY VIEWED")

  filtered = filtered[which(filtered$type == "userPageVisit"),]
  
  if (nrow(filtered) > 0) return (paste("@", gsub("https://twitter.com/([^/]+)/?.*", "\\1", filtered[1,]$context), sep=""))
  
  return ("NO USER PAGE VISIT")
}

test = event.queries %>% filter(type == "recentTweetMentionClick" | type == "TweetMentionClick")
test$mention = apply(test, 1, function(x) getMentionTest(x["tweetID"], x["pluginUserID"], x["timestamp"]))



getMentionTest = function(id, user, queryTime) {
  tweet = tweets.content[tweets.content$tweetID == id,]$tweetText
  mentions = unlist(str_extract_all(tweet, "@\\S+"))

  if (length(mentions) < 1) return ("NO MENTIONS")
  
  maxTime = as.numeric(queryTime) + 120
  filtered = events.unique %>% filter(pluginUserID == user & timestamp > queryTime & timestamp < maxTime) %>% arrange(timestamp)
  visits = filtered[which(filtered$type == "userPageVisit"),]
  
  if (nrow(visits) < 1) return ("NO VISITS")
  
  visits$user = paste("@", gsub("https://twitter.com/([^/]+)/?.*", "\\1", visits$context), sep="")
  
  matching = mentions[mentions %in% visits$user]
  
  if (length(matching) == 1) return (matching[1])
  return (length(matching))
}


tweet = tweets.content[tweets.content$tweetID == 619691788752654336,]$tweetText
mentions = unlist(str_extract_all(tweet, "@\\S+"))
testx = events.unique %>% filter(pluginUserID == 53 & timestamp > 1436760938 & timestamp < 1436761050) %>% arrange(timestamp)
visits = testx[which(testx$type == "userPageVisit"),]
visits$user = paste("@", gsub("https://twitter.com/([^/]+)/?.*", "\\1", visits$context), sep="")

matching = mentions[mentions %in% visits$user]

length(matching)



testx = events.unique %>% filter(pluginUserID == 3 & timestamp >= 1433490233 & timestamp < 1433490353) %>% arrange(timestamp)
tweets.content[tweets.content$tweetID == 596816674562670592,]$tweetText












getSpecialValue = function(type, id, user, queryTime) {
  if (type == "TweetAuthorClick" | type == "recentTweetAuthorClick") return (getAuthor(id))
  else if (type == "TweetClickonHashtag" | type == "rightClickonHashtag") return (getHashtag(user, queryTime))
  else if (type == "TweetMentionClick" | type == "recentTweetMentionClick") return (getMention(user, queryTime))
}

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

queries = rbind(event.queries.filtered, search.queries)
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
relevant.content = tweets.content[,c("tweetID", "tweetAuthorUsername", "tweetAuthorScreenname", "tweetText", "tweetAbsolutPath", "mention_count", "link_count", "hashtag_count")] # CHEK RT/FAV

duplicates = relevant.content %>% group_by(tweetID) %>% filter(n() >= 2) # duplicate entries

parseURL = function(url) {
  split = strsplit(url, "/")[[1]]
  return (split[length(split)])
}

duplicates$tweetID = apply(duplicates, 1, function(x) parseURL(x["tweetAbsolutPath"]))

relevant.content = relevant.content[!(relevant.content$tweetID %in% duplicates$tweetID),] # filter out all duplicates for now

tweets = merge(all.tweets, relevant.content, by.x="tweetID", by.y="tweetID") # merge both dataframes
Encoding(tweets$tweetText) = "UTF-8" # Change text encoding to UTF-8

tweets.filtered = tweets[complete.cases(tweets[,c("tweetText")]),]

getSentiment = function(text) {
  sent = sentiment(text)$sentiment
  return (mean(sent))
}

getWordCount = function(text) {
  text = gsub('http.* *', '', text)
  text = gsub('https.* *', '', text)
  text = gsub('pic.twitter.* *', '', text)
  text = gsub("/", " ", text)
  text = gsub("[[:punct:]]", "", text)
  text = str_replace(gsub("\\s+", " ", str_trim(text)), "B", "b")
  return (sapply(gregexpr("\\W+", text), length) + 1)
}

getCharCount = function(text) {
  text = gsub('http.* *', '', text)
  text = gsub('https.* *', '', text)
  text = gsub('pic.twitter.* *', '', text)
  return (nchar(text))
}

# NOT WORKING YET, getting different results for tweet.features test. Remove links? Special chars? etc.
getReadability = function(text) {
  text = gsub('http.* *', '', text)
  text = gsub('https.* *', '', text)
  text = gsub('pic.twitter.* *', '', text)
  text = tokenize(text, format="obj", lang="de")
  text = flesch(text, force.lang = "de")
  return (slot(text, "Flesch")$RE)
}

tweets.filtered$sentiment_score = apply(tweets.filtered, 1, function(x) getSentiment(x["tweetText"]))
tweets.filtered$wordCount = apply(tweets.filtered, 1, function(x) getWordCount(x["tweetText"]))
tweets.filtered$charCount = apply(tweets.filtered, 1, function(x) getCharCount(x["tweetText"]))
tweets.filtered$avgWordLength = tweets.filtered$charCount/tweets.filtered$wordCount





###############

tweets.filtered = na.omit(tweets)
tweets.filtered[tweets.filtered$tweetText == "",]$tweetText = NA
tweets.filtered$tweetTimeStamp = format(as.POSIXct(tweets.filtered$tweetTimeStamp, origin="1970-01-01"), format = "%Y-%m-%dT%H:%M:%SZ")
tweets.filtered = tweets.filtered[,-8] # Remove link
tweets.filtered = na.omit(tweets.filtered)

colnames(tweets.filtered) = c("id", "date", "fav_count", "rt_count", "user_name", "user_screen_name", "content", "mention_count.x", "link_count.x", "hashtag_count.x")

tweet.features = read.csv("C:\\Programming\\Twitter\\tweetsClickedFeatures.csv", header = TRUE, sep = ",")
tweets.merged = merge(tweets.filtered, tweet.features, by.x="id", by.y = "tweetID")
tweets.merged = tweets.merged[,c("id", "date", "fav_count", "rt_count", "user_name", "user_screen_name", "content", "mention_count", "link_count", "hashtag_count", "poc", "common.noun", "pronoun.not.possessive", "nominal.possessive", "proper.noun", "proper.noun.possessive", "nominal.verbal", "proper.noun.verbal", "verb.or.auxiliary", "adjective", "adverb", "interjection", "determiner", "preposition", "coordinating.conjunction", "verb.particle", "existential", "Twitter.discourse", "emoticon", "punctuation", "other", "sentiment_score", "charCount", "wordCount", "avgWordLength", "readability_score", "fracTweetsWithMentions", "fracTweetsWithHashtag", "fracTweetsWithLinks", "sender_statusesCount", "sender_followersCount", "sender_favoritesCount", "sender_friendsCount")]
# tweets.merged = na.omit(tweets.merged) --- FIND SOLUTION!!!!
tweets.merged[is.na(tweets.merged)] = 0 # Replace NA with 0 for now - find better solution!!!
tweets.merged = tweets.merged[tweets.merged$wordCount > 0,]
tweets.merged = tweets.merged[!duplicated(tweets.merged[,1]),] # REMOVE DUPLICATES FOR NOW - ASK!!!!!!!!

write.csv(tweets.merged, file="tweets_merged.csv", row.names=FALSE, fileEncoding = "UTF-8")

solr_connect("http://localhost:8983")
delete_by_query(query = "*:*", "twitter")
update_csv("tweets_merged.csv", "twitter", commit = TRUE)


#################
# TRAINING FILE #
#################
training = proxies[,c("special_value", "firstHover")]
training$relevance = 1.0
training$source = "CLICK_LOGS"
training$special_value = unlist(training$special_value)
training = training[!duplicated(training[,1]),] # REMOVE DUPLICATES FOR NOW - ASK!!!!!!!!!!!!

training = training[training$lastClick != 130360992,] # Manually remove Tweet that wasn't saved properly
training[25,]$special_value = "@alexbervoetsbe" # Manually correct query mistake (ASK!!!!!!!!!!)

all.ids = tweets.merged$id

get_examples = function(special_value, id) {
  ids = all.ids[all.ids != id]
  positive = data.frame(special_value = special_value, firstHover = id, relevance = 1.0, source = "CLICK_LOGS")
  negative = data.frame(special_value = special_value, firstHover = ids, relevance = 0, source = "CLICK_LOGS")
  result = rbind(positive, negative)
  rownames(result) = NULL
  return(result)
}

training.set = do.call("rbind", apply(training, 1, function(x) get_examples(x["special_value"], x["firstHover"])))

write.table(training.set, file = "C:\\Programming\\Twitter\\user_queries_first_hover.txt", quote=FALSE, col.names = FALSE, row.names = FALSE, sep = "|", dec = ".")

########
# SOLR #
########
url = "http://localhost:8983/solr/twitter/select"
solr_connect(url)
nrows = 1000

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

getPosition = function(query, nrows, target, ltr) {
  # Get the query proxies
  if (is.na(target)) return (NA)
  if (ltr) result = solr_search(q=query, rows=nrows, rq="{!ltr model=TwitterModel reRankDocs=1000}")
  else result = solr_search(q=query, rows=nrows)
  
  position = which(result$id == target)
  
  if (length(position) == 0) return (-1)
  else return(position)
}

testcopy = training[,c("special_value", "firstHover")]
testcopy$pos = apply(testcopy, 1, function(x) getPosition(x["special_value"], nrows, x["firstHover"], FALSE))
testcopy$ltrPos = apply(testcopy, 1, function(x) getPosition(x["special_value"], nrows, x["firstHover"], TRUE))

getMRR(testcopy$pos)
getMRR(testcopy$ltrPos)

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

getMRR = function(positions) {
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



#################

workingcopy = tweets.collected
workingcopy$diff = workingcopy$timestamp - workingcopy$tweetTimeStamp
summary(workingcopy$diff)
