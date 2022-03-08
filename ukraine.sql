

DROP TABLE master..NER_tagged
DROP TABLE master..UkraineRussia_all

SELECT count(Word)
FROM Portfolio..NER_tagged

SELECT *
FROM Portfolio..hashtags_ranked

SELECT *
FROM Portfolio..empath_scores

SELECT *
FROM Portfolio..freq_words_stem

SELECT *
FROM Portfolio..NER_tagged

-- remove index column
ALTER TABLE Portfolio..freq_words_unstem
DROP COLUMN column1

--- freq words based on Name Entity Recognition
WITH CTE_NER AS(
SELECT *
FROM Portfolio..NER_tagged
WHERE Label = 'ORGANIZATION' OR Label = 'PERSON' OR Label = 'LOCATION' 
) 
SELECT Label, Word, sum(count(Word)) OVER (PARTITION BY Word) as cnt
FROM CTE_NER
GROUP BY Word, Label
ORDER BY cnt DESC

-- stemmed words ranked by frequency
SELECT *
FROM Portfolio..freq_words_stem
ORDER BY Word_Count DESC

-- unstemmed words ranked by frequency
SELECT *
FROM Portfolio..freq_words_unstem
ORDER BY Word_Count DESC

-- empathy scores ranked by frequency
SELECT *
FROM Portfolio..empath_scores
ORDER BY empath_score DESC

-- hashtags ranked by frequency
SELECT *
FROM Portfolio..hashtags_ranked




ALTER TABLE Portfolio..UkraineRussia_all
DROP COLUMN column1

SELECT *
FROM Portfolio..UkraineRussia_all

-- Top language tweets frequency over time
SELECT date, language, max(count(language)) over (partition by date) as cnt
FROM Portfolio..UkraineRussia_all
WHERE [language] in ('English', 'Turkish', 'Undefined', 'Hindi', 'French', 'German')
GROUP BY date, language
ORDER BY date

--number of English tweets over time
SELECT date, max(count(date)) over (partition by date) as cnt
FROM Portfolio..UkraineRussia_all
WHERE [language] = 'English'
GROUP BY date
ORDER BY date

--number of Hindi tweets over time
SELECT date, max(count(date)) over (partition by date) as cnt
FROM Portfolio..UkraineRussia_all
WHERE [language] = 'Hindi'
GROUP BY date
ORDER BY date



-- DROP TABLE Portfolio..UkraineRussia

-- number of tweets by date
SELECT date, max(count(date)) over (partition by date) as date_count
FROM Portfolio..UkraineRussia_all
GROUP BY date

-- number of distinct languages
SELECT count(distinct(language)) 
FROM Portfolio..UkraineRussia_all


-- which language populates the tweets? who are most interested in this affair?
-- get countries that speak this language and are closely related to this affair (make a disclaimer that it is an assumption and did not take into account other variables)
SELECT language, max(count(language)) over (PARTITION BY LANGUAGE) as tweet_count
FROM Portfolio..UkraineRussia_all
GROUP BY language
ORDER BY tweet_count DESC


-- calculate 10 percent of all distinct language
SELECT round(count(distinct(language))*0.1, 1)
FROM Portfolio..UkraineRussia_all

-- Get Top 10percent language
-- surprising that Russia is not in the rank (perhaps Undefined)
WITH CTE_lang AS(
SELECT language, max(count(language)) over (PARTITION BY LANGUAGE) as tweet_count
FROM Portfolio..UkraineRussia_all
GROUP BY language
)
SELECT TOP(6) *
FROM CTE_lang 
ORDER BY tweet_count DESC


-- number of reply, retweet, likes throughout the dates
SELECT date, sum(replies_count) as reply_cnt, sum(retweets_count) as retweet_cnt, sum(likes_count) as likes_cnt
FROM Portfolio..UkraineRussia_all
GROUP BY date
ORDER BY date

-- which language has the most number of reply, retweet, likes (we would assume country through language)
SELECT date, language, sum(replies_count) as reply_cnt, sum(retweets_count) as retweet_cnt, sum(likes_count) as likes_cnt
FROM Portfolio..UkraineRussia_all
GROUP BY date, language
ORDER BY retweet_cnt DESC

-- number of reply, retweet, likes of tweets in Hindi, grouped throughout time
SELECT date, sum(replies_count) as reply_cnt, sum(retweets_count) as retweet_cnt, sum(likes_count) as likes_cnt
FROM Portfolio..UkraineRussia_all
WHERE language = 'French'
GROUP BY date
ORDER BY date



-- replace language code to full language name
UPDATE Portfolio..UkraineRussia_all
SET language = REPLACE(language, 'en', 'English')
WHERE language = 'en'

UPDATE Portfolio..UkraineRussia_all
SET language = REPLACE(language, 'tr', 'Turkish')
WHERE language = 'tr'

UPDATE Portfolio..UkraineRussia_all
SET language = REPLACE(language, 'und', 'Undefined')
WHERE language = 'und'

UPDATE Portfolio..UkraineRussia_all
SET language = REPLACE(language, 'hi', 'Hindi')
WHERE language = 'hi'

UPDATE Portfolio..UkraineRussia_all
SET language = REPLACE(language, 'fr', 'French')
WHERE language = 'fr'

UPDATE Portfolio..UkraineRussia_all
SET language = REPLACE(language, 'de', 'German')
WHERE language = 'de'

UPDATE Portfolio..UkraineRussia_all
SET language = REPLACE(language, 'es', 'Spanish')
WHERE language = 'es'

UPDATE Portfolio..UkraineRussia_all
SET language = REPLACE(language, 'ta', 'Tamil')
WHERE language = 'ta'

UPDATE Portfolio..UkraineRussia_all
SET language = REPLACE(language, 'ar', 'Arabic')
WHERE language = 'ar'

UPDATE Portfolio..UkraineRussia_all
SET language = REPLACE(language, 'it', 'Italian')
WHERE language = 'it'

UPDATE Portfolio..UkraineRussia_all
SET language = REPLACE(language, 'gu', 'Gujarati')
WHERE language = 'gu'

UPDATE Portfolio..UkraineRussia_all
SET language = REPLACE(language, 'pl', 'Polish')
WHERE language = 'pl'

UPDATE Portfolio..UkraineRussia_all
SET language = REPLACE(language, 'ja', 'Japanese')
WHERE language = 'ja'

UPDATE Portfolio..UkraineRussia_all
SET language = REPLACE(language, 'pt', 'Portuguese')
WHERE language = 'pt'

UPDATE Portfolio..UkraineRussia_all
SET language = REPLACE(language, 'th', 'Thai')
WHERE language = 'th'

UPDATE Portfolio..UkraineRussia_all
SET language = REPLACE(language, 'mr', 'Marathi')
WHERE language = 'mr'

UPDATE Portfolio..UkraineRussia_all
SET language = REPLACE(language, 'ur', 'Urdu')
WHERE language = 'ur'

UPDATE Portfolio..UkraineRussia_all
SET language = REPLACE(language, 'nl', 'Dutch')
WHERE language = 'nl'

UPDATE Portfolio..UkraineRussia_all
SET language = REPLACE(language, 'bn', 'Bengali')
WHERE language = 'bn'

UPDATE Portfolio..UkraineRussia_all
SET language = REPLACE(language, 'uk', 'Ukrainian')
WHERE language = 'uk'

UPDATE Portfolio..UkraineRussia_all
SET language = REPLACE(language, 'te', 'Telugu')
WHERE language = 'te'

UPDATE Portfolio..UkraineRussia_all
SET language = REPLACE(language, 'fa', 'Persian')
WHERE language = 'fa'

UPDATE Portfolio..UkraineRussia_all
SET language = REPLACE(language, 'et', 'Estonian')
WHERE language = 'et'

UPDATE Portfolio..UkraineRussia_all
SET language = REPLACE(language, 'kn', 'Kannada')
WHERE language = 'kn'

UPDATE Portfolio..UkraineRussia_all
SET language = REPLACE(language, 'zh', 'Chinese')
WHERE language = 'zh'

UPDATE Portfolio..UkraineRussia_all
SET language = REPLACE(language, 'tl', 'Tagalog')
WHERE language = 'tl'

UPDATE Portfolio..UkraineRussia_all
SET language = REPLACE(language, 'iw', 'Hebrew')
WHERE language = 'iw'

UPDATE Portfolio..UkraineRussia_all
SET language = REPLACE(language, 'ml', 'Malayalam')
WHERE language = 'ml'

UPDATE Portfolio..UkraineRussia_all
SET language = REPLACE(language, 'ro', 'Romanian')
WHERE language = 'ro'

UPDATE Portfolio..UkraineRussia_all
SET language = REPLACE(language, 'or', 'Oriya')
WHERE language = 'or'

UPDATE Portfolio..UkraineRussia_all
SET language = REPLACE(language, 'pa', 'Panjabi')
WHERE language = 'pa'

UPDATE Portfolio..UkraineRussia_all
SET language = REPLACE(language, 'el', 'Greek')
WHERE language = 'el'

UPDATE Portfolio..UkraineRussia_all
SET language = REPLACE(language, 'da', 'Danish')
WHERE language = 'da'

UPDATE Portfolio..UkraineRussia_all
SET language = REPLACE(language, 'ca', 'Catalan')
WHERE language = 'ca'

UPDATE Portfolio..UkraineRussia_all
SET language = REPLACE(language, 'ne', 'Nepali')
WHERE language = 'ne'

UPDATE Portfolio..UkraineRussia_all
SET language = REPLACE(language, 'fi', 'Finnish')
WHERE language = 'fi'

UPDATE Portfolio..UkraineRussia_all
SET language = REPLACE(language, 'ht', 'Haitian')
WHERE language = 'ht'

UPDATE Portfolio..UkraineRussia_all
SET language = REPLACE(language, 'sv', 'Kirghiz')
WHERE language = 'sv'

UPDATE Portfolio..UkraineRussia_all
SET language = REPLACE(language, 'cs', 'Czech')
WHERE language = 'cs'

UPDATE Portfolio..UkraineRussia_all
SET language = REPLACE(language, 'ps', 'Pashto')
WHERE language = 'ps'

UPDATE Portfolio..UkraineRussia_all
SET language = REPLACE(language, 'no', 'Norwegian')
WHERE language = 'no'

UPDATE Portfolio..UkraineRussia_all
SET language = REPLACE(language, 'hu', 'Hungarian')
WHERE language = 'hu'

UPDATE Portfolio..UkraineRussia_all
SET language = REPLACE(language, 'lv', 'Latvian')
WHERE language = 'lv'

UPDATE Portfolio..UkraineRussia_all
SET language = REPLACE(language, 'sl', 'Slovenian')
WHERE language = 'sl'

UPDATE Portfolio..UkraineRussia_all
SET language = REPLACE(language, 'si', 'Singhalese')
WHERE language = 'si'

UPDATE Portfolio..UkraineRussia_all
SET language = REPLACE(language, 'ko', 'Korean')
WHERE language = 'ko'

UPDATE Portfolio..UkraineRussia_all
SET language = REPLACE(language, 'cy', 'Welch')
WHERE language = 'cy'

UPDATE Portfolio..UkraineRussia_all
SET language = REPLACE(language, 'lt', 'Lithuanian')
WHERE language = 'lt'

UPDATE Portfolio..UkraineRussia_all
SET language = REPLACE(language, 'hy', 'Armenian')
WHERE language = 'hy'

UPDATE Portfolio..UkraineRussia_all
SET language = REPLACE(language, 'sr', 'Serbian')
WHERE language = 'sr'

UPDATE Portfolio..UkraineRussia_all
SET language = REPLACE(language, 'sd', 'Sindhi')
WHERE language = 'sd'

UPDATE Portfolio..UkraineRussia_all
SET language = REPLACE(language, 'ckb', 'Central Kurdish')
WHERE language = 'ckb'

UPDATE Portfolio..UkraineRussia_all
SET language = REPLACE(language, 'bg', 'Bulgarian')
WHERE language = 'bg'

UPDATE Portfolio..UkraineRussia_all
SET language = REPLACE(language, 'vi', 'Vietnamese')
WHERE language = 'vi'

UPDATE Portfolio..UkraineRussia_all
SET language = REPLACE(language, 'eu', 'Basque')
WHERE language = 'eu'

UPDATE Portfolio..UkraineRussia_all
SET language = REPLACE(language, 'am', 'Amharic')
WHERE language = 'am'

UPDATE Portfolio..UkraineRussia_all
SET language = REPLACE(language, 'is', 'Icelandic')
WHERE language = 'is'

UPDATE Portfolio..UkraineRussia_all
SET language = REPLACE(language, 'ka', 'Georgian')
WHERE language = 'ka'

UPDATE Portfolio..UkraineRussia_all
SET language = REPLACE(language, 'my', 'Burmese')
WHERE language = 'my'

UPDATE Portfolio..UkraineRussia_all
SET language = REPLACE(language, 'ru', 'Russian')
WHERE language = 'ru'

UPDATE Portfolio..UkraineRussia_all
SET language = REPLACE(language, 'in', 'Indonesian')
WHERE language = 'in'


SELECT distinct(language)
FROM Portfolio..UkraineRussia_all